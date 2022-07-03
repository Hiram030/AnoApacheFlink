package anonymization;

import AggFunctions.MinDist;
import MapFunctions.*;
import common.Tree;
import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.schema.SchemaBuilder;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.api.*;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;

import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.Expressions.*;

public class Anonymization {

    private final Schema schema;
    private final String filePath;
    //todo: backup data
    private Table originalData;
    private Table data;
    private TableEnvironment tEnv;
    private boolean buildTime = false;
    private EnvironmentSettings settings;

    /**
     *
     * @param filePath path to csv file
     * @param schema the first column must be a primary key named "id"
     */
    public Anonymization(String filePath, Schema schema) {
        this.schema = schema;
        this.filePath = filePath;
    }

    /**
     * convert data from csv to Table class, stored in parameter data
     */
    public void buildTable() {
        settings = EnvironmentSettings
                .newInstance()
                .inBatchMode()
                .build();
        tEnv = TableEnvironment.create(settings);
        //source
        tEnv.createTemporaryTable("data", TableDescriptor.forConnector("filesystem")
            .schema(schema)
            .option("path", filePath)
            .format(FormatDescriptor.forFormat("csv")
                    .option("ignore-parse-error", "true")
                    .option("disable-quote-character", "true")
                    .build())
            .build());
        //sink
//        tEnv.createTemporaryTable("output", TableDescriptor.forConnector("filesystem")
//                .schema(schema)
//                .option("path", "output")
//                .format(FormatDescriptor.forFormat("csv")
//                        .option("field-delimiter", ",")
//                        .build())
//                .build());

        //get table and add id column
        data = tEnv.from("data").select($("*"));
        originalData = data;
    }

//    /**
//     * helper function for UMikroaggregation
//     */
//    private void buildTimeTable() {
//        if(buildTime)
//            return;
//        Schema newSchema = Schema.newBuilder()
//                .fromSchema(schema)
//                .columnByExpression("proc_time", "PROCTIME()")
//                .build();
//        tEnv.createTemporaryTable("procdata", TableDescriptor.forConnector("filesystem")
//                .schema(newSchema)
//                .option("path", filePath)
//                .format(FormatDescriptor.forFormat("csv")
//                        .option("ignore-parse-error", "true")
//                        .option("disable-quote-character", "true")
//                        .build())
//                .build());
//        buildTime = true;
//    }

    //helper map function for shuffle, add row number in chronological order
    public static class RowNumber extends ScalarFunction {
        private int counter;

        public RowNumber(int counter) {
            this.counter = counter;
        }

        public long eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object value) {
            return counter++;
        }
    }

    /**
     * randomly shuffle values in a column
     * @param columnName
     */
    public Table shuffle(String columnName) {
        //create shuffle column and add row number
        Table column1 = data
                .select($(columnName).as("new"))
                .orderBy(rand())
                .select($("new"), call(new RowNumber(1), $("new")).as("row_number1"));
        //add row number to original data
        Table column2 = data
                .select($("*"), call(new RowNumber(1), $(columnName)).as("row_number2"));
        //join 2 tables
        Table result = column2
                .join(column1)
                        .where($("row_number1").isEqual($("row_number2")))
                .select($("*"));
        //drop old column and unnecessary columns
        data = result
                .dropColumns($(columnName), $("row_number1"), $("row_number2"))
                .renameColumns($("new").as("name"));
        return data;
    }

    /**
     * generalize a column according to the generalization tree and the level
     * @param columnName
     * @param tree can be created by using Tree.convert([fileName]) to convert a text file to a Tree class
     *
     * @param level
     * @return
     */
    public Table generalize(String columnName, Tree tree, int level) {
        if (level <= 0)
            throw new IllegalArgumentException("Step must be greater than 0.");
        data = data.addOrReplaceColumns(call(new Generalization(tree, level), $(columnName)).as(columnName));
        return data;
    }

    /**
     * bucketize all values in the given column, the bucketized column will be added to the table data, named as new_[columnName]
     * @param columnName
     * @param step
     * @return
     */
    public Table bucketize(String columnName, int step) {
        if (step <= 0)
            throw new IllegalArgumentException("Step must be greater than 0.");
        data = data.addOrReplaceColumns(call(new Bucketizing(), $(columnName), step).as(columnName));
        return data;
    }

    public Table bucketize(String columnName, int[] steps) {
        if (steps == null)
            throw new IllegalArgumentException("Steps is null.");
        for(int i = 0; i < steps.length - 1; i++) {
            if(steps[i+1] <= steps[i])
                throw new IllegalArgumentException("Steps is not sorted.");
        }
        data = data.addOrReplaceColumns(call(new Bucketizing(), $(columnName), steps).as(columnName));
        return data;
    }

    public Table suppress(String columnName) {
        data = data.addOrReplaceColumns(call(new Suppression(), $(columnName)).as(columnName));
        return data;
    }

    public Table blurring(String columnName, int start, int end) {
        data = data.addOrReplaceColumns(call(new Blurring(start, end), $(columnName)).as(columnName));
        return data;
    }

    public Table tokenize(String columnName) {
        data = data.addOrReplaceColumns($(columnName).sha256().as(columnName));
        return data;
    }

    public Table addNoise(String columnName, double noise) {
        data = data.addOrReplaceColumns(call(new NoiseAdding(noise), $(columnName)).as(columnName));
        return data;
    }

    public Table substitute(String columnName, Map<?,?> map) {
        data = data.addOrReplaceColumns(call(new Substitution(map), $(columnName)).as(columnName));
        return data;
    }

    public Table conditionalSubstitute(String columnName, String conditionalColumn, Map<?, Map<?, ?>> map) {
        data= data.addOrReplaceColumns(call(new ConditionalSubstitution(map), $(columnName), $(conditionalColumn)).as(columnName));
        return data;
    }

    public Table average(String columnName, double deviation) {
        Table avg = data.select($(columnName).avg().as("avg"));
        Table result = data.leftOuterJoin(avg);
        data = result.addOrReplaceColumns(call(new Averaging(deviation), $("avg")).as(columnName))
                .dropColumns($("avg"));
        return data;
    }

    /**
     * aggregate column1 based on column2
     * @param columnName1
     * @return
     */
    public Table aggregate(String columnName1, String columnName2) {
        Table sum = data
                .groupBy($(columnName2))
                .select($(columnName2).as("new_"+columnName2), $(columnName1).sum().as("new_"+columnName1));
        data = joinTables(data, sum, columnName2, "new_"+columnName2)
                .dropColumns($(columnName1))
                .renameColumns($("new_"+columnName1).as(columnName1));
        return data;
    }

    public Table UMicroaggregation(String columnName, int n) {
        Table temp = data
                .orderBy($(columnName))
                .joinLateral(call(new UMikroAggregation(n), $(columnName), $("id")))
                //calculated average and ids
                .select($("d").as("new_"+columnName), $("new_id"));
        data = data
                .join(temp).where($("id").isEqual($("new_id")))
                .orderBy($("id"))
                .dropColumns($("new_id"))
                .dropColumns($(columnName))
                .renameColumns($("new_"+columnName).as(columnName));
        return data;
    }

    /**
     * perform k-anonymity algorithm on data
     * @param columnNames names of columns that are not to be anonymized
     * @return
     */
    public Table kAnonymity(int k) {
        return kAnonymity(data, k);
    }

    public Table kAnonymity(Table table, int k) {
        List<String> columnNames = schema.getColumns().stream().map(Schema.UnresolvedColumn::getName).collect(Collectors.toList());
        //read number of rows and store in n
        Table count = table.select($("id").count());
        long n = count.execute().collect().next().getFieldAs(0);

        if(n < k) {
            throw  new IllegalArgumentException("Data size is smaller than k");
        }

        //add columns
        Table clusters = table.select(
                $("*"),
                call(new Fill(1)).as("size"));
        for (String columnName : columnNames) {
            if(columnName.equals("id"))
                continue;
            clusters = clusters.addOrReplaceColumns(call(new ToString(), $(columnName)).as(columnName));
        }

        //transform to array
        HashMap<Long, Row> hashMap = new HashMap<>((int)n);
        List<Row> resultList = new LinkedList<>();
        CloseableIterator<Row> rows = clusters.execute().collect();
        while (rows.hasNext()) {
            Row row = rows.next();
            hashMap.put(row.getFieldAs(0), row);
        }

        DataTypes.Field[] fields = new DataTypes.Field[columnNames.size()+1];
        fields[0] = DataTypes.FIELD("id", DataTypes.BIGINT());
        for(int i = 1; i < columnNames.size(); i++) {
            fields[i] = DataTypes.FIELD(columnNames.get(i), DataTypes.STRING());
        }
        fields[columnNames.size()] = DataTypes.FIELD("size", DataTypes.DOUBLE());
        DataType rowType = DataTypes.ROW(fields);

        while (n > 1) {
            //rename columns to join
            Table clustersClone = clusters.renameColumns(
                    $("size").as("size1"));
            for (String columnName : columnNames) {
                clustersClone = clustersClone.renameColumns($(columnName).as(columnName+"1"));
            }
            //find min dist
            Table joined = clusters
                    .join(clustersClone)
                    .where($("id").isLess($("id1")));
            Table dist = joined
                    .select($("*"), call(new FindDist(), $("*")).as("dist"))
                    .aggregate(call(new MinDist(), $("id"), $("id1"), $("dist")).as("first", "second"))
                    .select($("first"), $("second"));
            Row row = dist.execute().collect().next();
            Long first = row.getFieldAs(0);
            Long second = row.getFieldAs(1);
            Row cluster1 = hashMap.get(first);
            Row cluster2 = hashMap.get(second);
            double newSize = update(cluster1, cluster2);
            hashMap.replace(first, cluster1);
            hashMap.remove(second);
            //if(a cluster more than k)
            if(newSize >= k) {
                Row firstRow = hashMap.remove(first);
                resultList.add(firstRow);
                n--;
            }

            //update cluster
            clusters = tEnv.fromValues(rowType, hashMap.values().toArray());
            //update n
            n--;
        }
        // deal with last cluster
        if(n != 0) {
            Row rest = (Row) hashMap.values().toArray()[0];
            double minDist = Double.MAX_VALUE;
            Row mergeCluster = null;
            for(Row row : resultList) {
                double dist = dist(row, rest);
                if(dist < minDist) {
                    minDist = dist;
                    mergeCluster = row;
                }
            }
            if(mergeCluster != null) {
                //there is any result
                resultList.remove(mergeCluster);
                update(mergeCluster, rest);
            }
            resultList.add(mergeCluster);
        }
        Table result = tEnv.fromValues(rowType, resultList).dropColumns($("id"));
//        //take all values in same cluster to 1 table and anonymize
        return result;
    }

    public Table kAnonymity(int k, String... intactColumnNames) {
        List<String> columnNames = schema.getColumns().stream().map(Schema.UnresolvedColumn::getName).collect(Collectors.toList());

        //table sink
        Schema.Builder builder = Schema.newBuilder();
        for (String columnName : columnNames) {
            if(Objects.equals(columnName, "id"))
                continue;
            builder.column(columnName, DataTypes.STRING());
        }
        Schema schema = builder.column("size", DataTypes.DOUBLE()).build();
        tEnv.createTemporaryTable("Result", TableDescriptor.forConnector("datagen")
                .schema(schema)
                .option("number-of-rows", "0")
                .build());
        Table result = tEnv.from("Result");

        ApiExpression[] intactColumns = new ApiExpression[intactColumnNames.length];
        for (int i = 0; i < intactColumnNames.length; i++) {
            intactColumns[i] = $(intactColumnNames[i]);
        }
        //get all values of intactcolumns
        CloseableIterator<Row> temp = data.groupBy(intactColumns).select(intactColumns).execute().collect();
        while (temp.hasNext()) {
            Row row = temp.next();
            Table table = data;
            //group table based on intactcolumns
            for (int i = 0; i < row.getArity(); i++) {
                table = table.where(intactColumns[i].isEqual(row.getField(i)));
            }
            Table iRes = kAnonymity(table, k);
            result = result.union(iRes);
        }
        return result;
    }

    //helper for k-anonymity, retruns distance between two rows
    private double dist(Row r1, Row r2) {
        int actualLength = r1.getArity();
        double dist = 0;
        for(int i = 1; i < actualLength-1; i++) {
            if(!r1.getField(i).equals(r2.getField(i))) {
                if(r1.getField(i).equals("****")) {
                    dist += (double) r2.getField(actualLength-1);
                }
                else if(r2.getField(i).equals("****")) {
                    dist += (double) r1.getField(actualLength-1);
                }
                else {
                    dist += (double) r2.getField(actualLength-1) + (double) r1.getField(actualLength-1);
                }
            }
        }
        return dist;
    }
    //helper for k-anonymity, updates cluster1 and returns the size of new cluster
    private double update(Row cluster1, Row cluster2) {
        for(int i = 1; i < cluster1.getArity()-1; i++) {
            if(!cluster1.getField(i).equals(cluster2.getField(i))) {
                cluster1.setField(i, "****");
            }
        }
        double totalSize = (Double) cluster1.getField(cluster1.getArity()-1) + (Double) cluster2.getField(cluster1.getArity()-1);
        cluster1.setField(cluster1.getArity()-1, totalSize);
        return totalSize;
    }

    public Table joinTables(Table t1, Table t2, String columnName1, String columnName2) {
        return t1
                .join(t2).where($(columnName1).isEqual($(columnName2)))
                .dropColumns($(columnName2));
    }

    public Table resetData() {
        data = originalData;
        return data;
    }

    public Table getData() { return data;}



    /**
     * print table data
     */
    public void printTable() {
        data.execute().print();
    }
}
