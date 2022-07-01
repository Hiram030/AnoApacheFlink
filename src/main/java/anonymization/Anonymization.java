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
    public void shuffle(String columnName) {
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
        data = data.select($("*"), call(new Generalization(tree, level), $(columnName)).as("new_"+columnName));
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
        return data.select($("*"), call(new Bucketizing(), $(columnName), step).as("new_"+columnName));
    }

    public Table bucketize(String columnName, int[] steps) {
        if (steps == null)
            throw new IllegalArgumentException("Steps is null.");
        for(int i = 0; i < steps.length - 1; i++) {
            if(steps[i+1] <= steps[i])
                throw new IllegalArgumentException("Steps is not sorted.");
        }
        return data.select($("*"), call(new Bucketizing(), $(columnName), steps).as("new_"+columnName));
    }

    public Table suppress(String columnName) {
        return data.select($("*"), call(new Suppression(), $(columnName)).as("new_"+columnName));
    }

    public Table blurring(String columnName) {
        return data.select($("*"), call(new Blurring(), $(columnName)).as("new_"+columnName));
    }

    public Table tokenize(String columnName) {
        return data.select($("*"), $(columnName).sha256().as("new_"+columnName));
    }

    public Table addNoise(String columnName, double noise) {
        return data.select($("*"), call(new NoiseAdding(noise), $(columnName)).as("new_" + columnName));
    }

    public Table substitute(String columnName, Map<?,?> map) {
        return data.select($("*"), call(new Substitution(map), $(columnName)).as("new_" + columnName));
    }

    public Table conditionalSubstitute(String columnName, String conditionalColumn, Map<?, Map<?, ?>> map) {
        return data.select($("*"), call(new ConditionalSubstitution(map), $(columnName), $(conditionalColumn)).as("new_" + columnName));
    }

    public Table average(String columnName, double deviation) {
        Table avg = data.select($(columnName).avg().as("avg"));
        Table result = data.leftOuterJoin(avg);
        return result.select($("*"), call(new Averaging(deviation), $("avg")).as("new_" + columnName))
                .dropColumns($("avg"));
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
        return joinTables(data, sum, columnName2, "new_"+columnName2);
    }

    public Table UMicroaggregation(String columnName, int n) {
        Table temp = data
                .orderBy($(columnName))
                .joinLateral(call(new UMikroAggregation(n), $(columnName), $("id")))
                //calculated average and ids
                .select($("d").as("new_"+columnName), $("new_id"));
        Table result = data
                .join(temp).where($("id").isEqual($("new_id")))
                .orderBy($("id"))
                .dropColumns($("new_id"));
        return result;
    }

    /**
     * perform k-anonymity algorithm on data
     * @return
     * @throws Exception
     */
    public Table kAnonymity(int k) throws Exception {
        List<String> columnNames = schema.getColumns().stream().map(Schema.UnresolvedColumn::getName).collect(Collectors.toList());
        //read number of rows and store in n
        Table count = data.select($("id").count());
        long n = count.execute().collect().next().getFieldAs(0);
        //build sink table
        Schema.Builder builder = Schema.newBuilder().column("id", DataTypes.BIGINT());
        for(String columnName : columnNames) {
            if(columnName.equals("id"))
                continue;
            builder.column(columnName, DataTypes.STRING());
        }
        builder.column("size", DataTypes.DOUBLE());
        tEnv.createTemporaryTable("Result", TableDescriptor.forConnector("print")
                .schema(builder.build())
                .build());

        //add columns
        Table clusters = data.select(
                $("*"),
                call(new Fill(1)).as("size"));
        for (String columnName : columnNames) {
            if(columnName.equals("id"))
                continue;
            clusters = clusters.addOrReplaceColumns(call(new ToString(), $(columnName)).as(columnName));
        }
        tEnv.createTemporaryView("clusters", clusters);

        //transform to array
        HashMap<Long, Row> hashMap = new HashMap<>((int)n);
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
            System.out.println(first + ", " +second);
            double newSize = update(hashMap, first, second);
            //update cluster
            clusters = tEnv.fromValues(rowType, hashMap.values().toArray());
            //if(a cluster more than k)
            if(newSize >= k) {
                hashMap.remove(first);
                Table completeCluster = clusters.where($("id").isEqual(first));
                completeCluster.executeInsert("Result");
                clusters.minus(completeCluster);
            }


            //update n
            count = clusters.select($("id").count());
            n = count.execute().collect().next().getFieldAs(0);
            System.out.println(n);
        }
//        //take all values in same cluster to 1 table and anonymize
        return clusters;
    }

    //helper for k-anonymity, return the size of new cluster
    private double update(HashMap<Long, Row> map, long first, long second) {
        Row cluster1 = map.get(first);
        Row cluster2 = map.get(second);
        for(int i = 1; i < cluster1.getArity()-1; i++) {
            if(!cluster1.getField(i).equals(cluster2.getField(i))) {
                cluster1.setField(i, "****");
            }
        }
        double totalSize = (Double) cluster1.getField(cluster1.getArity()-1) + (Double) cluster2.getField(cluster1.getArity()-1);
        cluster1.setField(cluster1.getArity()-1, totalSize);
        map.replace(first, cluster1);
        map.remove(second);
        return totalSize;
    }

    public Table joinTables(Table t1, Table t2, String columnName1, String columnName2) {
        return t1
                .join(t2).where($(columnName1).isEqual($(columnName2)))
                .dropColumns($(columnName2));
    }

    public Table getData() { return data;}

    /**
     * print table data
     */
    public void printTable() {
        data.execute().print();
    }
}
