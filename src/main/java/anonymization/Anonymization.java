package anonymization;

import MapFunctions.*;
import common.Tree;
import org.apache.flink.table.api.*;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.Map;

import static org.apache.flink.table.api.Expressions.*;

public class Anonymization {

    private final Schema schema;
    private final String filePath;
    private Table data;
    private TableEnvironment tEnv;
    private boolean buildTime = false;

    public Anonymization(String filePath, Schema schema) {
        this.schema = schema;
        this.filePath = filePath;
    }

    /**
     * convert data from csv to Table class, stored in parameter data
     */
    public void buildTable() {
        EnvironmentSettings settings = EnvironmentSettings
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

    /**
     * helper function for UMikroaggregation
     */
    private void buildTimeTable() {
        if(buildTime)
            return;
        Schema newSchema = Schema.newBuilder()
                .fromSchema(schema)
                .columnByExpression("proc_time", "PROCTIME()")
                .build();
        tEnv.createTemporaryTable("procdata", TableDescriptor.forConnector("filesystem")
                .schema(newSchema)
                .option("path", filePath)
                .format(FormatDescriptor.forFormat("csv")
                        .option("ignore-parse-error", "true")
                        .option("disable-quote-character", "true")
                        .build())
                .build());
        buildTime = true;
    }

    //helper map function for shuffle, add row number in chronological order
    public class RowNumber extends ScalarFunction {
        private int counter = 1;
        public long eval(String s) {
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
                .select($("new"), call(new RowNumber(), $("new")).as("row_number1"));
        //add row number to original data
        Table column2 = data
                .select($("*"), call(new RowNumber(), $(columnName)).as("row_number2"));
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
        return data.select($("*"), call(new Bucketizing(step), $(columnName)).as("new_"+columnName));

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
//        buildTimeTable();
//        Table procData = tEnv.from("procdata");
//        Table result = procData
//                .window(Over.orderBy($("proc_time")).preceding(rowInterval(n)).as("w"))
//                .select($("*"), $(columnName).avg().over($("w")).as("new_"+columnName))
//                .orderBy($("new_"+columnName));
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
