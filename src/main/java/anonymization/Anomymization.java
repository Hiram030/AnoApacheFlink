package anonymization;

import common.Tree;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.*;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;

public class Anomymization {

    private final MaskingFunctions maskingFunctions = new MaskingFunctions();
    private final Schema schema;
    private final String filePath;
    private Table data;

    public Anomymization(String filePath, Schema schema) {
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
        TableEnvironment tEnv = TableEnvironment.create(settings);
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
        data = tEnv.from("data").select(uuid().as("id"), $("*"));
    }

    public void generalization(String columnName, Tree<String> tree) {
        //maskingFunctions.generalize(...)
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

    public void average(String columnName, int n) {
        Table column = data.select($("id"),$(columnName));
    }

    public Table getData() { return data;}

    /**
     * print table data
     */
    public void printTable() {
        data.execute().print();
    }
}
