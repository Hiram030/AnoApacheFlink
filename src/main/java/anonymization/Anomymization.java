package anonymization;

import org.apache.flink.table.api.*;

import static org.apache.flink.table.api.Expressions.$;

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
     * convert data from csv to Table class
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

        //get table
        data = tEnv.from("data");
    }

    public void generalization() {
        //maskingFunctions.generalize(...)
    }

    public void shuffle(String columnName) {
        Table column = data.select($(columnName)).orderBy(Expressions.rand());
        Table result = data.dropColumns($(columnName)).addOrReplaceColumns($(columnName));
        result.execute().print();
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
