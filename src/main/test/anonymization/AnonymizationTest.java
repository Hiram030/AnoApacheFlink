package anonymization;

import common.Node;
import common.Tree;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.Expressions.$;

class AnonymizationTest {

    String FILE_PATH = "src/main/java/table/TrainingData.csv";
    Schema schema = Schema.newBuilder()
            .column("id", DataTypes.BIGINT())
            .column("gender", DataTypes.STRING())
            .column("age", DataTypes.INT())
            .column("name", DataTypes.STRING())
            .column("surname", DataTypes.STRING())
            .column("residence", DataTypes.STRING())
            .build();
    Anonymization anonymization;

    @BeforeEach
    public void init() {
        anonymization = new Anonymization(FILE_PATH, schema);
        anonymization.buildTable();
    }

    @Test
    public void testBuildTable() {
        anonymization.printTable();
    }

    @Test
    void shuffle() {
        anonymization.shuffle("name");
        anonymization.getData().execute().print();
    }

    @Test
    void generalize() {
        Tree tree = new Tree("gender");
        tree.convert("gender");
        anonymization.generalize("gender", tree, 1)
                .execute().print();
    }

    @Test
    void bucketize() {
        Table result = anonymization.bucketize("age", 5);
        List<String> columns = schema.getColumns().stream().map(Schema.UnresolvedColumn::getName).collect(Collectors.toList());
        result.select($(columns.get(0)),
                        $(columns.get(1)),
                        $(columns.get(2)),
                        $("new_age"),
                        $(columns.get(3)),
                        $(columns.get(4)),
                        $(columns.get(5)))
                .execute().print();
    }
}