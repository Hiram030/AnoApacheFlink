package anonymization;

import common.Tree;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;
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
    void suppress() {
        anonymization.suppress("name")
                .execute().print();
    }

    @Test
    void blurring() {
        anonymization.blurring("id", 2, 2)
                .execute().print();
    }

    @Test
    void tokenize() {
        anonymization.tokenize("name")
                .execute().print();
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
        //move new_age and age side by side
        List<String> columns = schema.getColumns()
                .stream().map(Schema.UnresolvedColumn::getName)
                .collect(Collectors.toList());
        result.select($(columns.get(0)),
                        $(columns.get(1)),
                        $(columns.get(2)),
                        $("new_age"),
                        $(columns.get(3)),
                        $(columns.get(4)),
                        $(columns.get(5)))
                .execute().print();
    }

    @Test
    void addNoise() {
        anonymization.addNoise("age", 0.1).execute().print();
    }

    @Test
    void substitute() {
        Map<String, String> map = new HashMap<>();
        map.put("M", "F");
        map.put("F", "M");
        anonymization.substitute("gender", map).execute().print();
    }
    @Test
    void average() {
        anonymization.average("age", 2).execute().print();
    }
    @Test
    void aggregate() {
        anonymization.aggregate("age", "gender").execute().print();
    }

    @Test
    void UMicroaggregation() {
        anonymization.UMicroaggregation("age", 5).execute().print();
    }

    @Test
    void kAnonymity() throws Exception {
        anonymization.kAnonymity(4).execute().print();
    }

    @Test
    void conditionalSubstitute() {
        Map<Integer, String> mapMale = new HashMap<>();
        Map<Integer, String> mapFemale = new HashMap<>();
        mapMale.put(24, "Adrew");
        mapFemale.put(24, "Maria");
        Map<String, Map<?, ?>> map = new HashMap<>();
        map.put("M", mapMale);
        map.put("F", mapFemale);
        anonymization.conditionalSubstitute("age", "gender", map)
                .execute().print();
    }
 }