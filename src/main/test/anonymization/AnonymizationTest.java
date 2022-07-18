package anonymization;

import common.MapReader;
import common.Tree;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.junit.Before;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.$;

class AnonymizationTest {

    String FILE_PATH = "src/main/test/anonymization/patients-500.csv";
    Schema schema = Schema.newBuilder()
            .column("id", DataTypes.BIGINT())
            .column("gender", DataTypes.STRING())
            .column("age", DataTypes.INT())
            .column("residence", DataTypes.STRING())
            .column("time", DataTypes.STRING())
            .column("empty", DataTypes.STRING())
            .build();
    Anonymization anonymization;
    long startTime;

    @BeforeEach
    public void init() {
        anonymization = new Anonymization(FILE_PATH, schema);
        anonymization.buildTable();
        startTime = System.currentTimeMillis();
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
        result.execute().print();
    }

    @Test
    void addNoise() {
        anonymization.addNoise("age", 0.1).execute().print();
    }

    @Test
    void substitute() throws IOException {
        Map<String, String> map = MapReader.read("maps/gender.txt");
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
        anonymization.kAnonymity(3);
    }

    @Test
    void kAnonymity1() throws Exception {
        List<String> columns = new LinkedList<>();
        columns.add("time");
        anonymization.kAnonymity(3, columns);
    }
    @Test
    void kAnonymity2() throws Exception {
        List<String> columns = new LinkedList<>();
        columns.add("time");
        columns.add("gender");
        anonymization.kAnonymity(3, columns);
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

    @AfterEach
    public void endTime() {
        System.out.println(System.currentTimeMillis() - startTime);
    }
 }