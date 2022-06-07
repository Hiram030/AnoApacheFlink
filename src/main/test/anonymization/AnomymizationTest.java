package anonymization;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.junit.Before;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AnomymizationTest {

    String FILE_PATH = "src/main/java/table/TrainingData.csv";
    Anomymization anomymization;

    @BeforeEach
    public void init() {
        Schema schema = Schema.newBuilder()
                .column("subject_id", DataTypes.BIGINT())
                .column("gender", DataTypes.STRING())
                .column("age", DataTypes.INT())
                .column("name", DataTypes.STRING())
                .column("surname", DataTypes.STRING())
                .column("residence", DataTypes.STRING())
                .build();
        anomymization = new Anomymization(FILE_PATH, schema);
        anomymization.buildTable();
    }

    @Test
    public void testBuildTable() {
        anomymization.printTable();
    }

    @Test
    void shuffle() {
        anomymization.shuffle("name");
        anomymization.getData().execute().print();
    }
}