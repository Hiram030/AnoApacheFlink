package anonymization;

import org.junit.Before;
import org.junit.jupiter.api.Test;
import table.Person;
import table.TableBuilder;

import java.util.ArrayList;

class MaskingFunctionsTest {
    ArrayList<Person> persons;
    @Before
    public void init() throws Exception {
        //prepare test data
        TableBuilder tableBuilder = new TableBuilder("TrainingData.csv");
        this.persons = tableBuilder.getPeople();
    }
    @Test
    void generalize() {
    }
}