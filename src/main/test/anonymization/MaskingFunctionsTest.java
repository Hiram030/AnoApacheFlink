package anonymization;

import common.Node;
import common.Tree;
import org.junit.Before;
import org.junit.jupiter.api.Test;
import table.Person;
import table.TableBuilder;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

class MaskingFunctionsTest {
    MaskingFunctions maskingFunctions = new MaskingFunctions();
    @Test
    void supress() {
        String value = "hello";
        assert "xxxxx" == maskingFunctions.suppress(value);
    }

    @Test
    void tokenize() throws NoSuchAlgorithmException, IOException {
        String value = "Adam";
        System.out.println(maskingFunctions.tokenize(value));
    }

    @Test
    void substitute() {
        Map<Integer, String> map = new HashMap<>();
        map.put(42, "Andrew");
        System.out.println(maskingFunctions.substitute(42, map));
    }

    @Test
    void conditionalSubstitute() {
        Map<String, String> mapMale = new HashMap<>();
        Map<String, String> mapFemale = new HashMap<>();
        mapMale.put("John", "Adrew");
        mapFemale.put("John", "Maria");
        Map<String, Map<?, ?>> map = new HashMap<>();
        map.put("Male", mapMale);
        map.put("Female", mapFemale);
        System.out.println(maskingFunctions.conditionalSubstitute("John", "Female", map));
    }

    @Test
    void blurring() {
        System.out.println(maskingFunctions.blurring("JohnWich"));
    }

    @Test
    void bucketize() {
        System.out.println(maskingFunctions.bucketize(144, 30));
    }
//    ArrayList<Person> persons;
//    @Before
//    public void init() throws Exception {
//        //prepare test data
//        TableBuilder tableBuilder = new TableBuilder("TrainingData.csv");
//        this.persons = tableBuilder.getPeople();
//    }
//    @Test
//    void generalize() {
//        Tree<String> tree = new Tree<>("city");
//        Node<String> world = new Node<>("World");
//        tree.setRoot(world);
//        world.addChild(new Node<>("Europe"));
//        Node<String> asia = new Node<>("Asia");
//        Node<String> australia = new Node<>("Australia");
//    }
}