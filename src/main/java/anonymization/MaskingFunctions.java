package anonymization;

import common.Node;
import common.Tree;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.types.DataType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import common.Tree;

import static org.apache.flink.table.api.Expressions.*;

public class MaskingFunctions {


    public String suppress(Object value){
        return "xxxxx";
    }

    public String tokenize(Object value) throws NoSuchAlgorithmException, IOException {
        MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
        ByteArrayOutputStream boas = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(boas);
        objectOutputStream.writeObject(value);
        byte[] bytes = messageDigest.digest(boas.toByteArray());
//        byte[] bytes = messageDigest.digest((byte[]) value);
        return bytes.toString();
    }

    public Object substitute(Object value, Map<?, ?> map) {
        return map.get(value);
    }

    /**
     * double substitute, first attribute then value
     * @param value
     * @param attribute
     * @param map
     * @return
     */
    public Object conditionalSubstitute(Object value, Object attribute, Map<?, Map<?, ?>> map) {
        Map<?,?> mapValue = map.get(attribute);
        return mapValue.get(value);
    }

    public String blurring(Object value) {
        String string = value.toString();
        char first = string.charAt(0);
        char last = string.charAt(string.length()-1);
        return first + "****" + last;
    }

    public String bucketize(Number value, int step) {
        if (step <= 0)
            throw new IllegalArgumentException("Step must be greater than 0.");
        int number = value.intValue();
        int floor = number - number % step;
        int ceil = floor + step;
        return floor + "-" + ceil;
    }

    /**
     * generalize a value according to the predefined generalization tree
     * @param value value to be generalized
     * @param tree the hierachical generalization tree
     * @param level the higher the level, the more data lost
     * @return genralized value
     */
    public String generalize(String value, Tree tree, int level) {
        if (level < 0) {
            throw new IllegalArgumentException("Level must be positive");
        }
        Node node = tree.findNode(value);
        for (int i = 0; i < level; i++) {
            node = node.getParent();
            if (node == null)
                throw new IllegalArgumentException("Level is bigger than the tree height.");
        }
        return node.getData();
    }
//    public Table shuffle() {
//        return column.orderBy(rand());
//    }
    public void average(Table column) {
//        DataStream<DataType> list = column.
//        DataTypes.of(column)
//        Table average = column.select($("*").avg());
//        for(int i = 0; i < column.select($("*").count()))
//        return column.map()

    }

    /**
     * noise between -1 to 1
     * @param value
     * @param noise
     * @return
     */
    public double addNoise(Number value, double noise) {
        double number = value.doubleValue();
        return number + noise * number;
    }

    public String aggregate() {
        return "";
    }
    //generalization(List list, String value)
}
