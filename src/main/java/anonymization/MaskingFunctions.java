package anonymization;

import common.Node;
import common.Tree;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.types.DataType;

import java.util.List;
import common.Tree;

import static org.apache.flink.table.api.Expressions.*;

public class MaskingFunctions {

    //Yannick
    public String supress(Table column, List<Integer> indices){
        return "xxxxx";
    }

    public String tokenize(Table column, List<Integer> indices) {
        return "";
    }
    public String substitute() {
        return "";
    }
    public String conditionalSubstitute() {
        return "";
    }
    public String blurring() {
        return "";
    }

    //Thai
    public String bucketize([12, 23, 43, 34]) {
        return "";
    }

    /**
     * generalize a value according to the predefined generalization tree
     * @param value value to be generalized
     * @param tree the hierachical generalization tree
     * @param level the higher the level, the more data lost
     * @return genralized value
     */
    public String generalize(String value, Tree<String> tree, int level) {
        if (level < 0) {
            throw new IllegalArgumentException("Level must be positive");
        }
        Node<String> node = tree.findNode(value);
        for (int i = 0; i < level; i++) {
            node = node.getParent();
            if (node == null)
                throw new IllegalArgumentException("Level is bigger than the tree height.");
        }
        return node.getData();
    }
    public Table shuffle() {
        return column.orderBy(rand());
    }
    public void average(Table column) {
//        DataStream<DataType> list = column.
//        DataTypes.of(column)
//        Table average = column.select($("*").avg());
//        for(int i = 0; i < column.select($("*").count()))
//        return column.map()

    }

    //Johannes
    public double addNoise(Table column) {
        return 0;
    }
    public String aggregate() {
        return "";
    }
    //generalization(List list, String value)
}
