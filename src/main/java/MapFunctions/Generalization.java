package MapFunctions;

import common.Node;
import common.Tree;
import org.apache.flink.table.functions.ScalarFunction;

import java.io.Serializable;

public class Generalization extends ScalarFunction {
    Tree tree;
    int level;
    public Generalization(Tree tree, int level) {
        this.tree = tree;
        this.level = level;
    }

    public String eval(String value) {
        Node node = tree.findNode(value);
        for (int i = 0; i < level; i++) {
            node = node.getParent();
            if (node == null)
                throw new IllegalArgumentException("Level is bigger than the tree height.");
        }
        return node.getData();
    }
}
