package common;

/**
 * @param <T>
 * Tree data structure
 * The root has to be set, example:
 *      Tree<String> tree = new Tree("tree name");
 *      tree.setRoot(new Node("root"));
 */
public class Tree<T> {
    private Node<T> root = null;
    private String name;

    public Tree(String name) {
        this.name = name;
    }

    public Node<T> getRoot() {
        return root;
    }

    public String getName() {
        return name;
    }

    public void setRoot(Node<T> root) {
        this.root = root;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * recursive helper function for printTree()
     */
    private void printNode(Node<T> node, String appender) {
        System.out.println(appender + node.getData());
        node.getChildren().forEach(each ->  printNode(each, appender + "  "));
    }

    /**
     * print tree to console, used for debugging
     */
    public void printTree() {
        printNode(this.root, "  ");
    }
}
