package common;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;

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

    public void convert(String filename) throws FileNotFoundException {
        String filepath = "trees/"+filename;
        BufferedReader bufferedReader = new BufferedReader(new FileReader(filepath));

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

    /**
     * find node of the data using BFS
     * @param value
     * @return
     */
    public Node<T> findNode(T value) {
        Queue<Node<T>> Q = new LinkedList<>();
        Q.add(root);
        while (!Q.isEmpty()) {
            Node<T> node = Q.poll();
            if (node.getData() == value)
                return node;
            List<Node<T>> children = node.getChildren();
            Q.addAll(children);
        }
        throw new NoSuchElementException("The value " + value + "was not found in the tree.");
    }
}
