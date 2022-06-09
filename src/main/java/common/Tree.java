package common;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * Tree data structure for generalization
 */
public class Tree implements Serializable{
    private static final long serialVersionUID = 1L;
    private Node root = null;
    private String name;

    public Tree(String name) {
        this.name = name;
    }

    /**
     * convert a file in folder trees to this Tree data structure
     * Text File convention: each line represents a node and the first line is the root.
     * Parent - Child relationship cam be represented by having the following lines having more spaces on the left than the previous line
     * @param filename
     */
    public void convert(String filename)  {
        String filepath = "trees/"+filename;
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(filepath));
            String line = bufferedReader.readLine();
            if(line == null)
                throw new IllegalArgumentException("File is empty!");

            List<Node> previousNodes = new LinkedList<>(); //remember the parent nodes
            Node root = new Node(line);
            this.setRoot((root));
            previousNodes.add(root);
            line = bufferedReader.readLine();

            while (line != null) {
                int spaceCounter = 0;
                //count spaces before words
                for (int i = 0; i < line.length(); i++) {
                    if (line.charAt(i) == ' ')
                        spaceCounter++;
                    else
                        break;
                }
                if (spaceCounter == 0) {
                    throw new IllegalArgumentException("There can only be 1 root.");
                }
                Node currentNode = new Node(line.substring(spaceCounter));
                if (spaceCounter > previousNodes.size()) { //in case more than spaces than needed
                    spaceCounter = previousNodes.size();
                }
                previousNodes.subList(0, spaceCounter); //remove other siblings
                Node parent = previousNodes.get(spaceCounter - 1); //get last parent node
                parent.addChild(currentNode);
                previousNodes.add(currentNode);
                line = bufferedReader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Node getRoot() {
        return root;
    }

    public String getName() {
        return name;
    }

    public void setRoot(Node root) {
        this.root = root;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * recursive helper function for printTree()
     */
    private void printNode(Node node, String appender) {
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
    public Node findNode(String value) {
        Queue<Node> Q = new LinkedList<>();
        Q.add(root);
        while (!Q.isEmpty()) {
            Node node = Q.poll();
            if (value.equals(node.getData()))
                return node;
            List<Node> children = node.getChildren();
            Q.addAll(children);
        }
        throw new NoSuchElementException("The value " + value + " was not found in the tree.");
    }
}
