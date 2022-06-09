package common;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * actual content stored in data
 */
public class Node implements Serializable {
    private static final long serialVersionUID = 1L;
    private String data;
    List<Node> children = new LinkedList<>();
    Node parent = null;

    public Node(String data) {
        this.data = data;
    }

    public void addChild(Node child) {
        this.children.add((child));
        child.setParent(this);
    }

    private void setParent(Node parent) {
        this.parent = parent;
    }

    public List<Node> getChildren() {
        return children;
    }

    public Node getParent() {
        return parent;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }
}
