package common;

import java.util.LinkedList;
import java.util.List;

/**
 * @param <T>
 * actual content stored in data
 */
public class Node<T> {
    private T data;
    List<Node<T>> children = new LinkedList<>();
    Node<T> parent = null;

    public Node(T data) {
        this.data = data;
    }

    public void addChild(Node<T> child) {
        this.children.add((child));
        child.setParent(parent);
    }

    private void setParent(Node<T> parent) {
        this.parent = parent;
    }

    public List<Node<T>> getChildren() {
        return children;
    }

    public Node<T> getParent() {
        return parent;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }
}
