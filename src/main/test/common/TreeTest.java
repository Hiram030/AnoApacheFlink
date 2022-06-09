package common;

import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;

import static org.junit.jupiter.api.Assertions.*;

class TreeTest {

    @Test
    void convert() {
        String fileName = "gender";
        Tree tree = new Tree("gender");
        tree.convert(fileName);
        tree.printTree();
    }
}