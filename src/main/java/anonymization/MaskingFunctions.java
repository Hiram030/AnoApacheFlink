package anonymization;

import org.apache.flink.table.api.Table;

import java.util.List;

public class MaskingFunctions {

    public String supress(Table column, List<Integer> indices){
        return "xxxxx";
    }
    public String generalize(Table column, Tree tree) {
        return "";
    }
    //generalization(List list, String value)
    public String tokenize(Table column, List<Integer> indices) {
        return "";
    }
    public String bucketize() {
        return "";
    }
    public double addNoise(Table column) {
        return 0;
    }
}
