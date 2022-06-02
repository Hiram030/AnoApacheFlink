package anonymization;

import common.Tree;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
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
    public String bucketize() {
        return "";
    }
    public String generalize(Table column, Tree tree) {
        return "";
    }
    public Table shuffle(Table column) {
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
