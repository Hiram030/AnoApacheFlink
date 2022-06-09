package MapFunctions;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

public class Bucketizing extends ScalarFunction {
    int step;
    public Bucketizing(int step) {
        this.step = step;
    }
    public String eval(Integer value) {
        int number = value;
        int floor = number - number % step;
        int ceil = floor + step;
        return floor + "-" + ceil;
    }

    public String eval(Double value) {
        int number = value.intValue();
        int floor = number - number % step;
        int ceil = floor + step;
        return floor + "-" + ceil;
    }
}
