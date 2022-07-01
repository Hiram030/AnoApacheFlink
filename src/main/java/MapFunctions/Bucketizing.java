package MapFunctions;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

public class Bucketizing extends ScalarFunction {

    public String eval(Integer value, Integer step) {
        int number = value;
        int floor = number - number % step;
        int ceil = floor + step;
        return floor + "-" + ceil;
    }

    public String eval(Double value, Integer step) {
        int number = value.intValue();
        int floor = number - number % step;
        int ceil = floor + step;
        return floor + "-" + ceil;
    }

    public String eval(Integer value, Integer[] steps) {
        int number = value;
        if(number <= steps[0]) {
            return "<" + steps[0];
        }
        for (int i = 1; i < steps.length; i++) {
            if (number <= steps[i]){
                return (steps[i-1]+1) + "-" + steps[i];
            }
        }
        return ">" + steps[steps.length-1];
    }

    public String eval(Double value, Integer[] steps) {
        int number = value.intValue();
        if(number <= steps[0]) {
            return "<" + steps[0];
        }
        for (int i = 1; i < steps.length; i++) {
            if (number <= steps[i]){
                return (steps[i-1]+1) + "-" + steps[i];
            }
        }
        return ">" + steps[steps.length-1];
    }
}
