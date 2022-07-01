package MapFunctions;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;

public class FindDist extends ScalarFunction {

    public Double eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object... values) {
        int actualLength = values.length / 2;
        double dist = 0;
        for(int i = 1; i < actualLength-2; i++) {
            if(!values[i].equals(values[actualLength+i])) {
                dist += (double) values[actualLength-1] + (double) values[2*actualLength-1];
            }
        }
        return dist;
    }
}
