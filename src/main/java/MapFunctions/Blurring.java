package MapFunctions;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;

public class Blurring extends ScalarFunction {
    public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object value) {
        String string = value.toString();
        char first = string.charAt(0);
        char last = string.charAt(string.length()-1);
        return first + "****" + last;
    }
}
