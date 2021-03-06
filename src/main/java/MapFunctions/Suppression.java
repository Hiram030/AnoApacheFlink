package MapFunctions;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;

public class Suppression extends ScalarFunction {
    public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object value) {
        return "****";
    }

    public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object value1, @DataTypeHint(inputGroup = InputGroup.ANY) Object value2) {
        if(value1.equals(value2)) {
            return value1.toString();
        }
        return "****";
    }
}
