package MapFunctions;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;

public class ToString extends ScalarFunction {

    public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object o) {
        return o.toString();
    }
}
