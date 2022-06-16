package MapFunctions;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import java.util.Map;

public class Substitution extends ScalarFunction {
    Map<?, ?> map;

    public Substitution(Map<?, ?> map) {
        this.map = map;
    }

    public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object value) {
        String res;
        if(!map.containsKey(value)) {
            res = value.toString();
        } else {
            res = map.get(value).toString();
        }
        return res;
    }
}
