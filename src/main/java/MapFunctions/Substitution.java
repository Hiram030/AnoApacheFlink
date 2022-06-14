package MapFunctions;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.Map;

public class Substitution extends ScalarFunction {
    Map<?, ?> map;

    public Substitution(Map<?, ?> map) {
        this.map = map;
    }

    public Object eval(Object value) {
        return map.get(value);
    }
}
