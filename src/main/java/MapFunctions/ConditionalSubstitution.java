package MapFunctions;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.Map;

public class ConditionalSubstitution extends ScalarFunction {
    Map<?, Map<?, ?>> map;

    public ConditionalSubstitution(Map<?, Map<?, ?>> map) {
        this.map = map;
    }

    public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object value,
                       @DataTypeHint(inputGroup = InputGroup.ANY) Object condition) {
        if(map.containsKey(condition)) {
            Map<?,?> mapValue = map.get(condition);
            if(mapValue.containsKey(value)) {
                return mapValue.get(value).toString();
            }
        }
        return value.toString();
    }
}
