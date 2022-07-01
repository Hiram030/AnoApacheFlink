package MapFunctions;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;

public class Blurring extends ScalarFunction {

    int start;
    int end;

    public Blurring(int start, int end) {
        this.start = start;
        this.end = end;
    }

    public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object value) {
        String string = value.toString();
        if(start + end > string.length()){
            return string;
        }
        StringBuilder first = new StringBuilder();
        StringBuilder last = new StringBuilder();
        for (int i = 0; i < start; i++) {
            first.append(string.charAt(i));
        }
        for (int i = 0; i < end; i++) {
            last.append(string.charAt(string.length() - 1 - i));
        }
        return first + "****" + last;
    }
}
