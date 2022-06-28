package MapFunctions;

import org.apache.flink.table.functions.ScalarFunction;

public class Fill extends ScalarFunction {
    double number;

    public Fill(double number) {
        this.number = number;
    }

    public Double eval() {
        return number;
    }
}
