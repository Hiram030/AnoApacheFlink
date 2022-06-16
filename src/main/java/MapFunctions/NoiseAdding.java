package MapFunctions;

import org.apache.flink.table.functions.ScalarFunction;

public class NoiseAdding extends ScalarFunction {
    double noise;

    public NoiseAdding(double noise) {
        this.noise = noise;
    }

    public Double eval(Double value) {
        return value + noise * value;
    }

    public Double eval(Integer value) {
        return value + noise * value;
    }
}
