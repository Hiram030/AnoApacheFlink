package MapFunctions;

import org.apache.flink.table.functions.ScalarFunction;

import java.util.Random;

public class Averaging extends ScalarFunction {
    double deviation;

    public Averaging(double deviation) {
        this.deviation = deviation;
    }

    public Double eval(Double value) {
        Random random = new Random();
        double var = random.nextGaussian();
        return var * deviation + value;
    }

    public Integer eval(Integer value) {
        Random random = new Random();
        double var = random.nextGaussian();
        double res = var * deviation + value;
        return (int) res;
    }
}
