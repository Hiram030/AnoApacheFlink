package AggFunctions;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

@FunctionHint(output = @DataTypeHint("ROW<cluster BIGINT, cluster1 BIGINT>"))
public class MinDist extends AggregateFunction<Row, Accumulator> {

    public void accumulate(Accumulator acc, Long cluster1, Long cluster2, Double dist) {
        if(dist < acc.minDist) {
            acc.minDist = dist;
            acc.cluster1 = cluster1;
            acc.cluster2 = cluster2;
        }
    }

    @Override
    public Row getValue(Accumulator accumulator) {
        return Row.of(accumulator.cluster1, accumulator.cluster2);
    }

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator();
    }
}
