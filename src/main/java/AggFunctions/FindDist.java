package AggFunctions;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.AggregateFunction;


public class FindDist extends AggregateFunction<Integer, Accumulator> {

    public void accumulate(Accumulator acc, String value) {
        acc.counter++;
        if(acc.string == null) {
            acc.string = value;
        } else {
            if(!acc.string.equals(value)) {
                acc.suppressionFlag = true;
            }
        }
    }

    public void accumulate(Accumulator acc, Integer value) {
        acc.counter++;
        if(acc.number == Double.MAX_VALUE) {
            acc.number = value;
        } else {
            if(acc.number != value) {
                acc.suppressionFlag = true;
            }
        }
    }
    public void accumulate(Accumulator acc, Double value) {
        acc.counter++;
        if(acc.number == Double.MAX_VALUE) {
            acc.number = value;
        } else {
            if(acc.number != value) {
                acc.suppressionFlag = true;
            }
        }
    }

    @Override
    public Integer getValue(Accumulator acc) {
        if(acc.suppressionFlag)
            return acc.counter;
        else
            return 0;
    }

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator();
    }

}

