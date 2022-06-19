package MapFunctions;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class UMikroAggregation extends TableFunction<Row> {

    int counter = 0;
    double sum = 0;
    int n;
    long[] ids;
    public UMikroAggregation(int n) {
        this.n = n;
        ids = new long[n];
    }

    @DataTypeHint("ROW<d DOUBLE, new_id BIGINT>")
    public void eval(Integer value, Long id) {
        sum += value;
        ids[counter] = id;
        counter++;
        if(counter == n) {
            double avg = sum / counter;
            for (int i = 0; i < counter; i++) {
                collect(Row.of(avg, ids[i]));
            }
            counter = 0;
            sum = 0;
        }
    }

    @DataTypeHint("ROW<d DOUBLE, new_id BIGINT>")
    public void eval(Double value, Long id) {
        sum += value;
        ids[counter] = id;
        counter++;
        if(counter == n) {
            double avg = sum / counter;
            for (int i = 0; i < counter; i++) {
                collect(Row.of(avg, ids[i]));
            }
            counter = 0;
            sum = 0;
        }
    }
}
