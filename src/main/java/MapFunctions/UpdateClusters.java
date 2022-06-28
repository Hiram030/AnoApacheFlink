package MapFunctions;

import org.apache.flink.table.functions.ScalarFunction;

public class UpdateClusters extends ScalarFunction {
    int target;
    int toBeUpdated;
    int counter = 1;

    public UpdateClusters(int target, int toBeUpdated) {
        this.target = target;
        this.toBeUpdated = toBeUpdated;
    }

    public Integer eval(Integer oldCluster) {
        if(oldCluster == toBeUpdated) {
            return target;
        }
        if(oldCluster == target) { //update target
            target = counter;
        }
        return counter++;
    }
}
