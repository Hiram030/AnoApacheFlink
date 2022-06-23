//package AggFunctions;
//
//import org.apache.flink.table.functions.AggregateFunction;
//import org.apache.flink.types.Row;
//
//public class FindClusterMean extends AggregateFunction<Row, Row> {
//
//    int counter = 0;
//    public void accumulate(Row rowAcc, Row row) {
//        int columnNumber = row.getArity();
//        for(int i = 0; i < columnNumber; i++) {
//
//            if(row.getField(i) instanceof Integer ||
//                    row.getField(i) instanceof Long ||
//                    row.getField(i) instanceof Double)
//            rowAcc.setField(i, row.);
//        }
//    }
//}
