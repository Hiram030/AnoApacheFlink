package table;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.ArrayList;

public class TableBuilder {
    @JsonPropertyOrder({"subject_id","gender","age","name","surname","residence"})
    private ArrayList<Person> people = new ArrayList<>();

    //use default pre-defined path
    public TableBuilder() throws Exception {
        this("src/main/java/table/TrainingData.csv");
    }

    //specified path Tabel connstruction
    public TableBuilder(String path) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //read csv file and construct a dataset
        DataSet<Tuple6<Integer, String, Integer, String, String, String>> dataSet = env.readCsvFile(path)
                .ignoreFirstLine()
                .parseQuotedStrings('"')
                .ignoreInvalidLines()
                .types(Integer.class, String.class, Integer.class, String.class, String.class, String.class);

        //fill ArrayList people with data
        for (Tuple6 t: dataSet.collect()){
            people.add(new Person(t));
        }
    }

    public ArrayList<Person> getPeople() {
        return people;
    }

    public void setPeople(ArrayList<Person> people) {
        this.people = people;
    }

    public static void main (String[] args) {
        try {
            TableBuilder tb = new TableBuilder();
            System.out.println(tb.getPeople());
            System.out.println(tb.getPeople().size());
            System.out.println(tb.getPeople().get(0));
        } catch (Exception e) {
            e.printStackTrace();


        }
    }
}
