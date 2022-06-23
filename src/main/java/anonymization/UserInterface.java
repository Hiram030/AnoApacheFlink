package anonymization;

import common.Tree;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class UserInterface {

    Scanner cli;
    String PATH;
    Schema schema;

    Anonymization anonymization;
    public UserInterface() throws Exception {
        cli = new Scanner(System.in);
        // System.out.println("Please provide PATH (can be empty for default table):");
        // String path = cli.next();
        // System.out.println("Please provide the columns, write finish to finish columns:");

        PATH = "src/main/java/table/TrainingData.csv";
        schema = Schema.newBuilder()
                .column("id", DataTypes.BIGINT())
                .column("gender", DataTypes.STRING())
                .column("age", DataTypes.INT())
                .column("name", DataTypes.STRING())
                .column("surname", DataTypes.STRING())
                .column("residence", DataTypes.STRING())
                .build();


        runUI();
    }

    public void printTable(){
        System.out.println("The table currently looks as followed:");
        anonymization.printTable();
    }

    public void runUI() throws Exception {
        anonymization = new Anonymization(PATH, schema);
        anonymization.buildTable();
        printTable();

        boolean exit = false;
        while (!exit){
            System.out.println("Please choose an operation:");
            System.out.println("The operations are: shuffle, generalize, bucketize, suppress, blurring, tokenize, addNoise, substitute, conditionalSubstitute, average, aggregate,UMicroaggregation, kAnonymity");
            System.out.println("Enter exit to exit the UI");
            String command = cli.next();

            switch (command){
                case "shuffle":
                    System.out.println("Which column do you want to shuffle?");
                    anonymization.shuffle(cli.next());
                    anonymization.getData().execute().print();

                    break;
                case "generalize":
                    System.out.println("Which column do you want to generalize?");
                    String treeName = cli.next();
                    Tree tree = new Tree(treeName);
                    tree.convert(treeName);

                    System.out.println("How many levels are to be generalized?");
                    int level = Integer.parseInt(cli.next());

                    anonymization.generalize(treeName, tree, level).execute().print();
                    break;
                case "bucketize":
                    System.out.println("Which column do you want to bucketize?");
                    String column = cli.next();
                    System.out.println("How big is the bucket supposed to be?");
                    int step = Integer.parseInt(cli.next());

                    anonymization.bucketize(column, step).execute().print();
                    break;
                case "suppress":
                    System.out.println("Which column do you want to suppress?");
                    anonymization.suppress(cli.next()).execute().print();
                    break;
                case "blurring":
                    System.out.println("Which column do you want to blur?");
                    anonymization.blurring(cli.next()).execute().print();
                    break;
                case "tokenize":
                    System.out.println("Which column do you want to tokenize?");
                    anonymization.tokenize(cli.next()).execute().print();
                    break;
                case "addNoise":
                    System.out.println("What noise should be added?");
                    double noise = Double.parseDouble(cli.next());
                    System.out.println("In which column do you want to add Noise?");
                    anonymization.addNoise(cli.next(), noise).execute().print();
                    break;
                case "substitute":
                    Map<String, String> map = new HashMap<>();
                    map.put("M", "F");
                    map.put("F", "M");
                    anonymization.substitute("gender", map).execute().print();
                    break;
                case "conditionalSubstitute":
                    break;
                case "average":
                    System.out.println("What deviation should be used?");
                    double deviation = Double.parseDouble(cli.next());
                    System.out.println("Which column do you want to average?");
                    anonymization.average(cli.next(), deviation).execute().print();
                    break;
                case "aggregate":
                    System.out.println("Which two columns do you want to aggregate?");
                    String column1 = cli.next();
                    String column2 = cli.next();
                    anonymization.aggregate(column1, column2).execute().print();
                    break;
                case "UMicroaggregation":
                    System.out.println("What n should be used?");
                    int n = Integer.parseInt(cli.next());
                    System.out.println("Which column do you want to microaggregate?");
                    anonymization.UMicroaggregation(cli.next(), n).execute().print();
                    break;
                case "kAnonymity":
                    System.out.println("How big is k?");
                    int k = Integer.parseInt(cli.next());
                    anonymization.kAnonymity(k).execute().print();
                case "exit":
                    exit = true;
                    break;
                default:
                    System.out.println("Please check spelling");
                    break;
            }


        }



    }
    public static void main(String[] args) {
        try {
            UserInterface ui = new UserInterface();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
