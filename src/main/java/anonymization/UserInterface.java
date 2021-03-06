package anonymization;

import common.MapReader;
import common.Tree;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;

import java.util.HashMap;
import java.util.InputMismatchException;
import java.util.Map;
import java.util.Scanner;

public class UserInterface {

    Scanner cli;
    String PATH = "src/main/java/table/TrainingData.csv";
    Schema schema = Schema.newBuilder()
            .column("id", DataTypes.BIGINT())
            .column("gender", DataTypes.STRING())
            .column("age", DataTypes.INT())
            .column("name", DataTypes.STRING())
            .column("surname", DataTypes.STRING())
            .column("residence", DataTypes.INT())
            .build();

    Anonymization anonymization;


    public void printTable(){
        System.out.println("The table currently looks as followed:");
        anonymization.printTable();
    }


    public void runUI(){
        cli = new Scanner(System.in);
        anonymization = new Anonymization(PATH, schema);
        anonymization.buildTable();
        printTable();

        boolean exit = false;
        while (!exit) {
            System.out.println("Please choose an operation:");
            System.out.println("The operations are: shuffle, generalize, bucketize, suppress, blurring, tokenize, addNoise, substitute, conditionalSubstitute, average, aggregate, UMicroaggregation, kAnonymity");
            System.out.println("Enter exit to exit the UI");
            String command = cli.next();

            try {
                switch (command) {
                    case "shuffle":
                        System.out.println("Which column do you want to shuffle?");
                        anonymization.shuffle(cli.next()).execute().print();

                        break;
                    case "generalize":
                        System.out.println("Which column do you want to generalize?");
                        String treeName = cli.next();
                        Tree tree = new Tree(treeName);
                        tree.convert(treeName);

                        System.out.println("How many levels are to be generalized?");
                        int level = cli.nextInt();

                        anonymization.generalize(treeName, tree, level).execute().print();
                        break;
                    case "bucketize":
                        System.out.println("Which column do you want to bucketize?");
                        try {
                            String column = cli.next();
                            System.out.println("How big is the bucket supposed to be? You can also provide the buckets by seperating them with a comma");
                            String step = cli.next();
                            String[] steps = step.split(",");

                            if(steps.length>1){
                                int[] stepsInt = new int[steps.length];
                                for (int i = 0; i<steps.length; i++){
                                    stepsInt[i] = Integer.parseInt(steps[i]);
                                }
                                anonymization.bucketize(column, stepsInt).execute().print();
                            }else{
                                anonymization.bucketize(column, Integer.parseInt(step)).execute().print();
                            }
                            break;


                        } catch (InputMismatchException e) {
                            System.out.println("ERROR: Please use integers as a step size.");
                        } catch (IllegalArgumentException e) {
                            System.out.println("ERROR: Step must be greater than 0.");
                        }
                        break;


                    case "suppress":
                        System.out.println("Which column do you want to suppress?");
                        anonymization.suppress(cli.next()).execute().print();
                        break;
                    case "blurring":
                        System.out.println("Which column do you want to blur?");
                        String blurColumn = cli.next();
                        System.out.println("How many characters should remain in the beginning");
                        int amountOfFirstChars = cli.nextInt();
                        System.out.println("How many characters should remain at the end?");
                        int amountOfLastChars = cli.nextInt();

                        anonymization.blurring(blurColumn, amountOfFirstChars, amountOfLastChars).execute().print();
                        break;
                    case "tokenize":
                        System.out.println("Which column do you want to tokenize?");
                        anonymization.tokenize(cli.next()).execute().print();
                        break;
                    case "addNoise":
                        System.out.println("What noise should be added?");
                        double noise = cli.nextDouble();
                        System.out.println("In which column do you want to add Noise?");
                        anonymization.addNoise(cli.next(), noise).execute().print();
                        break;
                    case "substitute":
                        System.out.println("Which column do you want to tokenize?");
                        String column = cli.next();
                        System.out.println("Where is the map file located?");
                        String mapFile = cli.next();
                        Map<String, String> map = MapReader.read(mapFile);
                        anonymization.substitute(column, map).execute().print();
                        break;
                    case "conditionalSubstitute":
                        break;
                    case "average":
                        System.out.println("What deviation should be used?");
                        double deviation = cli.nextDouble();
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
                        int n = cli.nextInt();
                        System.out.println("Which column do you want to microaggregate?");
                        anonymization.UMicroaggregation(cli.next(), n).execute().print();
                        break;
                    case "kAnonymity":
                        System.out.println("How big is k?");
                        int k = cli.nextInt();
                        anonymization.kAnonymity(k);
                        break;
                    case "reset":
                        anonymization.resetData().execute().print();
                        break;
                    case "exit":
                        exit = true;
                        break;
                    default:
                        System.out.println("Please check spelling");
                        break;
                }


            }catch (ValidationException e) {
                System.out.println("ERROR: The selected column does not exist or is incompatible with the function.");
            } catch(Exception e){
                e.printStackTrace();
            }
        }

    }



    public static void main (String[] args){
        try {
            UserInterface ui = new UserInterface();
            ui.runUI();
        }catch (Exception e){
            e.printStackTrace();
        }
    }


}
