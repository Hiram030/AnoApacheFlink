package anonymization;

import common.MapReader;
import common.Tree;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.DataType;

import javax.xml.crypto.Data;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.InputMismatchException;
import java.util.Map;

public class FlinkJob {
    public static void main(String[] args) throws IOException {
        String schemaPath = args[0];
        String dataPath = args[1];
        String operatorPath = args[2];

        //read schema
        BufferedReader reader = new BufferedReader(new FileReader(schemaPath));
        String[] columnNames = reader.readLine().split(",");
        String[] dsString = reader.readLine().split(",");
        DataType[] datatypes = new DataType[columnNames.length];
        for(int i = 0; i < columnNames.length; i++) {
            DataTypes dataTypes;
            switch (dsString[i]) {
                case "INT" :
                    datatypes[i] = DataTypes.INT();
                    break;
                case "BIGINT":
                    datatypes[i] = DataTypes.BIGINT();
                    break;
                case "STRING":
                    datatypes[i] = DataTypes.STRING();
                    break;
                case "DOUBLE":
                    datatypes[i] = DataTypes.DOUBLE();
                    break;
                case "FLOAT":
                    datatypes[i] = DataTypes.FLOAT();
                    break;
                default:
                    throw new IllegalArgumentException("The programm does not suppport this data type: " +dsString[i]);
            }
        }

        Schema.Builder schemaBuilder = Schema.newBuilder();
        for (int i = 0; i < columnNames.length; i++) {
            schemaBuilder = schemaBuilder.column(columnNames[i], String.valueOf(datatypes[i]));
        }
        Schema schema = schemaBuilder.build();
        Anonymization anonymization = new Anonymization(dataPath, schema);
        anonymization.buildTable();
        //read operators
        reader = new BufferedReader(new FileReader(operatorPath));
        boolean flag = false; //whether kanonymity is used
        for (String line = reader.readLine(); line != null; line = reader.readLine()) {
            String[] opArgs = line.split(" ");
            switch (opArgs[0]) {
                case "shuffle":
                    anonymization.shuffle(opArgs[1]);
                    break;
                case "generalize":
                    Tree tree = new Tree(opArgs[1]);
                    tree.convert(opArgs[1]);
                    int level = Integer.parseInt(opArgs[2]);
                    anonymization.generalize(opArgs[1], tree, level);
                    break;
                case "bucketize":
                    System.out.println("Which column do you want to bucketize?");
                    try {
                        String column = opArgs[1];
                        String step = opArgs[2];
                        String[] steps = step.split(",");

                        if (steps.length > 1) {
                            int[] stepsInt = new int[steps.length];
                            for (int i = 0; i < steps.length; i++) {
                                stepsInt[i] = Integer.parseInt(steps[i]);
                            }
                            anonymization.bucketize(column, stepsInt);
                        } else {
                            anonymization.bucketize(column, Integer.parseInt(step));
                        }
                        break;
                    } catch (InputMismatchException e) {
                        System.out.println("ERROR: Please use integers as a step size.");
                    } catch (IllegalArgumentException e) {
                        System.out.println("ERROR: Step must be greater than 0.");
                    }
                    break;


                case "suppress":
                    anonymization.suppress(opArgs[1]);
                    break;
                case "blurring":
                    String blurColumn = opArgs[1];
                    int amountOfFirstChars = Integer.parseInt(opArgs[2]);
                    int amountOfLastChars = Integer.parseInt(opArgs[3]);
                    anonymization.blurring(blurColumn, amountOfFirstChars, amountOfLastChars);
                    break;
                case "tokenize":
                    anonymization.tokenize(opArgs[1]);
                    break;
                case "addNoise":
                    double noise = Double.parseDouble(opArgs[2]);
                    anonymization.addNoise(opArgs[1], noise);
                    break;
                case "substitute":
                    //todo
                    String column = opArgs[1];
                    String mapFile = opArgs[2];
                    Map<String, String> map = MapReader.read(mapFile);
                    anonymization.substitute(column, map);
                    break;
                case "conditionalSubstitute":
                    //todo
                    break;
                case "average":
                    double deviation = Double.parseDouble(opArgs[2]);
                    anonymization.average(opArgs[1], deviation);
                    break;
                case "aggregate":
                    String column1 = opArgs[1];
                    String column2 = opArgs[2];
                    anonymization.aggregate(column1, column2);
                    break;
                case "UMicroaggregation":
                    int n = Integer.parseInt(opArgs[2]);
                    anonymization.UMicroaggregation(opArgs[1], n);
                    break;
                case "kAnonymity":
                    int k = Integer.parseInt(opArgs[1]);
                    anonymization.kAnonymity(k);
                    flag = true;
                    break;
                case "reset":
                    anonymization.resetData();
                    break;
                default:
                    System.out.println("Unknow command: " + opArgs[0]);
            }
        }
        if(!flag)
            anonymization.printTable();
    }
}
