package AggFunctions;

public class Accumulator {
    public int counter = 0;
    public boolean suppressionFlag = false; //false if all values are identical, true otherwise
    public String string = null;
    public double number = Double.MAX_VALUE;
}
