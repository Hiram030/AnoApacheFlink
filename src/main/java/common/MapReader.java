package common;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MapReader {
    public static Map<String, String> read(String path) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(path));
        String line = bufferedReader.readLine();
        Map<String, String> map = new HashMap<>();
        while (line != null) {
            String[] keyValuePair = line.split(",");
            if(keyValuePair.length < 2) {
                throw new IllegalArgumentException("Map file is not correct. There must be atleast 2 parameters on each line.");
            }
            map.put(keyValuePair[0], keyValuePair[1]);
            line = bufferedReader.readLine();
        }
        return map;
    }
}
