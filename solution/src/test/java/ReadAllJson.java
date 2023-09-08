import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;

public class ReadAllJson {
    public static void main(String[] args) {
        String folderPath = "./";
        String namingConvention = "(.*)_performer_output.json";

        // Create an ObjectMapper instance to parse JSON
        ObjectMapper objectMapper = new ObjectMapper();

        // List files in the folder
        File folder = new File(folderPath);
        File[] files = folder.listFiles((dir, name) -> name.matches(namingConvention));

        if (files != null) {
            for (File file : files) {
                try {
                    // Read and parse the JSON file
                    JsonNode jsonNode = objectMapper.readTree(file);

                    // Do something with the JSON data
                    System.out.println("Reading file: " + file.getName());
                    System.out.println("JSON content: " + jsonNode.toString());
                } catch (IOException e) {
                    System.err.println("Error reading JSON file: " + file.getName());
                    e.printStackTrace();
                }
            }
        } else {
            System.err.println("No files found in the folder matching the naming convention.");
        }
    }
}
