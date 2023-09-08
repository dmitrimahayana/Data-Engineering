package org.solution;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import static org.json.simple.JSONValue.parse;

public class ReadAllPerformerOutput {
    public static void readAllPerformerOutput(String FINAL_OUTPUT){
        String folderPath = "./";
        String namingConvention = "(.*)_performer_output.json";

        // List files in the folder
        File folder = new File(folderPath);
        File[] files = folder.listFiles((dir, name) -> name.matches(namingConvention));

        if (files != null) {
            JSONArray listJson;
            for (File filename : files) {
                try {
                    listJson = (JSONArray) parse(new FileReader(filename));
                    for (Object objJson : listJson) {
                        JSONObject collection = (JSONObject) objJson;

                        String title = (String) collection.get("title");
                        String location = (String) collection.get("location");
                        JSONArray description = (JSONArray) collection.get("description");
                        JSONArray qualification = (JSONArray) collection.get("qualification");
                        String job_type = (String) collection.get("job_type");
                        String postedBy = (String) collection.get("postedBy");
                        System.out.println("Object: " + title + " --- " + location + " --- " + job_type + " --- " + postedBy);
                        System.out.println("Object: " + description + " --- " + qualification);
//                        PerformerCollection collectionJob = new PerformerCollection(title, location, description, qualification, job_type, postedBy);
                    }
                } catch (IOException e) {
                    System.err.println("Error reading JSON file: " + filename.getName() + e.getMessage());
                }
            }
        }
    }
}
