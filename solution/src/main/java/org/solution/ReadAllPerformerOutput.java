package org.solution;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.io.*;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ReadAllPerformerOutput {
    public static void readAllPerformerOutput(Set<String> uniqueDepartments, String finalOutput, String dispPerfFilePath) {
        String namingConvention = "(.*)_performer_output.json";

        // List files in the folder
        File folder = new File(dispPerfFilePath);
        File[] files = folder.listFiles((dir, name) -> name.matches(namingConvention));

        if (files != null) {
            List<PerformerCollection> collectionWorkerList = new ArrayList<>();
            for (File filename : files) {
                Gson gson = new Gson();
                try (Reader reader = new FileReader(filename)) {
                    System.out.println("Filename: " + filename);
                    Type PerformerCollectionType = new TypeToken<ArrayList<PerformerCollection>>() {
                    }.getType();
                    List<PerformerCollection> collectionList = gson.fromJson(reader, PerformerCollectionType);
                    collectionWorkerList.addAll(collectionList);
                    for (String department : uniqueDepartments) {
                        System.out.println(department);
                    }
//                    for (PerformerCollection item : collectionList) {
//                        collectionWorkerList.add(item);
//                        System.out.println("Current Item: " + item.title + " --- " + item.location + " --- " + item.job_type + " --- " + item.postedBy);
//                    }
                } catch (IOException e) {
                    System.err.println(e.getMessage());
                }
            }

//            try (Writer writer = new FileWriter(finalOutput)) {
//                Gson gson = new GsonBuilder().setPrettyPrinting().create();
//                gson.toJson(collectionWorkerList, new TypeToken<List<DispatcherCollection>>() {
//                }.getType(), writer);
//            } catch (IOException e) {
//                System.err.println(e.getMessage());
//            }
        }
//
//            try (Reader reader = new FileReader(finalOutput)) {
//                Gson gson = new Gson();
//                Type PerformerCollectionType = new TypeToken<ArrayList<PerformerCollection>>() {
//                }.getType();
//                List<PerformerCollection> collectionList = gson.fromJson(reader, PerformerCollectionType);
//                int Counter = 0;
//                for (PerformerCollection item : collectionList) {
//                    Counter = Counter + 1;
//                    System.out.println("Current Item: " + Counter + " --- " + item.title + " --- " + item.location + " --- " + item.job_type + " --- " + item.postedBy);
//                }
//            } catch (IOException e) {
//                System.err.println(e.getMessage());
//            }

    }
}
