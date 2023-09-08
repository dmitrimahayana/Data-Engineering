package org.solution;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.microsoft.playwright.*;
import com.microsoft.playwright.options.AriaRole;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.simple.JSONArray;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import static java.util.Arrays.asList;
import static org.json.simple.JSONValue.parse;

public class solution {

    private static Queue<String> queue = new LinkedList<>();
    private static Integer workerNumber;

    public static void runDispatcher(Page page, String dispatcherFileName) {
        List<DispatcherCollection> collectionList = DispatcherExtractTable.ScrapeTable(page);
        for (DispatcherCollection item : collectionList) {
            System.out.println(item.id + " --- " + item.selector);
        }

        try {
            // Create an ObjectMapper
            ObjectMapper objectMapper = new ObjectMapper();

            // Serialize the collectionList to a JSON file
            objectMapper.writeValue(new File(dispatcherFileName), collectionList);
            System.out.println("Data written to " + dispatcherFileName);
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }

    public static void createQueue(String filename) throws IOException {
        // parsing file "JSONExample.json"
        JSONArray listJson = (JSONArray) parse(new FileReader(filename));

        for (Object objJson : listJson) {
            JSONObject collection = (JSONObject) objJson;

            Long id = (Long) collection.get("id");
            String fullText = (String) collection.get("fullText");
            String selector = (String) collection.get("selector");
            queue.add(selector);
            System.out.println("Add to Queue: " + id + " --- " + fullText + " --- " + selector);
        }
    }

    public static void runPerformer(Page page) {
//        while (true){
//            // Check if the queue is empty
//            if (queue.peek() == null) {
//                System.out.println("Queue is empty");
//                break;
//            } else {
//                // Remove elements from the queue
//                String selector = queue.poll();
//                System.out.println("Queue Selector: " + selector);
//            }
//        }
        for (Integer worker = 1; worker <= workerNumber; worker++) {
            Thread thread = new PerformerExtractData(worker);
            thread.start();
        }
    }

    public static void main(String[] args) {
        // For Debugging process, run cmd below:
        // "C:\Program Files\Google\Chrome\Application\chrome.exe" --remote-debugging-port=8888
        boolean debugRun = true;
        // Define dispatcher output in json format
        String DISPATCHER_OUTPUT = "dispatcher_output.json";
        // Define performer worker number
        workerNumber = 3;
        try (Playwright playwright = Playwright.create()) {
            Page page = null;
//            if (!debugRun) {
//                Browser browser = playwright.chromium().launch(new BrowserType.LaunchOptions().setHeadless(false).setSlowMo(50));
//                page = browser.newPage();
//            } else {
//                System.out.println("Running Debug Mode");
//                Browser browser = playwright.chromium().connectOverCDP("http://localhost:8888");
//                BrowserContext defaultContext = browser.contexts().get(0);
//                page = defaultContext.pages().get(0);
//            }
//
//            page.navigate("https://www.cermati.com/karir");
//            page.getByRole(AriaRole.LINK, new Page.GetByRoleOptions().setName("View All Jobs").setExact(true)).click();
//            page.waitForTimeout(5_000);
//            runDispatcher(page, DISPATCHER_OUTPUT);
            createQueue(DISPATCHER_OUTPUT);
            runPerformer(page);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}