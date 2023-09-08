package org.solution;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.microsoft.playwright.*;
import com.microsoft.playwright.options.AriaRole;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONArray;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Array;
import java.util.*;
import java.util.regex.Pattern;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.opentest4j.AssertionFailedError;
import sun.awt.windows.WPrinterJob;
import sun.security.util.Length;

import static com.microsoft.playwright.assertions.PlaywrightAssertions.assertThat;
import static java.util.Arrays.asList;
import static org.json.simple.JSONValue.parse;
import static org.solution.ReadAllPerformerOutput.readAllPerformerOutput;

public class solution {

//    private static final Queue<String> queue = new LinkedList<>();
    private static final Queue<DispatcherCollection> queue = new LinkedList<>();
    private static Integer workerNumber;

    public static List<DispatcherCollection> runDispatcher(Page page, String dispatcherFileName) {
        List<DispatcherCollection> collectionList = DispatcherExtractTable.ScrapeTable(page);
//        for (DispatcherCollection item : collectionList) {
//            System.out.println(item.id + " --- " + item.selector);
//        }

        try {
            // Create an ObjectMapper
            ObjectMapper objectMapper = new ObjectMapper();

            // Serialize the collectionList to a JSON file
            objectMapper.writeValue(new File(dispatcherFileName), collectionList);
            System.out.println("Data written to " + dispatcherFileName);
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
        return collectionList;
    }

    public static void createQueue(List<DispatcherCollection> collectionList, String filename) throws IOException {
        for (DispatcherCollection item : collectionList) {
            queue.offer(new DispatcherCollection(item.id, item.fullText, item.selector, item.getDepartment()));
            System.out.println("Add to Queue: " + item.id + " --- " + item.selector);
        }
//        JSONArray listJson = (JSONArray) parse(new FileReader(filename));
//        for (Object objJson : listJson) {
//            JSONObject collection = (JSONObject) objJson;
//
//            Long id = (Long) collection.get("id");
//            String fullText = (String) collection.get("fullText");
//            String selector = (String) collection.get("selector");
//            queue.add(selector);
//            System.out.println("Add to Queue: " + id + " --- " + fullText + " --- " + selector);
//        }
    }

    public static void runPerformer(String performerOutputFile) throws InterruptedException {
        List<Thread> threads = new ArrayList<>();
        for (Integer worker = 1; worker <= workerNumber; worker++) {
            String newPerformerOutputFile = worker + "_" + performerOutputFile;
            Thread thread = new PerformerExtractData(worker, queue, newPerformerOutputFile);
            threads.add(thread);
            // Start thread
            thread.start();
        }

        // Wait for all threads to finish
        for (Thread thread : threads) {
            thread.join();
        }
        System.out.println("All workers have finished...");
    }

    public static void main(String[] args) {
        // For Debugging process, run cmd below:
        // "C:\Program Files\Google\Chrome\Application\chrome.exe" --remote-debugging-port=8888
        boolean debugRun = true;

        // Define dispatcher output in json format
        String DISPATCHER_OUTPUT = "dispatcher_output.json";

        // Define performer worker number
        workerNumber = 8;

        // Define Performer output in json format
        String PERFORMER_OUTPUT = "performer_output.json";
        String FINAL_OUTPUT = "solution.json";

        try (Playwright playwright = Playwright.create()) {
            Page page = null;
            if (!debugRun) {
                Browser browser = playwright.chromium().launch(new BrowserType.LaunchOptions().setHeadless(false).setSlowMo(50));
                page = browser.newPage();
            } else {
                System.out.println("Running Debug Mode");
                Browser browser = playwright.chromium().connectOverCDP("http://localhost:8888");
                BrowserContext defaultContext = browser.contexts().get(0);
                page = defaultContext.pages().get(0);
            }

            page.navigate("https://www.cermati.com/karir");
            page.getByRole(AriaRole.LINK, new Page.GetByRoleOptions().setName("View All Jobs").setExact(true)).click();
            List<DispatcherCollection> collectionList = runDispatcher(page, DISPATCHER_OUTPUT);
            createQueue(collectionList, DISPATCHER_OUTPUT);
            runPerformer(PERFORMER_OUTPUT);
//            readAllPerformerOutput(FINAL_OUTPUT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}