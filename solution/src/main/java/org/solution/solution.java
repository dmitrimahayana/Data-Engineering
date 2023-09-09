package org.solution;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import com.microsoft.playwright.*;
import com.microsoft.playwright.options.AriaRole;

import java.io.*;
import java.lang.reflect.Type;
import java.util.*;

import static org.solution.ReadAllPerformerOutput.readAllPerformerOutput;

public class solution {

    private static final Queue<DispatcherCollection> queue = new LinkedList<>();
    private static final String DISPATCHER_OUTPUT = "dispatcher_output.json";
    private static final int WORKER_NUMBER = 8;
    private static final String DISP_PERF_FOLDER = "./json/";
    private static final String ERROR_FOLDER = "./screenshot/";
    private static final String PERFORMER_OUTPUT = "[worker]_performer_output.json";
    private static final String FINAL_OUTPUT = "solution.json";

    public static void runDispatcher(Page page, String dispatcherFilePath) {
        List<DispatcherCollection> collectionList = DispatcherExtractTable.ScrapeTable(page, ERROR_FOLDER);

        try (Writer writer = new FileWriter(dispatcherFilePath)) {
            Gson gson = new  GsonBuilder().setPrettyPrinting().create();
            gson.toJson(collectionList, new TypeToken<List<DispatcherCollection>>() {}.getType(), writer);
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }

    public static void createQueue(String dispatcherFilePath) {
        Gson gson = new Gson();
        try (Reader reader = new FileReader(dispatcherFilePath)) {
            Type dispatcherCollectionType = new TypeToken<ArrayList<DispatcherCollection>>(){}.getType();
            List<DispatcherCollection> collectionList = gson.fromJson(reader, dispatcherCollectionType);
            for(DispatcherCollection item : collectionList) {
            queue.offer(new DispatcherCollection(item.id, item.fullText, item.selector, item.department));
            System.out.println("Add to Queue: " + item.id + " --- " + item.fullText + " --- " + item.selector + " --- " + item.department);
            }
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }

    public static void runPerformer(String PerformerOutputPath){
        List<Thread> threads = new ArrayList<>();
        for (int worker = 1; worker <= WORKER_NUMBER; worker++) {
            String newPerformerOutputPath = PerformerOutputPath.replace("[worker]", String.valueOf(worker));
            Thread thread = new PerformerExtractData(worker, queue, newPerformerOutputPath);
            threads.add(thread);
            // Start thread
            thread.start();
        }

        try {
            // Wait for all threads to finish
            for (Thread thread : threads) {
                thread.join();
            }
        } catch (InterruptedException e){
            throw new RuntimeException(e);
        }
        System.out.println("All workers have finished...");
    }

    public static Set<String> getAllDepartment(String dispatcherFilePath){
        Set<String> uniqueDepartments = new HashSet<>();
        try (FileReader fileReader = new FileReader(dispatcherFilePath)) {
            JsonArray jsonArray = JsonParser.parseReader(fileReader).getAsJsonArray();

            // Iterate through the JSON array and extract departments
            for (JsonElement element : jsonArray) {
                String department = element.getAsJsonObject().get("department").getAsString();
                uniqueDepartments.add(department);
            }
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }

        return uniqueDepartments;
    }

    public static void main(String[] args) {
        // For Debugging process, run cmd below:
        // "C:\Program Files\Google\Chrome\Application\chrome.exe" --remote-debugging-port=8888
        boolean debugRun = false;

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

//            page.navigate("https://www.cermati.com/karir");
//            page.getByRole(AriaRole.LINK, new Page.GetByRoleOptions().setName("View All Jobs").setExact(true)).click();
//            runDispatcher(page, DISP_PERF_FOLDER + DISPATCHER_OUTPUT);
//            createQueue(DISP_PERF_FOLDER + DISPATCHER_OUTPUT);
//            runPerformer(DISP_PERF_FOLDER + PERFORMER_OUTPUT);
            Set<String> uniqueDeptName = getAllDepartment(DISP_PERF_FOLDER + DISPATCHER_OUTPUT);
            readAllPerformerOutput(uniqueDeptName, DISP_PERF_FOLDER + FINAL_OUTPUT, DISP_PERF_FOLDER);
        }
    }
}