package org.solution;

import com.microsoft.playwright.*;

import java.nio.file.Paths;

import static java.util.Arrays.asList;

public class PerformerExtractData extends Thread {

    private final Integer worker;

    public PerformerExtractData(Integer worker) {
        this.worker = worker;
    }

//    private final String browserName;
//
//    private PerformerExtractData(String browserName) {
//        this.browserName = browserName;
//    }

//    public static void main(String[] args) throws InterruptedException {
//        // Create separate playwright thread for each browser.
//        for (Integer worker : asList(1, 2, 3)) {
//            Thread thread = new PerformerExtractData(worker);
////        for (String browserName : asList("chromium", "webkit", "firefox")) {
////            Thread thread = new PerformerExtractData(browserName);
//            thread.start();
//        }
//    }

    @Override
    public void run() {
        try (Playwright playwright = Playwright.create()) {
            Browser browser = playwright.chromium().launch(new BrowserType.LaunchOptions().setHeadless(false).setSlowMo(50));
            Page page = browser.newPage();
            String output = navigatePage(worker, page);
            System.out.println(output);
            page.waitForTimeout(10_000);
            page.screenshot(new Page.ScreenshotOptions().setPath(Paths.get("worker-" + worker + ".png")));
        }
        System.out.println("Worker " + worker + " done...");
    }

    private static String navigatePage(Integer worker, Page page) {
        String output;
        switch (worker) {
            case 1:
                output = "Worker 1 Playwright";
                page.navigate("https://playwright.dev/");
//                System.out.println(output);
                return output;
            case 2:
                output = "Worker 2 Google";
                page.navigate("https://www.google.com/");
//                System.out.println(output);
                return output;
            case 3:
                output = "Worker 3 Bing";
                page.navigate("https://www.bing.com/");
//                System.out.println(output);
                return output;
            default:
                throw new IllegalArgumentException();
        }
    }
}
