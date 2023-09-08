package org.solution;

import com.microsoft.playwright.*;
import com.microsoft.playwright.options.AriaRole;
import org.opentest4j.AssertionFailedError;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.regex.Pattern;

import static com.microsoft.playwright.assertions.PlaywrightAssertions.assertThat;

public class DispatcherExtractTable {
    public static List<DispatcherCollection> ScrapeTable(Page page) {
        System.out.println("Start Table Scraping");
        int counterPage = 1;
        int counterURL = 1;
        List<DispatcherCollection> collectionList = new ArrayList<>();

        Locator currentPage = page.getByRole(AriaRole.BUTTON, new Page.GetByRoleOptions().setName(String.valueOf(counterPage)));
        assertThat(currentPage).isVisible();
        while (true) {
            try {
                Locator listJob = page.locator("css=.page-job-list-wrapper");
                for (int i = 0; i < listJob.count(); i++) {
                    String fullText = listJob.nth(i).textContent();
                    String rawSelector = fullText
                            .replace(".", "\\.")
                            .replace(",", "\\,")
                            .replace("(", "\\(")
                            .replace(")", "\\)")
                            .replace("/", "\\/")
                            .replace("-", "\\-");
                    String[] newSelector = rawSelector.split("·");
                    System.out.println(counterURL + " -- " + newSelector[0].trim());
                    DispatcherCollection collection = new DispatcherCollection(counterURL, fullText, newSelector[0].trim(), "");
                    collectionList.add(collection);
                    counterURL = counterURL + 1;

////                    Test Click Apply !!!
//                    Page newPage = page.waitForPopup(() -> {
//                        page.locator("div").filter(
//                                        new Locator.FilterOptions().setHasText(
//                                                Pattern.compile("^" + collection.selector + "$")))
//                                .getByRole(AriaRole.LINK)
//                                .click();
//                    });
//                    newPage.close();

                }

                counterPage = counterPage + 1;
                Locator nextPage = page.getByRole(AriaRole.BUTTON, new Page.GetByRoleOptions().setName(String.valueOf(counterPage)));
                assertThat(nextPage).isVisible();
                nextPage.click();
            } catch (PlaywrightException e) {
                // Handle the exception that occurred during the operation
                page.screenshot(new Page.ScreenshotOptions().setPath(Paths.get("error.png")));
                System.out.println("PlaywrightException: " + e.getMessage());
                break;
            } catch (AssertionFailedError e) {
                // Handle AssertionFailedError if the locator is not visible
                page.screenshot(new Page.ScreenshotOptions().setPath(Paths.get("error.png")));
                System.out.println("AssertionFailedError: " + e.getMessage());
                break;
            }
        }

        return collectionList;
    }
}
