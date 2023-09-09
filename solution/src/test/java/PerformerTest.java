import com.microsoft.playwright.*;
import org.apache.commons.lang3.StringUtils;
import org.opentest4j.AssertionFailedError;
import org.solution.PerformerCollection;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;
import java.util.regex.Pattern;

import static com.microsoft.playwright.assertions.PlaywrightAssertions.assertThat;

public class PerformerTest {

    private static Queue<String> queue = new LinkedList<>();
    private static Integer workerNumber;

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
            if (!debugRun) {
                Browser browser = playwright.chromium().launch(new BrowserType.LaunchOptions().setHeadless(false).setSlowMo(50));
                page = browser.newPage();
            } else {
                System.out.println("Running Debug Mode");
                Browser browser = playwright.chromium().connectOverCDP("http://localhost:8888");
                BrowserContext defaultContext = browser.contexts().get(0);
                page = defaultContext.pages().get(0);
            }

            String jobTitle = page.locator("//h1[@class='job-title']").textContent();
            String jobLocation = page.locator("css=.c-spl-job-location__place").textContent();
            String jobType = page.locator("//li[@itemprop='employmentType']").textContent();
            String jobPostedBy = "";
            try {
                Locator locJobPostedBy = page.locator("div").filter(new Locator.FilterOptions().setHasText(Pattern.compile("^Posted by.+$"))).nth(2);
                assertThat(locJobPostedBy).isVisible();
                jobPostedBy = locJobPostedBy.textContent();
                jobPostedBy = StringUtils.capitalize(jobPostedBy.toLowerCase().replace("posted by", ""));
            } catch (AssertionFailedError e) {
                jobPostedBy = "";
            }

            Locator listJobDescription = page.locator("//div[@itemprop='responsibilities'] //ul //li");
            String[] jobDescription = {};
            if (listJobDescription.count() == 0) {
                String[] paraJobDescription = page.locator("//div[@itemprop='responsibilities']").textContent().split("\\n");
                jobDescription = new String[paraJobDescription.length];
                for (int i = 0; i < paraJobDescription.length; i++) {
                    jobDescription[i] = paraJobDescription[i].replace("\u00a0", "").replace("•", "").trim();
                }
            } else if (listJobDescription.count() > 0) {
                jobDescription = new String[listJobDescription.count()];
                for (int i = 0; i < listJobDescription.count(); i++) {
                    jobDescription[i] = listJobDescription.nth(i).textContent().replace("\u00a0", "").replace("•", "").trim();
                }
            }

            String[] jobQualification = {};
            Locator listJobQualification = page.locator("//div[@itemprop='qualifications'] //ul //li");
            if (listJobQualification.count() == 0) {
                String[] paraJobQualification = page.locator("//div[@itemprop='qualifications']").textContent().split("\\n");
                jobQualification = new String[paraJobQualification.length];
                for (int i = 0; i < paraJobQualification.length; i++) {
                    jobQualification[i] = paraJobQualification[i].replace("\u00a0", "").replace("•", "").trim();
                }
            } else if (listJobQualification.count() > 0) {
                jobQualification = new String[listJobQualification.count()];
                for (int i = 0; i < listJobQualification.count(); i++) {
                    jobQualification[i] = listJobQualification.nth(i).textContent().replace("\u00a0", "").replace("•", "").trim();
                }
            }
            PerformerCollection collectionJob = new PerformerCollection("", jobTitle, jobLocation, jobDescription, jobQualification, jobType, jobPostedBy);

            System.out.println("title: "+collectionJob.title);
            System.out.println("location: "+collectionJob.location);
            System.out.println("description: "+ Arrays.toString(collectionJob.description));
            System.out.println("qualification: "+Arrays.toString(collectionJob.qualification));
            System.out.println("job_type: "+collectionJob.job_type);
            System.out.println("postedBy: "+collectionJob.postedBy);
        }
    }
}
