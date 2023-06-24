package io.id.stock.analysis;

import com.google.gson.*;
import io.id.stock.analysis.Module.IdxStock;
import io.id.stock.analysis.Module.KafkaStockProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.math.BigInteger;

public class GetStockPrice {

    private static final Logger log = LoggerFactory.getLogger(GetStockPrice.class.getSimpleName());

    private static StringBuilder getAPIResponse(HttpURLConnection connection) throws IOException {
        // Read the response from the API
        BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        String line;
        StringBuilder response = new StringBuilder();

        while ((line = reader.readLine()) != null) {
            response.append(line);
        }
        reader.close();

        return response;
    }

    private static String CountCompany(String value) {
        return JsonParser.parseString(value)
            .getAsJsonObject()
            .get("data")
            .getAsJsonObject()
            .get("count")
            .getAsString();
    }

    private static JsonArray getAPIResults(String apiUrl){
        try {
            // Create a URL object from the API URL
            URL url = new URL(apiUrl);

            // Open a connection to the API URL
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();

            // Set the HTTP method to GET
            connection.setRequestMethod("GET");

            // Set the Accept header to request JSON content
            connection.setRequestProperty("Accept", "*/*");

            // Set the X-API-KEY header
            connection.setRequestProperty("X-API-KEY", "pKg7UnAUzqKj8GMKQWu2R83e2N7Jno");

            // Set User Agent
            connection.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.95 Safari/537.11");

            // Get the response code
            int responseCode = connection.getResponseCode();

            if (responseCode == 200){
                //Get Response message
                String responseMessage = getAPIResponse(connection).toString();

                //Read Array Json
                Gson gson = new Gson();
                JsonObject jsonObject = gson.fromJson(responseMessage, JsonObject.class);
                JsonArray resultsArray = jsonObject.getAsJsonObject("data").getAsJsonArray("results");

                // Close the connection
                connection.disconnect();

                return resultsArray;
            } else {
                log.info("ERROR with Response Code: " + responseCode+" URL: "+url);
                JsonArray emptyArray = new JsonArray();
                return emptyArray;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) throws UnsupportedEncodingException {
        //Create Kafka Producer
        String topic = "streaming.goapi.idx.stock.json";
        KafkaStockProducer producer = new KafkaStockProducer(true, topic);

        //Base API URL
        String baseUrl = "https://api.goapi.id/v1/stock/idx/";

        //URL Company
        String apiUrl = baseUrl + "trending";

        //Query Company
        JsonArray companyResults = getAPIResults(apiUrl);

        try {
            producer.createProducerConn();

            //Counter
            int Counter = 0;

            for (JsonElement companyElement : companyResults) {
                Counter++;
                JsonObject companyObject = companyElement.getAsJsonObject();
                String emitent = companyObject.get("ticker").getAsString();
                String change = companyObject.get("change").getAsString();
                String percent = companyObject.get("percent").getAsString();
                log.info("number: "+ String.valueOf(Counter));
                log.info("emitent: " + emitent);
                log.info("change: " + change);
                log.info("percent: " + percent);

                //Query Historical Stock Price
                String apiUrl2 = baseUrl + emitent + "/historical";

                // Encode the parameter values
                String encodedParam1 = URLEncoder.encode("2020-01-01", StandardCharsets.UTF_8.toString());
                String encodedParam2 = URLEncoder.encode("2023-05-31", StandardCharsets.UTF_8.toString());

                // Create the complete API URL with parameters
                apiUrl2 = apiUrl2 + "?from=" + encodedParam1 + "&to=" + encodedParam2;

                //Query Stock Price
                JsonArray priceResults = getAPIResults(apiUrl2);
                if (priceResults.isJsonNull() != true){
                    if (priceResults.size() > 0){
                        for (JsonElement priceElement : priceResults) {
                            JsonObject priceObject = priceElement.getAsJsonObject();
                            String ticker = priceObject.get("ticker").getAsString();
                            String date = priceObject.get("date").getAsString();
                            String open = priceObject.get("open").getAsString();
                            String high = priceObject.get("high").getAsString();
                            String low = priceObject.get("low").getAsString();
                            String close = priceObject.get("close").getAsString();
                            String volume = priceObject.get("volume").getAsString();
                            String id = ticker + "_" + date;

                            log.info("ticker: " + ticker);
                            log.info("date: " + date);
                            log.info("open: " + open);
                            log.info("high: " + high);
                            log.info("low: " + low);
                            log.info("close: " + close);
                            log.info("volume: " + volume);
                            log.info(" ");

                            IdxStock stock = new IdxStock(id, ticker, date, Double.valueOf(open),Double.valueOf(high),Double.valueOf(low),Double.valueOf(close),new BigInteger(volume));
                            String jsonStock = new Gson().toJson(stock);

                            //Send Producer
                            producer.startProducer(id, jsonStock);

                        }
                    } else {
                        log.info("Empty array");
                    }
                }

//                //Only call the first 10 of Company
//                if (Counter >= 1){
//                    break;
//                }

                log.info("-----------------------------");
            }

            //Close Producer
            producer.flushAndCloseProducer();
        } catch (Exception e) {
            log.info("Error producer: "+e);
        } finally {
            try{
                Thread.sleep(10000);
            } catch (InterruptedException error){
                error.printStackTrace();
            }

        }

    }

}