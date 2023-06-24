package io.kafka.consumer.opensearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class OpenSearchConsumerReplaying {
    private static final Logger log = LoggerFactory.getLogger(OpenSearchConsumerReplaying.class.getSimpleName());
    public static RestHighLevelClient createOpenSearchClient(){
        //Required to have opensearch run in localhost
        String connString = "http://localhost:9200/";

        //Build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        //Extract login information if it exist
        String userinfo = connUri.getUserInfo();

        //Rest client without config
        restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort())));

        return restHighLevelClient;
    }

    private static KafkaConsumer<String, String> createKafkaConsumer(Boolean localServer) {

        String bootStrapServer1 = "192.168.207.8:9092";
        String bootStrapServer2 = "cluster.playground.cdkt.io:9092"; //kafka server from Conductor
        String groupId = "consumer-opensearch-application";

        //Create producer properties
        Properties properties = new Properties();
        if(localServer){
            log.info("Call Kafka Local Server...");
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer1);
        } else {
            log.info("Call Kafka Conductor Server...");
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer2);
            properties.setProperty("security.protocol", "SASL_SSL");
            properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"6W1Ja4rpKmarggy5YCr7In\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiI2VzFKYTRycEttYXJnZ3k1WUNyN0luIiwib3JnYW5pemF0aW9uSWQiOjczMzM5LCJ1c2VySWQiOjg1MjY1LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiIxNWRmZjljZC1hYTgwLTRlMmItYTAzYi1iMjUxYWMyNDA5YzMifX0.Dg8zCO6dJq9hDbVsuvzmlba2RWv6g0WBw0hkhpawX-w\";");
        }
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty("sasl.mechanism", "PLAIN");

        //create consumer config
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); //chose none/earliest/latest
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); //enable or disable auto commit offset

        //create a consumer
        return new KafkaConsumer<>(properties);
    }

    private static String extractID(String value) {
        return JsonParser.parseString(value)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    public static void main(String[] args) throws IOException {
        //Create Open Search Client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        //create a consumer
        Boolean localServer = true;
        KafkaConsumer<String, String> consumer = createKafkaConsumer(localServer);

        //get a reference to the main thread
        final Thread mainThread = Thread.currentThread();

        //adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                log.info("Detected a shutdown, let's exit by calling consumer consumer.wakeup()...");
                consumer.wakeup();
                log.info("Consumer has sent wakeup signal...");
                //join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        //we need to create the index on Opensearch if it does not exist already
        try(openSearchClient; consumer) {
            String topic = "wikimedia.recentchange";
            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest(topic), RequestOptions.DEFAULT);
            if (!indexExists){
                CreateIndexRequest createIndexRequest = new CreateIndexRequest(topic);
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("Wikimedia Index has been created");
            } else {
                log.info("Wikimedia Index already exist");
            }

            //subscribe to a topic
            consumer.subscribe(Arrays.asList(topic)); //can mention multiple topic in here

            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
                int recordCount = records.count();
                log.info("Received " + recordCount + " record(s)");

                BulkRequest bulkRequest = new BulkRequest();

                for (ConsumerRecord<String, String> record : records) {
                    // send the record into OpenSearch
                    try {
                        // strategy 2
                        // we extract the ID from the JSON value
                        String id = extractID(record.value());

                        IndexRequest indexRequest = new IndexRequest("wikimedia")
                                .source(record.value(), XContentType.JSON)
                                .id(id);

                        //IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
                        bulkRequest.add(indexRequest);
                    } catch (Exception e){
                        log.info("Error: ", e);
                    }
                }

                if (bulkRequest.numberOfActions() > 0){
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Inserted " + bulkResponse.getItems().length + " record(s).");

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    // commit offsets after the batch is consumed
                    consumer.commitSync();
                    log.info("Offsets have been committed!");
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer is starting to shut down...");
        } catch (Exception e){
            log.info("Unexpected exception in the consumer", e);
        } finally {
            log.info("starting to close now...");
            consumer.close();
            openSearchClient.close();
            log.info("Consumer was closed gracefully");
        }
    }

}
