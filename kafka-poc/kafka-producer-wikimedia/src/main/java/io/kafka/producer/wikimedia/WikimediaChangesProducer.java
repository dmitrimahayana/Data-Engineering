package io.kafka.producer.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

    private static final Logger log = LoggerFactory.getLogger(WikimediaChangesHandler.class.getSimpleName());

    private static KafkaProducer<String, String> createKafkaProducer(Boolean localServer) {
        String bootStrapServer1 = "192.168.207.8:9092";
        String bootStrapServer2 = "cluster.playground.cdkt.io:9092"; //kafka server from Conductor

        //Create producer properties
        Properties properties = new Properties();
        if(localServer){
            log.info("Call Kafka Local Server...");
            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer1);
        } else {
            log.info("Call Kafka Conductor Server...");
            properties.setProperty("security.protocol", "SASL_SSL");
            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer2);
            properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"6W1Ja4rpKmarggy5YCr7In\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiI2VzFKYTRycEttYXJnZ3k1WUNyN0luIiwib3JnYW5pemF0aW9uSWQiOjczMzM5LCJ1c2VySWQiOjg1MjY1LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiIxNWRmZjljZC1hYTgwLTRlMmItYTAzYi1iMjUxYWMyNDA5YzMifX0.Dg8zCO6dJq9hDbVsuvzmlba2RWv6g0WBw0hkhpawX-w\";");
        }
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty("sasl.mechanism", "PLAIN");

        //Important Config for Safe Producer especially for old version (Kafka <= 2.8)
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all"); //Ensure data is properly replicated before ack is received
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); //Broker will not allow any duplicate data from producer
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE)); //Retry until it meet delivery.timeout.ms
        properties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000"); //Fail after retrying for 2 minutes
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); //Maximum 5 batches that run concurrently in single broker
        //min.insync.replica=2 //Ensure 2 brokers (1 leader and 1 in-sync replica) are available [THIS IS BROKER SETTING!!!]

        //Set high throughput producer config
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20"); //maximum wait is 20 ms before creating single batch
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); //Batch size is 32KB
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); //Compression for message text based i.e. Log Lines or JSON

        //Create the producer
        return new KafkaProducer<>(properties);
    }

    public static void main(String[] args) throws InterruptedException {
        //Create the producer
        Boolean localServer = true;
        KafkaProducer<String, String> producer = createKafkaProducer(localServer);

        String topic = "wikimedia.recentchange";
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventHandler eventHandler = new WikimediaChangesHandler(producer, topic);
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource = builder.build();

        //start the producer in another thread
        eventSource.start();

        //we produce for 10 mins and block the program until then
        TimeUnit.MINUTES.sleep(10);

        //Tell the producer to send all data and block until done -- synchronous
        producer.flush();
        log.info("Flush the producer at the end...");

        //Close the producer
        producer.close();
        log.info("Close the producer...");

    }

}
