package io.kafka.stream.wikimedia;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class POCKStreamConsumer {

    private static final Logger log = LoggerFactory.getLogger(POCKStreamConsumer.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {
        log.info("I am a Kafka Stream Consumer...");

        String groupId = "console-poc-kafka-stream-consumer";
        String topic = "POCKSTREAM";
        String bootStrapServer = "192.168.207.8:9092"; //connect to local server

        //Create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootStrapServer); //connect to kafka server from Conductor
        properties.setProperty("sasl.mechanism", "PLAIN");

        //create consumer config
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest"); //chose none/earliest/latest

        //create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

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

        try {
            consumer.subscribe(Arrays.asList(topic)); //can mention multiple topic in here
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000)); //call every 1 sec when there is no data
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + " | Value: " + record.value());
                }
            }
        }catch (WakeupException e) {
            log.info("Consumer is starting to shut down...");
        }catch (Exception e){
            log.info("Unexpected exception in the consumer", e);
        }finally {
            log.info("starting to close now...");
            consumer.close();
            log.info("Consumer was closed gracefully");
        }
    }
}
