package io.kafka.basic;
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

public class ConsumerDemoWithShutdown {

    private static final Logger Log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());

    private static void delayRun(Integer delayTime){
        try{
            Log.info("Delay "+delayTime+" ms");
            Thread.sleep(delayTime);
        } catch (InterruptedException error){
            error.printStackTrace();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        Log.info("I am a Kafka Consumer...");

        String groupId = "my-new-java-application";
        String topic = "demo_java";
        String bootStrapServer = "192.168.207.8:9092"; //connect to local server

        //Create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootStrapServer); //connect to kafka server from Conductor
        properties.setProperty("sasl.mechanism", "PLAIN");
//        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092"); //connect to kafka server from Conductor
//        properties.setProperty("security.protocol", "SASL_SSL");
//        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"6W1Ja4rpKmarggy5YCr7In\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiI2VzFKYTRycEttYXJnZ3k1WUNyN0luIiwib3JnYW5pemF0aW9uSWQiOjczMzM5LCJ1c2VySWQiOjg1MjY1LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiIxNWRmZjljZC1hYTgwLTRlMmItYTAzYi1iMjUxYWMyNDA5YzMifX0.Dg8zCO6dJq9hDbVsuvzmlba2RWv6g0WBw0hkhpawX-w\";");
//        properties.setProperty("sasl.mechanism", "PLAIN");

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
            Log.info("Detected a shutdown, let's exit by calling consumer consumer.wakeup()...");
            consumer.wakeup();
            Log.info("Consumer has sent wakeup signal...");
            //join the main thread to allow the execution of the code in the main thread
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            }
        });


        try {
            //subscribe to a topic
            consumer.subscribe(Arrays.asList(topic)); //can mention multiple topic in here

            //poll for data
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000)); //call every 1 sec when there is no data

                for (ConsumerRecord<String, String> record : records) {
                    Log.info("Key: " + record.key() + " Value: " + record.value());
                    Log.info("Partition: " + record.partition() + " Offset: " + record.offset());
                }
            }
        }catch (WakeupException e) {
            Log.info("Consumer is starting to shut down...");
        }catch (Exception e){
            Log.info("Unexpected exception in the consumer", e);
        }finally {
            Log.info("starting to close now...");
            consumer.close();
            Log.info("Consumer was closed gracefully");
        }
    }
}
