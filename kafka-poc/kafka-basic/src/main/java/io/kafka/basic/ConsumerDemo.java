package io.kafka.basic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger Log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {
        Log.info("I am a Kafka Consumer...");

        String groupId = "my-java-application";
        String topic = "demo_java";

        //Create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092"); //connect to kafka server from Conductor
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"6W1Ja4rpKmarggy5YCr7In\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiI2VzFKYTRycEttYXJnZ3k1WUNyN0luIiwib3JnYW5pemF0aW9uSWQiOjczMzM5LCJ1c2VySWQiOjg1MjY1LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiIxNWRmZjljZC1hYTgwLTRlMmItYTAzYi1iMjUxYWMyNDA5YzMifX0.Dg8zCO6dJq9hDbVsuvzmlba2RWv6g0WBw0hkhpawX-w\";");
        properties.setProperty("sasl.mechanism", "PLAIN");

        //create consumer config
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest"); //chose none/earliest/latest

        //create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //subscribe to a topic
        consumer.subscribe(Arrays.asList(topic)); //can mention multiple topic in here

        //poll for data
        while (true){
            Log.info("Polling...");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000)); //call every 1 sec when there is no data

            for(ConsumerRecord<String, String> record: records){
                Log.info("Key: "+record.key()+" Value: "+record.value());
                Log.info("Partition: "+record.partition()+" Offset: "+record.offset());
            }
        }
    }
}
