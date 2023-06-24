package io.kafka.basic;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class ProducerDemo {

    private static final Logger Log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {
        Log.info("I am a Kafka Producer...");

        //String bootStrapServer = "cluster.playground.cdkt.io:9092"; //connect to kafka server Conductor
        String bootStrapServer = "192.168.207.8:9092"; //connect to local server
        //String topic = "demo_java";
        String topic = "wikimedia.recentchange";

        //Create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootStrapServer);
        //properties.setProperty("security.protocol", "SASL_SSL"); //connect to local server
        //properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"6W1Ja4rpKmarggy5YCr7In\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiI2VzFKYTRycEttYXJnZ3k1WUNyN0luIiwib3JnYW5pemF0aW9uSWQiOjczMzM5LCJ1c2VySWQiOjg1MjY1LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiIxNWRmZjljZC1hYTgwLTRlMmItYTAzYi1iMjUxYWMyNDA5YzMifX0.Dg8zCO6dJq9hDbVsuvzmlba2RWv6g0WBw0hkhpawX-w\";"); //connect to local server
        properties.setProperty("sasl.mechanism", "PLAIN");

        //Set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("batch.size", "400"); //For testing purposes 400 bytes
        //properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName()); //For testing purposes use round robbin partition

        //Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for(int header=0; header<10; header++) {
            for (int count = 0; count < 30; count++) {
                //Create a producer record
                DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
                LocalDateTime now = LocalDateTime.now();
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, "Dmitri_" + count + "_" + dtf.format(now));

                //Send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        //executed everytime a record successfully sent or an exception is thrown
                        if (exception == null) {
                            Log.info("Received new metadata \n" +
                                    //"Topic: " + metadata.topic() + "\n" +
                                    "Partition: " + metadata.partition() + "\n" +
                                    "Offset: " + metadata.offset() + "\n" +
                                    "Timestamp: " + metadata.timestamp() + "\n"
                            );
                        } else {
                            Log.info("Error while producing ", exception);
                        }
                    }
                });
            }

            try{
                Thread.sleep(500);
            } catch (InterruptedException error){
                error.printStackTrace();
            }
        }

        //Tell the producer to send all data and block until done -- synchronous
        producer.flush();

        //close the producer
        producer.close();
    }
}
