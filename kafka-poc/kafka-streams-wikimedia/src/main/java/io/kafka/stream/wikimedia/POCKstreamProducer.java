package io.kafka.stream.wikimedia;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.Properties;
import java.util.Random;

public class POCKstreamProducer {

    private static final Logger log = LoggerFactory.getLogger(POCKstreamProducer.class.getSimpleName());
    private static final DecimalFormat df = new DecimalFormat("0.00");

    public static void main(String[] args) throws InterruptedException {
        log.info("I am a Kafka Stream Producer...");
        String bootStrapServer = "192.168.207.8:9092"; //connect to local server
        String topic = "POCKSTREAM";

        //Create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootStrapServer); //connect to kafka server from Conductor
        properties.setProperty("sasl.mechanism", "PLAIN");

        //Set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName()); //For testing purposes use round robbin partition

        //Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        Integer counter = 0;
        String[] products = {"IPhone", "Samsung", "Oppo", "Telkomsel", "Indosat"};
        String[] uom = {"pcs", "pcs", "pcs", "pkg", "pkg"};

        for (int header = 0; header < 1; header++) {
            for (int item = 0; item < products.length; item++) {
                String key = counter.toString();
//                Random r = new Random();
//                double randomValue = 10 + (100 - 10) * r.nextDouble();
                Double value = Double.valueOf(2 * counter);
                String jsonValue = "{\"id\": \"" + key + "\", \"UoM\": \"" + uom[item] + "\", \"ProductName\": \"" + products[item] + "\", \"Qty\": \""+ df.format(value) +"\"}";

                //Create a producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, jsonValue);

                //Send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        //executed everytime a record successfully sent or an exception is thrown
                        if (exception == null) {
                            log.info("key: " + key + " | " + "jsonValue: " + jsonValue + " | " + "Partition: " + metadata.partition());
                        } else {
                            log.info("Error while producing ", exception);
                        }
                    }
                });

                counter++;
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
