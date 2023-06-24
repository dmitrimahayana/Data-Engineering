package io.kafka.producer.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class WikimediaChangesHandler implements EventHandler {

    KafkaProducer<String, String> producer;
    String topic;
    private static final Logger log = LoggerFactory.getLogger(WikimediaChangesHandler.class.getSimpleName());

    public WikimediaChangesHandler(KafkaProducer<String, String> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    @Override
    public void onOpen() throws Exception {
        //when stream is open
        log.info("Stream Opening...");
    }

    @Override
    public void onClosed() throws Exception {
        //when stream is close
        log.info("Stream Closing...");
        producer.flush();
        log.info("Flush the producer...");
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {
        //when stream has received a message coming from HTTP Stream data
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        log.info(dtf.format(now) + "-" + messageEvent.getData());
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, messageEvent.getData());
        producer.send(producerRecord);
    }

    @Override
    public void onComment(String comment) throws Exception {
        //nothing to do
    }

    @Override
    public void onError(Throwable t) {
        log.info("Error in Stream Reading: ",t);
    }
}
