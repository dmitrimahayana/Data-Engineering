package io.kafka.sql.demo;

import io.confluent.ksql.api.client.Row;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RowSubscriber implements Subscriber<Row> {

    private static final Logger log = LoggerFactory.getLogger(KafkaSQLDemo.class.getSimpleName());
    private Subscription subscription;

    public RowSubscriber() {
    }

    @Override
    public synchronized void onSubscribe(Subscription subscription) {
        log.info("Subscriber is subscribed.");
        this.subscription = subscription;

        // Request the first row
        subscription.request(1);
    }

    @Override
    public synchronized void onNext(Row row) {
//        log.info("Received a row!");
        log.info("Row: " + row.values());

        // Request the next row
        subscription.request(1);
    }

    @Override
    public synchronized void onError(Throwable t) {
        System.out.println("Received an error: " + t);
    }

    @Override
    public synchronized void onComplete() {
        System.out.println("Query has ended.");
    }
}
