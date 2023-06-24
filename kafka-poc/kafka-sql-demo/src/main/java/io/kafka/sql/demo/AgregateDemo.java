package io.kafka.sql.demo;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.StreamedQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class AgregateDemo {

    public static String KSQLDB_SERVER_HOST = "192.168.207.8";
    public static int KSQLDB_SERVER_PORT = 8088;
    private static final Logger log = LoggerFactory.getLogger(KafkaSQLDemo.class.getSimpleName());

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ClientOptions options = ClientOptions.create()
                .setHost(KSQLDB_SERVER_HOST)
                .setPort(KSQLDB_SERVER_PORT);

        Client client = Client.create(options);

        Map<String, Object> properties = Collections.singletonMap(
                "auto.offset.reset", "earliest"
        );

        //Async way to call push query and need to find a way to close this client async
        String pushQuery = "SELECT profileId, count(*) FROM riderLocations" +
                " GROUP BY profileId EMIT CHANGES;";
        client.streamQuery(pushQuery, properties)
                .thenAccept(localStreamQuery -> {
                    log.info("Push query has started. Query ID: " + localStreamQuery.queryID());
                    RowSubscriber subscriber = new RowSubscriber();
                    localStreamQuery.subscribe(subscriber);
                }).exceptionally(e -> {
                    log.info("Request failed: " + e);
                    return null;
                });

        // Add shutdown hook to stop the Kafka client threads.
        // You can optionally provide a timeout to `close`.
        Runtime.getRuntime().addShutdownHook(new Thread(client::close));

        // Add shutdown hook to stop the Kafka client threads.
        // You can optionally provide a timeout to `close`.
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                log.info("close ksql client");
                client.close();
            }
        }));

//        //Using sync query
//        String pullQuery3 = "SELECT profileId, count(*) FROM riderLocations" +
//                " GROUP BY profileId EMIT CHANGES;";
//        StreamedQueryResult streamedQueryResult = client.streamQuery(pullQuery3, properties).get();
//
//        while(true) {
//            // Block until a new row is available
//            Row row = streamedQueryResult.poll();
//            if (row != null) {
//                log.info(row.values().toString());
//            } else {
//                log.info("Query has ended.");
//            }
//        }
    }
}
