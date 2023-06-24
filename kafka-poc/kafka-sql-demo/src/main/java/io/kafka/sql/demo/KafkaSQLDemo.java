package io.kafka.sql.demo;

import io.confluent.ksql.api.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaSQLDemo {

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

//        //Async way to call push query and need to find a way to close this client async
//        String pushQuery = "SELECT * FROM riderLocations WHERE GEO_DISTANCE(latitude, longitude, 37.4133, -122.1162) <= 5 EMIT CHANGES;";
//        client.streamQuery(pushQuery, properties)
//                .thenAccept(streamedQueryResult2 -> {
//                    log.info("Push query has started. Query ID: " + streamedQueryResult2.queryID());
//                    RowSubscriber subscriber = new RowSubscriber();
//                    streamedQueryResult2.subscribe(subscriber);
//                }).exceptionally(e -> {
//                    log.info("Request failed: " + e);
//                    return null;
//                });


//        //terminate a push query
//        String pushQuery2 = "SELECT * FROM riderLocations WHERE GEO_DISTANCE(latitude, longitude, 37.4133, -122.1162) <= 5;";
//        StreamedQueryResult streamedResult = client.streamQuery(pushQuery2, properties).get();
//        String queryId = streamedResult.queryID();
//        client.terminatePushQuery(queryId).get();
//
//
//        //Need to disable push query before run this sync/pull query"
//        String pullQuery = "SELECT * FROM riderLocations WHERE GEO_DISTANCE(latitude, longitude, 37.4133, -122.1162) <= 5;";
//        List<Row> resultRows = client.executeQuery(pullQuery, properties).get();
//        log.info("Pull query has started");
//
//        for (int i =0; i<resultRows.stream().count(); i++){
//            System.out.println(resultRows.get(i).values());
//        }
//        log.info("Pull query has ended");
//
//
//        //Batch query pull
//        String pullQuery2 = "SELECT * FROM riderLocations WHERE GEO_DISTANCE(latitude, longitude, 37.4133, -122.1162) <= 5;";
//        BatchedQueryResult batchedQueryResult = client.executeQuery(pullQuery2, properties);
//        log.info("Batch Pull query has started");
//
//        List<Row> resultRows2 = batchedQueryResult.get();
//        log.info("Received results. Num rows: " + resultRows2.size());
//        for (Row row : resultRows2) {
//            System.out.println(row.values());
//        }
//        log.info("Batch Pull query has ended");


        //Insert single row
        KsqlObject row = new KsqlObject()
                .put("profileId", "12345678")
                .put("latitude", 37.1234)
                .put("longitude", -122.1234);
        client.insertInto("riderLocations", row).get();


        //Insert multiple rows
        InsertsPublisher insertsPublisher = new InsertsPublisher();
        AcksPublisher acksPublisher = client.streamInserts("riderLocations", insertsPublisher).get();

        for (long i = 0; i < 10; i++) {
            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd");
            LocalDateTime now = LocalDateTime.now();

            Random r = new Random();
            int low = 10;
            int high = 100;
            int result = r.nextInt(high-low) + low;

            KsqlObject row2 = new KsqlObject()
                    .put("profileId", dtf.format(now)+" "+i)
                    .put("latitude", result)
                    .put("longitude", result);
            insertsPublisher.accept(row2);

            try{
                Thread.sleep(10);
            } catch (InterruptedException error){
                error.printStackTrace();
            }
        }
        insertsPublisher.complete();
        acksPublisher.subscribe(new AcksSubscriber());

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
//        // Terminate any open connections and close the client
//        client.close();
    }
}