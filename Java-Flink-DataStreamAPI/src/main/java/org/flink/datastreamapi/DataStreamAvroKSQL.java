package org.flink.datastreamapi;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class DataStreamAvroKSQL {
    public static void main(String[] args) throws Exception {
        String bootStrapServer = "localhost:39092,localhost:39093,localhost:39094";
        String topic1 = "streaming.goapi.idx.stock.json";
        String topic2 = "streaming.goapi.idx.companies.json";
        String topic3 = "KSQLGROUPSTOCK"; //KSQLDB Table
        String topic4 = "KSQLGROUPCOMPANY"; //KSQLDB Table
        String schemaHost = "http://localhost:8282";
        String group = "my-flink-group-ksql";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //SQL TABLE MUST USE UPPERCASE COLUMN NAME
        tableEnv.executeSql("CREATE TABLE flink_ksql_groupstock (" +
                "  `event_time` TIMESTAMP(3) METADATA FROM 'timestamp', " +
                "  `STOCKID` STRING, " +
                "  `TICKER` STRING, " +
                "  `DATE` STRING, " +
                "  `OPEN` DOUBLE, " +
                "  `HIGH` DOUBLE, " +
                "  `LOW` DOUBLE, " +
                "  `CLOSE` DOUBLE, " +
                "  `VOLUME` BIGINT " +
                ") WITH (" +
                "  'connector' = 'kafka', " +
                "  'topic' = '" + topic3 + "', " +
                "  'properties.bootstrap.servers' = '" + bootStrapServer + "', " +
                "  'properties.group.id' = '" + group + "', " +
                "  'scan.startup.mode' = 'earliest-offset', " +
                "  'value.format' = 'avro-confluent', " +
                "  'value.avro-confluent.url' = '" + schemaHost + "' " +
                ")");

        //SQL TABLE MUST USE UPPERCASE COLUMN NAME
        tableEnv.executeSql("CREATE TABLE flink_ksql_groupcompany (" +
                "  `event_time` TIMESTAMP(3) METADATA FROM 'timestamp', " +
                "  `COMPANYID` STRING, " +
                "  `TICKER` STRING, " +
                "  `NAME` STRING, " +
                "  `LOGO` STRING " +
                ") WITH (" +
                "  'connector' = 'kafka', " +
                "  'topic' = '" + topic4 + "', " +
                "  'properties.bootstrap.servers' = '" + bootStrapServer + "', " +
                "  'properties.group.id' = '" + group + "', " +
                "  'scan.startup.mode' = 'earliest-offset', " +
                "  'value.format' = 'avro-confluent', " +
                "  'value.avro-confluent.url' = '" + schemaHost + "' " +
                ")");

        tableEnv.executeSql("CREATE TABLE flink_mongodb_stock (" +
                "  `id` STRING, " +
                "  `ticker` STRING, " +
                "  `date` STRING, " +
                "  `open` DOUBLE, " +
                "  `high` DOUBLE, " +
                "  `low` DOUBLE, " +
                "  `close` DOUBLE " +
//                "  `volume` STRING " +
                ") WITH (" +
                "   'connector' = 'mongodb'," +
                "   'uri' = 'mongodb://localhost:27017'," +
                "   'database' = 'kafka'," +
                "   'collection' = 'ksql-stock-stream'" +
                ");");

        tableEnv.executeSql("CREATE TABLE flink_kafka_topic_stock (" +
                "  `id` STRING, " +
                "  `ticker` STRING, " +
                "  `name` STRING, " +
                "  `logo` STRING " +
                ") WITH (" +
                "  'connector' = 'kafka', " +
                "  'topic' = '" + topic2 + "', " +
                "  'properties.bootstrap.servers' = '" + bootStrapServer + "', " +
                "  'properties.group.id' = '" + group + "', " +
                "  'scan.startup.mode' = 'earliest-offset', " +
                "  'value.format' = 'avro-confluent', " +
                "  'value.avro-confluent.url' = '" + schemaHost + "' " +
                ")");

        tableEnv.executeSql("CREATE TABLE flink_kafka_topic_company (" +
                "  `id` STRING, " +
                "  `ticker` STRING, " +
                "  `date` STRING, " +
                "  `open` DOUBLE, " +
                "  `high` DOUBLE, " +
                "  `low` DOUBLE, " +
                "  `close` DOUBLE, " +
                "  `volume` BIGINT " +
                ") WITH (" +
                "  'connector' = 'kafka', " +
                "  'topic' = '" + topic1 + "', " +
                "  'properties.bootstrap.servers' = '" + bootStrapServer + "', " +
                "  'properties.group.id' = '" + group + "', " +
                "  'scan.startup.mode' = 'earliest-offset', " +
                "  'value.format' = 'avro-confluent', " +
                "  'value.avro-confluent.url' = '" + schemaHost + "' " +
                ")");

        // Define a query using the Kafka source
        Table resultTable1 = tableEnv.sqlQuery("SELECT * FROM flink_ksql_groupstock");
        Table resultTable2 = tableEnv.sqlQuery("SELECT * FROM flink_ksql_groupcompany");
        Table resultTable3 = tableEnv.sqlQuery("SELECT * FROM flink_mongodb_stock LIMIT 5");
        Table resultTable4 = tableEnv.sqlQuery("SELECT * FROM flink_kafka_topic_stock LIMIT 5");
        Table resultTable5 = tableEnv.sqlQuery("SELECT * FROM flink_kafka_topic_company LIMIT 5");

        DataStream<Row> resultStream1 = tableEnv.toAppendStream(resultTable1, Row.class);
        DataStream<Row> resultStream2 = tableEnv.toAppendStream(resultTable2, Row.class);
//        DataStream<Row> resultStream3 = tableEnv.toAppendStream(resultTable3, Row.class);
//        DataStream<Row> resultStream4 = tableEnv.toAppendStream(resultTable4, Row.class);
//        DataStream<Row> resultStream5 = tableEnv.toAppendStream(resultTable5, Row.class);

        // Print the results to stdout for testing
        resultStream1.print();
        resultStream2.print();
//        resultStream3.print();
//        resultStream4.print();
//        resultStream5.print();

        // Execute the Flink job
        env.execute("Kafka Flink SQL Consumer");
    }
}
