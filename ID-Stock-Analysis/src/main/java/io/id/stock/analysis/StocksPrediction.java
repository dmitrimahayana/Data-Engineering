package io.id.stock.analysis;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

public class StocksPrediction {

    private static final Logger log = LoggerFactory.getLogger(StocksPrediction.class.getSimpleName());

    public static void main(String[] args) throws TimeoutException, InterruptedException {
        String topic = "group.stock"; //Must check if topic has been created by KSQL or KStream
        String bootStrapServer = "192.168.207.8:9092"; //connect to local server

        // Create SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("StocksPredictionAppJAVA")
//                .master("local[2]")
                .master("spark://192.168.207.8:7777")
//                .config("spark.executor.memory", "4g")
//                .config("spark.sql.files.maxPartitionBytes", "12345678")
//                .config("spark.sql.files.openCostInBytes", "12345678")
//                .config("spark.sql.broadcastTimeout", "1000")
//                .config("spark.sql.autoBroadcastJoinThreshold", "100485760")
//                .config("spark.sql.shuffle.partitions", "1000")
                .getOrCreate();

        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootStrapServer)
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .load();
        df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

        log.info("Spark Version: "+spark.version());

        df.printSchema();

//        StreamingQuery query = df.writeStream().format("console").start();
//        Thread.sleep(5000);
//        query.stop();

        spark.close();
    }
}
