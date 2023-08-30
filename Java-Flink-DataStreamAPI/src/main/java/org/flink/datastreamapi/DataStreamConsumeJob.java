package org.flink.datastreamapi;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataStreamConsumeJob {

    public static void main(String[] args) throws Exception {
        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String bootStrapServer = "localhost:39092,localhost:39093,localhost:39094";
        String topic = "flink_input";
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootStrapServer)
                .setTopics(topic)
                .setGroupId("my-flink-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Create a Kafka source stream
        DataStream<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // Process the Kafka data, you can apply transformations or other operations here
        kafkaStream.print();

        env.execute("Kafka Flink Consumer");
    }
}
