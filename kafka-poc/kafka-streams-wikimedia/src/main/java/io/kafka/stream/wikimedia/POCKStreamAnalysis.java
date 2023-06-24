package io.kafka.stream.wikimedia;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.KeyPair;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;

public class POCKStreamAnalysis {

    private static final Logger log = LoggerFactory.getLogger(POCKStreamAnalysis.class.getSimpleName());
    private static Properties properties;

    private static Properties createProperties(){
        properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "poc-stream-application");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.207.8:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        return properties;
    }

    public static void productCount(KStream<String, String> inputStream){
        String DUMMY_MATERIALIZED = "product-count-store";
        String TOPIC_OUTPUT = "product.count";
        ObjectMapper OBJECT_MAPPER = new ObjectMapper();

        inputStream
            .selectKey((key, valueJson) -> {
                try {
                    final JsonNode jsonNode = OBJECT_MAPPER.readTree(valueJson);
                    return jsonNode.get("ProductName").asText();
                } catch (IOException e) {
                    return "parse-error " + e.getMessage();
                }
            })
            .groupByKey()
            .count(Materialized.as(DUMMY_MATERIALIZED))
            .toStream()
            .mapValues((key, value) -> {
                final Map<String, Long> kvMap = Map.of(String.valueOf(key), value);
                log.info("key:"+key+" value:"+value);
                try {
                    return OBJECT_MAPPER.writeValueAsString(kvMap);
                } catch (JsonProcessingException e) {
                    return null;
                }
            })
            .to(TOPIC_OUTPUT);

    }

    public static void productCountWindowed(KStream<String, String> inputStream){
        String DUMMY_MATERIALIZED = "product-count-store2";
        String TOPIC_OUTPUT = "product.count2";
        ObjectMapper OBJECT_MAPPER = new ObjectMapper();

        final TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1L));

        inputStream
            .selectKey((key, valueJson) -> {
                try {
                    final JsonNode jsonNode = OBJECT_MAPPER.readTree(valueJson);
                    return jsonNode.get("ProductName").asText();
                } catch (IOException e) {
                    return "parse-error " + e.getMessage();
                }
            })
            .groupByKey()
            .windowedBy(timeWindows)
            .count(Materialized.as(DUMMY_MATERIALIZED))
            .toStream()
            .mapValues((key, value) -> {
                final Map<String, Object> kvMap = Map.of(
                        "ProductName", key.key(),
                        "count", value
                );
                log.info("key:"+key+" key.key:"+key.key()+" value:"+value);
                try {
                    return OBJECT_MAPPER.writeValueAsString(kvMap);
                } catch (JsonProcessingException e) {
                    return null;
                }
            })
            .to(TOPIC_OUTPUT, Produced.with(
                    WindowedSerdes.timeWindowedSerdeFrom(String.class, timeWindows.size()),
                    Serdes.String()
            ));

    }

    public static void productSUMWindowed(KStream<String, String> inputStream){
        String DUMMY_MATERIALIZED = "product-sum-store3";
        String TOPIC_OUTPUT = "product.sum3";
        ObjectMapper OBJECT_MAPPER = new ObjectMapper();

        final TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1L));

        inputStream
                //Use selectKey-groupByKey or directly use groupBy to filter the column
                .selectKey((key, valueJson) -> {
                    try {
                        final JsonNode jsonNode = OBJECT_MAPPER.readTree(valueJson);
                        return jsonNode.get("ProductName").asText();
                    } catch (IOException e) {
                        return "parse-error " + e.getMessage();
                    }
                })
                .groupByKey()
//                .groupBy((key, value) -> {
//                    try {
//                        final JsonNode jsonNode = OBJECT_MAPPER.readTree(value);
//                        return jsonNode.get("ProductName").asText()+"-"+jsonNode.get("UoM").asText();
//                    } catch (IOException e) {
//                        return "parse-error " + e.getMessage();
//                    }
//                })
//                .windowedBy(timeWindows)
//                .count(Materialized.as(DUMMY_MATERIALIZED))
                .aggregate(
                        () -> "0.0",
                        (key, value, aggregate) -> {
                            try {
                                final JsonNode jsonNode = OBJECT_MAPPER.readTree(value);
                                Double result = Double.parseDouble(String.valueOf(aggregate)) + jsonNode.get("Qty").asDouble();
                                log.info("value:"+value+" aggregate:"+aggregate);
                                aggregate = String.valueOf(result);
                                return aggregate;
                            } catch (JsonProcessingException e) {
                                throw new RuntimeException(e);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        },
                        Materialized.as(DUMMY_MATERIALIZED))
                .toStream()
                .peek((key, value) -> log.info("key:"+key+" value:"+value))
                .mapValues((key, value) -> {
                    final Map<String, Object> kvMap = Map.of(
                            "ProductName", key,
                            "SUM", value
                    );
                    try {
                        return OBJECT_MAPPER.writeValueAsString(kvMap);
                    } catch (JsonProcessingException e) {
                        return null;
                    }
                })
                .to(TOPIC_OUTPUT, Produced.with(
//                        WindowedSerdes.timeWindowedSerdeFrom(String.class, timeWindows.size()),
                        Serdes.String(),
                        Serdes.String()));

    }

    public static void productDeDuplicate(KStream<String, String> inputStream){
        String DUMMY_MATERIALIZED = "product-deduplication";
        String TOPIC_OUTPUT = "product.deduplication";
        ObjectMapper OBJECT_MAPPER = new ObjectMapper();

        inputStream
                .groupByKey()
                .aggregate(
                        () -> "",
                        (key, value, aggregate) -> {
                            aggregate = value;
                            return aggregate;
                        },
                        Materialized.as(DUMMY_MATERIALIZED)
                )
                .toStream()
                .peek((key, value) -> log.info("key:"+key+" value:"+value))
                .mapValues((key, value) -> {
                    final Map<String, Object> kvMap = Map.of(
                            "ProductName", key,
                            "SUM", value
                    );
                    try {
                        return OBJECT_MAPPER.writeValueAsString(kvMap);
                    } catch (JsonProcessingException e) {
                        return null;
                    }
                })
                .to(TOPIC_OUTPUT, Produced.with(
                        Serdes.String(),
                        Serdes.String()));
    }

    public static void main(String[] args) {
        String inputTopic = "POCKSTREAM";

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> inputStream = builder.stream(inputTopic);
//        productCount(inputStream);
//        productCountWindowed(inputStream);
//        productSUMWindowed(inputStream);
        productDeDuplicate(inputStream);

        final Topology appTopology = builder.build();
        log.info("Topology: {}", appTopology.describe());
        KafkaStreams streams = new KafkaStreams(appTopology, createProperties());

        //Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        //Start stream
        streams.start();

    }
}
