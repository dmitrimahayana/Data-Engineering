package org.flink.datastreamapi;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.avro.Schema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.avro.Stock;
import org.avro.Company;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.avro.generic.GenericRecord;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DataStreamAvroConsumeJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String bootStrapServer = "localhost:39092,localhost:39093,localhost:39094";
        String topic1 = "streaming.goapi.idx.stock.json";
        String topic2 = "streaming.goapi.idx.companies.json";
        String schemaHost = "http://localhost:8282";
        String group = "my-flink-group";

        Properties kafkaConsumerConfig = new Properties();
        kafkaConsumerConfig.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        kafkaConsumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group);
        kafkaConsumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        String SCHEMA_STOCK_PATH = "src/main/avro/avro-stock.avsc";
        String avroStockSchema = new String(Files.readAllBytes(Paths.get(SCHEMA_STOCK_PATH)));
        Schema schemaStock = new Schema.Parser().parse(avroStockSchema);
        DataStreamSource<GenericRecord> kafkaStream1 =
                env.addSource(
                        new FlinkKafkaConsumer<>(
                                topic1,
                                ConfluentRegistryAvroDeserializationSchema.forGeneric(schemaStock, schemaHost),
                                kafkaConsumerConfig)
                                .setStartFromEarliest());
        kafkaStream1.print();

        DataStreamSource<Company> kafkaStream2 =
                env.addSource(
                        new FlinkKafkaConsumer<>(
                                topic2,
                                ConfluentRegistryAvroDeserializationSchema.forSpecific(Company.class, schemaHost),
                                kafkaConsumerConfig)
                                .setStartFromEarliest());
        kafkaStream2.print();

        env.execute("Kafka Flink Consumer");
    }

    // Avro Deserialization Schema
    private static class AvroDeserializationSchema implements KeyedDeserializationSchema<GenericRecord> {
        private static final long serialVersionUID = 1L;

        @Override
        public GenericRecord deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) {
//            KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
//            // Configure the deserializer with the appropriate schema registry URL
//            // deserializer.configure(schemaRegistryConfig, isKey);
//            return (GenericRecord) deserializer.deserialize(topic, message);

            KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
            Map<String, String> config = new HashMap<>();
            config.put("schema.registry.url", "http://localhost:8282");
            deserializer.configure(config, false);
            return (GenericRecord) deserializer.deserialize(topic, message);

//            KafkaAvroDeserializer valueDeserializer = new KafkaAvroDeserializer();
//            Map<String, Object> valueDeserializerConfig = new HashMap<>();
//            valueDeserializerConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8282");
//            valueDeserializer.configure(valueDeserializerConfig, false);
//            return (GenericRecord) valueDeserializer.deserialize(topic, message);
        }

        @Override
        public boolean isEndOfStream(GenericRecord nextElement) {
            return false;
        }

        @Override
        public TypeInformation<GenericRecord> getProducedType() {
            // Adjust this as per your needs
            return TypeInformation.of(GenericRecord.class);
        }
    }
}
