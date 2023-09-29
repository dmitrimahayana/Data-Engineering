from confluent_kafka import Consumer, KafkaException, KafkaError
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField

from Module.Authentication import Authentication, auth_to_dict, dct_to_auth
from pymongo import MongoClient
from SendEmail import send_email
import random
import sys
import json

# Define Topics and Config
topic_user_authentication_result = 'upwork_user_authentication_result'
bootstrap_servers = 'localhost:39092,localhost:39093,localhost:39094'

sr_config = {
    'url': 'http://localhost:8282'
}

# Define Kafka Deserializer and Schema
schema_registry_client = SchemaRegistryClient(sr_config)
auth_schema = schema_registry_client.get_latest_version("upwork_user_auth")
auth_avro_deserializer = AvroDeserializer(schema_registry_client,
                                          auth_schema.schema.schema_str,
                                          dct_to_auth)

auth = Authentication('vara', 'dmitri.mahayana@gmail.com', "", "", "", "")
print(auth.username, auth.email)


kafka_config = {
    'bootstrap.servers': bootstrap_servers,
    'auto.offset.reset': 'earliest',
    'group.id': 'upwork_' + auth.username + '_auth_result',
}
consumer = Consumer(kafka_config)
consumer.subscribe([topic_user_authentication_result])

try:
    for msg in consumer.consume():
        auth_msg = auth_avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
        status_auth = auth_msg['status']
        topic_name = msg.topic()
        print(auth_msg)
    # while True:
    #     msg = consumer.poll(timeout=1.0)
    #     if msg is None: continue
    #
    #     if msg.error():
    #         if msg.error().code() == KafkaError._PARTITION_EOF:
    #             sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
    #                              (msg.topic(), msg.partition(), msg.offset()))
    #         elif msg.error():
    #             raise KafkaException(msg.error())
    #     else:
    #         if msg.topic() == topic_user_authentication_result:
    #             auth_msg = auth_avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
    #             print("Auth Result:", auth_msg)
                # consumer.close()
                # break

except KeyboardInterrupt:
    print("Close Consumer...")
    consumer.close()