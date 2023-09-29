import datetime

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
auth_avro_serializer = AvroSerializer(schema_registry_client,
                                      auth_schema.schema.schema_str,
                                      auth_to_dict)


def define_kafka_producer():
    kraft_config = {
        'bootstrap.servers': bootstrap_servers,
    }
    producer = Producer(kraft_config)
    return producer


def delivery_report(err, event):
    if err is not None:
        print(f"Error ID: {event.key().decode('utf8')}: {err}")
    else:
        print(f"Success: {event.key().decode('utf8')}")


def send_producer(topic, key_value, object_data, producer, avro_serializer):
    producer.produce(topic=topic,
                     key=StringSerializer('utf_8')(key_value),
                     value=avro_serializer(object_data, SerializationContext(topic, MessageField.VALUE)),
                     on_delivery=delivery_report)
    producer.flush()


i = 0
while i <= 5:
    i = i + 1
    dateFormat = '%Y%m%d'
    timeFormat = '%H%M%S'
    current_date = datetime.datetime.now().strftime(dateFormat)
    current_time = datetime.datetime.now().strftime(timeFormat)
    producer = define_kafka_producer()
    new_auth_otp = Authentication("Test123", "Test@mail.com", current_date, current_time, "", "statusss")
    send_producer(topic_user_authentication_result, "Test123", new_auth_otp, producer, auth_avro_serializer)
