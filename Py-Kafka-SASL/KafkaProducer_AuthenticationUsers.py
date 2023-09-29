from confluent_kafka import Producer
from confluent_kafka import Consumer, KafkaException, KafkaError
from getmac import get_mac_address as gma
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.admin import AdminClient, NewTopic
from Module.Authentication import Authentication, auth_to_dict, dct_to_auth
from Module.Messages import UserMessages, um_to_dict, dct_to_um
from itertools import permutations
import datetime
import asyncio
import sys
import json

# Define Topics and Config
topic_user_authentication = 'upwork_user_authentication'
topic_otp_request = 'upwork_user_otp_request'
topic_user_authentication_result = 'upwork_user_authentication_result'
topic_otp_request_result = 'upwork_user_otp_request_result'
bootstrap_servers = 'localhost:39092,localhost:39093,localhost:39094'
kraft_config = {
    'bootstrap.servers': bootstrap_servers,
}
sr_config = {
    'url': 'http://localhost:8282'
}
admin_client = AdminClient({
    "bootstrap.servers": bootstrap_servers
})

# Define Kafka Serializer and Schema
schema_registry_client = SchemaRegistryClient(sr_config)
auth_schema = schema_registry_client.get_latest_version("upwork_user_auth")
auth_avro_serializer = AvroSerializer(schema_registry_client,
                                      auth_schema.schema.schema_str,
                                      auth_to_dict)
auth_avro_deserializer = AvroDeserializer(schema_registry_client,
                                          auth_schema.schema.schema_str,
                                          dct_to_auth)

message_schema = schema_registry_client.get_latest_version("upwork_user_messages")
message_avro_serializer = AvroSerializer(schema_registry_client,
                                         message_schema.schema.schema_str,
                                         um_to_dict)

# Get Current Time
dateFormat = '%Y%m%d'
timeFormat = '%H%M%S'
current_date = datetime.datetime.now().strftime(dateFormat)
current_time = datetime.datetime.now().strftime(timeFormat)

# Define Kafka Producer
producer = Producer(kraft_config)

# User creation request
print('Enter your Username:')
username_input = input()
auth = Authentication(username_input.lower(), 'dmitri.mahayana@gmail.com', gma())
# auth = Authentication("jack", 'jack.mahayana@gmail.com', gma(), current_time, "", "")
print("Initialize user:", auth.username, "email:", auth.email)


def define_kafka_consumer(username, group_name, topics):
    kafka_config = {
        'bootstrap.servers': bootstrap_servers,
        'auto.offset.reset': 'earliest',
        'group.id': 'upwork_' + username + '_' + group_name,
    }
    consumer = Consumer(kafka_config)
    consumer.subscribe(topics)

    status_auth = ""
    topic_name = ""

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            if status_auth != "":
                break
        else:
            if msg.topic() == topics[0]:
                auth_msg = auth_avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
                # print(auth_msg)
                if username == auth_msg['username']:
                    status_auth = auth_msg['status']
                    topic_name = msg.topic()

    print(f"Status Result {username}:", status_auth)
    consumer.close()
    return status_auth


# def consumer_kafka_result(username, consumer):


def delivery_report(err, event):
    if err is not None:
        print(f"Error ID: {event.key().decode('utf8')}: {err}")
    # else:
    #     print("Sending data to kafka Topic:", event.topic(), "Key:", event.key())


def send_producer(topic, key_value, object_data, producer, avro_serializer):
    producer.produce(topic=topic,
                     key=StringSerializer('utf_8')(key_value),
                     value=avro_serializer(object_data, SerializationContext(topic, MessageField.VALUE)),
                     on_delivery=delivery_report)


try:
    authenticated = False
    friends_topic = ""
    chat_room = False
    while True:
        if not authenticated:
            send_producer(topic_user_authentication, auth.username, auth, producer, auth_avro_serializer)
            producer.flush()

            print('Authenticating user...')
            status_auth = define_kafka_consumer(auth.username, 'auth', [topic_user_authentication_result])
            if status_auth == "required_otp":
                while True:
                    print('Please check the OTP in your email...')
                    print('Enter your OTP:')
                    otp_input = input()
                    auth.otp = otp_input  # Update OTP
                    send_producer(topic_otp_request, auth.username, auth, producer, auth_avro_serializer)
                    producer.flush()
                    status_otp = define_kafka_consumer(auth.username, 'otp', [topic_otp_request_result])
                    if status_otp == "otp_success":
                        authenticated = True
                        break
            elif status_auth == "valid":
                authenticated = True
            # print('Type your message and press enter')
            # msg_text = input()
        else:
            if not chat_room:
                print("Start Chatting...")
                print('Who do you want to talk? (split by comma):')
                friends_input = input()
                # friends_input = "jack,mac"
                friends_input = friends_input.replace(",", "-").replace(" ", "_").lower()
                friends_input = auth.username + "-" + friends_input
                people_list = friends_input.split("-")
                friends_topic = ""
                dict_topics = admin_client.list_topics().topics
                for key, value in dict_topics.items():
                    for perm in permutations(people_list):
                        perm_topic = '-'.join(map(str, perm))
                        perm_topic = 'upwork_user_' + perm_topic
                        if perm_topic == key:
                            friends_topic = key
                            print("Found existing topic:", friends_topic)
                            break
                if friends_topic == "":
                    topic_list = []
                    friends_topic = 'upwork_user_' + friends_input
                    topic_list.append(NewTopic(friends_topic, 1, 1))
                    admin_client.create_topics(topic_list)
                    print("Create a new topic:", friends_topic)
                chat_room = True
                print('Type your message and press enter')

            msg_text = input()
            current_date = datetime.datetime.now().strftime(dateFormat)
            current_time = datetime.datetime.now().strftime(timeFormat)
            msg_obj = UserMessages(auth.username, current_date + "_" + current_time, msg_text)
            send_producer(friends_topic, auth.username, msg_obj, producer, message_avro_serializer)
            producer.flush()
except KeyboardInterrupt:
    print("Close Producer...")
