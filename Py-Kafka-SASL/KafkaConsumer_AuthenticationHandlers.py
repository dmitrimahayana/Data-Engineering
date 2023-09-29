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
topic_user_authentication = 'upwork_user_authentication'
topic_otp_request = 'upwork_user_otp_request'
topic_user_authentication_result = 'upwork_user_authentication_result'
topic_otp_request_result = 'upwork_user_otp_request_result'
bootstrap_servers = 'localhost:39092,localhost:39093,localhost:39094'
kafka_config = {
    'bootstrap.servers': bootstrap_servers,
    'auto.offset.reset': 'earliest',
    'group.id': 'upwork_otp_handlers_group',
}
sr_config = {
    'url': 'http://localhost:8282'
}

# Define Kafka Deserializer and Schema
schema_registry_client = SchemaRegistryClient(sr_config)
auth_schema = schema_registry_client.get_latest_version("upwork_user_auth")
auth_avro_deserializer = AvroDeserializer(schema_registry_client,
                                          auth_schema.schema.schema_str,
                                          dct_to_auth)
auth_avro_serializer = AvroSerializer(schema_registry_client,
                                      auth_schema.schema.schema_str,
                                      auth_to_dict)

# Define MongoDB
mongoClient = MongoClient('mongodb://localhost:27017')
mongoDB = mongoClient["Upwork_User"]  # Get Database Name


def generate_otp():
    otp = random.randint(101010, 505050)
    return str(otp)


def define_kafka_producer():
    kraft_config = {
        'bootstrap.servers': bootstrap_servers,
    }
    producer = Producer(kraft_config)
    return producer


def delivery_report(err, event):
    if err is not None:
        print(f"Error ID: {event.key().decode('utf8')}: {err}")


def send_producer(topic, key_value, object_data, producer, avro_serializer):
    producer.produce(topic=topic,
                     key=StringSerializer('utf_8')(key_value),
                     value=avro_serializer(object_data, SerializationContext(topic, MessageField.VALUE)),
                     on_delivery=delivery_report)
    producer.flush()


def send_otp(msg, producer):
    current_otp = generate_otp()
    username = msg['username']
    email = msg['email']
    machine_id = msg['machine_id']
    session_id = msg['session_id']

    body = "Please use this OTP: " + current_otp
    subject = "Upwork User OTP"
    # send_email([email], body, subject)

    query_user = {"username": username}
    user_mongo = mongoDB['user'].find_one(query_user)
    if user_mongo is not None:
        if machine_id != user_mongo['machine_id'] or session_id != user_mongo['session_id']:
            print("Existing User ID and Waiting OTP:", username, "Email:", email, "machine_id:", machine_id, msg['status'], current_otp)
            msg['status'] = "required_otp"
            msg['otp'] = current_otp  # Replace empty OTP with random OTP
            json_str = json.dumps(msg, indent=4)
            json_obj_user = json.loads(json_str)
            update_user = mongoDB['user'].replace_one(query_user, json_obj_user)

            new_auth_otp = Authentication(username, email, machine_id, session_id, msg['otp'], msg['status'])
            send_producer(topic_user_authentication_result, username, new_auth_otp, producer, auth_avro_serializer)
            print("Update DB Count:", str(update_user.matched_count))
        else:
            print("Existing User ID Session is still valid:", username, "Email:", email, "machine_id:", machine_id, msg['status'], msg['otp'])
            msg['status'] = "valid"
            msg['otp'] = ""
            json_str = json.dumps(msg, indent=4)
            json_obj_user = json.loads(json_str)
            update_user = mongoDB['user'].replace_one(query_user, json_obj_user)

            new_auth_otp = Authentication(username, email, machine_id, session_id, msg['otp'], msg['status'])
            send_producer(topic_user_authentication_result, username, new_auth_otp, producer, auth_avro_serializer)
            print("Update DB Count:", str(update_user.matched_count))
    else:
        print("New User ID and Waiting OTP:", username, "Email:", email, "machine_id:", machine_id, "OTP:", current_otp)
        msg['status'] = "required_otp"
        msg['otp'] = current_otp  # Replace empty OTP with random OTP
        json_str = json.dumps(msg, indent=4)
        json_obj_user = json.loads(json_str)
        insert_user = mongoDB['user'].insert_one(json_obj_user)

        new_auth_otp = Authentication(username, email, machine_id, session_id, msg['otp'], msg['status'])
        send_producer(topic_user_authentication_result, username, new_auth_otp, producer, auth_avro_serializer)
        print("Insert DB ID:", str(insert_user.inserted_id))
    print("-----------------------------------------------------------------------------------------------------------")
    pass


def validate_otp(msg, producer):
    username = msg['username']
    email = msg['email']
    machine_id = msg['machine_id']
    session_id = msg['session_id']
    current_otp = msg['otp']
    query_user = {"username": username}
    user_mongo = mongoDB['user'].find_one(query_user)
    if user_mongo is not None:
        if user_mongo['otp'] == current_otp:
            print("OTP DB:",user_mongo['otp'], "OTP Request:", current_otp)
            print("OTP", username, "is match")
            msg['status'] = "otp_success"
            msg['otp'] = user_mongo['otp']
            json_str = json.dumps(msg, indent=4)
            json_obj_user = json.loads(json_str)

            update_user = mongoDB['user'].replace_one(query_user, json_obj_user)
            print("Update DB Count:", str(update_user.matched_count))
            new_auth_otp = Authentication(username, email, machine_id, session_id, user_mongo['otp'], msg['status'])
            send_producer(topic_otp_request_result, username, new_auth_otp, producer, auth_avro_serializer)
            # send_email([email],
            #            "Successful OTP from Username: " + username + " Email: "
            #            + email, "Upwork User Registration Success")
            print("---------------------------------------------------------------------------------------------------")
        else:
            print("OTP", username, "is not match")
            print("OTP DB:",user_mongo['otp'], "OTP Request:", current_otp)
            msg['status'] = "otp_failed"
            msg['otp'] = user_mongo['otp']
            json_str = json.dumps(msg, indent=4)
            json_obj_user = json.loads(json_str)

            update_user = mongoDB['user'].replace_one(query_user, json_obj_user)
            print("Update DB Count:", str(update_user.matched_count))
            new_auth_otp = Authentication(username, email, machine_id, session_id, user_mongo['otp'], msg['status'])
            send_producer(topic_otp_request_result, username, new_auth_otp, producer, auth_avro_serializer)
            # send_email([email],
            #            "Incorrect OTP from Username: " + username + " Email: "
            #            + email, "Upwork User Registration Failed")
            print("---------------------------------------------------------------------------------------------------")
    pass


try:
    # Define Consumer
    consumer = Consumer(kafka_config)
    consumer.subscribe([topic_user_authentication, topic_otp_request])
    producer = define_kafka_producer()

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None: continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                 (msg.topic(), msg.partition(), msg.offset()))
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            if msg.topic() == topic_user_authentication:
                auth_msg = auth_avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
                send_otp(auth_msg, producer)
            elif msg.topic() == topic_otp_request:
                auth_msg = auth_avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
                validate_otp(auth_msg, producer)
except KeyboardInterrupt:
    print("Close Consumer...")
    consumer.close()
