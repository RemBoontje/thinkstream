import os
from dotenv import load_dotenv
from confluent_kafka import SerializingProducer, DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import StringSerializer, StringDeserializer

load_dotenv()

# 1. Path Setup (Keep this, it's good)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 2. Schema Registry Client (This was already working)
sr_conf = {
    'url': os.getenv('SCHEMA_REGISTRY_URL'),
    'basic.auth.user.info': f"{os.getenv('SCHEMA_REGISTRY_USER')}:{os.getenv('SCHEMA_REGISTRY_PASSWORD')}"
}
schema_registry_client = SchemaRegistryClient(sr_conf)

def get_producer(schema_path="schema.avsc"):
    full_schema_path = os.path.join(BASE_DIR, schema_path)
    with open(full_schema_path, "r") as f:
        schema_str = f.read()

    avro_serializer = AvroSerializer(
        schema_registry_client,
        schema_str,
        lambda obj, ctx: obj
    )

    producer_conf = {
        'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP'),
        # SWITCH TO SASL_SSL (Password Auth)
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': os.getenv('KAFKA_API_KEY'),     # Ensure this matches your .env var name
        'sasl.password': os.getenv('KAFKA_API_SECRET'),  # Ensure this matches your .env var name
        # Keep CA location to trust the server
        'ssl.ca.location': os.path.join(BASE_DIR, 'ca.pem'),
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': avro_serializer
    }
    return SerializingProducer(producer_conf)

def get_consumer(group_id, schema_path="schema.avsc"):
    full_schema_path = os.path.join(BASE_DIR, schema_path)
    with open(full_schema_path, "r") as f:
        schema_str = f.read()

    avro_deserializer = AvroDeserializer(
        schema_registry_client,
        schema_str,
        lambda obj, ctx: obj
    )

    consumer_conf = {
        'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP'),
        # SWITCH TO SASL_SSL (Password Auth)
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': os.getenv('KAFKA_API_KEY'),
        'sasl.password': os.getenv('KAFKA_API_SECRET'),
        'ssl.ca.location': os.path.join(BASE_DIR, 'ca.pem'),
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'key.deserializer': StringDeserializer('utf_8'),
        'value.deserializer': avro_deserializer
    }
    return DeserializingConsumer(consumer_conf)