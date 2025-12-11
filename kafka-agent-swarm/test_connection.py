import os
import time
import json
import certifi
import google.generativeai as genai
from confluent_kafka import Producer, Consumer
from dotenv import load_dotenv

# 1. Load Secrets
load_dotenv()

# 2. Configure Gemini (The Brain)
genai.configure(api_key=os.getenv("GEMINI_KEY"))
model = genai.GenerativeModel('gemini-2.5-flash-lite')

# 3. Configure Kafka (The Spine)
kafka_conf = {
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP"),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv("KAFKA_KEY"),
    'sasl.password': os.getenv("KAFKA_SECRET"),
    "ssl.ca.location": 'ca.pem'
}

def verification_test():
    print("🚀 Starting Systems Check...")

    # --- TEST 1: The Brain ---
    print("\n🧠 Testing Gemini API...")
    try:
        response = model.generate_content("Say 'Hello Kafka' in a robotic voice.")
        print(f"✅ Gemini Says: {response.text.strip()}")
    except Exception as e:
        print(f"❌ Gemini Failed: {e}")
        return

    # --- TEST 2: The Spine (Producer) ---
    print("\nloudspeaker Testing Kafka Producer...")
    producer = Producer(kafka_conf)
    topic = "test-topic-01" # Make sure this topic exists or auto-create is on
    
    test_message = {"source": "setup_script", "content": response.text.strip()}
    
    def delivery_report(err, msg):
        if err: print(f"❌ Message failed: {err}")
        else: print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")

    producer.produce(topic, json.dumps(test_message), callback=delivery_report)
    producer.flush()

    # --- TEST 3: The Spine (Consumer) ---
    print("\n👂 Testing Kafka Consumer...")
    consumer_conf = kafka_conf.copy()
    consumer_conf.update({
        'group.id': 'verification-group-01',
        'auto.offset.reset': 'earliest'
    })
    
    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    try:
        # Wait up to 10 seconds for a message
        msg = consumer.poll(10.0)
        if msg is None:
            print("❌ No message received (Timeout)")
        elif msg.error():
            print(f"❌ Consumer Error: {msg.error()}")
        else:
            print(f"✅ Received from Kafka: {msg.value().decode('utf-8')}")
    finally:
        consumer.close()

if __name__ == "__main__":
    verification_test()