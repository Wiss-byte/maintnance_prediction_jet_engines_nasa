import os
import time

import paho.mqtt.client as mqtt
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
KAFKA = os.getenv("KAFKA", "localhost:9092")
TOPIC = os.getenv("TOPIC", "sensors.raw")


def make_producer():
    while True:
        try:
            return KafkaProducer(bootstrap_servers=KAFKA, acks="all", linger_ms=50, key_serializer=str.encode)
        except NoBrokersAvailable:
            print("Kafka indisponible, nouvel essai...")
            time.sleep(3)


prod = make_producer()
count = 0


def on_connect(client, userdata, flags, reason_code, properties=None):
    print("MQTT connecté", reason_code)
    client.subscribe("engines/+/telemetry", qos=1)


def on_message(client, userdata, msg):
    global count
    prod.send(TOPIC, key=msg.topic.split("/")[1], value=msg.payload)
    count += 1
    if count % 1000 == 0:
        print(f"{count} messages relayés", flush=True)


cli = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="mqtt-kafka-bridge", clean_session=False)
cli.on_connect, cli.on_message = on_connect, on_message
cli.connect_async(MQTT_HOST, 1883, 60)
cli.loop_forever(retry_first_connection=True)