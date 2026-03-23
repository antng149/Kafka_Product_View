import os
from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv
from utils import setup_logger

load_dotenv()

BATCH_SIZE = 100

log = setup_logger("producer.log")

source_conf = {
    "bootstrap.servers": os.getenv("SOURCE_BOOTSTRAP_SERVERS"),
    "security.protocol": os.getenv("SOURCE_SECURITY_PROTOCOL"),
    "sasl.mechanism": os.getenv("SOURCE_SASL_MECHANISM"),
    "sasl.username": os.getenv("SOURCE_USERNAME"),
    "sasl.password": os.getenv("SOURCE_PASSWORD"),
    "group.id": "product-view-bridge",
    "auto.offset.reset": "earliest",
    "session.timeout.ms": 45000,
}

local_conf = {
    "bootstrap.servers": os.getenv("LOCAL_BOOTSTRAP_SERVERS"),
    "acks": "all",
}

source_topic = os.getenv("SOURCE_TOPIC")
local_topic = os.getenv("LOCAL_TOPIC")


def delivery_report(err, msg):
    if err is not None:
        log.error(f"Delivery failed: {err}")


consumer = None
producer = None

try:
    consumer = Consumer(source_conf)
    producer = Producer(local_conf)
    consumer.subscribe([source_topic])

    log.info("Bridge started: Remote Kafka -> Local Kafka")

    count = 0
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            log.error(f"Kafka consume error: {msg.error()}")
            continue

        producer.produce(
            topic=local_topic,
            value=msg.value(),
            on_delivery=delivery_report,
        )
        count += 1

        if count % BATCH_SIZE == 0:
            producer.flush(timeout=30)
            log.info(f"Flushed {count} messages to local Kafka")

        producer.poll(0)

except KeyboardInterrupt:
    log.info("Stopping bridge...")

finally:
    if consumer:
        consumer.close()
    if producer:
        producer.flush(timeout=30)
    log.info("Bridge stopped cleanly.")
