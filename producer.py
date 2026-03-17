import os
import logging
from logging.handlers import RotatingFileHandler
from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv

load_dotenv()

LOG_FILE = "producer.log"
BATCH_SIZE = 100

# Logging setup: console + rotating file (max 5MB, keep 3 backups)
formatter = logging.Formatter(
    fmt="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

file_handler = RotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3)
file_handler.setFormatter(formatter)

logging.basicConfig(level=logging.INFO, handlers=[console_handler, file_handler])
log = logging.getLogger(__name__)

# Kafka config
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

consumer = Consumer(source_conf)
producer = Producer(local_conf)
consumer.subscribe([source_topic])

log.info("Bridge started: Remote Kafka -> Local Kafka")

def delivery_report(err, msg):
    if err is not None:
        log.error(f"Delivery failed: {err}")

try:
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
    consumer.close()
    producer.flush(timeout=30)
    log.info("Bridge stopped cleanly.")
