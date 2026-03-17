import os
import json
import time
import logging
from logging.handlers import RotatingFileHandler
from confluent_kafka import Consumer
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

LOG_FILE = "consumer.log"

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

BATCH_SIZE = 100
BATCH_TIMEOUT = 5  # seconds

consumer_conf = {
    "bootstrap.servers": os.getenv("LOCAL_BOOTSTRAP_SERVERS"),
    "group.id": "mongo-consumer-group",
    "auto.offset.reset": "earliest",
}

topic = os.getenv("LOCAL_TOPIC")
mongo_uri = os.getenv("MONGO_URI")
mongo_db = os.getenv("MONGO_DB")
mongo_collection = os.getenv("MONGO_COLLECTION")

consumer = Consumer(consumer_conf)
consumer.subscribe([topic])

client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
collection = client[mongo_db][mongo_collection]

log.info("Consumer started: Local Kafka -> MongoDB")

def flush_batch(batch):
    if not batch:
        return
    try:
        collection.insert_many(batch, ordered=False)
        log.info(f"Inserted batch of {len(batch)} documents into MongoDB")
    except Exception as e:
        log.error(f"MongoDB batch insert error: {e}")

try:
    batch = []
    last_flush = time.time()

    while True:
        msg = consumer.poll(1.0)

        if msg is not None:
            if msg.error():
                log.error(f"Kafka consume error: {msg.error()}")
            else:
                raw_value = msg.value().decode("utf-8")
                try:
                    data = json.loads(raw_value)
                except json.JSONDecodeError:
                    data = {"raw_message": raw_value}
                batch.append(data)

        # Flush if batch is full or timeout reached
        if len(batch) >= BATCH_SIZE or (batch and time.time() - last_flush >= BATCH_TIMEOUT):
            flush_batch(batch)
            batch = []
            last_flush = time.time()

except KeyboardInterrupt:
    log.info("Stopping consumer...")

finally:
    flush_batch(batch)  # flush any remaining messages
    consumer.close()
    client.close()
    log.info("Consumer stopped cleanly.")
