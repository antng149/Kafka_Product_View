import os
import json
import time
from confluent_kafka import Consumer
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv
from utils import setup_logger

load_dotenv()

BATCH_SIZE = 100
BATCH_TIMEOUT = 5  # seconds

log = setup_logger("consumer.log")

consumer_conf = {
    "bootstrap.servers": os.getenv("LOCAL_BOOTSTRAP_SERVERS"),
    "group.id": "mongo-consumer-group",
    "auto.offset.reset": "earliest",
}

topic = os.getenv("LOCAL_TOPIC")
mongo_uri = os.getenv("MONGO_URI")
mongo_db = os.getenv("MONGO_DB")
mongo_collection = os.getenv("MONGO_COLLECTION")


def flush_batch(batch, collection):
    if not batch:
        return
    try:
        operations = [
            UpdateOne(
                {"_id": doc["_id"]},
                {"$setOnInsert": doc},
                upsert=True
            )
            for doc in batch
        ]
        result = collection.bulk_write(operations, ordered=False)
        log.info(f"Upserted batch: {result.upserted_count} new, {result.matched_count} already existed")
    except Exception as e:
        log.error(f"MongoDB batch upsert error: {e}")


consumer = None
client = None

try:
    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    collection = client[mongo_db][mongo_collection]

    log.info("Consumer started: Local Kafka -> MongoDB")

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

        if len(batch) >= BATCH_SIZE or (batch and time.time() - last_flush >= BATCH_TIMEOUT):
            flush_batch(batch, collection)
            batch = []
            last_flush = time.time()

except KeyboardInterrupt:
    log.info("Stopping consumer...")

finally:
    if consumer:
        flush_batch(batch, collection)
        consumer.close()
    if client:
        client.close()
    log.info("Consumer stopped cleanly.")
