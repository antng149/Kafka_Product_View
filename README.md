# Kafka to MongoDB Streaming Pipeline

## Project Description

This project implements a streaming data pipeline using Python, Apache Kafka, and MongoDB.

The pipeline does the following:

1. Consumes data from a remote Kafka topic: `product_view`
2. Produces the data into a local Kafka topic: `product_view_local`
3. Consumes data from the local Kafka topic
4. Stores the data into MongoDB

## Architecture

Remote Kafka (`product_view`)
→ `producer.py`
→ Local Kafka (`product_view_local`)
→ `consumer.py`
→ MongoDB (`kafka_project.product_view`)

## Technologies Used

- Python
- Apache Kafka
- confluent-kafka
- MongoDB
- pymongo
- Docker

## Files

- `producer.py`: consumes from remote Kafka and forwards messages to local Kafka
- `consumer.py`: consumes from local Kafka and inserts data into MongoDB
- `.env.example`: sample environment configuration
- `requirements.txt`: Python dependencies

## Setup

### 1. Create virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
