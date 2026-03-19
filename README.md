# Kafka to MongoDB Streaming Pipeline

Một pipeline streaming dữ liệu thời gian thực sử dụng Apache Kafka và MongoDB, được viết bằng Python.

A real-time data streaming pipeline using Apache Kafka and MongoDB, written in Python.

---

## Project Description / Mô tả dự án

**English:**
This project implements a streaming data pipeline as part of the K20 Data Engineering course (Apache Kafka module). It consumes `product_view` events from a remote Kafka server, re-produces them into a self-hosted local Kafka topic, and stores the data into MongoDB.

**Tiếng Việt:**
Dự án này xây dựng một pipeline streaming dữ liệu trong khuôn khổ môn học K20 Data Engineering (module Apache Kafka). Chương trình đọc dữ liệu sự kiện `product_view` từ Kafka server được cung cấp, tái sản xuất vào một Kafka topic tự dựng, và lưu trữ dữ liệu vào MongoDB.

---

## Architecture / Kiến trúc

```
[Remote Kafka]          [Local Kafka]          [MongoDB]
 topic:                  topic:                 db: kafka_project
 product_view    →→→     product_view_local →→→ collection: product_view
                producer.py              consumer.py
```

---

## Technologies Used / Công nghệ sử dụng

| Component | Technology |
|---|---|
| Language | Python 3.12 |
| Kafka Client | confluent-kafka 2.13.2 |
| MongoDB Client | pymongo 4.16.0 |
| Local Kafka | Apache Kafka 7.6.1 (Docker) |
| Local MongoDB | MongoDB 7 (Docker) |
| Kafka UI | AKHQ 0.25.0 (Docker) |
| VM | Ubuntu 24.04 |

---

## Features / Tính năng

- **Batch processing** — flushes every 100 messages or every 5 seconds, whichever comes first
- **Rotating file logging** — logs to `producer.log` / `consumer.log`, max 5MB per file, 3 backups
- **Timeout handling** — prevents hanging on slow or overloaded servers
- **Graceful shutdown** — flushes remaining messages on `Ctrl+C`
- **Duplicate prevention** — `insert_many` with `ordered=False` skips duplicate documents

---

## Project Structure / Cấu trúc dự án

```
kafka-project/
├── producer.py        # Consumes from remote Kafka → produces to local Kafka
├── consumer.py        # Consumes from local Kafka → stores to MongoDB
├── requirements.txt   # Python dependencies
├── .env.example       # Sample environment configuration
└── README.md
```

---

## Prerequisites / Yêu cầu

- Python 3.10+
- A Linux VM (Ubuntu recommended) with Docker and Docker Compose installed
- Local Kafka and MongoDB running via Docker

---

## Setup / Cài đặt

### 1. Clone the repository

```bash
git clone https://github.com/antng149/Kafka_Product_View.git
cd Kafka_Product_View
```

### 2. Create virtual environment / Tạo môi trường ảo

```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install dependencies / Cài đặt thư viện

```bash
pip install -r requirements.txt
```

### 4. Configure environment / Cấu hình môi trường

```bash
cp .env.example .env
```

Edit `.env` with your own values:

```env
# Remote Kafka (provided by coach)
SOURCE_BOOTSTRAP_SERVERS=<remote_kafka_servers>
SOURCE_SECURITY_PROTOCOL=SASL_PLAINTEXT
SOURCE_SASL_MECHANISM=PLAIN
SOURCE_USERNAME=<username>
SOURCE_PASSWORD=<password>
SOURCE_TOPIC=product_view

# Local Kafka
LOCAL_BOOTSTRAP_SERVERS=<vm_ip>:9092
LOCAL_TOPIC=product_view_local

# MongoDB
MONGO_URI=mongodb://<user>:<password>@<vm_ip>:27017/?authSource=admin
MONGO_DB=kafka_project
MONGO_COLLECTION=product_view
```

### 5. Start local Kafka and MongoDB / Khởi động Kafka và MongoDB

```bash
docker compose up -d
```

### 6. Run the pipeline / Chạy pipeline

Open two terminals:

**Terminal 1 — Producer:**
```bash
python producer.py
```

**Terminal 2 — Consumer:**
```bash
python consumer.py
```

---

## Results / Kết quả

| Metric | Value |
|---|---|
| Messages in local Kafka topic | ~1,993,806 |
| Local Kafka topic size | 1.712 GB |
| Documents stored in MongoDB | 10,626,394 |
| Consumer lag | 0 (real-time) |
| Duplicate documents | 0 |
| Batch size | 100 messages |
| Batch timeout | 5 seconds |

---

## Sample Document / Dữ liệu mẫu

```json
{
  "_id": "5eb5455f7d602a3748c723a3",
  "time_stamp": 1772479319,
  "ip": "173.252.87.117",
  "user_agent": "facebookexternalhit/1.1",
  "resolution": "2000x2000",
  "device_id": "bc56aafd-8526-43c0-857d-cd3ca9234e00",
  "store_id": "14",
  "local_time": "2026-03-03 02:21:59",
  "current_url": "https://www.glamira.it/classic-gaze-3mm.html",
  "collection": "view_product_detail",
  "product_id": "92674"
}
```
