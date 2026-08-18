# 🚖 NYC Taxi Trip Kafka Producer

A Python-based Kafka producer that reads NYC Yellow Taxi trip records from Parquet files and publishes them to a Kafka topic as JSON messages, simulating a real-time data stream.

---

## 📌 Purpose

This producer is the **data ingestion layer** of the NYC Taxi Real-Time Analytics Pipeline. It:

1. Reads historical NYC Yellow Taxi trip records from a Parquet file.
2. Converts each record to a JSON object.
3. Publishes it to the `taxi-trips` Kafka topic.
4. Simulates real-time traffic with configurable delays (peak / normal / off-peak).

Downstream components (Spark Structured Streaming → ClickHouse) consume from this topic.

---

## 📂 Dataset

### Source

NYC Taxi & Limousine Commission (TLC) Yellow Taxi Trip Records:

> <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>

### Download (Recommended — Sampled Dataset)

For development and testing, use the included `download_sample.py` script which
downloads a monthly Parquet file and extracts a random sample of N records
(default: 10,000). This avoids committing or working with unnecessarily large files.

```bash
# Download and sample 10,000 records from January 2025 (~0.5 MB sample)
python producer/download_sample.py

# Custom sample size
python producer/download_sample.py --rows 5000

# Different month
python producer/download_sample.py --month 2025-02

# Keep the full file alongside the sample
python producer/download_sample.py --keep-full
```

The sample file is saved to `data/yellow_tripdata_2025-01_sample_10000.parquet`.

**Sampling method:** A random sample of N rows is drawn (seed=42 for reproducibility)
from the full monthly Parquet file. The sample preserves the complete original schema
so it remains fully compatible with the full dataset and downstream pipeline components.

### Download (Full Dataset)

If you need the complete monthly file:

```bash
mkdir data
curl -o data/yellow_tripdata_2025-01.parquet \
  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet
```

> **Note:** Dataset files are excluded from Git via `.gitignore`. Do NOT commit data files.

### Dataset Schema

| Column                   | Type      | Description                        |
|--------------------------|-----------|------------------------------------|
| `VendorID`               | int       | TPEP provider code                 |
| `tpep_pickup_datetime`   | datetime  | Pickup timestamp                   |
| `tpep_dropoff_datetime`  | datetime  | Drop-off timestamp                 |
| `passenger_count`        | float     | Number of passengers               |
| `trip_distance`          | float     | Trip distance in miles             |
| `RatecodeID`             | float     | Rate code                          |
| `store_and_fwd_flag`     | string    | Store and forward flag             |
| `PULocationID`           | int       | Pickup location zone ID            |
| `DOLocationID`           | int       | Drop-off location zone ID          |
| `payment_type`           | int       | Payment type code                  |
| `fare_amount`            | float     | Base fare                          |
| `extra`                  | float     | Extras and surcharges              |
| `mta_tax`                | float     | MTA tax                            |
| `tip_amount`             | float     | Tip amount                         |
| `tolls_amount`           | float     | Tolls amount                       |
| `improvement_surcharge`  | float     | Improvement surcharge              |
| `total_amount`           | float     | Total charge                       |
| `congestion_surcharge`   | float     | Congestion surcharge               |
| `Airport_fee`            | float     | Airport fee                        |
| `cbd_congestion_fee`     | float     | CBD congestion fee                 |

---

## ⚙️ Dependencies

Install from the project root:

```bash
pip install -r requirements.txt
```

Required packages:

- `kafka-python-ng` — Kafka client (maintained fork of `kafka-python`)
- `pandas` — DataFrame processing
- `pyarrow` — Parquet file support

---

## 🔧 Kafka Setup

### Start Kafka

From the project root:

```bash
docker compose up kafka -d
```

### Verify Kafka is Running

```bash
docker ps | findstr kafka
```

Kafka is accessible at:

| From              | Address          |
|-------------------|------------------|
| Host machine      | `localhost:9092` |
| Docker container  | `kafka:9092`     |

### Kafka Topic

The producer publishes to the `taxi-trips` topic (auto-created on first publish).

---

## 🚀 Running the Producer

### Basic Usage

```bash
python producer/taxi_producer.py --data data/yellow_tripdata_2025-01.parquet
```

### All CLI Options

| Option                | Default          | Description                                      |
|-----------------------|------------------|--------------------------------------------------|
| `--data`              | *(required)*     | Path to the Parquet dataset file                 |
| `--bootstrap-server`  | `localhost:9092` | Kafka bootstrap server address                   |
| `--topic`             | `taxi-trips`     | Kafka topic to publish to                        |
| `--mode`              | `normal`         | Traffic simulation mode: `peak`, `normal`, `offpeak` |
| `--delay`             | *(none)*         | Custom fixed delay in seconds (overrides `--mode`) |
| `--loop`              | `false`          | Loop over the dataset continuously               |
| `--batch-log-interval`| `100`            | Print progress every N records                   |

---

## 🚦 Traffic Simulation Modes

The producer supports three built-in traffic simulation modes that control how fast records are published:

| Mode      | Base Delay | Jitter  | Effective Range      |
|-----------|------------|---------|----------------------|
| `peak`    | 0.05 s     | ±40%    | 0.03 – 0.07 s       |
| `normal`  | 0.5 s      | ±30%    | 0.35 – 0.65 s       |
| `offpeak` | 2.0 s      | ±25%    | 1.50 – 2.50 s       |

Each mode adds a small random jitter so records don't arrive at perfectly uniform intervals, simulating realistic traffic patterns.

### How `--mode` and `--delay` Interact

- If **only `--mode`** is set → delay is calculated from the mode's base delay ± jitter.
- If **only `--delay`** is set → that exact delay is used for every record (no jitter).
- If **both** are set → `--delay` takes precedence; `--mode` is ignored.
- If **neither** is set → defaults to `--mode normal`.

---

## 📋 Example Commands

```bash
# Normal mode (default) — ~0.5 s between records
python producer/taxi_producer.py --data data/yellow_tripdata_2025-01.parquet

# Peak mode — very fast (~0.05 s)
python producer/taxi_producer.py --data data/yellow_tripdata_2025-01.parquet --mode peak

# Off-peak mode — slow (~2.0 s)
python producer/taxi_producer.py --data data/yellow_tripdata_2025-01.parquet --mode offpeak

# Custom delay of 0.1 seconds
python producer/taxi_producer.py --data data/yellow_tripdata_2025-01.parquet --delay 0.1

# Loop continuously with peak traffic
python producer/taxi_producer.py --data data/yellow_tripdata_2025-01.parquet --mode peak --loop

# Use a different Kafka server and topic
# Single-line command (PowerShell / Command Prompt / Bash):
python producer/taxi_producer.py --data data/yellow_tripdata_2025-01_sample_10000.parquet --bootstrap-server localhost:9092 --topic taxi-trips --mode peak

# PowerShell multi-line syntax (uses backtick `):
python producer/taxi_producer.py `
    --data data/yellow_tripdata_2025-01_sample_10000.parquet `
    --bootstrap-server localhost:9092 `
    --topic taxi-trips `
    --mode peak

# Log progress every 500 records instead of every 100
python producer/taxi_producer.py \
    --data data/yellow_tripdata_2025-01.parquet \
    --batch-log-interval 500
```

---

## ✅ Verifying Messages in Kafka

After starting the producer, verify that messages are reaching Kafka:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic taxi-trips \
    --from-beginning \
    --max-messages 5
```

You should see JSON objects like:

```json
{
  "VendorID": 1,
  "tpep_pickup_datetime": "2025-01-01T00:28:09",
  "tpep_dropoff_datetime": "2025-01-01T00:44:51",
  "passenger_count": 1.0,
  "trip_distance": 3.2,
  "RatecodeID": 1.0,
  "store_and_fwd_flag": "N",
  "PULocationID": 140,
  "DOLocationID": 236,
  "payment_type": 1,
  "fare_amount": 18.4,
  "extra": 3.5,
  "mta_tax": 0.5,
  "tip_amount": 2.0,
  "tolls_amount": 0.0,
  "improvement_surcharge": 1.0,
  "total_amount": 25.4,
  "congestion_surcharge": 2.5,
  "Airport_fee": 0.0
}
```

---

## 🔍 Troubleshooting

### "Could not connect to Kafka" / NoBrokersAvailable

- **Is Kafka running?** Check with `docker ps | findstr kafka`.
- **Is the port correct?** From the host use `localhost:9092`, from inside Docker use `kafka:9092`.
- **Firewall?** Ensure port 9092 is not blocked.

### "Dataset not found"

- Verify the `--data` path is correct.
- Download the dataset (see [Dataset](#-dataset) section above).

### "Failed to read dataset"

- Ensure the file is a valid Parquet file.
- Ensure `pyarrow` is installed (`pip install pyarrow`).

### Records not appearing in Kafka

- Wait a few seconds — the producer batches messages.
- Check the producer's console output for errors.
- Try consuming with `--from-beginning` flag.

### Ctrl+C doesn't stop the producer

- Press Ctrl+C once. The producer will finish the current record, flush pending messages, and exit.
- If it doesn't respond, press Ctrl+C again (Python's default KeyboardInterrupt will terminate it).

---

## 🏗️ Architecture

```text
Parquet File
     │
     ▼
 pandas.read_parquet()
     │
     ▼
 Row-by-Row Iteration
     │
     ▼
 Python Dict Conversion
 (Timestamps → ISO strings,
  NumPy types → native Python,
  NaN/NaT → null)
     │
     ▼
 JSON Serialization
     │
     ▼
 KafkaProducer.send()
     │
     ▼
 Kafka Topic: taxi-trips
     │
     ▼
 time.sleep(delay)
     │
     ▼
 Next Record
```
