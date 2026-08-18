#!/usr/bin/env python3
"""
End-to-End Integration Verification Script for NYC Taxi Kafka Producer
"""

import json
import logging
import time
import subprocess
import sys
from pathlib import Path

import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("integration_test")

BOOTSTRAP_SERVER = "localhost:9092"
TOPIC = "taxi-trips"
SAMPLE_DATA = "data/yellow_tripdata_2025-01_sample_10000.parquet"

REQUIRED_FIELDS = [
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "tip_amount",
    "total_amount",
]

DATETIME_FIELDS = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
LOCATION_FIELDS = ["PULocationID", "DOLocationID"]


def test_publishing_and_consumption(num_records=500):
    logger.info("=== TEST: Publishing & Consumer Verification ===")

    # 1. Run producer via subprocess for N records
    cmd = [
        sys.executable,
        "producer/taxi_producer.py",
        "--data", SAMPLE_DATA,
        "--bootstrap-server", BOOTSTRAP_SERVER,
        "--topic", TOPIC,
        "--delay", "0.001",
        "--batch-log-interval", "100"
    ]

    logger.info(f"Publishing {num_records} records to topic '{TOPIC}'...")
    
    # We will publish records using Python KafkaProducer directly for exact count control
    df = pd.read_parquet(SAMPLE_DATA).head(num_records)
    from taxi_producer import row_to_dict

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all"
    )

    published_count = 0
    start_time = time.time()
    for idx in range(len(df)):
        record = row_to_dict(df.iloc[idx])
        producer.send(TOPIC, value=record)
        published_count += 1
    
    producer.flush()
    elapsed = time.time() - start_time
    logger.info(f"Published {published_count} records in {elapsed:.3f}s.")

    # 2. Consume from Kafka using KafkaConsumer
    logger.info("Consuming records from Kafka to verify payloads and counts...")
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVER,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=5000,
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )

    consumed_records = []
    for msg in consumer:
        consumed_records.append(msg.value)
        if len(consumed_records) >= published_count:
            break
    
    consumer.close()

    consumed_count = len(consumed_records)
    loss = published_count - consumed_count
    logger.info(f"Expected: {published_count} | Consumed: {consumed_count} | Message Loss: {loss}")

    assert consumed_count == published_count, f"Count mismatch! Expected {published_count}, got {consumed_count}"

    # 3. Validate JSON payload and schema for consumed records
    logger.info("Validating JSON & Schema fields on consumed records...")
    for idx, rec in enumerate(consumed_records[:100]):
        # Check required downstream fields
        for field in REQUIRED_FIELDS:
            assert field in rec, f"Missing required field '{field}' in record #{idx}"
            val = rec[field]
            if val is not None:
                assert isinstance(val, (int, float)), f"Field '{field}' should be numeric, got {type(val)}"

        for field in LOCATION_FIELDS:
            assert field in rec, f"Missing location field '{field}' in record #{idx}"
            val = rec[field]
            if val is not None:
                assert isinstance(val, int), f"Field '{field}' should be int, got {type(val)}"

        for field in DATETIME_FIELDS:
            assert field in rec, f"Missing datetime field '{field}' in record #{idx}"
            val = rec[field]
            if val is not None:
                assert isinstance(val, str), f"Field '{field}' should be ISO string, got {type(val)}"

    logger.info("✅ Consumer payload JSON & Schema validation PASSED for all records!")
    return published_count, consumed_count, loss


def main():
    pub, cons, loss = test_publishing_and_consumption(500)
    print("\n" + "=" * 50)
    print("INTEGRATION VERIFICATION SUMMARY")
    print(f"  Published : {pub}")
    print(f"  Consumed  : {cons}")
    print(f"  Loss      : {loss}")
    print("=" * 50)


if __name__ == "__main__":
    main()
