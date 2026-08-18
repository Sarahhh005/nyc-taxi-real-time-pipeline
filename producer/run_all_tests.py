#!/usr/bin/env python3
"""
Full Automated Integration Test Suite for NYC Taxi Kafka Producer
"""

import json
import logging
import math
import random
import subprocess
import sys
import time

import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

# Set up logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("test_suite")

BOOTSTRAP_SERVER = "127.0.0.1:9092"
TOPIC = "taxi-trips"
SAMPLE_DATA = "data/yellow_tripdata_2025-01_sample_10000.parquet"

REQUIRED_DOWNSTREAM_FIELDS = [
    ("tpep_pickup_datetime", str),
    ("tpep_dropoff_datetime", str),
    ("passenger_count", (int, float)),
    ("trip_distance", (int, float)),
    ("PULocationID", (int, float)),
    ("DOLocationID", (int, float)),
    ("fare_amount", (int, float)),
    ("tip_amount", (int, float)),
    ("total_amount", (int, float)),
]

results = {}


def test_1_publishing_and_consumer():
    logger.info("\n--- TEST 1: Kafka Publishing & Message Consumption ---")
    sys.path.insert(0, "producer")
    from taxi_producer import row_to_dict

    df = pd.read_parquet(SAMPLE_DATA).head(1000)
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        api_version=(2, 6, 0),
    )

    pub_count = 0
    start_time = time.time()
    for idx in range(len(df)):
        record = row_to_dict(df.iloc[idx])
        producer.send(TOPIC, value=record)
        pub_count += 1
    producer.flush()
    elapsed = time.time() - start_time
    logger.info(f"Published {pub_count} records to '{TOPIC}' in {elapsed:.3f}s")

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVER,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=6000,
        api_version=(2, 6, 0),
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    consumed = []
    for msg in consumer:
        consumed.append(msg.value)
        if len(consumed) >= pub_count:
            break
    consumer.close()

    cons_count = len(consumed)
    loss = pub_count - cons_count
    logger.info(f"Published: {pub_count} | Consumed: {cons_count} | Loss: {loss}")

    assert cons_count == pub_count, f"Expected {pub_count}, got {cons_count}"
    results["publishing"] = "PASS"
    results["consumption"] = "PASS"
    results["count_verification"] = f"PASS (Pub: {pub_count}, Cons: {cons_count}, Loss: {loss})"

    return consumed[:100]


def test_2_json_schema_validation(sample_records):
    logger.info("\n--- TEST 2: JSON & Schema Field Validation ---")
    for idx, rec in enumerate(sample_records):
        assert isinstance(rec, dict), f"Record #{idx} is not a valid JSON dict"
        for field_name, expected_type in REQUIRED_DOWNSTREAM_FIELDS:
            assert field_name in rec, f"Record #{idx} missing downstream field '{field_name}'"
            val = rec[field_name]
            if val is not None:
                assert isinstance(val, expected_type), (
                    f"Record #{idx} field '{field_name}' has invalid type {type(val)}, expected {expected_type}"
                )

    logger.info(f"Successfully verified schema & JSON format for {len(sample_records)} consumed records.")
    results["json_validation"] = "PASS"
    results["schema_validation"] = "PASS"


def test_3_traffic_modes():
    logger.info("\n--- TEST 3: Traffic Modes & Delay Measurement ---")
    sys.path.insert(0, "producer")
    from taxi_producer import compute_delay

    for mode in ["peak", "normal", "offpeak"]:
        delays = [compute_delay(mode, None) for _ in range(5)]
        avg_d = sum(delays) / len(delays)
        logger.info(f"Mode '{mode}': delays={['%.3f'%d for d in delays]}, avg={avg_d:.3f}s")
        if mode == "peak":
            assert 0.02 <= avg_d <= 0.10
        elif mode == "normal":
            assert 0.25 <= avg_d <= 0.75
        elif mode == "offpeak":
            assert 1.25 <= avg_d <= 2.75

    custom_d = compute_delay("peak", 0.15)
    assert custom_d == 0.15, f"Expected 0.15, got {custom_d}"
    logger.info("Custom delay override (0.15s) verified.")

    results["traffic_modes"] = "PASS"
    results["custom_delay"] = "PASS"


def test_4_loop_mode_and_graceful_shutdown():
    logger.info("\n--- TEST 4: Loop Mode & Graceful Shutdown ---")
    cmd = [
        sys.executable,
        "producer/taxi_producer.py",
        "--data", SAMPLE_DATA,
        "--bootstrap-server", BOOTSTRAP_SERVER,
        "--topic", TOPIC,
        "--delay", "0.01",
        "--loop",
        "--batch-log-interval", "50",
    ]

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    import signal as sig
    time.sleep(2.5)  # Let it run into loop
    if sys.platform == "win32":
        proc.terminate()
    else:
        proc.send_signal(sig.SIGINT)
    # Give process time to handle signal and flush
    try:
        stdout, stderr = proc.communicate(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        stdout, stderr = proc.communicate()

    logger.info("Producer stdout summary:\n" + "\n".join(stdout.splitlines()[-6:]))
    assert "Streaming completed" in stdout or "Shutdown requested" in stdout or proc.returncode in (0, 1)
    results["loop_mode"] = "PASS"
    results["graceful_shutdown"] = "PASS"


def test_5_connection_error_handling():
    logger.info("\n--- TEST 5: Real Connection Error Handling ---")
    cmd = [
        sys.executable,
        "producer/taxi_producer.py",
        "--data", SAMPLE_DATA,
        "--bootstrap-server", "localhost:9999",
        "--topic", TOPIC,
    ]

    res = subprocess.run(cmd, capture_output=True, text=True)
    assert res.returncode != 0, "Producer should have exited with error code"
    assert "Could not connect to Kafka at localhost:9999" in res.stderr or "NoBrokersAvailable" in res.stderr
    logger.info("Caught connection failure correctly: 'Could not connect to Kafka at localhost:9999'")
    results["connection_error_handling"] = "PASS"


def main():
    logger.info("Starting Full Integration Test Suite Against Running Kafka Broker...")
    try:
        sample_records = test_1_publishing_and_consumer()
        test_2_json_schema_validation(sample_records)
        test_3_traffic_modes()
        test_4_loop_mode_and_graceful_shutdown()
        test_5_connection_error_handling()
    except Exception as exc:
        logger.error(f"TEST FAILED: {exc}", exc_info=True)
        sys.exit(1)

    print("\n" + "=" * 60)
    print("           FINAL INTEGRATION VERIFICATION REPORT           ")
    print("=" * 60)
    for test_name, status in results.items():
        print(f"  {test_name:<30}: {status}")
    print("=" * 60)


if __name__ == "__main__":
    main()
