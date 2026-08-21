#!/usr/bin/env python3
"""
Kafka → ClickHouse Streaming Consumer
=======================================
Consumes JSON taxi trip records from a Kafka topic and inserts them
into ClickHouse in micro-batches.

This is a lightweight Python alternative to a full Spark Structured
Streaming job — suitable for development and moderate workloads.

Usage:
    python spark/streaming_consumer.py
    python spark/streaming_consumer.py --bootstrap-server localhost:9092
    python spark/streaming_consumer.py --batch-size 500 --flush-interval 5
"""

import argparse
import json
import logging
import signal
import sys
import time
from datetime import datetime

import clickhouse_connect
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("streaming_consumer")

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
DEFAULT_BOOTSTRAP_SERVER = "localhost:9092"
DEFAULT_TOPIC = "taxi-trips"
DEFAULT_BATCH_SIZE = 200
DEFAULT_FLUSH_INTERVAL = 10  # seconds

CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DB = "NYC_TAXI"
CLICKHOUSE_TABLE = "taxi_trips"

# Column order must match the ClickHouse table schema
COLUMNS = [
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "pickup_location_id",
    "dropoff_location_id",
    "fare_amount",
    "tip_amount",
    "total_amount",
    "payment_type",
]

# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------
_shutdown = False


def _signal_handler(signum, frame):
    global _shutdown
    _shutdown = True
    logger.info("Shutdown requested. Finishing current batch...")


signal.signal(signal.SIGINT, _signal_handler)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_datetime(val):
    """Convert an ISO datetime string to a Python datetime."""
    if val is None:
        return datetime(1970, 1, 1)
    if isinstance(val, str):
        try:
            return datetime.fromisoformat(val)
        except ValueError:
            return datetime(1970, 1, 1)
    return val


def _safe_float(val, default=0.0):
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _safe_int(val, default=0):
    if val is None:
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def record_to_row(rec: dict) -> list:
    """Convert a JSON record dict to a list of values matching COLUMNS order."""
    return [
        _parse_datetime(rec.get("tpep_pickup_datetime")),
        _parse_datetime(rec.get("tpep_dropoff_datetime")),
        _safe_int(rec.get("passenger_count")),
        _safe_float(rec.get("trip_distance")),
        _safe_int(rec.get("PULocationID")),
        _safe_int(rec.get("DOLocationID")),
        _safe_float(rec.get("fare_amount")),
        _safe_float(rec.get("tip_amount")),
        _safe_float(rec.get("total_amount")),
        _safe_int(rec.get("payment_type")),
    ]


# ---------------------------------------------------------------------------
# Main consumer loop
# ---------------------------------------------------------------------------


def run_consumer(
    bootstrap_server: str,
    topic: str,
    ch_host: str,
    ch_port: int,
    batch_size: int,
    flush_interval: int,
    username: str = "spark_user",
    password: str = "spark_pass",
):
    global _shutdown

    # Connect to ClickHouse
    logger.info("Connecting to ClickHouse at %s:%d (user=%s) ...", ch_host, ch_port, username)
    try:
        ch_client = clickhouse_connect.get_client(
            host=ch_host,
            port=ch_port,
            username=username,
            password=password,
            database=CLICKHOUSE_DB,
        )
        # Verify table exists
        result = ch_client.query("SELECT count(*) FROM taxi_trips")
        logger.info("ClickHouse connected. Current row count: %s", result.result_rows[0][0])
    except Exception as e:
        logger.error("Failed to connect to ClickHouse: %s", e)
        sys.exit(1)

    # Connect to Kafka
    logger.info("Connecting to Kafka at %s ...", bootstrap_server)
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_server,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="streaming-consumer",
            consumer_timeout_ms=1000,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
        logger.info("Kafka consumer connected to topic '%s'.", topic)
    except NoBrokersAvailable:
        logger.error("Could not connect to Kafka at %s", bootstrap_server)
        sys.exit(1)

    total_inserted = 0
    batch = []
    last_flush = time.time()

    logger.info("=" * 60)
    logger.info("Streaming consumer running.")
    logger.info("  Kafka:  %s / %s", bootstrap_server, topic)
    logger.info("  CH:     %s:%d / %s.%s", ch_host, ch_port, CLICKHOUSE_DB, CLICKHOUSE_TABLE)
    logger.info("  Batch:  %d records or %ds", batch_size, flush_interval)
    logger.info("=" * 60)

    try:
        while not _shutdown:
            try:
                for msg in consumer:
                    if _shutdown:
                        break
                    batch.append(record_to_row(msg.value))

                    if len(batch) >= batch_size:
                        _flush_batch(ch_client, batch)
                        total_inserted += len(batch)
                        logger.info("Inserted batch of %d (total: %s)", len(batch), f"{total_inserted:,}")
                        batch = []
                        last_flush = time.time()
            except StopIteration:
                pass

            # Time-based flush
            if batch and (time.time() - last_flush) >= flush_interval:
                _flush_batch(ch_client, batch)
                total_inserted += len(batch)
                logger.info("Flushed %d records (timeout, total: %s)", len(batch), f"{total_inserted:,}")
                batch = []
                last_flush = time.time()

            if not _shutdown:
                time.sleep(0.5)

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received.")
    finally:
        # Final flush
        if batch:
            _flush_batch(ch_client, batch)
            total_inserted += len(batch)
            logger.info("Final flush: %d records", len(batch))

        consumer.close()
        logger.info("=" * 60)
        logger.info("Consumer stopped. Total records inserted: %s", f"{total_inserted:,}")
        logger.info("=" * 60)


def _flush_batch(ch_client, batch):
    """Insert a batch of rows into ClickHouse."""
    try:
        ch_client.insert(
            CLICKHOUSE_TABLE,
            batch,
            column_names=COLUMNS,
        )
    except Exception as e:
        logger.error("ClickHouse insert failed: %s", e)
        raise


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args():
    parser = argparse.ArgumentParser(
        description="Kafka → ClickHouse streaming consumer for NYC Taxi data."
    )
    parser.add_argument(
        "--bootstrap-server",
        default=DEFAULT_BOOTSTRAP_SERVER,
        help=f"Kafka bootstrap server (default: {DEFAULT_BOOTSTRAP_SERVER})",
    )
    parser.add_argument(
        "--topic",
        default=DEFAULT_TOPIC,
        help=f"Kafka topic (default: {DEFAULT_TOPIC})",
    )
    parser.add_argument(
        "--clickhouse-host",
        default=CLICKHOUSE_HOST,
        help=f"ClickHouse host (default: {CLICKHOUSE_HOST})",
    )
    parser.add_argument(
        "--clickhouse-port",
        type=int,
        default=CLICKHOUSE_PORT,
        help=f"ClickHouse HTTP port (default: {CLICKHOUSE_PORT})",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Records per batch insert (default: {DEFAULT_BATCH_SIZE})",
    )
    parser.add_argument(
        "--flush-interval",
        type=int,
        default=DEFAULT_FLUSH_INTERVAL,
        help=f"Max seconds between flushes (default: {DEFAULT_FLUSH_INTERVAL})",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    run_consumer(
        bootstrap_server=args.bootstrap_server,
        topic=args.topic,
        ch_host=args.clickhouse_host,
        ch_port=args.clickhouse_port,
        batch_size=args.batch_size,
        flush_interval=args.flush_interval,
    )


if __name__ == "__main__":
    main()
