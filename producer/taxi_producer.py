#!/usr/bin/env python3
"""
NYC Taxi Trip Kafka Producer
=============================

Reads NYC Yellow Taxi trip records from a Parquet file and publishes
them to a Kafka topic as JSON messages, simulating a real-time data stream.

Usage:
    python producer/taxi_producer.py --data data/yellow_tripdata_2025-01.parquet
    python producer/taxi_producer.py --data data/yellow_tripdata_2025-01.parquet --mode peak
    python producer/taxi_producer.py --data data/yellow_tripdata_2025-01.parquet --delay 0.1

See --help for all options.
"""

import argparse
import json
import logging
import math
import random
import signal
import sys
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_BOOTSTRAP_SERVER = "localhost:9092"
DEFAULT_TOPIC = "taxi-trips"
DEFAULT_MODE = "normal"
DEFAULT_BATCH_LOG_INTERVAL = 100

# Traffic mode delay settings (base_delay_seconds, jitter_fraction)
# jitter_fraction: actual delay = base * uniform(1 - jitter, 1 + jitter)
TRAFFIC_MODES = {
    "peak": {"base_delay": 0.05, "jitter": 0.4},
    "normal": {"base_delay": 0.5, "jitter": 0.3},
    "offpeak": {"base_delay": 2.0, "jitter": 0.25},
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("taxi_producer")

# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------

_shutdown_requested = False


def _signal_handler(signum, frame):
    """Handle Ctrl+C (SIGINT) gracefully."""
    global _shutdown_requested
    _shutdown_requested = True
    logger.info("Shutdown requested (Ctrl+C). Finishing current record...")


signal.signal(signal.SIGINT, _signal_handler)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _safe_value(val):
    """Convert a single value to a JSON-compatible Python type.

    Handles Pandas Timestamps, NumPy scalars, NaN, NaT, etc.
    """
    if val is None:
        return None
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return None
    if isinstance(val, pd.Timestamp):
        if pd.isna(val):
            return None
        return val.isoformat()
    if hasattr(val, "item"):
        # NumPy scalar → native Python type
        converted = val.item()
        if isinstance(converted, float) and (math.isnan(converted) or math.isinf(converted)):
            return None
        return converted
    if pd.isna(val):
        return None
    return val


def row_to_dict(row):
    """Convert a Pandas Series (DataFrame row) to a JSON-safe dict."""
    return {col: _safe_value(row[col]) for col in row.index}


def compute_delay(mode: str, custom_delay: float | None) -> float:
    """Return a delay in seconds for the current iteration.

    If *custom_delay* is set it takes precedence over the traffic mode.
    Otherwise the delay is drawn from the mode's base delay ± jitter.
    """
    if custom_delay is not None:
        return custom_delay

    cfg = TRAFFIC_MODES[mode]
    base = cfg["base_delay"]
    jitter = cfg["jitter"]
    return base * random.uniform(1.0 - jitter, 1.0 + jitter)


# ---------------------------------------------------------------------------
# Kafka helpers
# ---------------------------------------------------------------------------


def create_producer(bootstrap_server: str) -> KafkaProducer:
    """Create and return a KafkaProducer with JSON serialization."""
    logger.info("Connecting to Kafka at %s ...", bootstrap_server)
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_server,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=5,
            retry_backoff_ms=500,
            request_timeout_ms=30000,
            max_block_ms=60000,
            linger_ms=10,
            api_version=(2, 6, 0),
        )
        logger.info("Connected to Kafka successfully.")
        return producer
    except NoBrokersAvailable:
        logger.error(
            "Could not connect to Kafka at %s. "
            "Make sure Kafka is running (docker compose up kafka -d).",
            bootstrap_server,
        )
        sys.exit(1)
    except KafkaError as exc:
        logger.error("Kafka connection error: %s", exc)
        sys.exit(1)


def on_send_success(record_metadata):
    """Callback for successful sends (used for debug-level logging)."""
    logger.debug(
        "Message delivered → topic=%s  partition=%s  offset=%s",
        record_metadata.topic,
        record_metadata.partition,
        record_metadata.offset,
    )


def on_send_error(excp):
    """Callback for failed sends."""
    logger.error("Message delivery failed: %s", excp)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_dataset(data_path: str) -> pd.DataFrame:
    """Load and validate a Parquet dataset.

    Returns a DataFrame. Exits with an error if the file is missing,
    unreadable, or empty.
    """
    path = Path(data_path)
    if not path.exists():
        logger.error("Dataset not found: %s", path.resolve())
        logger.error(
            "Download it from: "
            "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
        )
        sys.exit(1)

    if not path.suffix.lower() == ".parquet":
        logger.warning(
            "Expected a .parquet file but got '%s'. Attempting to load anyway...",
            path.suffix,
        )

    try:
        df = pd.read_parquet(path)
    except Exception as exc:
        logger.error("Failed to read dataset: %s", exc)
        sys.exit(1)

    if df.empty:
        logger.error("Dataset is empty — nothing to stream.")
        sys.exit(1)

    logger.info("Dataset loaded: %s", path.name)
    logger.info("  Rows   : %s", f"{len(df):,}")
    logger.info("  Columns: %s", list(df.columns))
    return df


# ---------------------------------------------------------------------------
# Main streaming loop
# ---------------------------------------------------------------------------


def stream_records(
    producer: KafkaProducer,
    df: pd.DataFrame,
    topic: str,
    mode: str,
    custom_delay: float | None,
    loop: bool,
    batch_log_interval: int,
):
    """Stream dataset rows to Kafka one by one."""
    global _shutdown_requested

    total_sent = 0
    iteration = 0

    logger.info("=" * 60)
    logger.info("Starting stream")
    logger.info("  Topic         : %s", topic)
    if custom_delay is not None:
        logger.info("  Delay         : %.3f s (custom)", custom_delay)
    else:
        cfg = TRAFFIC_MODES[mode]
        logger.info(
            "  Traffic mode  : %s  (base=%.3f s, jitter=±%d%%)",
            mode,
            cfg["base_delay"],
            int(cfg["jitter"] * 100),
        )
    logger.info("  Loop          : %s", loop)
    logger.info("  Log interval  : every %d records", batch_log_interval)
    logger.info("=" * 60)

    try:
        while True:
            iteration += 1
            if iteration > 1:
                logger.info("--- Loop iteration #%d ---", iteration)

            for idx in range(len(df)):
                if _shutdown_requested:
                    break

                row = df.iloc[idx]
                record = row_to_dict(row)

                producer.send(topic, value=record).add_callback(
                    on_send_success
                ).add_errback(on_send_error)

                total_sent += 1

                if total_sent % batch_log_interval == 0:
                    logger.info("Sent record #%s", f"{total_sent:,}")

                delay = compute_delay(mode, custom_delay)
                time.sleep(delay)

            if _shutdown_requested or not loop:
                break

    except KeyboardInterrupt:
        _shutdown_requested = True
        logger.info("KeyboardInterrupt received.")
    finally:
        logger.info("Flushing remaining messages...")
        producer.flush(timeout=10)
        producer.close(timeout=5)
        logger.info("=" * 60)
        logger.info("Streaming completed.")
        logger.info("  Total records sent: %s", f"{total_sent:,}")
        logger.info("=" * 60)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv=None):
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="NYC Taxi Trip Kafka Producer — stream taxi records to Kafka.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
  python producer/taxi_producer.py --data data/yellow_tripdata_2025-01.parquet
  python producer/taxi_producer.py --data data/yellow_tripdata_2025-01.parquet --mode peak
  python producer/taxi_producer.py --data data/yellow_tripdata_2025-01.parquet --mode offpeak
  python producer/taxi_producer.py --data data/yellow_tripdata_2025-01.parquet --delay 0.1
  python producer/taxi_producer.py --data data/yellow_tripdata_2025-01.parquet --loop
""",
    )

    parser.add_argument(
        "--data",
        required=True,
        help="Path to the NYC Yellow Taxi Parquet dataset file.",
    )
    parser.add_argument(
        "--bootstrap-server",
        default=DEFAULT_BOOTSTRAP_SERVER,
        help=f"Kafka bootstrap server address (default: {DEFAULT_BOOTSTRAP_SERVER}).",
    )
    parser.add_argument(
        "--topic",
        default=DEFAULT_TOPIC,
        help=f"Kafka topic to publish to (default: {DEFAULT_TOPIC}).",
    )
    parser.add_argument(
        "--mode",
        default=DEFAULT_MODE,
        choices=list(TRAFFIC_MODES.keys()),
        help=(
            "Traffic simulation mode. "
            "peak=fast (~0.05 s), normal=medium (~0.5 s), offpeak=slow (~2.0 s). "
            f"Default: {DEFAULT_MODE}. "
            "Ignored if --delay is set."
        ),
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=None,
        help=(
            "Custom fixed delay between records (seconds). "
            "Overrides --mode when set."
        ),
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        default=False,
        help="Loop over the dataset continuously until stopped (Ctrl+C).",
    )
    parser.add_argument(
        "--batch-log-interval",
        type=int,
        default=DEFAULT_BATCH_LOG_INTERVAL,
        help=(
            "Print a progress log every N records. "
            f"Default: {DEFAULT_BATCH_LOG_INTERVAL}."
        ),
    )

    args = parser.parse_args(argv)

    # Validate custom delay
    if args.delay is not None and args.delay < 0:
        parser.error("--delay must be >= 0")

    return args


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main():
    args = parse_args()

    # 1. Load dataset
    df = load_dataset(args.data)

    # 2. Connect to Kafka
    producer = create_producer(args.bootstrap_server)

    # 3. Stream
    stream_records(
        producer=producer,
        df=df,
        topic=args.topic,
        mode=args.mode,
        custom_delay=args.delay,
        loop=args.loop,
        batch_log_interval=args.batch_log_interval,
    )


if __name__ == "__main__":
    main()
