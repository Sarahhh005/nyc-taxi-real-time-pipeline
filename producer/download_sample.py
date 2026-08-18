#!/usr/bin/env python3
"""
Download and sample the NYC Yellow Taxi dataset for development/testing.

Downloads one monthly Parquet file from the NYC TLC website, then extracts
a random sample of N records and saves it as a smaller Parquet file suitable
for development, testing, and Kafka stream simulation.

Usage:
    python producer/download_sample.py
    python producer/download_sample.py --rows 5000
    python producer/download_sample.py --month 2025-02 --rows 20000
"""

import argparse
import sys
import urllib.request
from pathlib import Path

import pandas as pd

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DEFAULT_MONTH = "2025-01"
DEFAULT_ROWS = 10000
DEFAULT_OUTPUT_DIR = "data"
RANDOM_SEED = 42


def main():
    parser = argparse.ArgumentParser(
        description="Download and sample NYC Yellow Taxi dataset."
    )
    parser.add_argument(
        "--month",
        default=DEFAULT_MONTH,
        help=f"Year-month to download, e.g. 2025-01 (default: {DEFAULT_MONTH}).",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=DEFAULT_ROWS,
        help=f"Number of rows to sample (default: {DEFAULT_ROWS}).",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory to save the files (default: {DEFAULT_OUTPUT_DIR}).",
    )
    parser.add_argument(
        "--keep-full",
        action="store_true",
        help="Keep the full downloaded file in addition to the sample.",
    )
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    filename = f"yellow_tripdata_{args.month}.parquet"
    full_path = out_dir / filename
    sample_filename = f"yellow_tripdata_{args.month}_sample_{args.rows}.parquet"
    sample_path = out_dir / sample_filename

    # --- Download -------------------------------------------------------
    url = f"{TLC_BASE_URL}/{filename}"
    print(f"Downloading {url} ...")
    try:
        urllib.request.urlretrieve(url, full_path)
    except Exception as exc:
        print(f"ERROR: Download failed — {exc}", file=sys.stderr)
        sys.exit(1)
    print(f"Downloaded: {full_path}  ({full_path.stat().st_size / 1e6:.1f} MB)")

    # --- Read & sample --------------------------------------------------
    df = pd.read_parquet(full_path)
    total_rows = len(df)
    print(f"Total rows in full file: {total_rows:,}")

    n = min(args.rows, total_rows)
    sample = df.sample(n=n, random_state=RANDOM_SEED)
    sample = sample.sort_index()  # preserve original ordering
    print(f"Sampled {n:,} rows (seed={RANDOM_SEED})")

    # --- Save sample ----------------------------------------------------
    sample.to_parquet(sample_path, index=False)
    print(f"Sample saved: {sample_path}  ({sample_path.stat().st_size / 1e6:.1f} MB)")

    # --- Cleanup --------------------------------------------------------
    if not args.keep_full:
        full_path.unlink()
        print(f"Removed full file: {full_path}")

    # --- Schema summary -------------------------------------------------
    print("\n--- Dataset Schema ---")
    print(f"{'Column':<30} {'Dtype':<20} {'Nulls':>8}")
    print("-" * 60)
    for col in sample.columns:
        null_count = sample[col].isna().sum()
        print(f"{col:<30} {str(sample[col].dtype):<20} {null_count:>8}")

    print(f"\nDone. Use this file with the producer:")
    print(f"  python producer/taxi_producer.py --data {sample_path}")


if __name__ == "__main__":
    main()
