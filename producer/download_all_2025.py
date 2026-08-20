#!/usr/bin/env python3
import urllib.request
import sys
import os

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
out_dir = "data/2025_full"
os.makedirs(out_dir, exist_ok=True)

print("Starting download of 12 months of 2025 NYC Taxi data...")

for month in range(1, 13):
    month_str = f"2025-{month:02d}"
    filename = f"yellow_tripdata_{month_str}.parquet"
    full_path = os.path.join(out_dir, filename)
    url = f"{TLC_BASE_URL}/{filename}"
    
    if os.path.exists(full_path):
        print(f"Skipping {filename}, already exists.")
        continue
        
    print(f"Downloading {filename} from {url}...")
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req) as response, open(full_path, 'wb') as out_file:
            data = response.read()
            out_file.write(data)
        size_mb = os.path.getsize(full_path) / 1e6
        print(f"Successfully downloaded {filename} ({size_mb:.1f} MB)")
    except urllib.error.HTTPError as e:
        if e.code == 403 or e.code == 404:
            print(f"File {filename} not yet available on TLC website.")
        else:
            print(f"ERROR: HTTP {e.code} for {filename}", file=sys.stderr)
    except Exception as exc:
        print(f"ERROR: Download failed for {filename} — {exc}", file=sys.stderr)

print("\nFinished downloading to data/2025_full/")
