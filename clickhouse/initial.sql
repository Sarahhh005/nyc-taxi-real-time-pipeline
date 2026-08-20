CREATE DATABASE IF NOT EXISTS NYC_TAXI;
CREATE TABLE IF NOT EXISTS NYC_TAXI.taxi_trips
(
    pickup_datetime DateTime,
    dropoff_datetime DateTime,
    passenger_count Int32,
    trip_distance Float32,
    pickup_location_id Int32,
    dropoff_location_id Int32,
    fare_amount Float32,
    tip_amount Float32,
    total_amount Float32,
    payment_type UInt8)
ENGINE = MergeTree()
ORDER BY pickup_datetime;

-- Windowed streaming aggregate written by spark/spark_streaming.py (outputMode="update").
-- Each micro-batch appends the latest rollup for a window as a new row, so
-- ReplacingMergeTree keyed on (window_start, window_end) keeps only the most
-- recent version per window after a merge (or use the `FINAL` modifier in
-- queries for correctness before a merge has run).
CREATE TABLE IF NOT EXISTS NYC_TAXI.hourly_demand
(
    window_start DateTime,
    window_end DateTime,
    trip_count UInt64,
    avg_fare Float64,
    total_revenue Float64,
    inserted_at DateTime DEFAULT now())
ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (window_start, window_end);

CREATE TABLE IF NOT EXISTS NYC_TAXI.zone_statistics
(
    window_start DateTime,
    window_end DateTime,
    pickup_location_id Int32,
    trip_count UInt64,
    total_revenue Float64,
    avg_tip Float64,
    inserted_at DateTime DEFAULT now())
ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (window_start, window_end, pickup_location_id);
