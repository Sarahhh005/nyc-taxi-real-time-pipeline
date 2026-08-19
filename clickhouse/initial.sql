CREATE DATABASE IF NOT EXISTS NYC_TAXI;
CREATE TABLE IF NOT EXISTS NYC_TAXI.taxi_trips
(   pickup_datetime DateTime,
    dropoff_datetime DateTime,
    passenger_count UInt8,
    trip_distance Float32,
    pickup_location_id UInt16,
    dropoff_location_id UInt16,
    fare_amount Float32,
    tip_amount Float32,
    total_amount Float32,
    payment_type UInt8)
ENGINE = MergeTree()
ORDER BY pickup_datetime;
