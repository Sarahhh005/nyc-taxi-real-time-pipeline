import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, from_json, sum as _sum, window
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

KAFKA_BOOTSTRAP_SERVERS = "kafka:9094"
KAFKA_TOPIC = "taxi-trips"
CLICKHOUSE_JDBC_URL = "jdbc:clickhouse://clickhouse:8123/NYC_TAXI"
CLICKHOUSE_TABLE = "taxi_trips"

# Watermark: how late a trip event is allowed to arrive before its window is
# considered final and dropped from state. 10 minutes is generous for a
# simulated stream where events arrive within seconds of being produced.
WATERMARK_DELAY = "10 minutes"
WINDOW_DURATION = "1 minute"  # small window on purpose: makes the "live" effect visible in a demo

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
logger = logging.getLogger("spark_streaming")

# Kafka JSON schema definition
kafka_json_schema = StructType([
    StructField("tpep_pickup_datetime", StringType(), True),
    StructField("tpep_dropoff_datetime", StringType(), True),
    StructField("passenger_count", FloatType(), True),
    StructField("trip_distance", FloatType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("fare_amount", FloatType(), True),
    StructField("tip_amount", FloatType(), True),
    StructField("total_amount", FloatType(), True),
    StructField("payment_type", IntegerType(), True),
])

def _write_micro_batch(df, epoch_id, table_name):
    # Shared JDBC writer for any micro-batch -> ClickHouse table sink.
    logger.info(f"[{table_name}] Writing micro-batch {epoch_id} ({df.count()} rows)...")
    df.write \
        .format("jdbc") \
        .mode("append") \
        .option("url", CLICKHOUSE_JDBC_URL) \
        .option("dbtable", table_name) \
        .option("user", "spark_user") \
        .option("password", "spark_pass") \
        .option("driver", "ru.yandex.clickhouse.ClickHouseDriver") \
        .save()


def write_to_clickhouse(df, epoch_id):
    _write_micro_batch(df, epoch_id, CLICKHOUSE_TABLE)


def write_hourly_demand(df, epoch_id):
    _write_micro_batch(df, epoch_id, "hourly_demand")


def write_zone_statistics(df, epoch_id):
    _write_micro_batch(df, epoch_id, "zone_statistics")

def main():
    # Init Spark
    spark = SparkSession.builder \
        .appName("KafkaToClickHouseStreaming") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    # Read Kafka stream
    raw_kafka_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON
    parsed_stream = raw_kafka_stream \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), kafka_json_schema).alias("data")) \
        .select("data.*")

    # Transform to ClickHouse schema & clean data
    transformed_stream = parsed_stream \
        .withColumn("pickup_datetime", col("tpep_pickup_datetime").cast(TimestampType())) \
        .withColumn("dropoff_datetime", col("tpep_dropoff_datetime").cast(TimestampType())) \
        .withColumn("passenger_count", col("passenger_count").cast(IntegerType())) \
        .withColumn("trip_distance", col("trip_distance").cast(FloatType())) \
        .withColumn("pickup_location_id", col("PULocationID").cast(IntegerType())) \
        .withColumn("dropoff_location_id", col("DOLocationID").cast(IntegerType())) \
        .withColumn("fare_amount", col("fare_amount").cast(FloatType())) \
        .withColumn("tip_amount", col("tip_amount").cast(FloatType())) \
        .withColumn("total_amount", col("total_amount").cast(FloatType())) \
        .withColumn("payment_type", col("payment_type").cast(IntegerType())) \
        .select(
            "pickup_datetime",
            "dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "pickup_location_id",
            "dropoff_location_id",
            "fare_amount",
            "tip_amount",
            "total_amount",
            "payment_type"
        ) \
        .dropDuplicates() \
        .dropna(subset=["pickup_datetime", "dropoff_datetime"]) \
        .fillna(0, subset=["passenger_count"]) \
        .filter(
            (col("fare_amount") >= 0) & 
            (col("passenger_count") <= 8) & 
            (col("dropoff_datetime") > col("pickup_datetime")) & 
            ~((col("trip_distance") == 0) & (col("fare_amount") > 10)) &
            col("pickup_location_id").between(1, 265) & 
            col("dropoff_location_id").between(1, 265) &
            col("payment_type").between(0, 6)
        )

    # Sink 1: raw cleaned trips -> taxi_trips (unchanged, append mode)
    raw_query = transformed_stream \
        .writeStream \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoints/raw_trips") \
        .foreachBatch(write_to_clickhouse) \
        .start()

    # From here on, everything is a *windowed streaming aggregation* — this is
    # what makes the pipeline more than "read Kafka, insert row": Spark keeps
    # running state per time-window and per zone, and emits updated rollups
    # as new events keep arriving, bounded by the watermark above.
    watermarked_stream = transformed_stream.withWatermark("pickup_datetime", WATERMARK_DELAY)

    # Sink 2: hourly_demand — trip count / avg fare / revenue per time window.
    hourly_demand = watermarked_stream \
        .groupBy(window(col("pickup_datetime"), WINDOW_DURATION)) \
        .agg(
            count("*").alias("trip_count"),
            avg("fare_amount").alias("avg_fare"),
            _sum("total_amount").alias("total_revenue"),
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "trip_count",
            "avg_fare",
            "total_revenue",
        )

    hourly_query = hourly_demand \
        .writeStream \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/checkpoints/hourly_demand") \
        .foreachBatch(write_hourly_demand) \
        .start()

    # Sink 3: zone_statistics — same idea, broken down by pickup zone.
    zone_statistics = watermarked_stream \
        .groupBy(window(col("pickup_datetime"), WINDOW_DURATION), col("pickup_location_id")) \
        .agg(
            count("*").alias("trip_count"),
            _sum("total_amount").alias("total_revenue"),
            avg("tip_amount").alias("avg_tip"),
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "pickup_location_id",
            "trip_count",
            "total_revenue",
            "avg_tip",
        )

    zone_query = zone_statistics \
        .writeStream \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/checkpoints/zone_statistics") \
        .foreachBatch(write_zone_statistics) \
        .start()

    logger.info("All 3 streaming queries started: taxi_trips, hourly_demand, zone_statistics")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
