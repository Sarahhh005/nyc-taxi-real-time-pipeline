import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
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

def write_to_clickhouse(df, epoch_id):
    # Write micro-batch to ClickHouse via JDBC
    logger.info(f"Writing micro-batch {epoch_id} to ClickHouse...")
    df.write \
        .format("jdbc") \
        .mode("append") \
        .option("url", "jdbc:clickhouse://clickhouse:8123/NYC_TAXI") \
        .option("dbtable", CLICKHOUSE_TABLE) \
        .option("user", "default") \
        .option("password", "") \
        .option("driver", "ru.yandex.clickhouse.ClickHouseDriver") \
        .save()

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

    # Start writing stream
    query = transformed_stream \
        .writeStream \
        .outputMode("append") \
        .foreachBatch(write_to_clickhouse) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
