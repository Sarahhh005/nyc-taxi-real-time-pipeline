#!/bin/bash

# Submit the Spark Streaming job to the local master
# This script is intended to be run INSIDE the spark-master container

# Define dependencies
# Assuming Spark 3.5.x and Scala 2.12
KAFKA_PKG="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
CLICKHOUSE_PKG="com.clickhouse:clickhouse-jdbc:0.6.0"

# Path to the python script
SCRIPT_PATH="/opt/workspace/spark/spark_streaming.py"

echo "Submitting Spark Streaming Job..."
echo "Dependencies:"
echo " - $KAFKA_PKG"
echo " - $CLICKHOUSE_PKG"

/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages ${KAFKA_PKG},${CLICKHOUSE_PKG} \
  ${SCRIPT_PATH}
