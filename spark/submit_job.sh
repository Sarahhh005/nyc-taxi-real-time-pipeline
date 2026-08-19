#!/bin/bash

# Submit the Spark Streaming job to the local master
# This script is intended to be run INSIDE the spark-master container

# Define dependencies
# Assuming Spark 4.2.x and Scala 2.13
KAFKA_PKG="org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
CLICKHOUSE_PKG="ru.yandex.clickhouse:clickhouse-jdbc:0.3.2"

# Path to the python script
SCRIPT_PATH="/opt/workspace/spark/spark_streaming.py"

echo "Submitting Spark Streaming Job..."
echo "Dependencies:"
echo " - $KAFKA_PKG"
echo " - $CLICKHOUSE_PKG"

/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf "spark.jars.ivy=/tmp/.ivy" \
  --packages ${KAFKA_PKG},${CLICKHOUSE_PKG} \
  ${SCRIPT_PATH}
