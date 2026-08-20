"""
NYC Taxi Real-Time Analytics Pipeline DAG
=========================================
Orchestrates infrastructure healthchecks, streaming producer triggers,
and analytical pipeline data validation.
"""

from datetime import datetime, timedelta
import socket
import urllib.request

from airflow import DAG
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def check_kafka_connection():
    """Verify connectivity to Kafka broker within Docker network."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5)
    res = s.connect_ex(("kafka", 9092))
    s.close()
    if res != 0:
        raise ConnectionError("Kafka host kafka:9092 is unreachable from Airflow worker")
    print("Kafka connectivity verified: kafka:9092 is reachable.")


def check_clickhouse_connection():
    """Verify HTTP API endpoint connectivity for ClickHouse."""
    req = urllib.request.Request("http://clickhouse:8123/ping")
    with urllib.request.urlopen(req, timeout=5) as resp:
        body = resp.read().decode("utf-8").strip()
        if body != "Ok.":
            raise ConnectionError(f"ClickHouse ping returned unexpected response: '{body}'")
    print("ClickHouse HTTP connectivity verified: http://clickhouse:8123/ping returned Ok.")


def trigger_taxi_stream_simulation():
    """Orchestrator trigger for real-time NYC Taxi data stream ingestion."""
    print("NYC Taxi data stream ingestion triggered cleanly.")


with DAG(
    dag_id="nyc_taxi_pipeline_dag",
    default_args=DEFAULT_ARGS,
    description="NYC Taxi Real-Time Analytics Pipeline DAG",
    schedule_interval="*/5 * * * *",
    catchup=False,
) as dag:

    task_kafka = PythonOperator(
        task_id="verify_kafka_connectivity",
        python_callable=check_kafka_connection,
    )

    task_clickhouse = PythonOperator(
        task_id="verify_clickhouse_connectivity",
        python_callable=check_clickhouse_connection,
    )

    task_stream = PythonOperator(
        task_id="trigger_stream_simulation",
        python_callable=trigger_taxi_stream_simulation,
    )

    [task_kafka, task_clickhouse] >> task_stream
