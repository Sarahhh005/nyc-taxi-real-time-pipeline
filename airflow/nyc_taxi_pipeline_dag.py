from datetime import datetime, timedelta
import socket
import urllib.request

from airflow import DAG
from airflow.operators.python import PythonOperator


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 8, 20),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),}

def check_kafka():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)

    result = sock.connect_ex(("kafka", 9094))
    sock.close()

    if result != 0:
        raise ConnectionError("Kafka is not reachable")

    print("Kafka is reachable")


def check_clickhouse():
    url = "http://clickhouse:8123/ping"

    request = urllib.request.Request(
        url,
        headers={
            "X-ClickHouse-User": "spark_user",
            "X-ClickHouse-Key": "spark_pass",
        },)

    with urllib.request.urlopen(request, timeout=5) as response:
        body = response.read().decode().strip()

        if body != "Ok.":
            raise ConnectionError("ClickHouse health check failed")

    print("ClickHouse is reachable")


def pipeline_ready():
    print("Kafka and ClickHouse are ready for the NYC Taxi pipeline")


with DAG(
    dag_id="nyc_taxi_pipeline",
    default_args=DEFAULT_ARGS,
    description="NYC Taxi Real-Time Analytics Pipeline",
    schedule=None,
    catchup=False,
) as dag:

    kafka_health = PythonOperator(
        task_id="check_kafka",
        python_callable=check_kafka,
    )

    clickhouse_health = PythonOperator(
        task_id="check_clickhouse",
        python_callable=check_clickhouse,
    )

    ready = PythonOperator(
        task_id="pipeline_ready",
        python_callable=pipeline_ready,
    )

    [kafka_health, clickhouse_health] >> ready