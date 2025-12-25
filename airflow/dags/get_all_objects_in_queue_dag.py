from datetime import datetime, timedelta
import logging
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "Vlad",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def trigger_endpoint_1():
    url = "http://api_app:8000/kafka/consumer"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

with DAG(
    dag_id="endpoint_1_every_2_minutes",
    default_args=default_args,
    start_date=datetime(2025, 11, 10),
    schedule_interval=timedelta(minutes=2),
    catchup=False,
    tags=["endpoint-1"],
) as dag:

    task_1 = PythonOperator(
        task_id="trigger_endpoint_1",
        python_callable=trigger_endpoint_1,
    )
