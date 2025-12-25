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

def trigger_endpoint_2():
    url = "http://api_app:8000/minino/getallfiles"
    logging.info("Triggering endpoint 2")
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def trigger_endpoint_3():
    url = "http://api_app:8000/minino/movetoarchive"
    logging.info("Triggering endpoint 3")
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()

with DAG(
    dag_id="endpoint_2_and_3_every_4_minutes_v2",
    default_args=default_args,
    start_date=datetime(2025, 11, 10),
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    tags=["endpoint-2", "endpoint-3"],
) as dag:

    task_2 = PythonOperator(
        task_id="trigger_endpoint_2",
        python_callable=trigger_endpoint_2,
    )

    task_3 = PythonOperator(
        task_id="trigger_endpoint_3",
        python_callable=trigger_endpoint_3,
    )


    task_2 >> task_3