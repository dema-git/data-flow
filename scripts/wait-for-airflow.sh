#!/bin/bash
set -e

AIRFLOW_CMD=$1

echo "Waiting for Airflow DB to be initialized..."
while [ ! -f /opt/airflow/.airflow_initialized ]; do
  sleep 2
done

echo "Airflow DB initialized. Starting: $AIRFLOW_CMD"
exec airflow $AIRFLOW_CMD