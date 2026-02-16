##############################################################
# http_client.py
#
# Shared HTTP client utilities for Airflow DAGs.
# Contains reusable logic for calling internal ETL API endpoints.
#
##############################################################
import os
import requests


API_BASE_URL = os.getenv("ETL_API_BASE_URL", "http://api_app:8000")


def call_api(path: str, timeout_s: int = 600) -> dict:
    """
    Executes a GET request to the internal ETL API.

    Raises:
    - requests.HTTPError if response status != 2xx
    - ValueError if response is not valid JSON
    """
    url = f"{API_BASE_URL}{path}"
    response = requests.get(url, timeout=timeout_s)
    response.raise_for_status()
    return response.json()
