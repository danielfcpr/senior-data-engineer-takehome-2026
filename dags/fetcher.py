from datetime import datetime, timedelta, timezone
import os
import logging
import requests
import json
import uuid

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_weather():
    lat = "9.9339"
    lon = "-84.0849"
    api_key = os.getenv("OPENWEATHER_API_KEY")

    if not api_key:
        raise ValueError("OPENWEATHER_API_KEY environment variable is not set")

    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "lat": lat,
        "lon": lon,
        "appid": api_key,
        "units": "metric",
        "lang": "en",
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    payload = response.json()

    ingestion_record = {
        "ingestion_id": str(uuid.uuid4()),
        "ingestion_ts": datetime.now(timezone.utc).isoformat(),
        "payload_json": json.dumps(payload),
    }

    logging.info("Fetched payload for %s", payload.get("name"))
    logging.info("Ingestion ID: %s", ingestion_record["ingestion_id"])

    return ingestion_record

with DAG(
        'fetcher',
        default_args=default_args,
        description='To fetch the weather data',
        schedule_interval=None,
        start_date=datetime(2026, 4, 18),
        catchup=False,
        tags=['take-home'],
) as dag:

    t1 = PythonOperator(
        task_id='data_ingestion_dag',
        python_callable=fetch_weather,
    )

    t2 = PostgresOperator(
        task_id="create_raw_dataset",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS raw_current_weather (
                ingestion_id VARCHAR(50) PRIMARY KEY,
                ingestion_ts TIMESTAMP NOT NULL,
                payload JSONB NOT NULL
            );
            """,
    )

    t3 = PostgresOperator(
        task_id="store_dataset",
        postgres_conn_id="postgres_default",
        sql="""
            INSERT INTO raw_current_weather (ingestion_id, ingestion_ts, payload)
            VALUES (
                '{{ ti.xcom_pull(task_ids="data_ingestion_dag")["ingestion_id"] }}',
                '{{ ti.xcom_pull(task_ids="data_ingestion_dag")["ingestion_ts"] }}'::timestamp,
                '{{ ti.xcom_pull(task_ids="data_ingestion_dag")["payload_json"] | replace("'", "''") }}'::jsonb
            );
            """,
    )

    t1 >> t2 >> t3