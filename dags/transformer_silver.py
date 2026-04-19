from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.providers.postgres.operators.postgres import PostgresOperator

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
with DAG(
        'transformer_silver',
        default_args=default_args,
        description='To transform the raw current weather to a modeled dataset',
        schedule_interval=None,
        start_date=datetime(2026, 4, 18),
        catchup=False,
        tags=['take-home'],
) as dag:

    t1 = PostgresOperator(
        task_id="create_modeled_dataset_table",
        sql="""
            CREATE TABLE IF NOT EXISTS silver_current_weather (
                source_observation_ts TIMESTAMPTZ NOT NULL PRIMARY KEY,
                ingestion_id VARCHAR(50) NOT NULL,
                ingestion_ts TIMESTAMP NOT NULL,
                city_name TEXT,
                city_id INTEGER,
                country_code TEXT,
                city_longitude REAL,
                city_latitude REAL,
                temperature_celsius REAL,
                humidity_percent INTEGER,
                weather_main TEXT,
                weather_description TEXT
           );
          """,
    )

    t2 = PostgresOperator(
        task_id="transform_raw_into_modelled",
        sql="""
            INSERT INTO silver_current_weather (
                source_observation_ts,
                city_id,
                city_name,
                country_code,
                city_longitude,
                city_latitude,
                temperature_celsius,
                humidity_percent,
                weather_main,
                weather_description,
                ingestion_id,
                ingestion_ts
            )
            WITH typed_cte AS (
                SELECT
                    to_timestamp((payload ->> 'dt')::bigint)              AS source_observation_ts,
                    (payload ->> 'id')::integer                           AS city_id,
                    payload ->> 'name'                                    AS city_name,
                    payload -> 'sys' ->> 'country'                        AS country_code,
                    (payload -> 'coord' ->> 'lon')::numeric(10,6)         AS city_longitude,
                    (payload -> 'coord' ->> 'lat')::numeric(10,6)         AS city_latitude,
                    (payload -> 'main' ->> 'temp')::numeric(10,2)         AS temperature_celsius,
                    (payload -> 'main' ->> 'humidity')::integer           AS humidity_percent,
                    payload -> 'weather' -> 0 ->> 'main'                  AS weather_main,
                    payload -> 'weather' -> 0 ->> 'description'           AS weather_description,
                    ingestion_id,
                    ingestion_ts
                FROM raw_current_weather
                WHERE payload ->> 'dt' IS NOT NULL
            ),
            dedup_cte AS (
                SELECT *
                FROM (
                    SELECT
                        t.*,
                        ROW_NUMBER() OVER (
                            PARTITION BY source_observation_ts
                            ORDER BY ingestion_ts DESC
                        ) AS rn
                    FROM typed_cte t
                ) x
                WHERE rn = 1
            )
            SELECT
                d.source_observation_ts,
                d.city_id,
                d.city_name,
                d.country_code,
                d.city_longitude,
                d.city_latitude,
                d.temperature_celsius,
                d.humidity_percent,
                d.weather_main,
                d.weather_description,
                d.ingestion_id,
                d.ingestion_ts
            FROM dedup_cte d
            WHERE NOT EXISTS (
                SELECT s.source_observation_ts
                FROM silver_current_weather s
                WHERE s.source_observation_ts = d.source_observation_ts
            );
            """,
    )

    t1 >> t2