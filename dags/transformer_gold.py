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
        'transformer_gold',
        default_args=default_args,
        description='To transform the silver current weather data into gold business datasets',
        schedule_interval=None,
        start_date=datetime(2026, 4, 18),
        catchup=False,
        tags=['take-home'],
) as dag:

    t1 = PostgresOperator(
        task_id="create_gold_temperature_history_table",
        sql="""
            CREATE TABLE IF NOT EXISTS gold_temperature_history (
                source_observation_ts TIMESTAMPTZ NOT NULL PRIMARY KEY,
                observation_date DATE NOT NULL,
                city_id INTEGER,
                city_name TEXT,
                country_code TEXT,
                temperature_celsius REAL
            );
        """,
    )

    t2 = PostgresOperator(
        task_id="load_gold_temperature_history",
        sql="""
            INSERT INTO gold_temperature_history (
                source_observation_ts,
                observation_date,
                city_id,
                city_name,
                country_code,
                temperature_celsius
            )
            SELECT
                s.source_observation_ts,
                s.source_observation_ts::date AS observation_date,
                s.city_id,
                s.city_name,
                s.country_code,
                s.temperature_celsius
            FROM silver_current_weather s
            WHERE s.temperature_celsius IS NOT NULL
              AND NOT EXISTS (
                    SELECT 1
                    FROM gold_temperature_history g
                    WHERE g.source_observation_ts = s.source_observation_ts
              );
        """,
    )

    t3 = PostgresOperator(
        task_id="create_gold_temperature_daily_table",
        sql="""
            CREATE TABLE IF NOT EXISTS gold_temperature_daily (
                observation_date DATE NOT NULL,
                city_id INTEGER NOT NULL,
                city_name TEXT,
                country_code TEXT,
                avg_temperature_celsius REAL,
                min_temperature_celsius REAL,
                max_temperature_celsius REAL,
                observation_count INTEGER,
                PRIMARY KEY (observation_date, city_id)
            );
        """,
    )

    t4 = PostgresOperator(
        task_id="load_gold_temperature_daily",
        sql="""
            INSERT INTO gold_temperature_daily (
                observation_date,
                city_id,
                city_name,
                country_code,
                avg_temperature_celsius,
                min_temperature_celsius,
                max_temperature_celsius,
                observation_count
            )
            WITH daily_cte AS (
                SELECT
                    s.source_observation_ts::date AS observation_date,
                    s.city_id,
                    s.city_name,
                    s.country_code,
                    AVG(s.temperature_celsius) AS avg_temperature_celsius,
                    MIN(s.temperature_celsius) AS min_temperature_celsius,
                    MAX(s.temperature_celsius) AS max_temperature_celsius,
                    COUNT(*) AS observation_count
                FROM silver_current_weather s
                WHERE s.temperature_celsius IS NOT NULL
                GROUP BY
                    s.source_observation_ts::date,
                    s.city_id,
                    s.city_name,
                    s.country_code
            )
            SELECT
                d.observation_date,
                d.city_id,
                d.city_name,
                d.country_code,
                d.avg_temperature_celsius,
                d.min_temperature_celsius,
                d.max_temperature_celsius,
                d.observation_count
            FROM daily_cte d
            WHERE NOT EXISTS (
                SELECT 1
                FROM gold_temperature_daily g
                WHERE g.observation_date = d.observation_date
                  AND g.city_id = d.city_id
            );
        """,
    )

    t5 = PostgresOperator(
        task_id="create_gold_weather_latest_table",
        sql="""
            CREATE TABLE IF NOT EXISTS gold_weather_latest (
                city_id INTEGER NOT NULL PRIMARY KEY,
                city_name TEXT,
                country_code TEXT,
                source_observation_ts TIMESTAMPTZ NOT NULL,
                latest_temperature_celsius REAL,
                latest_humidity_percent INTEGER,
                gold_loaded_ts TIMESTAMPTZ NOT NULL,
                freshness_minutes INTEGER
            );
        """,
    )

    t6 = PostgresOperator(
        task_id="load_gold_weather_latest",
        sql="""
            TRUNCATE TABLE gold_weather_latest;

            INSERT INTO gold_weather_latest (
                city_id,
                city_name,
                country_code,
                source_observation_ts,
                latest_temperature_celsius,
                latest_humidity_percent,
                gold_loaded_ts,
                freshness_minutes
            )
            WITH latest_cte AS (
                SELECT *
                FROM (
                    SELECT
                        s.city_id,
                        s.city_name,
                        s.country_code,
                        s.source_observation_ts,
                        s.temperature_celsius AS latest_temperature_celsius,
                        s.humidity_percent AS latest_humidity_percent,
                        CURRENT_TIMESTAMP AS gold_loaded_ts,
                        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - s.source_observation_ts))::INTEGER / 60.0 AS freshness_minutes,
                        ROW_NUMBER() OVER (
                            PARTITION BY s.city_id
                            ORDER BY s.source_observation_ts DESC, s.ingestion_ts DESC
                        ) AS rn
                    FROM silver_current_weather s
                    WHERE s.city_id IS NOT NULL
                ) x
                WHERE rn = 1
            )
            SELECT
                city_id,
                city_name,
                country_code,
                source_observation_ts,
                latest_temperature_celsius,
                latest_humidity_percent,
                gold_loaded_ts,
                freshness_minutes
            FROM latest_cte;
        """,
    )

    t1 >> t2
    t3 >> t4
    t5 >> t6