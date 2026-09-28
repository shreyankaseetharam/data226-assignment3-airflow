# In Cloud Composer, add apache-airflow-providers-snowflake to PYPI Packages
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests


def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()


@task
def extract():
    latitude = float(Variable.get("weather_latitude"))
    longitude = float(Variable.get("weather_longitude"))

    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "past_days": 60,
        "forecast_days": 0,
        "daily": (
            "temperature_2m_max,"
            "temperature_2m_min,"
            "precipitation_sum,"
            "weather_code"
        ),
        "timezone": "America/Los_Angeles"
    }

    response = requests.get(url, params=params)
    response.raise_for_status()

    data = response.json()

    return {
        "latitude": latitude,
        "longitude": longitude,
        "daily": data["daily"]
    }


@task
def transform(data):
    latitude = data["latitude"]
    longitude = data["longitude"]
    daily = data["daily"]

    records = []

    for i in range(len(daily["time"])):
        records.append([
            latitude,
            longitude,
            daily["time"][i],
            daily["temperature_2m_max"][i],
            daily["temperature_2m_min"][i],
            daily["precipitation_sum"][i],
            daily["weather_code"][i]
        ])

    return records

@task
def load(records, target_table):
    cur = return_snowflake_conn()
    cur.execute("USE WAREHOUSE DEMO_DB")

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {target_table} (
        latitude FLOAT NOT NULL,
        longitude FLOAT NOT NULL,
        date DATE NOT NULL,
        temp_max FLOAT,
        temp_min FLOAT,
        precipitation FLOAT,
        weather_code INTEGER,
        CONSTRAINT pk_weather_data_hw
            PRIMARY KEY (latitude, longitude, date)
    )
    """

    insert_sql = f"""
    INSERT INTO {target_table}
    (
        latitude,
        longitude,
        date,
        temp_max,
        temp_min,
        precipitation,
        weather_code
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    try:
        cur.execute(create_table_sql)
        cur.execute("BEGIN;")
        cur.execute(f"DELETE FROM {target_table}")
        cur.executemany(insert_sql, records)
        cur.execute("COMMIT;")
        print(f"Successfully loaded {len(records)} weather records.")

    except Exception as e:
        cur.execute("ROLLBACK;")
        print(f"Load failed: {e}")
        raise


with DAG(
    dag_id = 'Homework3_Weather',
    start_date = datetime(2026,2,23),
    catchup=False,
    tags=['ETL'],
    schedule = '30 2 * * *'
) as dag:
    target_table = "DEMO_DB.RAW.WEATHER_DATA_HW"
    data = extract()    
    lines = transform(data)
    load(lines, target_table)
