from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
import json
import logging
import os
from dotenv import load_dotenv

load_dotenv()

WEATHER_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
WEATHER_API_TOKEN = os.getenv("WEATHER_API_TOKEN")


def kelvin_to_fahrenheit(kelvin):
    return (kelvin - 273.15) * (9/5) + 32


def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_weather_data")
    if not data:
        raise ValueError("No data received from XCom. Check upstream tasks.")

    logging.debug(data)

    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp"])
    feels_like_fahrenheit = kelvin_to_fahrenheit(data["main"]["feels_like"])
    min_temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp_min"])
    max_temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

    transformed_data = {
        "City": city,
        "Description": weather_description,
        "Temperature (F)": temp_fahrenheit,
        "Feels Like (F)": feels_like_fahrenheit,
        "Minimum Temp (F)": min_temp_fahrenheit,
        "Maximum Temp (F)": max_temp_fahrenheit,
        "Pressure": pressure,
        "Humidity": humidity,
        "Wind Speed": wind_speed,
        "Time of Record": time_of_record,
        "Sunrise (Local Time)": sunrise_time,
        "Sunset (Local Time)": sunset_time
    }

    df_data = pd.DataFrame([transformed_data])
    dt_string = datetime.now().strftime("current_weather_data_jerusalem_%d%m%Y%H%M%S")

    csv_string = df_data.to_csv(index=False)

    s3_hook = S3Hook(aws_conn_id='aws_default') 
    s3_hook.load_string(
        string_data=csv_string,
        key=f"{dt_string}.csv",
        bucket_name=WEATHER_BUCKET_NAME,
        replace=True
    )


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 10),
    'email': ['ramimajadbeh@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG('weather_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    is_weather_api_ready = HttpSensor(
        task_id='is_weather_api_ready',
        http_conn_id='weathermap_api',
        endpoint=f'/data/2.5/weather?q=Jerusalem&APPID={WEATHER_API_TOKEN}'
    )

    extract_weather_data = HttpOperator(
        task_id='extract_weather_data',
        http_conn_id='weathermap_api',
        endpoint=f'/data/2.5/weather?q=Jerusalem&APPID={WEATHER_API_TOKEN}',
        method='GET',
        response_filter=lambda r: json.loads(r.text),
        log_response=True
    )

    transform_load_weather_data = PythonOperator(
        task_id='transform_load_weather_data',
        python_callable=transform_load_data,
    )

    is_weather_api_ready >> extract_weather_data >> transform_load_weather_data
