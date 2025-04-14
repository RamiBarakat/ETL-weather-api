# 🌦️ Weather ETL Pipeline with Apache Airflow & AWS

This is a hands-on ETL project where I built and automated a data pipeline using **Apache Airflow** to fetch live weather data from the OpenWeatherMap API, transform the data, and load it into an **AWS S3** bucket.

I created this project to showcase my understanding of ETL workflows, cloud integration, and orchestration using Airflow — and how to solve real-world infra issues along the way.  

---

## 🚀 Project Overview

This ETL pipeline performs the following steps:

1. **Extract**:  
   - Pings the OpenWeatherMap API to check availability using `HttpSensor`.
   - Extracts current weather data for **Jerusalem** via `HttpOperator`.

2. **Transform**:  
   - Converts temperature values from **Kelvin to Fahrenheit**.
   - Parses and reshapes the raw JSON into a cleaned Pandas DataFrame.

3. **Load**:  
   - Saves the transformed data into a **.csv** file.
   - Uploads it directly into an S3 bucket using `S3Hook`.

---

## 🔧 Stack Used

- **Apache Airflow** (installed and configured on EC2 from scratch)
- **AWS EC2** (Ubuntu instance used to host Airflow)
- **AWS S3** (for storing the processed weather data)
- **OpenWeatherMap API** (free tier API to fetch weather data)
- **Pandas & Python** (for data transformation)
- **Airflow Sensors & Operators** (HttpSensor, HttpOperator, PythonOperator)

---

## 🧠 What I Learned

- Spinning up an **EC2 instance** and setting up Airflow from zero
- Resolving memory issues on `t3.micro` by using `fallocate` to simulate swap space — which got Airflow up and running 💡
- Writing and scheduling a custom **Airflow DAG** with dependencies
- Creating **IAM roles and policies** for secure S3 access from EC2
- Using `S3Hook` to push transformed data into AWS S3 programmatically
- Working with **XComs** to pass data between tasks in Airflow

---

## 📸 Example Output

After each daily run, the pipeline produces a CSV file like:

```csv
City,Description,Temperature (F),Feels Like (F),Minimum Temp (F),Maximum Temp (F),Pressure,Humidty,Wind Speed,Time of Record,Sunrise (Local Time),Sunset (Local Time)
Jerusalem,clear sky,72.5,71.6,70.0,75.0,1012,50,4.1,2025-04-13 05:12:00,2025-04-13 03:52:00,2025-04-13 18:14:00
