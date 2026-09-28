# DATA 226 Assignment 3: Airflow Weather Pipeline

This project ports the weather data pipeline from Homework 2 to Apache Airflow.

## Project Description

The Airflow DAG retrieves the previous 60 days of weather data for San Jose, California, from the Open-Meteo API. It transforms the response into individual weather records and loads the records into Snowflake.

The DAG contains three tasks:

```text
extract → transform → load
