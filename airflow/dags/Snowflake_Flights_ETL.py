from datetime import timedelta, datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

## Define the default arguments for the DAG
default_args = {
    'owner': 'Dmitri',
    'start_date': datetime(2023, 9, 12),
    'retries': 1,  # Number of retries if a task fails
    'retry_delay': timedelta(minutes=5),  # Time between retries
}

## Create a DAG instance
dag = DAG(
    'Snowflake_Flights_ETL',
    default_args=default_args,
    description='An Airflow DAG for Snowflake',
    schedule_interval=None,  # Set the schedule interval (e.g., None for manual runs)
    catchup=False  # Do not backfill (run past dates) when starting the DAG
)

## Variable Config Snowflake
SNOWFLAKE_CONN = "snowflake_default"
SNOWFLAKE_DB = "FLIGHTS"
SNOWFLAKE_SCHEMA = "FLIGHT_SCHEMA"
SNOWFLAKE_TABLE = "DUMMY_TABLE"

## Task SnowflakeOperator: Query Table
snowflake_count_data = SnowflakeOperator(
    task_id = "snowflake_count_data",
    sql = f"SELECT * FROM {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} WHERE NAME LIKE '%I%'",
    snowflake_conn_id = SNOWFLAKE_CONN,
    dag = dag
)

## Define task
snowflake_count_data