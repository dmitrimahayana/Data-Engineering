from datetime import timedelta, datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from moduleFlights.CollectFlights import Collect_Flights
# from airflow.operators.python_operator import PythonOperator

## Define the default arguments for the DAG
default_args = {
    'owner': 'Dmitri',
    'start_date': datetime(2023, 9, 12),
    'retries': 1,  # Number of retries if a task fails
    'retry_delay': timedelta(minutes=5),  # Time between retries
}

## Create a DAG instance
dag = DAG(
    'Snowflake_Flights_ETL_Source_Postgres',
    default_args=default_args,
    description='An Airflow DAG to perform ETL data from Postgres to Snowflake',
    schedule_interval=None,  # Set the schedule interval (e.g., None for manual runs)
    catchup=False  # Do not backfill (run past dates) when starting the DAG
)

## Variable Config Snowflake
SNOWFLAKE_CONN = "snowflake_default"
SNOWFLAKE_DB = "FLIGHTS"
SNOWFLAKE_SCHEMA = "FLIGHT_SCHEMA"
SNOWFLAKE_TABLE = "Flight_List"

## Tas SnowflakeOperator: Create Table
CREATE_TABLE_SQL_STRING = (
    f"CREATE OR REPLACE TRANSIENT TABLE {SNOWFLAKE_TABLE} "
    f"(id INT, aircraft_type VARCHAR(250), flight_rules VARCHAR(250), plots_altitude INT, plots_baro_vert_rate FLOAT, plots_mach FLOAT, plots_measured_flight_level FLOAT, start_time DATETIME, time_of_track DATETIME);"
)
snowflake_create_table = SnowflakeOperator(
    task_id = "snowflake_create_table", 
    sql = CREATE_TABLE_SQL_STRING,
    snowflake_conn_id = SNOWFLAKE_CONN,
    dag = dag
)

## Function to Extract Data from Flight Dataset
def extract_flight_data():
    LIMIT_JSON_FILE = 100000
    collect_obj = Collect_Flights('/opt/airflow/dataset/Revalue_Nature/Case 2/', LIMIT_JSON_FILE)
    df = collect_obj.collect_data()
    
    # Convert SQL Insert Format
    sql_list = []
    for index, row in df.iterrows():
            sql_values = ", ".join([str(val) if isinstance(val, (int, float)) else f"'{val}'" for val in row])
            sql_statement = f"INSERT INTO {SNOWFLAKE_TABLE} VALUES ({sql_values});"
            sql_list.append(sql_statement)
            
    sql_multiple_stmts = " ".join(sql_list)
    print(sql_multiple_stmts)

    return sql_multiple_stmts

## Task SnowflakeOperator: insert Data into Table
sql_multiple_stmts = extract_flight_data()
snowflake_insert_data = SnowflakeOperator(
    task_id = "snowflake_insert_data",
    # sql = f"INSERT INTO {SNOWFLAKE_TABLE} VALUES (100000, 'B738', 'I', 25000.0, 3037.5, 0.6, 146.25, '2016-10-20 11:37:51.764000', '2016-10-20 11:40:02.898437'); INSERT INTO {SNOWFLAKE_TABLE} VALUES (100000, 'B738', 'I', 25000.0, 3200.0, 0.608, 149.0, '2016-10-20 11:37:51.764000', '2016-10-20 11:40:07.898437');",
    sql = sql_multiple_stmts,
    split_statements = True,
    snowflake_conn_id = SNOWFLAKE_CONN,
    dag = dag
)

## Task SnowflakeOperator: Query Table
snowflake_count_row = SnowflakeOperator(
    task_id = "snowflake_count_row",
    sql = f"SELECT * FROM {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}",
    snowflake_conn_id = SNOWFLAKE_CONN,
    dag = dag
)

## Define task
snowflake_create_table >> snowflake_insert_data >> snowflake_count_row