from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

# Define your Snowflake connection ID
SNOWFLAKE_CONN_ID = 'snowflake'

# List of fields to extract
FIELDS_TO_EXTRACT = [
    'AREA_NM',
    'MALE_PPLTN_RATE',
    'FEMALE_PPLTN_RATE',
    'PPLTN_RATE_0',
    'PPLTN_RATE_10',
    'PPLTN_RATE_20',
    'PPLTN_RATE_30',
    'PPLTN_RATE_40',
    'PPLTN_RATE_50',
    'PPLTN_RATE_60',
    'PPLTN_RATE_70',
]

# Define default_args and other DAG configurations as needed
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'extract_and_append_to_existing_table',
    default_args=default_args,
    description='DAG to extract specific fields from CityData table and append to an existing table',
    schedule_interval=None,
    catchup=False,
) as dag:

    # Set up SnowflakeOperator to delete existing data in the table
    delete_data_query = """
    DELETE FROM DEV.analytics.PopulationData;
    """
    delete_data_task = SnowflakeOperator(
        task_id='delete_existing_data_from_population_data',
        sql=delete_data_query,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    # Set up SnowflakeOperator to insert data
    insert_data_query = f"""
    INSERT INTO DEV.analytics.PopulationData ({', '.join(FIELDS_TO_EXTRACT)})
    SELECT {', '.join(FIELDS_TO_EXTRACT)}
    FROM DEV.RAW_DATA.CityData;
    """
    insert_data_task = SnowflakeOperator(
        task_id='insert_data_into_analytics_population_data',
        sql=insert_data_query,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    # Set up task dependencies
    delete_data_task >> insert_data_task
