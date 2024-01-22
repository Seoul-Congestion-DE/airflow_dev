from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

# Define your Snowflake connection ID
SNOWFLAKE_CONN_ID = 'snowflake'

# List of fields to extract
FIELDS_TO_EXTRACT = [
    'AREA_NM',
    'AREA_CD',
    'AREA_CONGEST_LVL',
    'AREA_CONGEST_MSG',
    'AREA_PPLTN_MIN',
    'AREA_PPLTN_MAX',
    'RESNT_PPLTN_RATE',
    'NON_RESNT_PPLTN_RATE',
    'REPLACE_YN',
    'PPLTN_TIME'
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
    'congestion_analytics_dag',
    default_args=default_args,
    description='DAG to create citydata_ppltn table and load data from CityData table',
    schedule_interval=None,
    catchup=False,
) as dag:

    # Set up SnowflakeOperator to delete existing data in the table
    delete_data_query = """
    DELETE FROM DEV.analytics.CongestionData;
    """
    delete_data_task = SnowflakeOperator(
        task_id='delete_existing_data_from_citydata_ppltn',
        sql=delete_data_query,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    # Set up SnowflakeOperator to insert data
    insert_data_query = f"""
    INSERT INTO DEV.analytics.CongestionData ({', '.join(FIELDS_TO_EXTRACT)})
    SELECT {', '.join(FIELDS_TO_EXTRACT)}
    FROM DEV.RAW_DATA.CityData;
    """
    insert_data_task = SnowflakeOperator(
        task_id='insert_data_into_citydata_ppltn',
        sql=insert_data_query,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    # Set up task dependencies
    delete_data_task >> insert_data_task
