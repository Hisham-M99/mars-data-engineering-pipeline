from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
NASA_API_KEY_VALUE = "mnaWSH658TvacBhr4cvOVWyUbIWb8pmHBgeGN1xr"
# Define the base path for your project. This should be the root of your data_engineering_portfolio directory.
# IMPORTANT: Replace this with the actual absolute path on your Airflow worker machine.
PROJECT_BASE_PATH = os.environ.get("AIRFLOW_HOME", "/opt/airflow") + "/dags/data_engineering_portfolio"
#PROJECT_BASE_PATH = os.environ.get"/home/hisham/airflow_work/airflow_2.9_home/dags/data_engineering_portfolio/python scripts"
# Define the path where you will extract your Talend jobs
# IMPORTANT: Replace this with the actual absolute path on your Airflow worker machine
TALEND_JOBS_BASE_PATH = os.environ.get("AIRFLOW_HOME", "/opt/airflow") + "/talend_executables"
#TALEND_JOBS_BASE_PATH = os.environ.get"/home/hisham/airflow_work/airflow_2.9_home/talend_executables/"
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='mars_etl_pipeline',
    default_args=default_args,
    description='Comprehensive Airflow DAG for Mars Data ETL Pipeline',
    schedule_interval=timedelta(days=1),  # Example: run daily
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['mars', 'etl', 'data_pipeline', 'talend'],
) as dag:
    # Task 1: Extract Mars Rover Photos
    extract_rover_photos = BashOperator(
        task_id='extract_mars_rover_photos',
        bash_command=f'export NASA_API_KEY={NASA_API_KEY_VALUE} && python {PROJECT_BASE_PATH}/scripts/extract_mars_rover_photos.py',
    )

    # Task 2: Extract Insight Weather Data
    extract_insight_weather = BashOperator(
        task_id='extract_insight_weather',
        bash_command=f'export NASA_API_KEY={NASA_API_KEY_VALUE} && python {PROJECT_BASE_PATH}/scripts/extract_insight_weather.py',
    )

    # Task 3: Convert JSON to Parquet
    convert_json_to_parquet = BashOperator(
        task_id='convert_json_to_parquet',
        bash_command=f'python {PROJECT_BASE_PATH}/scripts/convert_json_to_parquet.py',
    )

    # Task 4: Clean and Transform Data
    clean_and_transform = BashOperator(
        task_id='clean_and_transform',
        bash_command=f'python {PROJECT_BASE_PATH}/scripts/clean_and_transform.py',
    )

    # Task 5: Convert Parquet to CSV for Talend
    convert_parquet_to_csv = BashOperator(
        task_id='convert_parquet_to_csv',
        bash_command=f'python {PROJECT_BASE_PATH}/scripts/convert_parquet_to_csv.py',
    )

    # Task 6: Load Dimension Tables using Talend Job
    load_dim_tables_talend = BashOperator(
        task_id='load_dim_tables_talend',
    #    bash_command=f'{TALEND_JOBS_BASE_PATH}/Job_Load_Dim_Tables/Job_Load_Dim_Tables_run.sh',
        bash_command="{{ '" + TALEND_JOBS_BASE_PATH + "/Job_Load_Dim_Tables_0.1/Job_Load_Dim_Tables/Job_Load_Dim_Tables_run.sh' }}",

    )

    # Task 7: Load Fact Table using Talend Job
    load_fact_table_talend = BashOperator(
        task_id='load_fact_table_talend',
   #     bash_command=f'{TALEND_JOBS_BASE_PATH}/Job_Load_Fact_Table/Job_Load_Fact_Table_run.sh',
        bash_command="{{ '" + TALEND_JOBS_BASE_PATH + "/Job_Load_Fact_Table_0.1/Job_Load_Fact_Table/Job_Load_Fact_Table_run.sh' }}",
    )

    # Define task dependencies
    [extract_rover_photos, extract_insight_weather] >> convert_json_to_parquet
    convert_json_to_parquet >> clean_and_transform
    clean_and_transform >> convert_parquet_to_csv
    convert_parquet_to_csv >> load_dim_tables_talend
    load_dim_tables_talend >> load_fact_table_talend



