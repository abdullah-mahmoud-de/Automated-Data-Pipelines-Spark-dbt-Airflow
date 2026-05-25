import sys
import os
from datetime import datetime, timedelta

# Airflow specific imports
from airflow import DAG
from airflow.operators.python import PythonOperator

# --- Path Configuration ---
# This ensures Airflow can find your custom Python modules inside the src/ folder
# assuming your structure is:
# project_root/
# ├── dags/airflow_dag.py
# └── src/pipeline.py
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

# Now we can safely import your working pipeline function
from pipeline import run_data_pipeline

# --- DAG Configuration ---
# These default arguments align with the project's error handling and retry requirements
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

# --- DAG Definition ---
with DAG(
    'automated_data_pipeline',
    default_args=default_args,
    description='Daily ETL pipeline with Data Quality Gates and SCD2 Logic',
    schedule_interval='@daily', # Runs once a day at midnight
    catchup=False,
    tags=['coursera', 'etl']
) as dag:

    # Define the main execution task
    execute_pipeline_task = PythonOperator(
        task_id='run_full_etl_pipeline',
        python_callable=run_data_pipeline,
        # Airflow will inject the logical execution date (YYYY-MM-DD) into your script 
        # so your Database and API extractors know exactly which records to pull!
        op_kwargs={'execution_date': '{{ ds }}'} 
    )

    # If we had multiple tasks, we would define their order here (e.g., task1 >> task2).
    # Since your pipeline.py handles the internal sequence, we just have one master task.
    execute_pipeline_task