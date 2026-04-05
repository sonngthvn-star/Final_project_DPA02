from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

# Ensure Airflow can see the 'scripts' folder for imports
sys.path.append('/opt/airflow/scripts')

# Import specific functions for each Medallion stage
# (These functions exist within the run_pipeline.py)
from init_db import run_full_setup
from run_pipeline import run_bronze_layer, run_silver_layer, run_validation

# --- Add this function for upgrading version ---
from db_connection import get_db_connection

def on_failure_callback(context):
    """
    Professional Failure Logger:
    Captures the specific task that failed and why, 
    then saves it to the DB so our monitoring is complete.
    """
    dag_run_id = context.get('run_id')
    task_id = context.get('task_instance').task_id
    # We grab the exception and trim it so it fits in the 'message' column
    error_msg = f"Task Failed: {str(context.get('exception'))[:250]}"
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Note: We omit 'log_id' because it is SERIAL (auto-increment)
            cur.execute("""
                INSERT INTO pipeline_logs (dag_run_id, task_name, status, message, execution_time)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                dag_run_id, 
                task_id, 
                "CRITICAL_FAILURE", 
                error_msg, 
                datetime.now()
            ))
            conn.commit()
# --- End of adding section ---

# --- Update DAG default_args for upgrading version---
default_args = {
    'owner': 'Son Nguyen',
    'on_failure_callback': on_failure_callback, # Triggered whenever ANY task fails
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'air_quality_Medallion_pipeline',
    default_args=default_args,
    description='Professional Data Engineering Pipeline for WAQI Air Quality',
    schedule_interval= '@hourly',
    start_date=datetime(2026, 3, 25), # Reverted to original start_date
    catchup=False, 
    tags=['DPA', 'Medallion', 'Environmental', 'Air_quality_ETL']
) as dag:

    # TASK 1: Initialization
    # Ensures DB and tables exist before moving data
    task_init = PythonOperator(
        task_id='database_initialization',
        python_callable=run_full_setup
    )

    # TASK 2: Bronze Layer
    # Ingests raw data from API to the 'bronze' schema/table
    task_bronze = PythonOperator(
        task_id='ingest_bronze_raw',
        python_callable=run_bronze_layer
    )

    # TASK 3: Silver Layer
    # Cleans, transforms, and de-duplicates data into the 'silver' layer
    task_silver = PythonOperator(
        task_id='transform_silver_clean',
        python_callable=run_silver_layer
    )

    # TASK 4: Validation
    # Final check on data quality and integrity
    task_validation = PythonOperator(
        task_id='data_quality_check',
        python_callable=run_validation,
        # This passes the Airflow Run ID directly to python function
        op_kwargs={'dag_run_id': "{{ run_id }}"}
    )

    # DEFINE THE WORKFLOW (The "Medallion Flow")
    task_init >> task_bronze >> task_silver >> task_validation