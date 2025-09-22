#!/usr/bin/env python3
"""
Weather Data Pipeline DAG
Orchestrates: Bronze (55min streaming) -> Silver (batch) -> Gold (batch) -> Great Expectations -> S3 Copy
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import subprocess
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# DAG definition
dag = DAG(
    'weather_data_pipeline',
    default_args=default_args,
    description='Complete weather data pipeline: Bronze -> Silver -> Gold -> Validation -> S3 Copy',
    schedule_interval=None,  # Manual trigger only
    max_active_runs=1,
    tags=['weather', 'data-pipeline', 'bronze', 'silver', 'gold']
)

def start_bronze_streaming(**context):
    """Start bronze layer streaming for 55 minutes"""
    logger.info("Starting bronze layer streaming for 55 minutes...")
    
    # Start the bronze streaming job in background
    cmd = ["./run_transform.sh", "bronze", "weather-forecast", "noaa", "s3://data-eng-bucket-345/bronze/weather"]
    
    logger.info(f"Running command: {' '.join(cmd)}")
    
    # Start the process
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
    # Store the process ID for later cleanup
    context['task_instance'].xcom_push(key='bronze_process_id', value=process.pid)
    
    # Let it run for 55 minutes (3300 seconds)
    logger.info("Bronze streaming started. Will run for 55 minutes...")
    time.sleep(3300)  # 55 minutes
    
    # Stop the process
    logger.info("Stopping bronze streaming after 55 minutes...")
    process.terminate()
    process.wait()
    
    logger.info("Bronze streaming completed successfully!")
    return "Bronze streaming completed"

def stop_bronze_and_start_silver(**context):
    """Stop bronze streaming and start silver batch processing"""
    logger.info("Stopping bronze streaming and starting silver batch processing...")
    
    # Get the bronze process ID and ensure it's stopped
    bronze_pid = context['task_instance'].xcom_pull(key='bronze_process_id')
    if bronze_pid:
        try:
            import os
            os.kill(bronze_pid, 9)  # Force kill
            logger.info(f"Stopped bronze process {bronze_pid}")
        except ProcessLookupError:
            logger.info("Bronze process already stopped")
    
    # Start silver batch processing
    cmd = [
        "./run_transform.sh", 
        "silver", 
        "weather-forecast", 
        "noaa", 
        "s3://data-eng-bucket-345/bronze/weather", 
        "s3://data-eng-bucket-345/silver/weather"
    ]
    
    logger.info(f"Running silver command: {' '.join(cmd)}")
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode == 0:
        logger.info("Silver batch processing completed successfully!")
        logger.info(f"Silver output: {result.stdout}")
        return "Silver processing completed"
    else:
        logger.error(f"Silver processing failed: {result.stderr}")
        raise Exception(f"Silver processing failed: {result.stderr}")

def run_gold_layer(**context):
    """Run dbt gold layer processing"""
    logger.info("Starting gold layer processing with dbt...")
    
    cmd = [
        "./run_dbt_gold.sh",
        "data_engineering_project",
        "dev",
        "s3://data-eng-bucket-345/silver/weather",
        "s3://data-eng-bucket-345/gold/weather"
    ]
    
    logger.info(f"Running gold command: {' '.join(cmd)}")
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode == 0:
        logger.info("Gold layer processing completed successfully!")
        logger.info(f"Gold output: {result.stdout}")
        return "Gold processing completed"
    else:
        logger.error(f"Gold processing failed: {result.stderr}")
        raise Exception(f"Gold processing failed: {result.stderr}")

def run_great_expectations(**context):
    """Run Great Expectations validation on gold layer data"""
    logger.info("Starting Great Expectations validation...")
    
    cmd = [
        "python",
        "great_expectations/weather_data_suite.py",
        "s3",
        "s3://data-eng-bucket-345/gold/weather"
    ]
    
    logger.info(f"Running validation command: {' '.join(cmd)}")
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode == 0:
        logger.info("Great Expectations validation passed!")
        logger.info(f"Validation output: {result.stdout}")
        return "Validation passed"
    else:
        logger.error(f"Great Expectations validation failed: {result.stderr}")
        raise Exception(f"Validation failed: {result.stderr}")

def copy_to_processed_s3(**context):
    """Copy validated data to processed S3 location"""
    logger.info("Copying validated data to processed S3 location...")
    
    cmd = [
        "aws", "s3", "sync",
        "s3://data-eng-bucket-345/gold/weather/",
        "s3://data-eng-bucket-345/gold/weather/processed/"
    ]
    
    logger.info(f"Running S3 copy command: {' '.join(cmd)}")
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode == 0:
        logger.info("S3 copy completed successfully!")
        logger.info(f"Copy output: {result.stdout}")
        return "S3 copy completed"
    else:
        logger.error(f"S3 copy failed: {result.stderr}")
        raise Exception(f"S3 copy failed: {result.stderr}")

# Task definitions
start_pipeline = EmptyOperator(
    task_id='start_pipeline',
    dag=dag
)

bronze_streaming_task = PythonOperator(
    task_id='bronze_streaming_55min',
    python_callable=start_bronze_streaming,
    dag=dag
)

silver_batch_task = PythonOperator(
    task_id='silver_batch_processing',
    python_callable=stop_bronze_and_start_silver,
    dag=dag
)

gold_batch_task = PythonOperator(
    task_id='gold_batch_processing',
    python_callable=run_gold_layer,
    dag=dag
)

validation_task = PythonOperator(
    task_id='great_expectations_validation',
    python_callable=run_great_expectations,
    dag=dag
)

s3_copy_task = PythonOperator(
    task_id='copy_to_processed_s3',
    python_callable=copy_to_processed_s3,
    dag=dag
)

end_pipeline = EmptyOperator(
    task_id='end_pipeline',
    dag=dag
)

# Task dependencies - sequential pipeline
start_pipeline >> bronze_streaming_task >> silver_batch_task >> gold_batch_task >> validation_task >> s3_copy_task >> end_pipeline
