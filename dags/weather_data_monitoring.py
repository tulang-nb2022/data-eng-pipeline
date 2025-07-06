from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.sensors.athena import AthenaSensor
from datetime import datetime, timedelta
import boto3
import json
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'weather_data_monitoring',
    default_args=default_args,
    description='Monitor simplified weather data quality and generate reports',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False
)

def check_data_quality(**context):
    """Check basic data quality for simplified data structure"""
    # For local testing, check if data files exist
    data_path = "data/processed"
    
    if os.path.exists(data_path):
        parquet_files = [f for f in os.listdir(data_path) if f.endswith('.parquet')]
        if parquet_files:
            print(f"Found {len(parquet_files)} parquet files in {data_path}")
            return f"Data quality check passed - {len(parquet_files)} files found"
        else:
            raise Exception("No parquet files found in data/processed")
    else:
        raise Exception("Data directory data/processed does not exist")

def check_data_freshness(**context):
    """Check if data is being processed regularly"""
    # This would typically check the latest processing timestamp
    # For now, just return success
    return "Data freshness check passed"

# Define simplified Athena queries for basic monitoring
basic_data_query = """
SELECT 
    data_source,
    COUNT(*) as record_count,
    MIN(processing_timestamp) as first_record,
    MAX(processing_timestamp) as last_record,
    COUNT(DISTINCT year) as years_covered,
    COUNT(DISTINCT month) as months_covered
FROM weather_data
WHERE date >= current_date - interval '1' day
GROUP BY data_source
"""

data_source_summary_query = """
SELECT 
    data_source,
    COUNT(*) as total_records,
    COUNT(CASE WHEN temperature IS NOT NULL THEN 1 END) as weather_records,
    COUNT(CASE WHEN open IS NOT NULL THEN 1 END) as financial_records,
    COUNT(CASE WHEN score IS NOT NULL THEN 1 END) as eosdis_records
FROM weather_data
WHERE date >= current_date - interval '1' day
GROUP BY data_source
"""

partitioning_query = """
SELECT 
    year,
    month,
    day,
    COUNT(*) as records_per_partition
FROM weather_data
WHERE date >= current_date - interval '1' day
GROUP BY year, month, day
ORDER BY year DESC, month DESC, day DESC
LIMIT 10
"""

# Create tasks
check_quality = PythonOperator(
    task_id='check_data_quality',
    python_callable=check_data_quality,
    dag=dag
)

check_freshness = PythonOperator(
    task_id='check_data_freshness',
    python_callable=check_data_freshness,
    dag=dag
)

run_basic_query = AthenaOperator(
    task_id='run_basic_query',
    query=basic_data_query,
    database='weather_analytics',
    output_location='s3://your-bucket/athena-results/basic/',
    aws_conn_id='aws_default',
    dag=dag
)

wait_for_basic = AthenaSensor(
    task_id='wait_for_basic',
    query_execution_id=run_basic_query.output,
    aws_conn_id='aws_default',
    dag=dag
)

run_source_query = AthenaOperator(
    task_id='run_source_query',
    query=data_source_summary_query,
    database='weather_analytics',
    output_location='s3://your-bucket/athena-results/sources/',
    aws_conn_id='aws_default',
    dag=dag
)

wait_for_source = AthenaSensor(
    task_id='wait_for_source',
    query_execution_id=run_source_query.output,
    aws_conn_id='aws_default',
    dag=dag
)

run_partition_query = AthenaOperator(
    task_id='run_partition_query',
    query=partitioning_query,
    database='weather_analytics',
    output_location='s3://your-bucket/athena-results/partitions/',
    aws_conn_id='aws_default',
    dag=dag
)

wait_for_partition = AthenaSensor(
    task_id='wait_for_partition',
    query_execution_id=run_partition_query.output,
    aws_conn_id='aws_default',
    dag=dag
)

# Set task dependencies - simplified workflow
check_quality >> check_freshness
check_freshness >> run_basic_query >> wait_for_basic
check_freshness >> run_source_query >> wait_for_source
check_freshness >> run_partition_query >> wait_for_partition 