from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.sensors.athena import AthenaSensor
from datetime import datetime, timedelta
import boto3
import json

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
    description='Monitor weather data quality and generate reports',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False
)

def check_data_quality(**context):
    # Read validation results from S3
    s3 = boto3.client('s3')
    response = s3.get_object(
        Bucket='your-validation-results-bucket',
        Key='weather_data_validation_results.json'
    )
    results = json.loads(response['Body'].read())
    
    # Check if any validations failed
    failed_validations = [
        result for result in results['results']
        if not result['success']
    ]
    
    if failed_validations:
        raise Exception(f"Data quality check failed: {failed_validations}")
    
    return "Data quality check passed"

# Define Athena queries
data_quality_query = """
SELECT 
    station_id,
    AVG(data_quality_score) as avg_quality_score,
    COUNT(*) as record_count,
    MIN(processing_timestamp) as first_record,
    MAX(processing_timestamp) as last_record
FROM weather_data
WHERE date >= current_date - interval '1' day
GROUP BY station_id
"""

anomaly_query = """
SELECT 
    station_id,
    date,
    COUNT(*) as anomaly_count
FROM weather_data
WHERE temperature_anomaly = true
    AND date >= current_date - interval '1' day
GROUP BY station_id, date
"""

# Create tasks
check_quality = PythonOperator(
    task_id='check_data_quality',
    python_callable=check_data_quality,
    dag=dag
)

run_quality_query = AthenaOperator(
    task_id='run_quality_query',
    query=data_quality_query,
    database='weather_analytics',
    output_location='s3://your-bucket/athena-results/quality/',
    aws_conn_id='aws_default',
    dag=dag
)

wait_for_quality = AthenaSensor(
    task_id='wait_for_quality',
    query_execution_id=run_quality_query.output,
    aws_conn_id='aws_default',
    dag=dag
)

run_anomaly_query = AthenaOperator(
    task_id='run_anomaly_query',
    query=anomaly_query,
    database='weather_analytics',
    output_location='s3://your-bucket/athena-results/anomalies/',
    aws_conn_id='aws_default',
    dag=dag
)

wait_for_anomaly = AthenaSensor(
    task_id='wait_for_anomaly',
    query_execution_id=run_anomaly_query.output,
    aws_conn_id='aws_default',
    dag=dag
)

# Set task dependencies
check_quality >> run_quality_query >> wait_for_quality
check_quality >> run_anomaly_query >> wait_for_anomaly 