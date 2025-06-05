import great_expectations as ge
from great_expectations.dataset import SparkDFDataset
from great_expectations.core import ExpectationSuite
from great_expectations.data_context import DataContext
from great_expectations.validator import Validator
import boto3
import os

def create_weather_expectation_suite():
    context = DataContext()
    suite = context.create_expectation_suite(
        "weather_data_suite",
        overwrite_existing=True
    )
    
    # Connect to Athena
    athena_client = boto3.client('athena')
    
    # Define expectations
    expectations = [
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {
                "column": "temperature_celsius",
                "mostly": 0.95
            }
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "temperature_celsius",
                "min_value": -90,
                "max_value": 60,
                "mostly": 0.95
            }
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {
                "column": "precipitation_mm",
                "mostly": 0.95
            }
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "precipitation_mm",
                "min_value": 0,
                "max_value": 1000,
                "mostly": 0.95
            }
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {
                "column": "station_id"
            }
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {
                "column": "date"
            }
        }
    ]
    
    # Add expectations to suite
    for expectation in expectations:
        suite.add_expectation(expectation)
    
    return suite

def validate_weather_data(spark_session, s3_path):
    # Read data from S3
    df = spark_session.read.parquet(s3_path)
    
    # Create Great Expectations dataset
    ge_df = SparkDFDataset(df)
    
    # Get validation suite
    suite = create_weather_expectation_suite()
    
    # Run validations
    results = ge_df.validate(suite)
    
    # Save validation results
    results.save_to_s3(
        bucket="your-validation-results-bucket",
        key="weather_data_validation_results.json"
    )
    
    return results

if __name__ == "__main__":
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("Weather Data Validation") \
        .getOrCreate()
    
    s3_path = "s3://your-bucket/weather-data/"
    results = validate_weather_data(spark, s3_path)
    
    # Print validation results
    print(results) 