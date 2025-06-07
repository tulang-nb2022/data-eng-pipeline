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
        # Location metadata validation
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {
                "column": "grid_id",
                "mostly": 1.0
            }
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {
                "column": "forecast_office",
                "mostly": 1.0
            }
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {
                "column": "city",
                "mostly": 1.0
            }
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {
                "column": "state",
                "mostly": 1.0
            }
        },
        
        # URL validation
        {
            "expectation_type": "expect_column_values_to_match_regex",
            "kwargs": {
                "column": "forecast_url",
                "regex": "^https://api.weather.gov/gridpoints/.*$",
                "mostly": 1.0
            }
        },
        {
            "expectation_type": "expect_column_values_to_match_regex",
            "kwargs": {
                "column": "forecast_hourly_url",
                "regex": "^https://api.weather.gov/gridpoints/.*$",
                "mostly": 1.0
            }
        },
        
        # Distance and bearing validation
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "distance_meters",
                "min_value": 0,
                "max_value": 100000,
                "mostly": 0.95
            }
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "bearing_degrees",
                "min_value": 0,
                "max_value": 360,
                "mostly": 0.95
            }
        },
        
        # Data quality metrics validation
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "data_completeness_score",
                "min_value": 0,
                "max_value": 1,
                "mostly": 1.0
            }
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "location_accuracy_score",
                "min_value": 0,
                "max_value": 1,
                "mostly": 1.0
            }
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "overall_quality_score",
                "min_value": 0,
                "max_value": 1,
                "mostly": 1.0
            }
        },
        
        # Time-based validation
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {
                "column": "processing_timestamp",
                "mostly": 1.0
            }
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "data_freshness_minutes",
                "min_value": 0,
                "max_value": 60,
                "mostly": 0.95
            }
        },
        
        # Pattern validation
        {
            "expectation_type": "expect_column_values_to_be_in_set",
            "kwargs": {
                "column": "weather_pattern",
                "value_set": ["Coastal Summer", "Coastal Winter", "Inland Summer", "Inland Winter", "Transitional"],
                "mostly": 1.0
            }
        },
        {
            "expectation_type": "expect_column_values_to_be_in_set",
            "kwargs": {
                "column": "season",
                "value_set": ["Spring", "Summer", "Fall", "Winter"],
                "mostly": 1.0
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