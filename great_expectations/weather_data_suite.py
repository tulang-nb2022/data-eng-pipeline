import great_expectations as ge
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
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
    
    # Define expectations for simplified data structure
    expectations = [
        # Basic data presence validation
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {
                "column": "processing_timestamp",
                "mostly": 1.0
            }
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {
                "column": "year",
                "mostly": 1.0
            }
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {
                "column": "month",
                "mostly": 1.0
            }
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {
                "column": "day",
                "mostly": 1.0
            }
        },
        
        # Data source validation
        {
            "expectation_type": "expect_column_values_to_be_in_set",
            "kwargs": {
                "column": "data_source",
                "value_set": ["noaa", "alphavantage", "eosdis"],
                "mostly": 1.0
            }
        },
        
        # Time-based validation
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "year",
                "min_value": 2020,
                "max_value": 2030,
                "mostly": 1.0
            }
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "month",
                "min_value": 1,
                "max_value": 12,
                "mostly": 1.0
            }
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "day",
                "min_value": 1,
                "max_value": 31,
                "mostly": 1.0
            }
        },
        
        # Optional field validation (these may be null depending on data source)
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {
                "column": "temperature",
                "mostly": 0.8  # Allow some nulls for non-weather data
            }
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {
                "column": "city",
                "mostly": 0.8
            }
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {
                "column": "open",
                "mostly": 0.8  # Allow some nulls for non-financial data
            }
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {
                "column": "close",
                "mostly": 0.8
            }
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {
                "column": "score",
                "mostly": 0.8  # Allow some nulls for non-EOSDIS data
            }
        }
    ]
    
    # Add expectations to suite
    for expectation in expectations:
        suite.add_expectation(expectation)
    
    return suite

def validate_weather_data(spark_session, data_path):
    # Read data from local path or S3
    if data_path.startswith("s3://"):
        df = spark_session.read.parquet(data_path)
    else:
        df = spark_session.read.parquet(data_path)
    
    # Create Great Expectations dataset
    ge_df = SparkDFDataset(df)
    
    # Get validation suite
    suite = create_weather_expectation_suite()
    
    # Run validations
    results = ge_df.validate(suite)
    
    # Print validation results
    print("Validation Results:")
    print(f"Total expectations: {len(results.results)}")
    print(f"Successful: {len([r for r in results.results if r.success])}")
    print(f"Failed: {len([r for r in results.results if not r.success])}")
    
    # Print failed expectations
    failed_expectations = [r for r in results.results if not r.success]
    if failed_expectations:
        print("\nFailed Expectations:")
        for result in failed_expectations:
            print(f"- {result.expectation_config.expectation_type}: {result.result}")
    
    return results

if __name__ == "__main__":
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("Weather Data Validation") \
        .getOrCreate()
    
    # Use local path for testing
    data_path = "data/processed"
    results = validate_weather_data(spark, data_path)
    
    # Print validation results
    print(results) 