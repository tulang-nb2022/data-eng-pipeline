import pandas as pd
import numpy as np
from datetime import datetime
import os
import json
from great_expectations.data_context import FileDataContext
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.validator.validator import Validator
from great_expectations.core import ExpectationConfiguration
from great_expectations.core.expectation_suite import ExpectationSuite
import great_expectations as ge

def create_expectation_suite(context: FileDataContext, suite_name: str = "weather_data_suite"):
    """Create a Great Expectations suite for weather data validation"""
    
    # Create or get the expectation suite
    try:
        suite = context.get_expectation_suite(suite_name)
    except:
        suite = context.create_expectation_suite(suite_name)
    
    # Add expectations for weather data
    expectations = [
        # Data presence expectations
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "processing_timestamp"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "year"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "month"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "day"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "data_source"}
        ),
        
        # Data quality expectations
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "processing_timestamp"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "year"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "month"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "day"}
        ),
        
        # Data type expectations
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={"column": "year", "min_value": 2020, "max_value": 2030}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={"column": "month", "min_value": 1, "max_value": 12}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={"column": "day", "min_value": 1, "max_value": 31}
        ),
        
        # Data source validation
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={"column": "data_source", "value_set": ["noaa", "alphavantage", "eosdis"]}
        ),
        
        # Temperature validation (if present)
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={"column": "temperature", "min_value": -100, "max_value": 150}
        ),
        
        # Wind speed validation (if present)
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "wind_speed"}
        ),
        
        # Row count expectations
        ExpectationConfiguration(
            expectation_type="expect_table_row_count_to_be_between",
            kwargs={"min_value": 1, "max_value": 1000000}
        )
    ]
    
    # Add expectations to suite
    for expectation in expectations:
        suite.add_expectation(expectation)
    
    # Save the suite
    context.save_expectation_suite(suite)
    return suite

def validate_data_with_great_expectations(data_path: str, context_path: str = "great_expectations"):
    """Validate data using Great Expectations"""
    
    # Initialize Great Expectations context
    context = FileDataContext(project_root_dir=context_path)
    
    # Create expectation suite
    suite = create_expectation_suite(context)
    
    # Read data using pandas
    if data_path.startswith("s3://"):
        print("S3 reading not implemented - use local path")
        return None
    
    # Read all parquet files recursively
    import glob
    parquet_files = glob.glob(f"{data_path}/**/*.parquet", recursive=True)
    
    if not parquet_files:
        print(f"No parquet files found in {data_path}")
        return None
    
    print(f"Found {len(parquet_files)} parquet files")
    
    # Read and combine all parquet files
    dfs = []
    for file in parquet_files:
        try:
            df = pd.read_parquet(file)
            dfs.append(df)
            print(f"Loaded: {file}")
        except Exception as e:
            print(f"Error loading {file}: {e}")
    
    if not dfs:
        print("No data found in parquet files")
        return None
    
    # Combine all dataframes
    combined_df = pd.concat(dfs, ignore_index=True)
    
    # Create batch request for Great Expectations
    batch_request = RuntimeBatchRequest(
        datasource_name="pandas_datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="weather_data",
        runtime_parameters={"batch_data": combined_df},
        batch_identifiers={"default_identifier_name": "default_identifier"}
    )
    
    # Create validator
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite=suite
    )
    
    # Run validation
    results = validator.validate()
    
    # Process results
    validation_results = {
        "total_records": len(combined_df),
        "expectations_checked": len(results.results),
        "successful_expectations": len([r for r in results.results if r.success]),
        "failed_expectations": len([r for r in results.results if not r.success]),
        "success_rate": len([r for r in results.results if r.success]) / len(results.results) if results.results else 0,
        "results": results.results
    }
    
    # Print results
    print("\n" + "="*50)
    print("GREAT EXPECTATIONS VALIDATION RESULTS")
    print("="*50)
    print(f"Total records: {validation_results['total_records']}")
    print(f"Expectations checked: {validation_results['expectations_checked']}")
    print(f"Successful expectations: {validation_results['successful_expectations']}")
    print(f"Failed expectations: {validation_results['failed_expectations']}")
    print(f"Success rate: {validation_results['success_rate']:.2%}")
    
    # Print detailed results
    print("\nDetailed Results:")
    for result in results.results:
        status = "✅ PASS" if result.success else "❌ FAIL"
        print(f"{status} {result.expectation_config.expectation_type}")
        if not result.success:
            print(f"  Details: {result.result}")
    
    return validation_results

def initialize_great_expectations_project(project_root: str = "great_expectations"):
    """Initialize Great Expectations project structure"""
    
    # Create project directory
    os.makedirs(project_root, exist_ok=True)
    
    # Initialize Great Expectations context
    context = FileDataContext.create(project_root_dir=project_root)
    
    # Create datasource configuration
    datasource_config = {
        "name": "pandas_datasource",
        "class_name": "Datasource",
        "execution_engine": {
            "class_name": "PandasExecutionEngine"
        },
        "data_connectors": {
            "default_runtime_data_connector_name": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["default_identifier_name"]
            }
        }
    }
    
    # Add datasource to context
    context.add_datasource(**datasource_config)
    
    print(f"Great Expectations project initialized in: {project_root}")
    print("Project structure created successfully!")
    
    return context

if __name__ == "__main__":
    # Initialize Great Expectations project
    print("Initializing Great Expectations project...")
    context = initialize_great_expectations_project()
    
    # Validate data
    data_path = "data/processed"
    results = validate_data_with_great_expectations(data_path)
    
    if results:
        print(f"\nValidation completed with {results['success_rate']:.2%} success rate")
    else:
        print("Validation failed - no data found") 