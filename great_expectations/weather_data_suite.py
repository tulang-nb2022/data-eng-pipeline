import pandas as pd
import numpy as np
from datetime import datetime
import os
import json
import great_expectations as ge
from great_expectations.data_context import FileDataContext
from great_expectations.core.batch import RuntimeBatchRequest

def create_simple_expectation_suite(context: FileDataContext, suite_name: str = "weather_data_suite"):
    """Create a simple but comprehensive expectation suite for weather data"""
    
    # Create expectation suite
    suite = context.create_expectation_suite(suite_name, overwrite_existing=True)
    
    # Add basic expectations using the simple API
    suite.add_expectation(
        ge.core.ExpectationConfiguration(
            expectation_type="expect_table_columns_to_match_ordered_list",
            kwargs={
                "column_list": ["processing_timestamp", "year", "month", "day", "data_source"]
            }
        )
    )
    
    suite.add_expectation(
        ge.core.ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "processing_timestamp"}
        )
    )
    
    suite.add_expectation(
        ge.core.ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "data_source"}
        )
    )
    
    suite.add_expectation(
        ge.core.ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "data_source",
                "value_set": ["noaa", "alphavantage", "eosdis"]
            }
        )
    )
    
    suite.add_expectation(
        ge.core.ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "year",
                "min_value": 2020,
                "max_value": 2030
            }
        )
    )
    
    suite.add_expectation(
        ge.core.ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "month",
                "min_value": 1,
                "max_value": 12
            }
        )
    )
    
    suite.add_expectation(
        ge.core.ExpectationConfiguration(
            expectation_type="expect_table_row_count_to_be_between",
            kwargs={
                "min_value": 1,
                "max_value": 1000000
            }
        )
    )
    
    # Save the suite
    context.save_expectation_suite(suite)
    return suite

def validate_weather_data_simple(data_path: str, context_path: str = "great_expectations"):
    """Validate weather data using Great Expectations with simplified API"""
    
    print("="*60)
    print("GREAT EXPECTATIONS DATA QUALITY VALIDATION")
    print("="*60)
    
    # Initialize Great Expectations context
    try:
        context = FileDataContext(project_root_dir=context_path)
        print("✅ Great Expectations context initialized")
    except Exception as e:
        print(f"❌ Failed to initialize context: {e}")
        return None
    
    # Create expectation suite
    try:
        suite = create_simple_expectation_suite(context)
        print("✅ Expectation suite created successfully")
    except Exception as e:
        print(f"❌ Failed to create expectation suite: {e}")
        return None
    
    # Read data using pandas
    if data_path.startswith("s3://"):
        print("❌ S3 reading not implemented - use local path")
        return None
    
    # Read all parquet files recursively
    import glob
    parquet_files = glob.glob(f"{data_path}/**/*.parquet", recursive=True)
    
    if not parquet_files:
        print(f"❌ No parquet files found in {data_path}")
        return None
    
    print(f"📁 Found {len(parquet_files)} parquet files")
    
    # Read and combine all parquet files
    dfs = []
    for file in parquet_files:
        try:
            df = pd.read_parquet(file)
            dfs.append(df)
            print(f"📄 Loaded: {file} ({len(df)} rows)")
        except Exception as e:
            print(f"❌ Error loading {file}: {e}")
    
    if not dfs:
        print("❌ No data found in parquet files")
        return None
    
    # Combine all dataframes
    combined_df = pd.concat(dfs, ignore_index=True)
    print(f"📊 Combined dataset: {len(combined_df)} total rows")
    
    # Create batch request for Great Expectations
    try:
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
        
        # Process and display results
        display_validation_results(results, combined_df)
        
        return {
            "success": results.success,
            "total_records": len(combined_df),
            "expectations_checked": len(results.results),
            "successful_expectations": len([r for r in results.results if r.success]),
            "failed_expectations": len([r for r in results.results if not r.success])
        }
        
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        return None

def display_validation_results(results, df):
    """Display validation results in a professional format"""
    
    print("\n" + "="*60)
    print("VALIDATION RESULTS")
    print("="*60)
    
    # Overall statistics
    total_expectations = len(results.results)
    successful_expectations = len([r for r in results.results if r.success])
    failed_expectations = total_expectations - successful_expectations
    success_rate = (successful_expectations / total_expectations * 100) if total_expectations > 0 else 0
    
    print(f"📈 Overall Success Rate: {success_rate:.1f}%")
    print(f"✅ Successful Expectations: {successful_expectations}/{total_expectations}")
    print(f"❌ Failed Expectations: {failed_expectations}/{total_expectations}")
    print(f"📊 Total Records Processed: {len(df):,}")
    
    # Detailed results
    print("\n📋 Detailed Results:")
    print("-" * 50)
    
    for i, result in enumerate(results.results, 1):
        expectation_type = result.expectation_config.expectation_type
        success = result.success
        
        status_icon = "✅" if success else "❌"
        status_text = "PASS" if success else "FAIL"
        
        print(f"{i:2d}. {status_icon} {status_text} - {expectation_type}")
        
        if not success:
            # Show details for failed expectations
            if hasattr(result, 'result') and result.result:
                unexpected_values = result.result.get('unexpected_values', [])
                if unexpected_values:
                    print(f"    Unexpected values: {unexpected_values[:5]}...")  # Show first 5
                else:
                    print(f"    Details: {result.result}")
    
    # Data quality insights
    print("\n🔍 Data Quality Insights:")
    print("-" * 30)
    
    if "data_source" in df.columns:
        source_counts = df["data_source"].value_counts()
        print("Data sources distribution:")
        for source, count in source_counts.items():
            print(f"  {source}: {count:,} records")
    
    if "year" in df.columns:
        year_range = f"{df['year'].min()} - {df['year'].max()}"
        print(f"Year range: {year_range}")
    
    if "month" in df.columns:
        month_distribution = df["month"].value_counts().sort_index()
        print("Month distribution:")
        for month, count in month_distribution.items():
            print(f"  Month {month}: {count:,} records")
    
    print("\n" + "="*60)

def initialize_great_expectations_simple(project_root: str = "great_expectations"):
    """Initialize Great Expectations project with simplified setup"""
    
    print("🚀 Initializing Great Expectations project...")
    
    # Create project directory
    os.makedirs(project_root, exist_ok=True)
    
    try:
        # Initialize Great Expectations context using the correct method
        context = FileDataContext(project_root_dir=project_root)
        
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
        
        print(f"✅ Great Expectations project initialized in: {project_root}")
        print("✅ Project structure created successfully!")
        
        return context
        
    except Exception as e:
        print(f"❌ Failed to initialize Great Expectations: {e}")
        return None

def run_data_quality_validation():
    """Main function to run comprehensive data quality validation"""
    
    print("🔍 Starting Data Quality Validation Pipeline")
    print("="*60)
    
    # Initialize Great Expectations
    context = initialize_great_expectations_simple()
    if not context:
        print("❌ Failed to initialize Great Expectations")
        return
    
    # Validate data
    data_path = "data/processed"
    results = validate_weather_data_simple(data_path)
    
    if results:
        print(f"\n🎉 Validation completed!")
        print(f"📊 Success Rate: {results['successful_expectations']}/{results['expectations_checked']} expectations passed")
        
        if results['success']:
            print("✅ All data quality checks passed!")
        else:
            print("⚠️  Some data quality issues detected - review results above")
    else:
        print("❌ Validation failed - no data found or processing error")

if __name__ == "__main__":
    run_data_quality_validation() 