import pandas as pd
import numpy as np
from datetime import datetime
import os
import json
import great_expectations as ge
from great_expectations.data_context import BaseDataContext
from great_expectations.core.batch import RuntimeBatchRequest
import boto3
from typing import Dict, Any, Optional, List
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_weather_data_expectation_suite(context: BaseDataContext, suite_name: str = "weather_data_suite"):
    """Create a comprehensive expectation suite for weather data validation"""
    
    try:
        suite = context.get_or_create_expectation_suite(suite_name)
        print(f"✅ Created/retrieved expectation suite: {suite_name}")
    except Exception as e:
        print(f"❌ Failed to create expectation suite: {e}")
        return None
    
    # Clear existing expectations
    suite.expectations = []
    
    # Essential column expectations - least likely to fail
    essential_columns = [
        "processing_timestamp", "year", "month", "day", "data_source"
    ]
    
    # Add table-level expectations
    suite.add_expectation(
        ge.core.ExpectationConfiguration(
            expectation_type="expect_table_columns_to_match_ordered_list",
            kwargs={"column_list": essential_columns}
        )
    )
    
    suite.add_expectation(
        ge.core.ExpectationConfiguration(
            expectation_type="expect_table_row_count_to_be_between",
            kwargs={"min_value": 1, "max_value": 10000000}  # Big data range
        )
    )
    
    # Add column-level expectations for essential fields
    for column in essential_columns:
        # Non-null expectations for essential columns
        suite.add_expectation(
            ge.core.ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": column}
            )
        )
    
    # Data source validation
    suite.add_expectation(
        ge.core.ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "data_source",
                "value_set": ["noaa", "alphavantage", "eosdis", "openweather"]
            }
        )
    )
    
    # Date range validations
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
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "day",
                "min_value": 1,
                "max_value": 31
            }
        )
    )
    
    # Save the suite
    try:
        context.save_expectation_suite(suite)
        print("✅ Expectation suite saved successfully")
    except Exception as e:
        print(f"❌ Failed to save expectation suite: {e}")
    
    return suite

def read_data_from_s3(s3_path: str) -> Optional[pd.DataFrame]:
    """Read data from S3 using s3fs"""
    try:
        import s3fs
        fs = s3fs.S3FileSystem()
        
        if s3_path.endswith('/'):
            # Read all parquet files in directory
            parquet_files = fs.glob(f"{s3_path}*.parquet")
            if not parquet_files:
                print(f"❌ No parquet files found in {s3_path}")
                return None
            
            dfs = []
            for file in parquet_files:
                df = pd.read_parquet(f"s3://{file}")
                dfs.append(df)
            
            if dfs:
                return pd.concat(dfs, ignore_index=True)
        else:
            # Read single file
            return pd.read_parquet(s3_path)
            
    except Exception as e:
        print(f"❌ Error reading from S3: {e}")
        return None

def read_data_from_athena(database: str, table: str, region: str = "us-east-1") -> Optional[pd.DataFrame]:
    """Read data from Athena table"""
    try:
        from pyathena import connect
        
        # Create connection
        conn = connect(
            s3_staging_dir='s3://your-athena-results-bucket/temp/',
            region_name=region
        )
        
        # Query the table
        query = f"SELECT * FROM {database}.{table} LIMIT 10000"  # Limit for validation
        df = pd.read_sql(query, conn)
        
        return df
        
    except Exception as e:
        print(f"❌ Error reading from Athena: {e}")
        return None

def validate_weather_data_great_expectations(
    data_source: str,
    context_path: str = "great_expectations",
    **kwargs
) -> Optional[Dict[str, Any]]:
    """Validate weather data using Great Expectations"""
    
    print("="*60)
    print("GREAT EXPECTATIONS DATA QUALITY VALIDATION")
    print("="*60)
    
    # Initialize Great Expectations context
    try:
        context = BaseDataContext(project_root_dir=context_path)
        print("✅ Great Expectations context initialized")
    except Exception as e:
        print(f"❌ Failed to initialize context: {e}")
        return None
    
    # Create expectation suite
    suite = create_weather_data_expectation_suite(context)
    if not suite:
        return None
    
    # Read data based on source type
    df = None
    if data_source.startswith("s3://"):
        print(f"📁 Reading from S3: {data_source}")
        df = read_data_from_s3(data_source)
    elif data_source.startswith("athena://"):
        # Format: athena://database/table
        parts = data_source.replace("athena://", "").split("/")
        if len(parts) == 2:
            database, table = parts
            print(f"📊 Reading from Athena: {database}.{table}")
            df = read_data_from_athena(database, table, **kwargs)
    else:
        # Local file system
        print(f"📁 Reading from local path: {data_source}")
        if os.path.isdir(data_source):
            import glob
            parquet_files = glob.glob(f"{data_source}/**/*.parquet", recursive=True)
            if parquet_files:
                dfs = []
                for file in parquet_files:
                    try:
                        df_file = pd.read_parquet(file)
                        dfs.append(df_file)
                    except Exception as e:
                        print(f"❌ Error loading {file}: {e}")
                
                if dfs:
                    df = pd.concat(dfs, ignore_index=True)
        else:
            try:
                df = pd.read_parquet(data_source)
            except Exception as e:
                print(f"❌ Error loading {data_source}: {e}")
    
    if df is None or len(df) == 0:
        print("❌ No data found")
        return None
    
    print(f"📊 Loaded dataset: {len(df)} rows, {len(df.columns)} columns")
    print(f"📋 Columns: {list(df.columns)}")
    
    # Show data summary
    print("\n📈 Data Summary:")
    print(f"   Total records: {len(df):,}")
    if 'data_source' in df.columns:
        print(f"   Data sources: {df['data_source'].value_counts().to_dict()}")
    if 'year' in df.columns:
        print(f"   Year range: {df['year'].min()} - {df['year'].max()}")
    
    # Create batch request for Great Expectations
    try:
        batch_request = RuntimeBatchRequest(
            datasource_name="pandas_datasource",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="weather_data",
            runtime_parameters={"batch_data": df},
            batch_identifiers={"default_identifier_name": "default_identifier"}
        )
        
        # Create validator
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite_name="weather_data_suite"
        )
        
        # Run validation
        results = validator.validate()
        
        # Process and display results
        display_validation_results(results, df)
        
        return {
            "success": results.success,
            "total_records": len(df),
            "expectations_checked": len(results.results),
            "successful_expectations": len([r for r in results.results if r.success]),
            "failed_expectations": len([r for r in results.results if not r.success])
        }
        
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        import traceback
        traceback.print_exc()
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
    
    print("\n" + "="*60)

def initialize_great_expectations_project(project_root: str = "great_expectations"):
    """Initialize Great Expectations project with S3 and Athena support"""
    
    print("🚀 Initializing Great Expectations project...")
    
    # Create project directory
    os.makedirs(project_root, exist_ok=True)
    
    try:
        # Initialize Great Expectations context
        context = BaseDataContext(project_root_dir=project_root)
        
        # Create datasource configuration for v0.15.2
        datasource_config = {
            "name": "pandas_datasource",
            "class_name": "Datasource",
            "module_name": "great_expectations.datasource",
            "execution_engine": {
                "class_name": "PandasExecutionEngine",
                "module_name": "great_expectations.execution_engine"
            },
            "data_connectors": {
                "default_runtime_data_connector_name": {
                    "class_name": "RuntimeDataConnector",
                    "module_name": "great_expectations.datasource.data_connector",
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

def run_data_quality_validation(data_path: str = "data/processed"):
    """Main function to run comprehensive data quality validation"""
    
    print("🔍 Starting Data Quality Validation Pipeline")
    print("="*60)
    
    # Initialize Great Expectations
    context = initialize_great_expectations_project()
    if not context:
        print("❌ Failed to initialize Great Expectations")
        return
    
    # Validate data
    results = validate_weather_data_great_expectations(data_path)
    
    if results:
        print(f"\n🎉 Validation completed!")
        print(f"📊 Success Rate: {results['successful_expectations']}/{results['expectations_checked']} expectations passed")
        
        if results['success']:
            print("✅ All data quality checks passed!")
        else:
            print("⚠️  Some data quality issues detected - review results above")
    else:
        print("❌ Validation failed - no data found or processing error")

def validate_s3_data(s3_path: str):
    """Validate data stored in S3"""
    print(f"🔍 Validating S3 data: {s3_path}")
    return validate_weather_data_great_expectations(s3_path)

def validate_athena_table(database: str, table: str, region: str = "us-east-1"):
    """Validate data in Athena table"""
    athena_path = f"athena://{database}/{table}"
    print(f"🔍 Validating Athena table: {database}.{table}")
    return validate_weather_data_great_expectations(athena_path, region=region)

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "s3":
            if len(sys.argv) > 2:
                s3_path = sys.argv[2]
                validate_s3_data(s3_path)
            else:
                print("Usage: python weather_data_suite_v2.py s3 <s3_path>")
                print("Example: python weather_data_suite_v2.py s3 s3://my-bucket/weather-data/")
        elif sys.argv[1] == "athena":
            if len(sys.argv) > 3:
                database = sys.argv[2]
                table = sys.argv[3]
                validate_athena_table(database, table)
            else:
                print("Usage: python weather_data_suite_v2.py athena <database> <table>")
                print("Example: python weather_data_suite_v2.py athena weather_db processed_weather_data")
        else:
            print("Usage:")
            print("  python weather_data_suite_v2.py                    # Validate local data")
            print("  python weather_data_suite_v2.py s3 <s3_path>       # Validate S3 data")
            print("  python weather_data_suite_v2.py athena <db> <table> # Validate Athena table")
    else:
        run_data_quality_validation()