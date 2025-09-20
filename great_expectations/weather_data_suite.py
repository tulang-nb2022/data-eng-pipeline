#!/usr/bin/env python3
"""
Great Expectations Data Quality Validation for Weather Data Pipeline
Updated for the current gold layer schema
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os
import json
import great_expectations as ge
from great_expectations.data_context import FileDataContext
from great_expectations.core.batch import RuntimeBatchRequest
import boto3
from typing import Dict, Any, Optional, List
import logging
import glob

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_gold_layer_expectation_suite(context, suite_name: str = "gold_weather_metrics_suite"):
    """Create expectation suite for gold layer weather metrics"""
    
    try:
        # For v1.6.1, create suite directly without context
        suite = ge.ExpectationSuite(name=suite_name)
        print(f"✅ Created expectation suite: {suite_name}")
    except Exception as e:
        print(f"❌ Failed to create expectation suite: {e}")
        return None
    
    # Expected columns in gold layer (based on your current schema)
    expected_columns = [
        "data_source", "year", "month", "day", "city",
        "avg_temperature", "max_temperature", "min_temperature", "temperature_stddev",
        "avg_humidity", "max_humidity", "min_humidity",
        "avg_pressure", "max_pressure", "min_pressure",
        "avg_wind_speed", "max_wind_speed",
        "avg_visibility", "min_visibility",
        "avg_quality_score", "record_count",
        "latest_processing_timestamp", "earliest_processing_timestamp",
        "weather_alert_type", "alert_severity", "gold_processing_timestamp"
    ]
    
    # Table-level expectations (using direct method calls for v1.6.1)
    suite.add_expectation(
        ge.expectations.ExpectTableColumnsToMatchSet(
            column_set=set(expected_columns)
        )
    )
    
    suite.add_expectation(
        ge.expectations.ExpectTableRowCountToBeBetween(
            min_value=1, max_value=10000000
        )
    )
    
    # Essential columns that should not be null
    essential_columns = ["data_source", "year", "month", "day", "city"]
    for column in essential_columns:
        suite.add_expectation(
            ge.ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": column}
            )
        )
    
    # Data source validation
    suite.add_expectation(
        ge.ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "data_source",
                "value_set": ["noaa", "alphavantage", "eosdis", "openweather"]
            }
        )
    )
    
    # Date range validations
    suite.add_expectation(
        ge.ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "year",
                "min_value": 2020,
                "max_value": 2030
            }
        )
    )
    
    suite.add_expectation(
        ge.ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "month",
                "min_value": 1,
                "max_value": 12
            }
        )
    )
    
    suite.add_expectation(
        ge.ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "day",
                "min_value": 1,
                "max_value": 31
            }
        )
    )
    
    # Temperature validations (more lenient)
    suite.add_expectation(
        ge.ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "avg_temperature",
                "min_value": -100,  # Very lenient
                "max_value": 100,
                "mostly": 0.8  # Allow 20% of values to be outside range
            }
        )
    )
    
    # Record count validation
    suite.add_expectation(
        ge.ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "record_count",
                "min_value": 1,
                "max_value": 1000000
            }
        )
    )
    
    # Weather alert type validation
    suite.add_expectation(
        ge.ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "weather_alert_type",
                "value_set": ["HIGH_TEMPERATURE", "LOW_TEMPERATURE", "HIGH_WIND", 
                           "LOW_VISIBILITY", "LOW_PRESSURE", "NORMAL"]
            }
        )
    )
    
    # Alert severity validation
    suite.add_expectation(
        ge.ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "alert_severity",
                "value_set": ["SEVERE", "HIGH", "MEDIUM", "LOW"]
            }
        )
    )
    
    # Suite is ready to use (no need to save without context)
    print("✅ Expectation suite ready for validation")
    return suite

def read_data_from_s3(s3_path: str, end_date: str = None) -> Optional[pd.DataFrame]:
    """Read data from S3 using s3fs with optional date filtering"""
    try:
        import s3fs
        from datetime import datetime, timedelta
        
        fs = s3fs.S3FileSystem()
        
        if s3_path.endswith('/'):
            if end_date:
                # Parse end_date (YYYYMMDD format)
                try:
                    end_dt = datetime.strptime(end_date, "%Y%m%d")
                    print(f"📅 Reading data up to and including: {end_dt.strftime('%Y-%m-%d')}")
                except ValueError:
                    print(f"❌ Invalid date format: {end_date}. Expected YYYYMMDD")
                    return None
                
                # Generate all date paths up to end_date
                parquet_files = []
                current_date = datetime(2020, 1, 1)  # Start from reasonable date
                
                while current_date <= end_dt:
                    year = current_date.year
                    month = current_date.month
                    day = current_date.day
                    
                    # Check if path exists for this date
                    date_path = f"{s3_path}year={year}/month={month}/day={day}/"
                    date_files = fs.glob(f"{date_path}*.parquet")
                    
                    if date_files:
                        parquet_files.extend(date_files)
                        print(f"📁 Found {len(date_files)} files for {year}-{month:02d}-{day:02d}")
                    
                    current_date += timedelta(days=1)
                
                if not parquet_files:
                    print(f"❌ No parquet files found for dates up to {end_dt.strftime('%Y-%m-%d')}")
                    return None
                    
            else:
                # Read all parquet files in directory (original behavior)
                parquet_files = fs.glob(f"{s3_path}**/*.parquet")
                if not parquet_files:
                    print(f"❌ No parquet files found in {s3_path}")
                    return None
            
            print(f"📊 Loading {len(parquet_files)} parquet files...")
            dfs = []
            for i, file in enumerate(parquet_files):
                try:
                    df = pd.read_parquet(f"s3://{file}")
                    dfs.append(df)
                    if (i + 1) % 10 == 0:  # Progress indicator
                        print(f"   Loaded {i + 1}/{len(parquet_files)} files...")
                except Exception as e:
                    print(f"⚠️  Error loading {file}: {e}")
                    continue
            
            if dfs:
                combined_df = pd.concat(dfs, ignore_index=True)
                print(f"✅ Successfully loaded {len(combined_df):,} records from {len(dfs)} files")
                return combined_df
            else:
                print("❌ No valid data files found")
                return None
        else:
            # Read single file
            return pd.read_parquet(s3_path)
            
    except Exception as e:
        print(f"❌ Error reading from S3: {e}")
        return None

def read_data_from_duckdb(db_path: str, table_name: str) -> Optional[pd.DataFrame]:
    """Read data from DuckDB database"""
    try:
        import duckdb
        
        conn = duckdb.connect(db_path)
        query = f"SELECT * FROM {table_name}"
        df = conn.execute(query).fetchdf()
        conn.close()
        
        return df
        
    except Exception as e:
        print(f"❌ Error reading from DuckDB: {e}")
        return None

def validate_gold_layer_data(
    data_source: str,
    context_path: str = "great_expectations",
    end_date: str = None,
    **kwargs
) -> Optional[Dict[str, Any]]:
    """Validate gold layer weather data using Great Expectations"""
    
    print("="*60)
    print("GREAT EXPECTATIONS - GOLD LAYER VALIDATION")
    print("="*60)
    
    # Skip complex context initialization for v1.6.1
    print("✅ Using simplified Great Expectations approach (no context needed)")
    context = None  # We'll work without a complex context
    
    # Create expectation suite
    suite = create_gold_layer_expectation_suite(context)
    if not suite:
        return None
    
    # Read data based on source type
    df = None
    if data_source.startswith("s3://"):
        print(f"📁 Reading from S3: {data_source}")
        df = read_data_from_s3(data_source, end_date)
    elif data_source.startswith("duckdb://"):
        # Format: duckdb://path/to/db.db/schema.table
        parts = data_source.replace("duckdb://", "").split("/")
        if len(parts) >= 2:
            db_path = parts[0]
            table_name = "/".join(parts[1:])
            print(f"📊 Reading from DuckDB: {db_path}/{table_name}")
            df = read_data_from_duckdb(db_path, table_name)
    else:
        # Local file system
        print(f"📁 Reading from local path: {data_source}")
        if os.path.isdir(data_source):
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
    if 'city' in df.columns:
        print(f"   Cities: {df['city'].value_counts().head().to_dict()}")
    
    # Create validator for v1.6.1
    try:
        # For v1.6.1, create validator directly with DataFrame
        validator = ge.from_pandas(df, expectation_suite=suite)
        
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
    
    if "weather_alert_type" in df.columns:
        alert_counts = df["weather_alert_type"].value_counts()
        print("Weather alert distribution:")
        for alert, count in alert_counts.items():
            print(f"  {alert}: {count:,} records")
    
    print("\n" + "="*60)

def initialize_great_expectations_project(project_root: str = "great_expectations"):
    """Initialize Great Expectations project (simplified for v1.6.1)"""
    
    print("🚀 Initializing Great Expectations project (simplified)...")
    
    # Create project directory
    os.makedirs(project_root, exist_ok=True)
    
    print(f"✅ Great Expectations project directory created: {project_root}")
    print("✅ Using simplified approach (no complex context needed)")
    
    return None  # No context needed for simplified approach

def validate_s3_gold_data(s3_path: str, end_date: str = None):
    """Validate gold data stored in S3"""
    if end_date:
        print(f"🔍 Validating S3 gold data up to {end_date}: {s3_path}")
    else:
        print(f"🔍 Validating S3 gold data: {s3_path}")
    return validate_gold_layer_data(s3_path, end_date=end_date)

def validate_duckdb_gold_data(db_path: str, table_name: str):
    """Validate gold data in DuckDB"""
    duckdb_path = f"duckdb://{db_path}/{table_name}"
    print(f"🔍 Validating DuckDB gold data: {db_path}/{table_name}")
    return validate_gold_layer_data(duckdb_path)

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "s3":
            if len(sys.argv) > 2:
                s3_path = sys.argv[2]
                end_date = sys.argv[3] if len(sys.argv) > 3 else None
                validate_s3_gold_data(s3_path, end_date)
            else:
                print("Usage: python weather_data_suite.py s3 <s3_path> [end_date]")
                print("Example: python weather_data_suite.py s3 s3://data-eng-bucket-345/gold/weather/")
                print("Example: python weather_data_suite.py s3 s3://data-eng-bucket-345/gold/weather/ 20250919")
                print("         (end_date format: YYYYMMDD - validates all data up to and including that date)")
        elif sys.argv[1] == "duckdb":
            if len(sys.argv) > 3:
                db_path = sys.argv[2]
                table_name = sys.argv[3]
                validate_duckdb_gold_data(db_path, table_name)
            else:
                print("Usage: python weather_data_suite.py duckdb <db_path> <table_name>")
                print("Example: python weather_data_suite.py duckdb gold_layer_test.duckdb gold_layer_test.gold.weather_metrics")
        elif sys.argv[1] == "help":
            print("Great Expectations Weather Data Validation")
            print("=" * 50)
            print("Usage:")
            print("  python weather_data_suite.py s3 <s3_path> [end_date]")
            print("  python weather_data_suite.py duckdb <db_path> <table_name>")
            print("")
            print("S3 Examples:")
            print("  # Validate all S3 data")
            print("  python weather_data_suite.py s3 s3://data-eng-bucket-345/gold/weather/")
            print("")
            print("  # Validate S3 data up to specific date (inclusive)")
            print("  python weather_data_suite.py s3 s3://data-eng-bucket-345/gold/weather/ 20250919")
            print("  python weather_data_suite.py s3 s3://data-eng-bucket-345/gold/weather/ 20250101")
            print("")
            print("DuckDB Examples:")
            print("  python weather_data_suite.py duckdb gold_layer_test.duckdb gold_layer_test.gold.weather_metrics")
            print("")
            print("Date Format: YYYYMMDD (e.g., 20250919 for September 19, 2025)")
            print("The script will automatically find all partitioned data up to and including the specified date.")
        else:
            print("Usage:")
            print("  python weather_data_suite.py s3 <s3_path> [end_date]                    # Validate S3 gold data")
            print("  python weather_data_suite.py duckdb <db_path> <table_name>  # Validate DuckDB gold data")
            print("  python weather_data_suite.py help                                      # Show detailed help")
    else:
        # Default: validate DuckDB gold data
        print("🔍 Validating DuckDB gold data (default)")
        validate_duckdb_gold_data("gold_layer_test.duckdb", "gold_layer_test.gold.weather_metrics")
