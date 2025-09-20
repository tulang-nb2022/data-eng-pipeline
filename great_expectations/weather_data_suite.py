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

def run_simple_validations(df):
    """Run simple validations directly on DataFrame without expectation suites"""
    
    print("🔍 Running Simple Data Quality Validations:")
    print("-" * 50)
    
    validation_results = []
    
    # 1. Check table has data
    try:
        row_count = len(df)
        success = 1 <= row_count <= 10000000
        validation_results.append(("Table Row Count", success, f"{row_count:,} rows"))
        print(f"1. {'✅' if success else '❌'} Table Row Count: {row_count:,} rows")
    except Exception as e:
        validation_results.append(("Table Row Count", False, str(e)))
        print(f"1. ❌ Table Row Count: {e}")
    
    # 2. Check essential columns exist and are not null
    essential_columns = ["data_source", "year", "month", "day", "city"]
    for i, column in enumerate(essential_columns, 2):
        try:
            if column in df.columns:
                null_count = df[column].isnull().sum()
                success = null_count == 0
                validation_results.append((f"Column {column} Not Null", success, f"{null_count} nulls"))
                print(f"{i}. {'✅' if success else '❌'} Column '{column}': {null_count} null values")
            else:
                validation_results.append((f"Column {column} Exists", False, f"Column {column} not found"))
                print(f"{i}. ❌ Column '{column}': Column not found")
        except Exception as e:
            validation_results.append((f"Column {column} Not Null", False, str(e)))
            print(f"{i}. ❌ Column '{column}': {e}")
    
    # 3. Check data source values
    try:
        if 'data_source' in df.columns:
            valid_sources = ["noaa", "alphavantage", "eosdis", "openweather"]
            invalid_sources = df[~df['data_source'].isin(valid_sources)]['data_source'].unique()
            success = len(invalid_sources) == 0
            validation_results.append(("Data Source Values", success, f"Invalid sources: {list(invalid_sources)}"))
            if success:
                print(f"{len(essential_columns)+2}. ✅ Data Source Values: All sources valid")
            else:
                print(f"{len(essential_columns)+2}. ❌ Data Source Values: Invalid sources found: {list(invalid_sources)}")
        else:
            validation_results.append(("Data Source Values", False, "data_source column not found"))
            print(f"{len(essential_columns)+2}. ❌ Data Source Values: Column not found")
    except Exception as e:
        validation_results.append(("Data Source Values", False, str(e)))
        print(f"{len(essential_columns)+2}. ❌ Data Source Values: {e}")
    
    # 4. Check year range
    try:
        if 'year' in df.columns:
            year_col = df['year']
            if hasattr(year_col.dtype, 'categories'):
                year_col = year_col.cat.as_ordered()
            # Convert to numeric for comparison
            year_numeric = pd.to_numeric(year_col, errors='coerce')
            invalid_years = year_numeric[(year_numeric < 2020) | (year_numeric > 2030)].unique()
            success = len(invalid_years) == 0
            validation_results.append(("Year Range", success, f"Invalid years: {list(invalid_years)}"))
            if success:
                print(f"{len(essential_columns)+3}. ✅ Year Range: All years valid (2020-2030)")
            else:
                print(f"{len(essential_columns)+3}. ❌ Year Range: Invalid years found: {list(invalid_years)}")
        else:
            validation_results.append(("Year Range", False, "year column not found"))
            print(f"{len(essential_columns)+3}. ❌ Year Range: Column not found")
    except Exception as e:
        validation_results.append(("Year Range", False, str(e)))
        print(f"{len(essential_columns)+3}. ❌ Year Range: {e}")
    
    # 5. Check month range
    try:
        if 'month' in df.columns:
            month_col = df['month']
            if hasattr(month_col.dtype, 'categories'):
                month_col = month_col.cat.as_ordered()
            # Convert to numeric for comparison
            month_numeric = pd.to_numeric(month_col, errors='coerce')
            invalid_months = month_numeric[(month_numeric < 1) | (month_numeric > 12)].unique()
            success = len(invalid_months) == 0
            validation_results.append(("Month Range", success, f"Invalid months: {list(invalid_months)}"))
            if success:
                print(f"{len(essential_columns)+4}. ✅ Month Range: All months valid (1-12)")
            else:
                print(f"{len(essential_columns)+4}. ❌ Month Range: Invalid months found: {list(invalid_months)}")
        else:
            validation_results.append(("Month Range", False, "month column not found"))
            print(f"{len(essential_columns)+4}. ❌ Month Range: Column not found")
    except Exception as e:
        validation_results.append(("Month Range", False, str(e)))
        print(f"{len(essential_columns)+4}. ❌ Month Range: {e}")
    
    # 6. Check day range
    try:
        if 'day' in df.columns:
            day_col = df['day']
            if hasattr(day_col.dtype, 'categories'):
                day_col = day_col.cat.as_ordered()
            # Convert to numeric for comparison
            day_numeric = pd.to_numeric(day_col, errors='coerce')
            invalid_days = day_numeric[(day_numeric < 1) | (day_numeric > 31)].unique()
            success = len(invalid_days) == 0
            validation_results.append(("Day Range", success, f"Invalid days: {list(invalid_days)}"))
            if success:
                print(f"{len(essential_columns)+5}. ✅ Day Range: All days valid (1-31)")
            else:
                print(f"{len(essential_columns)+5}. ❌ Day Range: Invalid days found: {list(invalid_days)}")
        else:
            validation_results.append(("Day Range", False, "day column not found"))
            print(f"{len(essential_columns)+5}. ❌ Day Range: Column not found")
    except Exception as e:
        validation_results.append(("Day Range", False, str(e)))
        print(f"{len(essential_columns)+5}. ❌ Day Range: {e}")
    
    # 7. Check temperature range (lenient)
    try:
        if 'avg_temperature' in df.columns:
            temp_data = df['avg_temperature'].dropna()
            if len(temp_data) > 0:
                invalid_temps = temp_data[(temp_data < -100) | (temp_data > 100)]
                invalid_count = len(invalid_temps)
                total_count = len(temp_data)
                success_rate = (total_count - invalid_count) / total_count
                success = success_rate >= 0.8  # Allow 20% outliers
                validation_results.append(("Temperature Range", success, f"{invalid_count}/{total_count} outliers"))
                if success:
                    print(f"{len(essential_columns)+6}. ✅ Temperature Range: Reasonable temperatures ({success_rate:.1%} valid)")
                else:
                    print(f"{len(essential_columns)+6}. ❌ Temperature Range: Too many extreme temperatures ({success_rate:.1%} valid)")
            else:
                validation_results.append(("Temperature Range", False, "No temperature data"))
                print(f"{len(essential_columns)+6}. ❌ Temperature Range: No temperature data")
        else:
            validation_results.append(("Temperature Range", False, "avg_temperature column not found"))
            print(f"{len(essential_columns)+6}. ❌ Temperature Range: Column not found")
    except Exception as e:
        validation_results.append(("Temperature Range", False, str(e)))
        print(f"{len(essential_columns)+6}. ❌ Temperature Range: {e}")
    
    # 8. Check record count range
    try:
        if 'record_count' in df.columns:
            invalid_counts = df[(df['record_count'] < 1) | (df['record_count'] > 1000000)]['record_count'].unique()
            success = len(invalid_counts) == 0
            validation_results.append(("Record Count Range", success, f"Invalid counts: {list(invalid_counts)}"))
            if success:
                print(f"{len(essential_columns)+7}. ✅ Record Count Range: All counts valid (1-1M)")
            else:
                print(f"{len(essential_columns)+7}. ❌ Record Count Range: Invalid counts found: {list(invalid_counts)}")
        else:
            validation_results.append(("Record Count Range", False, "record_count column not found"))
            print(f"{len(essential_columns)+7}. ❌ Record Count Range: Column not found")
    except Exception as e:
        validation_results.append(("Record Count Range", False, str(e)))
        print(f"{len(essential_columns)+7}. ❌ Record Count Range: {e}")
    
    return validation_results

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
    print("✅ Using simplified validation approach (no Great Expectations context needed)")
    
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
        # Handle both numeric and categorical year columns
        year_col = df['year']
        if hasattr(year_col.dtype, 'categories'):
            # It's categorical, convert to ordered for min/max operations
            year_col = year_col.cat.as_ordered()
        print(f"   Year range: {year_col.min()} - {year_col.max()}")
    if 'city' in df.columns:
        print(f"   Cities: {df['city'].value_counts().head().to_dict()}")
    
    # Run simple validations directly on DataFrame
    try:
        validation_results = run_simple_validations(df)
        
        # Display summary
        display_simple_validation_results(validation_results, df)
        
        # Calculate summary statistics
        total_validations = len(validation_results)
        successful_validations = len([r for r in validation_results if r[1]])
        failed_validations = total_validations - successful_validations
        success_rate = (successful_validations / total_validations * 100) if total_validations > 0 else 0
        
        return {
            "success": success_rate >= 80,  # Consider success if 80%+ validations pass
            "total_records": len(df),
            "validations_checked": total_validations,
            "successful_validations": successful_validations,
            "failed_validations": failed_validations,
            "success_rate": success_rate
        }
        
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        import traceback
        traceback.print_exc()
        return None

def display_simple_validation_results(validation_results, df):
    """Display simple validation results in a professional format"""
    
    print("\n" + "="*60)
    print("VALIDATION SUMMARY")
    print("="*60)
    
    # Overall statistics
    total_validations = len(validation_results)
    successful_validations = len([r for r in validation_results if r[1]])
    failed_validations = total_validations - successful_validations
    success_rate = (successful_validations / total_validations * 100) if total_validations > 0 else 0
    
    print(f"📈 Overall Success Rate: {success_rate:.1f}%")
    print(f"✅ Successful Validations: {successful_validations}/{total_validations}")
    print(f"❌ Failed Validations: {failed_validations}/{total_validations}")
    print(f"📊 Total Records Processed: {len(df):,}")
    
    # Data quality insights
    print("\n🔍 Data Quality Insights:")
    print("-" * 30)
    
    if "data_source" in df.columns:
        source_counts = df["data_source"].value_counts()
        print("Data sources distribution:")
        for source, count in source_counts.items():
            print(f"  {source}: {count:,} records")
    
    if "year" in df.columns:
        year_col = df['year']
        if hasattr(year_col.dtype, 'categories'):
            # It's categorical, convert to ordered for min/max operations
            year_col = year_col.cat.as_ordered()
        year_range = f"{year_col.min()} - {year_col.max()}"
        print(f"Year range: {year_range}")
    
    if "weather_alert_type" in df.columns:
        alert_counts = df["weather_alert_type"].value_counts()
        print("Weather alert distribution:")
        for alert, count in alert_counts.items():
            print(f"  {alert}: {count:,} records")
    
    print("\n" + "="*60)

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
        year_col = df['year']
        if hasattr(year_col.dtype, 'categories'):
            # It's categorical, convert to ordered for min/max operations
            year_col = year_col.cat.as_ordered()
        year_range = f"{year_col.min()} - {year_col.max()}"
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
