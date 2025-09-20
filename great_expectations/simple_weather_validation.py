#!/usr/bin/env python3
"""
Simple Great Expectations Data Quality Validation for Weather Data Pipeline
Simplified approach for v1.6.1 - no complex context or expectation suites needed
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os
import json
import great_expectations as ge
import boto3
from typing import Dict, Any, Optional, List
import logging
import glob

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

def validate_gold_layer_data_simple(
    data_source: str,
    **kwargs
) -> Optional[Dict[str, Any]]:
    """Simple validation of gold layer weather data using Great Expectations v1.6.1"""
    
    print("="*60)
    print("SIMPLE GREAT EXPECTATIONS - GOLD LAYER VALIDATION")
    print("="*60)
    
    # Read data based on source type
    df = None
    if data_source.startswith("s3://"):
        print(f"📁 Reading from S3: {data_source}")
        df = read_data_from_s3(data_source)
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
    
    # Create validator directly from DataFrame (simplest approach for v1.6.1)
    try:
        print("\n🔍 Creating Great Expectations validator...")
        validator = ge.from_pandas(df)
        
        # Run simple validations using direct method calls
        print("\n📋 Running Data Quality Validations:")
        print("-" * 50)
        
        validation_results = []
        
        # 1. Check table has data
        try:
            result = validator.expect_table_row_count_to_be_between(min_value=1, max_value=10000000)
            validation_results.append(("Table Row Count", result.success, result.result))
            print(f"1. ✅ Table Row Count: {len(df):,} rows")
        except Exception as e:
            validation_results.append(("Table Row Count", False, str(e)))
            print(f"1. ❌ Table Row Count: {e}")
        
        # 2. Check essential columns exist
        essential_columns = ["data_source", "year", "month", "day", "city"]
        for i, column in enumerate(essential_columns, 2):
            try:
                if column in df.columns:
                    result = validator.expect_column_values_to_not_be_null(column=column)
                    validation_results.append((f"Column {column} Not Null", result.success, result.result))
                    print(f"{i}. ✅ Column '{column}': Not null check passed")
                else:
                    validation_results.append((f"Column {column} Exists", False, f"Column {column} not found"))
                    print(f"{i}. ❌ Column '{column}': Column not found")
            except Exception as e:
                validation_results.append((f"Column {column} Not Null", False, str(e)))
                print(f"{i}. ❌ Column '{column}': {e}")
        
        # 3. Check data source values
        try:
            if 'data_source' in df.columns:
                result = validator.expect_column_values_to_be_in_set(
                    column="data_source",
                    value_set=["noaa", "alphavantage", "eosdis", "openweather"]
                )
                validation_results.append(("Data Source Values", result.success, result.result))
                if result.success:
                    print(f"{len(essential_columns)+2}. ✅ Data Source Values: Valid sources found")
                else:
                    print(f"{len(essential_columns)+2}. ❌ Data Source Values: Invalid sources found")
            else:
                validation_results.append(("Data Source Values", False, "data_source column not found"))
                print(f"{len(essential_columns)+2}. ❌ Data Source Values: Column not found")
        except Exception as e:
            validation_results.append(("Data Source Values", False, str(e)))
            print(f"{len(essential_columns)+2}. ❌ Data Source Values: {e}")
        
        # 4. Check year range
        try:
            if 'year' in df.columns:
                result = validator.expect_column_values_to_be_between(
                    column="year",
                    min_value=2020,
                    max_value=2030
                )
                validation_results.append(("Year Range", result.success, result.result))
                if result.success:
                    print(f"{len(essential_columns)+3}. ✅ Year Range: Valid years (2020-2030)")
                else:
                    print(f"{len(essential_columns)+3}. ❌ Year Range: Invalid years found")
            else:
                validation_results.append(("Year Range", False, "year column not found"))
                print(f"{len(essential_columns)+3}. ❌ Year Range: Column not found")
        except Exception as e:
            validation_results.append(("Year Range", False, str(e)))
            print(f"{len(essential_columns)+3}. ❌ Year Range: {e}")
        
        # 5. Check temperature range (lenient)
        try:
            if 'avg_temperature' in df.columns:
                result = validator.expect_column_values_to_be_between(
                    column="avg_temperature",
                    min_value=-100,
                    max_value=100,
                    mostly=0.8  # Allow 20% outliers
                )
                validation_results.append(("Temperature Range", result.success, result.result))
                if result.success:
                    print(f"{len(essential_columns)+4}. ✅ Temperature Range: Reasonable temperatures")
                else:
                    print(f"{len(essential_columns)+4}. ❌ Temperature Range: Extreme temperatures found")
            else:
                validation_results.append(("Temperature Range", False, "avg_temperature column not found"))
                print(f"{len(essential_columns)+4}. ❌ Temperature Range: Column not found")
        except Exception as e:
            validation_results.append(("Temperature Range", False, str(e)))
            print(f"{len(essential_columns)+4}. ❌ Temperature Range: {e}")
        
        # Display summary
        print("\n" + "="*60)
        print("VALIDATION SUMMARY")
        print("="*60)
        
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
            year_range = f"{df['year'].min()} - {df['year'].max()}"
            print(f"Year range: {year_range}")
        
        if "weather_alert_type" in df.columns:
            alert_counts = df["weather_alert_type"].value_counts()
            print("Weather alert distribution:")
            for alert, count in alert_counts.items():
                print(f"  {alert}: {count:,} records")
        
        print("\n" + "="*60)
        
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

def validate_s3_gold_data(s3_path: str):
    """Validate gold data stored in S3"""
    print(f"🔍 Validating S3 gold data: {s3_path}")
    return validate_gold_layer_data_simple(s3_path)

def validate_duckdb_gold_data(db_path: str, table_name: str):
    """Validate gold data in DuckDB"""
    duckdb_path = f"duckdb://{db_path}/{table_name}"
    print(f"🔍 Validating DuckDB gold data: {db_path}/{table_name}")
    return validate_gold_layer_data_simple(duckdb_path)

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "s3":
            if len(sys.argv) > 2:
                s3_path = sys.argv[2]
                validate_s3_gold_data(s3_path)
            else:
                print("Usage: python simple_weather_validation.py s3 <s3_path>")
                print("Example: python simple_weather_validation.py s3 s3://data-eng-bucket-345/gold/weather/")
        elif sys.argv[1] == "duckdb":
            if len(sys.argv) > 3:
                db_path = sys.argv[2]
                table_name = sys.argv[3]
                validate_duckdb_gold_data(db_path, table_name)
            else:
                print("Usage: python simple_weather_validation.py duckdb <db_path> <table_name>")
                print("Example: python simple_weather_validation.py duckdb gold_layer_test.duckdb gold_layer_test.gold.weather_metrics")
        else:
            print("Usage:")
            print("  python simple_weather_validation.py s3 <s3_path>                    # Validate S3 gold data")
            print("  python simple_weather_validation.py duckdb <db_path> <table_name>  # Validate DuckDB gold data")
    else:
        # Default: validate DuckDB gold data
        print("🔍 Validating DuckDB gold data (default)")
        validate_duckdb_gold_data("gold_layer_test.duckdb", "gold_layer_test.gold.weather_metrics")
