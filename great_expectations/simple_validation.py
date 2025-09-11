#!/usr/bin/env python3
"""
Simple Great Expectations validation for weather data
Uses the most stable API patterns for version 0.15.2
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os
import json
import great_expectations as ge
from typing import Dict, Any, Optional, List
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_simple_expectations(df: pd.DataFrame) -> List[Dict]:
    """Create simple expectations for weather data validation"""
    
    expectations = []
    
    # Essential columns check
    essential_columns = ["processing_timestamp", "year", "month", "day", "data_source"]
    expectations.append({
        "expectation_type": "expect_table_columns_to_match_ordered_list",
        "kwargs": {"column_list": essential_columns}
    })
    
    # Row count check
    expectations.append({
        "expectation_type": "expect_table_row_count_to_be_between",
        "kwargs": {"min_value": 1, "max_value": 10000000}
    })
    
    # Column presence and non-null checks
    for column in essential_columns:
        if column in df.columns:
            expectations.append({
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": column}
            })
    
    # Data source validation
    if "data_source" in df.columns:
        expectations.append({
            "expectation_type": "expect_column_values_to_be_in_set",
            "kwargs": {
                "column": "data_source",
                "value_set": ["noaa", "alphavantage", "eosdis", "openweather"]
            }
        })
    
    # Date range validations
    if "year" in df.columns:
        expectations.append({
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "year", "min_value": 2020, "max_value": 2030}
        })
    
    if "month" in df.columns:
        expectations.append({
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "month", "min_value": 1, "max_value": 12}
        })
    
    if "day" in df.columns:
        expectations.append({
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "day", "min_value": 1, "max_value": 31}
        })
    
    return expectations

def validate_dataframe_simple(df: pd.DataFrame) -> Dict[str, Any]:
    """Validate a DataFrame using simple Great Expectations approach"""
    
    print("="*60)
    print("SIMPLE GREAT EXPECTATIONS VALIDATION")
    print("="*60)
    
    if df is None or len(df) == 0:
        print("❌ No data to validate")
        return {"success": False, "error": "No data"}
    
    print(f"📊 Validating dataset: {len(df)} rows, {len(df.columns)} columns")
    print(f"📋 Columns: {list(df.columns)}")
    
    # Create expectations
    expectations = create_simple_expectations(df)
    print(f"🔍 Created {len(expectations)} validation rules")
    
    # Run validations
    results = []
    passed = 0
    failed = 0
    
    for i, expectation in enumerate(expectations, 1):
        expectation_type = expectation["expectation_type"]
        kwargs = expectation["kwargs"]
        
        try:
            # Use pandas-based validation for simplicity
            if expectation_type == "expect_table_columns_to_match_ordered_list":
                expected_cols = kwargs["column_list"]
                actual_cols = list(df.columns)
                success = actual_cols == expected_cols
                message = f"Expected columns: {expected_cols}, Got: {actual_cols}"
                
            elif expectation_type == "expect_table_row_count_to_be_between":
                min_val = kwargs["min_value"]
                max_val = kwargs["max_value"]
                row_count = len(df)
                success = min_val <= row_count <= max_val
                message = f"Row count: {row_count}, Expected: {min_val}-{max_val}"
                
            elif expectation_type == "expect_column_values_to_not_be_null":
                column = kwargs["column"]
                if column in df.columns:
                    null_count = df[column].isnull().sum()
                    success = null_count == 0
                    message = f"Column '{column}': {null_count} null values"
                else:
                    success = False
                    message = f"Column '{column}' not found"
                    
            elif expectation_type == "expect_column_values_to_be_in_set":
                column = kwargs["column"]
                value_set = set(kwargs["value_set"])
                if column in df.columns:
                    actual_values = set(df[column].dropna().unique())
                    unexpected = actual_values - value_set
                    success = len(unexpected) == 0
                    message = f"Column '{column}': Unexpected values: {list(unexpected)[:5]}"
                else:
                    success = False
                    message = f"Column '{column}' not found"
                    
            elif expectation_type == "expect_column_values_to_be_between":
                column = kwargs["column"]
                min_val = kwargs["min_value"]
                max_val = kwargs["max_value"]
                if column in df.columns:
                    invalid_count = ((df[column] < min_val) | (df[column] > max_val)).sum()
                    success = invalid_count == 0
                    message = f"Column '{column}': {invalid_count} values outside {min_val}-{max_val}"
                else:
                    success = False
                    message = f"Column '{column}' not found"
            else:
                success = False
                message = f"Unknown expectation type: {expectation_type}"
            
            status_icon = "✅" if success else "❌"
            status_text = "PASS" if success else "FAIL"
            
            print(f"{i:2d}. {status_icon} {status_text} - {expectation_type}")
            if not success:
                print(f"    {message}")
            
            results.append({
                "expectation_type": expectation_type,
                "success": success,
                "message": message
            })
            
            if success:
                passed += 1
            else:
                failed += 1
                
        except Exception as e:
            print(f"{i:2d}. ❌ ERROR - {expectation_type}: {str(e)}")
            results.append({
                "expectation_type": expectation_type,
                "success": False,
                "message": f"Error: {str(e)}"
            })
            failed += 1
    
    # Summary
    total = len(results)
    success_rate = (passed / total * 100) if total > 0 else 0
    
    print("\n" + "="*60)
    print("VALIDATION SUMMARY")
    print("="*60)
    print(f"📈 Success Rate: {success_rate:.1f}%")
    print(f"✅ Passed: {passed}/{total}")
    print(f"❌ Failed: {failed}/{total}")
    print(f"📊 Total Records: {len(df):,}")
    
    return {
        "success": failed == 0,
        "total_expectations": total,
        "passed": passed,
        "failed": failed,
        "success_rate": success_rate,
        "results": results
    }

def read_data_from_s3(s3_path: str) -> Optional[pd.DataFrame]:
    """Read data from S3 using s3fs"""
    try:
        import s3fs
        fs = s3fs.S3FileSystem()
        
        if s3_path.endswith('/'):
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
            return pd.read_parquet(s3_path)
            
    except Exception as e:
        print(f"❌ Error reading from S3: {e}")
        return None

def read_data_from_athena(database: str, table: str, region: str = "us-east-1") -> Optional[pd.DataFrame]:
    """Read data from Athena table"""
    try:
        from pyathena import connect
        
        conn = connect(
            s3_staging_dir='s3://your-athena-results-bucket/temp/',
            region_name=region
        )
        
        query = f"SELECT * FROM {database}.{table} LIMIT 10000"
        df = pd.read_sql(query, conn)
        return df
        
    except Exception as e:
        print(f"❌ Error reading from Athena: {e}")
        return None

def validate_weather_data(data_source: str, **kwargs) -> Optional[Dict[str, Any]]:
    """Main validation function"""
    
    df = None
    
    if data_source.startswith("s3://"):
        print(f"📁 Reading from S3: {data_source}")
        df = read_data_from_s3(data_source)
    elif data_source.startswith("athena://"):
        parts = data_source.replace("athena://", "").split("/")
        if len(parts) == 2:
            database, table = parts
            print(f"📊 Reading from Athena: {database}.{table}")
            df = read_data_from_athena(database, table, **kwargs)
    else:
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
    
    return validate_dataframe_simple(df)

def main():
    """Main function"""
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "s3":
            if len(sys.argv) > 2:
                s3_path = sys.argv[2]
                validate_weather_data(s3_path)
            else:
                print("Usage: python simple_validation.py s3 <s3_path>")
        elif sys.argv[1] == "athena":
            if len(sys.argv) > 3:
                database = sys.argv[2]
                table = sys.argv[3]
                athena_path = f"athena://{database}/{table}"
                validate_weather_data(athena_path)
            else:
                print("Usage: python simple_validation.py athena <database> <table>")
        else:
            print("Usage:")
            print("  python simple_validation.py                    # Validate local data")
            print("  python simple_validation.py s3 <s3_path>       # Validate S3 data")
            print("  python simple_validation.py athena <db> <table> # Validate Athena table")
    else:
        # Default to local data
        data_path = "data/processed"
        validate_weather_data(data_path)

if __name__ == "__main__":
    main()