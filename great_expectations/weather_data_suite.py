import pandas as pd
import numpy as np
from datetime import datetime
import os
import json

def validate_data_quality(df):
    """Simple pandas-based data validation"""
    results = {
        "total_records": len(df),
        "validations": [],
        "passed": 0,
        "failed": 0
    }
    
    # Basic data presence validation
    required_columns = ["processing_timestamp", "year", "month", "day"]
    for col in required_columns:
        if col in df.columns:
            null_count = df[col].isnull().sum()
            null_pct = null_count / len(df)
            success = null_pct == 0
            results["validations"].append({
                "test": f"null_check_{col}",
                "success": success,
                "null_count": null_count,
                "null_percentage": null_pct
            })
            if success:
                results["passed"] += 1
            else:
                results["failed"] += 1
    
    # Data source validation
    if "data_source" in df.columns:
        valid_sources = ["noaa", "alphavantage", "eosdis"]
        invalid_sources = df[~df["data_source"].isin(valid_sources)]
        success = len(invalid_sources) == 0
        results["validations"].append({
            "test": "data_source_validation",
            "success": success,
            "invalid_sources": invalid_sources["data_source"].unique().tolist() if not success else []
        })
        if success:
            results["passed"] += 1
        else:
            results["failed"] += 1
    
    # Time-based validation
    if "year" in df.columns:
        valid_years = df["year"].between(2020, 2030)
        invalid_count = (~valid_years).sum()
        success = invalid_count == 0
        results["validations"].append({
            "test": "year_validation",
            "success": success,
            "invalid_count": invalid_count
        })
        if success:
            results["passed"] += 1
        else:
            results["failed"] += 1
    
    if "month" in df.columns:
        valid_months = df["month"].between(1, 12)
        invalid_count = (~valid_months).sum()
        success = invalid_count == 0
        results["validations"].append({
            "test": "month_validation",
            "success": success,
            "invalid_count": invalid_count
        })
        if success:
            results["passed"] += 1
        else:
            results["failed"] += 1
    
    return results

def validate_weather_data(data_path):
    """Validate data using pandas instead of Great Expectations"""
    # Read data using pandas
    if data_path.startswith("s3://"):
        # For S3, you'd need s3fs or boto3
        print("S3 reading not implemented - use local path")
        return None
    
    # Read all parquet files in directory
    import glob
    parquet_files = glob.glob(f"{data_path}/*.parquet")
    
    if not parquet_files:
        print(f"No parquet files found in {data_path}")
        return None
    
    # Read and combine all parquet files
    dfs = []
    for file in parquet_files:
        df = pd.read_parquet(file)
        dfs.append(df)
    
    if not dfs:
        print("No data found in parquet files")
        return None
    
    # Combine all dataframes
    combined_df = pd.concat(dfs, ignore_index=True)
    
    # Run validation
    results = validate_data_quality(combined_df)
    
    # Print results
    print("Validation Results:")
    print(f"Total records: {results['total_records']}")
    print(f"Tests passed: {results['passed']}")
    print(f"Tests failed: {results['failed']}")
    
    # Print detailed results
    for validation in results['validations']:
        status = "✅ PASS" if validation['success'] else "❌ FAIL"
        print(f"{status} {validation['test']}")
        if not validation['success']:
            print(f"  Details: {validation}")
    
    return results

if __name__ == "__main__":
    # Use local path for testing
    data_path = "data/processed"
    results = validate_weather_data(data_path)
    
    if results:
        print(f"\nValidation completed: {results['passed']} passed, {results['failed']} failed")
    else:
        print("Validation failed - no data found") 