import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema, Check
import glob
import os
from datetime import datetime
from typing import Optional, Dict, Any
try:
    from pyathena import connect
    ATHENA_AVAILABLE = True
except ImportError:
    ATHENA_AVAILABLE = False
    print("PyAthena not available. Install with: pip install PyAthena")


def create_weather_data_schema() -> DataFrameSchema:
    """Create Pandera schema for weather data validation"""
    
    schema = DataFrameSchema(
        columns={
            "processing_timestamp": Column(
                dtype="datetime64[ns]",
                nullable=False,
                description="Timestamp when data was processed"
            ),
            "temperature": Column(
                dtype="float64",
                nullable=True,
                description="Temperature measurement"
            ),
            "city": Column(
                dtype="object",
                nullable=True,
                description="City name"
            ),
            "open": Column(
                dtype="float64",
                nullable=True,
                description="Opening value"
            ),
            "score": Column(
                dtype="float64",
                nullable=True,
                description="Score value"
            ),
            "wind_speed": Column(
                dtype="float64",
                nullable=True,
                description="Wind speed measurement"
            ),
            "humidity": Column(
                dtype="float64",
                checks=[
                    Check.greater_than_or_equal_to(0),
                    Check.less_than_or_equal_to(100)
                ],
                nullable=True,
                description="Humidity percentage (0-100)"
            ),
            "timestamp": Column(
                dtype="object",
                nullable=True,
                description="Data timestamp"
            ),
            "hour": Column(
                dtype="int64",
                checks=[
                    Check.greater_than_or_equal_to(0),
                    Check.less_than_or_equal_to(23)
                ],
                nullable=True,
                description="Hour of the day (0-23)"
            ),
            "pressure": Column(
                dtype="float64",
                nullable=True,
                description="Atmospheric pressure"
            ),
            "minute": Column(
                dtype="int64",
                checks=[
                    Check.greater_than_or_equal_to(0),
                    Check.less_than_or_equal_to(59)
                ],
                nullable=True,
                description="Minute of the hour (0-59)"
            ),
            "close": Column(
                dtype="float64",
                nullable=True,
                description="Closing value"
            ),
            "visibility": Column(
                dtype="float64",
                nullable=True,
                description="Visibility measurement"
            ),
            "volume": Column(
                dtype="float64",
                nullable=True,
                description="Volume measurement"
            )
        },
        checks=[
            # Table-level checks
            Check(lambda df: len(df) >= 1, error="Table must have at least 1 row"),
            Check(lambda df: len(df) <= 1000000, error="Table must have at most 1,000,000 rows")
        ],
        ordered=False,  # Don't enforce column order since we have many columns
        strict=False,  # Allow additional columns beyond the schema
        description="Weather and financial data validation schema"
    )
    
    return schema


def validate_weather_data_pandera(data_path: str) -> Dict[str, Any]:
    """Validate weather data using Pandera schema validation"""
    
    print("=" * 60)
    print("PANDERA DATA QUALITY VALIDATION")
    print("=" * 60)
    
    # Create validation schema
    schema = create_weather_data_schema()
    
    # Find all parquet files
    if data_path.startswith("s3://"):
        print("❌ S3 reading not implemented - use local path")
        return {"success": False, "error": "S3 paths not supported"}
    
    parquet_files = glob.glob(f"{data_path}/**/*.parquet", recursive=True)
    
    if not parquet_files:
        print(f"❌ No parquet files found in {data_path}")
        print(f"🔍 Searched in: {os.path.abspath(data_path)}")
        return {"success": False, "error": "No parquet files found"}
    
    print(f"📁 Found {len(parquet_files)} parquet files")
    print("📋 Files found:")
    for file in parquet_files:
        print(f"   - {file}")
    
    # Read and combine all parquet files
    dfs = []
    total_rows = 0
    file_errors = []
    
    for file in parquet_files:
        try:
            df = pd.read_parquet(file)
            dfs.append(df)
            total_rows += len(df)
            
            print(f"📄 Loaded: {file} ({len(df)} rows)")
            print(f"   Columns: {list(df.columns)}")
            if len(df) > 0:
                print(f"   Sample data source: {df['data_source'].iloc[0] if 'data_source' in df.columns else 'N/A'}")
                print(f"   Sample year: {df['year'].iloc[0] if 'year' in df.columns else 'N/A'}")
                print(f"   Sample month: {df['month'].iloc[0] if 'month' in df.columns else 'N/A'}")
                print(f"   Sample day: {df['day'].iloc[0] if 'day' in df.columns else 'N/A'}")
            
        except Exception as e:
            error_msg = f"Error loading {file}: {e}"
            print(f"❌ {error_msg}")
            file_errors.append(error_msg)
    
    if not dfs:
        return {"success": False, "error": "No data could be loaded from parquet files", "file_errors": file_errors}
    
    # Combine all dataframes
    combined_df = pd.concat(dfs, ignore_index=True)
    print(f"📊 Combined dataset: {len(combined_df)} total rows")
    print(f"📊 Combined columns: {list(combined_df.columns)}")
    
    # Show data summary
    print("\n📈 Data Summary:")
    print(f"   Total records: {len(combined_df):,}")
    if 'data_source' in combined_df.columns:
        print(f"   Data sources: {combined_df['data_source'].value_counts().to_dict()}")
    if 'year' in combined_df.columns:
        print(f"   Year range: {combined_df['year'].min()} - {combined_df['year'].max()}")
    if 'month' in combined_df.columns:
        print(f"   Month range: {combined_df['month'].min()} - {combined_df['month'].max()}")
    if 'day' in combined_df.columns:
        print(f"   Day range: {combined_df['day'].min()} - {combined_df['day'].max()}")
    
    # Run Pandera validation
    try:
        print("\n🔍 Running Pandera validation...")
        validated_df = schema.validate(combined_df, lazy=True)
        
        print("✅ All validations passed!")
        
        return {
            "success": True,
            "total_records": len(combined_df),
            "validated_records": len(validated_df),
            "file_errors": file_errors,
            "schema_checks": len(schema.columns) + len(schema.checks),
            "data_summary": {
                "data_sources": combined_df['data_source'].value_counts().to_dict() if 'data_source' in combined_df.columns else {},
                "year_range": [int(combined_df['year'].min()), int(combined_df['year'].max())] if 'year' in combined_df.columns else None,
                "month_range": [int(combined_df['month'].min()), int(combined_df['month'].max())] if 'month' in combined_df.columns else None
            }
        }
        
    except pa.errors.SchemaErrors as e:
        print("❌ Pandera validation failed!")
        display_pandera_errors(e, combined_df)
        
        return {
            "success": False,
            "total_records": len(combined_df),
            "validation_errors": len(e.failure_cases),
            "file_errors": file_errors,
            "error_details": e.failure_cases.to_dict('records') if hasattr(e, 'failure_cases') else str(e)
        }
        
    except Exception as e:
        print(f"❌ Unexpected validation error: {e}")
        import traceback
        traceback.print_exc()
        
        return {
            "success": False,
            "total_records": len(combined_df),
            "error": str(e),
            "file_errors": file_errors
        }


def display_pandera_errors(schema_errors: pa.errors.SchemaErrors, df: pd.DataFrame):
    """Display Pandera validation errors in a professional format"""
    
    print("\n" + "=" * 60)
    print("VALIDATION ERRORS")
    print("=" * 60)
    
    failure_cases = schema_errors.failure_cases
    
    if failure_cases is not None and len(failure_cases) > 0:
        print(f"❌ Found {len(failure_cases)} validation errors:")
        print("-" * 50)
        
        # Group errors by check type
        error_groups = failure_cases.groupby(['schema_context', 'check'])
        
        for (context, check), group in error_groups:
            print(f"\n🔍 {context} - {check}")
            print(f"   Affected rows: {len(group)}")
            
            # Show sample failing values
            if 'failure_case' in group.columns:
                sample_failures = group['failure_case'].head(5).tolist()
                print(f"   Sample failing values: {sample_failures}")
            
            # Show affected columns if available
            if 'column' in group.columns:
                affected_columns = group['column'].unique()
                print(f"   Affected columns: {list(affected_columns)}")
    
    else:
        print("❌ Schema validation failed but no detailed error information available")
        print(f"Error message: {schema_errors}")
    
    # Additional data insights for debugging
    print("\n🔍 Data Quality Insights for Debugging:")
    print("-" * 40)
    
    # Check for missing columns
    expected_columns = ["processing_timestamp", "year", "month", "day", "data_source"]
    actual_columns = list(df.columns)
    missing_columns = set(expected_columns) - set(actual_columns)
    extra_columns = set(actual_columns) - set(expected_columns)
    
    if missing_columns:
        print(f"⚠️  Missing columns: {list(missing_columns)}")
    if extra_columns:
        print(f"ℹ️  Extra columns: {list(extra_columns)}")
    
    # Check data types
    print(f"📊 Current data types:")
    for col in actual_columns:
        if col in expected_columns:
            print(f"   {col}: {df[col].dtype}")
    
    # Check for null values
    print(f"📊 Null value counts:")
    for col in expected_columns:
        if col in df.columns:
            null_count = df[col].isnull().sum()
            print(f"   {col}: {null_count:,} nulls ({null_count/len(df)*100:.1f}%)")


def validate_single_file(file_path: str, schema: DataFrameSchema) -> Dict[str, Any]:
    """Validate a single parquet file using Pandera schema"""
    
    try:
        df = pd.read_parquet(file_path)
        validated_df = schema.validate(df, lazy=True)
        
        return {
            "file": file_path,
            "success": True,
            "rows": len(df),
            "columns": list(df.columns)
        }
        
    except pa.errors.SchemaErrors as e:
        return {
            "file": file_path,
            "success": False,
            "rows": len(pd.read_parquet(file_path)) if os.path.exists(file_path) else 0,
            "error_count": len(e.failure_cases) if hasattr(e, 'failure_cases') else 1,
            "errors": e.failure_cases.to_dict('records') if hasattr(e, 'failure_cases') else [str(e)]
        }
        
    except Exception as e:
        return {
            "file": file_path,
            "success": False,
            "error": str(e)
        }


def run_pandera_validation(data_path: str = "data/processed") -> Dict[str, Any]:
    """Main function to run Pandera-based data quality validation"""
    
    print("🔍 Starting Pandera Data Quality Validation Pipeline")
    print("=" * 60)
    
    # Create validation schema
    schema = create_weather_data_schema()
    
    # Validate the combined dataset
    results = validate_weather_data_pandera(data_path)
    
    return results


def validate_athena_table(
    database: str,
    table: str,
    region: str = 'us-east-1',
    s3_staging_dir: str = None,
    sample_size: Optional[int] = 10000,
    partition_filter: Optional[str] = None
) -> Dict[str, Any]:
    """
    Validate Athena table data using Pandera
    
    Args:
        database: Athena database name
        table: Athena table name
        region: AWS region
        s3_staging_dir: S3 path for Athena query results
        sample_size: Number of rows to validate (None for all)
        partition_filter: WHERE clause for partition filtering
    """
    
    if not ATHENA_AVAILABLE:
        return {
            'status': 'error',
            'error': 'PyAthena not installed. Run: pip install PyAthena'
        }
    
    if not s3_staging_dir:
        s3_staging_dir = f's3://your-athena-results-bucket/pandera-validation-results/'
    
    try:
        # Connect to Athena
        conn = connect(
            s3_staging_dir=s3_staging_dir,
            region_name=region
        )
        
        # Build query
        query = f"SELECT * FROM {database}.{table}"
        
        if partition_filter:
            query += f" WHERE {partition_filter}"
        
        if sample_size:
            query += f" LIMIT {sample_size}"
        
        print(f" Executing Athena query: {query}")
        
        # Read data into pandas DataFrame
        df = pd.read_sql(query, conn)
        
        print(f" Retrieved {len(df)} rows from Athena")
        
        # Get schema and validate
        schema = create_weather_data_schema()
        
        try:
            validated_df = schema.validate(df, lazy=True)
            
            return {
                'status': 'valid',
                'table': f"{database}.{table}",
                'rows_validated': len(df),
                'validation_timestamp': datetime.now().isoformat(),
                'query': query
            }
            
        except pa.errors.SchemaErrors as schema_errors:
            display_pandera_errors(schema_errors, df)
            
            return {
                'status': 'invalid',
                'table': f"{database}.{table}",
                'rows_validated': len(df),
                'validation_timestamp': datetime.now().isoformat(),
                'errors': str(schema_errors),
                'query': query
            }
            
    except Exception as e:
        return {
            'status': 'error',
            'table': f"{database}.{table}",
            'error': str(e),
            'validation_timestamp': datetime.now().isoformat()
        }
    
    finally:
        if 'conn' in locals():
            conn.close()


def run_validation_pipeline(
    mode: str = 'local',
    data_path: str = "data/processed",
    database: str = None,
    table: str = None,
    **kwargs
) -> Dict[str, Any]:
    """
    Run validation in either local or athena mode
    
    Args:
        mode: 'local' or 'athena'
        data_path: Path for local validation
        database: Athena database name
        table: Athena table name
        **kwargs: Additional arguments for athena validation
    """
    
    if mode == 'local':
        print(" Running LOCAL validation...")
        return run_pandera_validation(data_path)
    
    elif mode == 'athena':
        if not database or not table:
            return {
                'status': 'error',
                'error': 'Database and table required for Athena validation'
            }
        
        print(" Running ATHENA validation...")
        return validate_athena_table(database, table, **kwargs)
    
    else:
        return {
            'status': 'error',
            'error': f"Invalid mode: {mode}. Use 'local' or 'athena'"
        }


if __name__ == "__main__":
    import sys
    
    # Parse command line arguments for mode selection
    if len(sys.argv) > 1 and sys.argv[1] == 'athena':
        # Example: python pandera_weather_validation.py athena my_database my_table
        if len(sys.argv) >= 4:
            database = sys.argv[2]
            table = sys.argv[3]
            results = run_validation_pipeline(
                mode='athena',
                database=database,
                table=table,
                s3_staging_dir='s3://your-bucket/athena-results/'  # Update this
            )
        else:
            print("Usage: python pandera_weather_validation.py athena <database> <table>")
            sys.exit(1)
    else:
        # Default local validation
        results = run_validation_pipeline(mode='local')
    
    # Display results
    if results:
        print("\n" + "="*60)
        print(" VALIDATION SUMMARY")
        print("="*60)
        
        if isinstance(results, list):
            # Local validation results
            for result in results:
                status_emoji = "" if result['status'] == 'valid' else ""
                print(f"{status_emoji} {result['file']}: {result['status']}")
                if result['status'] != 'valid':
                    print(f"   Error: {result['error']}")
        else:
            # Athena validation result
            status_emoji = "" if results['status'] == 'valid' else ""
            print(f"{status_emoji} {results.get('table', 'Athena Table')}: {results['status']}")
            if results['status'] != 'valid':
                print(f"   Error: {results.get('error', 'Unknown error')}")
                
        print("="*60)
