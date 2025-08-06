import pandas as pd
import numpy as np
from datetime import datetime
import os
import json
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.data_context.types.resource_identifiers import GeCloudIdentifier
from great_expectations.core import ExpectationConfiguration
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.validator.validator import Validator
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.data_context.store import ExpectationsStore
from great_expectations.data_context.store.metric_store import MetricStore
from great_expectations.data_context.types.resource_identifiers import GeCloudIdentifier
from great_expectations.core.util import convert_to_json_serializable
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WeatherDataQualityValidator:
    """Great Expectations-based data quality validator for weather data"""
    
    def __init__(self, data_context_path: str = "great_expectations"):
        """Initialize the validator with Great Expectations context"""
        self.data_context_path = data_context_path
        self.context = self._initialize_context()
        self.expectation_suite_name = "weather_data_suite"
        
    def _initialize_context(self):
        """Initialize Great Expectations data context"""
        try:
            # Create data context configuration
            data_context_config = DataContextConfig(
                config_version=3.0,
                plugins_directory=None,
                config_variables_file_path=None,
                stores={
                    "expectations_store": {
                        "class_name": "ExpectationsStore",
                        "store_backend": {
                            "class_name": "TupleFilesystemStoreBackend",
                            "base_directory": f"{self.data_context_path}/expectations"
                        }
                    },
                    "validations_store": {
                        "class_name": "ValidationsStore",
                        "store_backend": {
                            "class_name": "TupleFilesystemStoreBackend",
                            "base_directory": f"{self.data_context_path}/validations"
                        }
                    },
                    "evaluation_parameter_store": {
                        "class_name": "EvaluationParameterStore",
                        "store_backend": {
                            "class_name": "TupleFilesystemStoreBackend",
                            "base_directory": f"{self.data_context_path}/evaluation_parameters"
                        }
                    },
                    "checkpoint_store": {
                        "class_name": "CheckpointStore",
                        "store_backend": {
                            "class_name": "TupleFilesystemStoreBackend",
                            "base_directory": f"{self.data_context_path}/checkpoints"
                        }
                    },
                    "profiler_store": {
                        "class_name": "ProfilerStore",
                        "store_backend": {
                            "class_name": "TupleFilesystemStoreBackend",
                            "base_directory": f"{self.data_context_path}/profilers"
                        }
                    },
                    "metric_store": {
                        "class_name": "MetricStore",
                        "store_backend": {
                            "class_name": "TupleFilesystemStoreBackend",
                            "base_directory": f"{self.data_context_path}/metrics"
                        }
                    }
                },
                expectations_store_name="expectations_store",
                validations_store_name="validations_store",
                evaluation_parameter_store_name="evaluation_parameter_store",
                checkpoint_store_name="checkpoint_store",
                profiler_store_name="profiler_store",
                metric_store_name="metric_store",
                data_docs_sites={
                    "local_site": {
                        "class_name": "SiteBuilder",
                        "store_backend": {
                            "class_name": "TupleFilesystemStoreBackend",
                            "base_directory": f"{self.data_context_path}/data_docs/local_site"
                        },
                        "site_index_builder": {
                            "class_name": "DefaultSiteIndexBuilder",
                            "show_cta_footer": True
                        }
                    }
                },
                validation_operators={
                    "action_list_operator": {
                        "class_name": "ActionListValidationOperator",
                        "action_list": [
                            {
                                "name": "store_validation_result",
                                "action": {
                                    "class_name": "StoreValidationResultAction"
                                }
                            },
                            {
                                "name": "store_evaluation_params",
                                "action": {
                                    "class_name": "StoreEvaluationParametersAction"
                                }
                            },
                            {
                                "name": "update_data_docs",
                                "action": {
                                    "class_name": "UpdateDataDocsAction"
                                }
                            }
                        ]
                    }
                }
            )
            
            context = BaseDataContext(project_config=data_context_config)
            logger.info("Great Expectations context initialized successfully")
            return context
            
        except Exception as e:
            logger.error(f"Failed to initialize Great Expectations context: {e}")
            raise
    
    def create_expectation_suite(self):
        """Create a comprehensive expectation suite for weather data"""
        try:
            # Create or get expectation suite
            suite = self.context.get_or_create_expectation_suite(
                expectation_suite_name=self.expectation_suite_name
            )
            
            # Clear existing expectations
            suite.expectations = []
            
            # Add comprehensive expectations for weather data
            expectations = [
                # Data presence and completeness
                ExpectationConfiguration(
                    expectation_type="expect_table_columns_to_match_ordered_list",
                    kwargs={
                        "column_list": [
                            "processing_timestamp", "year", "month", "day", "hour", "minute",
                            "data_source", "data_version", "temperature", "wind_speed", "city"
                        ]
                    }
                ),
                
                # Required columns should not be null
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_not_be_null",
                    kwargs={"column": "processing_timestamp"}
                ),
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_not_be_null",
                    kwargs={"column": "data_source"}
                ),
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_not_be_null",
                    kwargs={"column": "data_version"}
                ),
                
                # Data source validation
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_in_set",
                    kwargs={
                        "column": "data_source",
                        "value_set": ["noaa", "alphavantage", "eosdis"]
                    }
                ),
                
                # Data version validation
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_in_set",
                    kwargs={
                        "column": "data_version",
                        "value_set": ["4.0.0"]
                    }
                ),
                
                # Time-based validations
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_between",
                    kwargs={
                        "column": "year",
                        "min_value": 2020,
                        "max_value": 2030
                    }
                ),
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_between",
                    kwargs={
                        "column": "month",
                        "min_value": 1,
                        "max_value": 12
                    }
                ),
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_between",
                    kwargs={
                        "column": "day",
                        "min_value": 1,
                        "max_value": 31
                    }
                ),
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_between",
                    kwargs={
                        "column": "hour",
                        "min_value": 0,
                        "max_value": 23
                    }
                ),
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_between",
                    kwargs={
                        "column": "minute",
                        "min_value": 0,
                        "max_value": 59
                    }
                ),
                
                # Temperature validation (if present)
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_between",
                    kwargs={
                        "column": "temperature",
                        "min_value": -100,
                        "max_value": 150,
                        "mostly": 0.95  # Allow 5% outliers
                    }
                ),
                
                # Wind speed validation (if present)
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_between",
                    kwargs={
                        "column": "wind_speed",
                        "min_value": 0,
                        "max_value": 200,
                        "mostly": 0.95
                    }
                ),
                
                # Data freshness validation
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_between",
                    kwargs={
                        "column": "processing_timestamp",
                        "min_value": datetime.now().replace(hour=0, minute=0, second=0, microsecond=0),
                        "max_value": datetime.now(),
                        "parse_strings_as_datetimes": True
                    }
                ),
                
                # Row count validation
                ExpectationConfiguration(
                    expectation_type="expect_table_row_count_to_be_between",
                    kwargs={
                        "min_value": 1,
                        "max_value": 1000000
                    }
                ),
                
                # Uniqueness validation for processing timestamp
                ExpectationConfiguration(
                    expectation_type="expect_compound_columns_to_be_unique",
                    kwargs={
                        "column_list": ["processing_timestamp", "data_source"]
                    }
                )
            ]
            
            # Add expectations to suite
            for expectation in expectations:
                suite.add_expectation(expectation)
            
            # Save the suite
            self.context.save_expectation_suite(suite)
            logger.info(f"Created expectation suite: {self.expectation_suite_name}")
            
        except Exception as e:
            logger.error(f"Failed to create expectation suite: {e}")
            raise
    
    def validate_data(self, df: pd.DataFrame) -> dict:
        """Validate data using Great Expectations"""
        try:
            # Create expectation suite if it doesn't exist
            self.create_expectation_suite()
            
            # Create batch request
            batch_request = RuntimeBatchRequest(
                datasource_name="pandas_datasource",
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name="weather_data",
                runtime_parameters={"batch_data": df},
                batch_identifiers={"default_identifier_name": "default_identifier"}
            )
            
            # Get validator
            validator = self.context.get_validator(
                batch_request=batch_request,
                expectation_suite_name=self.expectation_suite_name
            )
            
            # Run validation
            validation_result = validator.validate()
            
            # Process results
            results = {
                "success": validation_result.success,
                "statistics": validation_result.statistics,
                "meta": validation_result.meta,
                "results": []
            }
            
            # Extract detailed results
            for result in validation_result.run_results:
                for expectation_result in result["validation_result"]["results"]:
                    results["results"].append({
                        "expectation_type": expectation_result["expectation_config"]["expectation_type"],
                        "success": expectation_result["success"],
                        "kwargs": expectation_result["expectation_config"]["kwargs"],
                        "exception_info": expectation_result.get("exception_info"),
                        "result": expectation_result.get("result", {})
                    })
            
            return results
            
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            raise
    
    def validate_weather_data_files(self, data_path: str) -> dict:
        """Validate weather data from parquet files using Great Expectations"""
        try:
            # Read data using pandas
            if data_path.startswith("s3://"):
                logger.warning("S3 reading not implemented - use local path")
                return None
            
            # Read all parquet files recursively
            import glob
            parquet_files = glob.glob(f"{data_path}/**/*.parquet", recursive=True)
            
            if not parquet_files:
                logger.warning(f"No parquet files found in {data_path}")
                return None
            
            logger.info(f"Found {len(parquet_files)} parquet files")
            
            # Read and combine all parquet files
            dfs = []
            for file in parquet_files:
                try:
                    df = pd.read_parquet(file)
                    dfs.append(df)
                    logger.info(f"Loaded: {file}")
                except Exception as e:
                    logger.error(f"Error loading {file}: {e}")
            
            if not dfs:
                logger.error("No data found in parquet files")
                return None
            
            # Combine all dataframes
            combined_df = pd.concat(dfs, ignore_index=True)
            logger.info(f"Combined {len(combined_df)} records from {len(dfs)} files")
            
            # Run Great Expectations validation
            validation_results = self.validate_data(combined_df)
            
            # Print results
            self._print_validation_results(validation_results)
            
            return validation_results
            
        except Exception as e:
            logger.error(f"Failed to validate weather data: {e}")
            raise
    
    def _print_validation_results(self, results: dict):
        """Print validation results in a readable format"""
        print("\n" + "="*60)
        print("GREAT EXPECTATIONS DATA QUALITY VALIDATION RESULTS")
        print("="*60)
        
        print(f"Overall Success: {'✅ PASS' if results['success'] else '❌ FAIL'}")
        print(f"Statistics: {results['statistics']}")
        
        print("\nDetailed Results:")
        print("-" * 40)
        
        passed = 0
        failed = 0
        
        for result in results['results']:
            status = "✅ PASS" if result['success'] else "❌ FAIL"
            expectation_type = result['expectation_type']
            
            print(f"{status} {expectation_type}")
            
            if result['success']:
                passed += 1
            else:
                failed += 1
                if 'exception_info' in result and result['exception_info']:
                    print(f"  Error: {result['exception_info']}")
                if 'result' in result and result['result']:
                    print(f"  Details: {result['result']}")
        
        print(f"\nSummary: {passed} passed, {failed} failed")
        print("="*60)

def main():
    """Main function to run weather data validation"""
    try:
        # Initialize validator
        validator = WeatherDataQualityValidator()
        
        # Validate data
        data_path = "data/processed"
        results = validator.validate_weather_data_files(data_path)
        
        if results:
            print(f"\nValidation completed successfully!")
            print(f"Success: {results['success']}")
        else:
            print("Validation failed - no data found")
            
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        raise

if __name__ == "__main__":
    main() 