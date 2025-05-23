import great_expectations as ge
from great_expectations.core import ExpectationSuite
from great_expectations.dataset import SparkDFDataset
from great_expectations.profile import BasicSuiteBuilderProfiler
import boto3
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class FinancialDataValidator:
    def __init__(self, context_path: str = "great_expectations"):
        """Initialize the validator with Great Expectations context."""
        self.context = ge.get_context(context_path)
        self.suite_name = "financial_data_suite"
        self._create_expectation_suite()

    def _create_expectation_suite(self) -> None:
        """Create or load the expectation suite for financial data."""
        try:
            self.suite = self.context.get_expectation_suite(self.suite_name)
        except:
            self.suite = self.context.create_expectation_suite(self.suite_name)
            self._add_expectations()

    def _add_expectations(self) -> None:
        """Add default expectations for financial data."""
        self.suite.add_expectation(
            ge.core.ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "close"}
            )
        )
        self.suite.add_expectation(
            ge.core.ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "daily_return",
                    "min_value": -0.5,
                    "max_value": 0.5
                }
            )
        )
        self.suite.add_expectation(
            ge.core.ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "volatility",
                    "min_value": 0.0,
                    "max_value": 1.0
                }
            )
        )
        self.suite.add_expectation(
            ge.core.ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_greater_than",
                kwargs={
                    "column": "close",
                    "value": 0
                }
            )
        )

    def validate_dataframe(self, df: Any) -> Dict[str, Any]:
        """Validate a Spark DataFrame using Great Expectations."""
        try:
            # Convert to Great Expectations dataset
            ge_df = SparkDFDataset(df)
            
            # Run validation
            validation_result = self.context.run_validation_operator(
                "action_list_operator",
                assets_to_validate=[(ge_df, self.suite_name)]
            )
            
            return {
                "success": validation_result.success,
                "statistics": validation_result.statistics,
                "results": [
                    {
                        "expectation_type": result.expectation_config.expectation_type,
                        "success": result.success,
                        "kwargs": result.expectation_config.kwargs
                    }
                    for result in validation_result.results
                ]
            }
        except Exception as e:
            logger.error(f"Error validating dataframe: {str(e)}")
            return {"success": False, "error": str(e)}

    def run_athena_validation(
        self,
        database: str,
        table: str,
        output_location: str
    ) -> Dict[str, Any]:
        """Run validation queries using Athena."""
        try:
            athena_client = boto3.client('athena')
            
            # Create validation query
            query = f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(CASE WHEN daily_return < -0.5 OR daily_return > 0.5 THEN 1 END) as extreme_returns,
                COUNT(CASE WHEN volatility > 1.0 THEN 1 END) as high_volatility,
                COUNT(CASE WHEN close <= 0 THEN 1 END) as invalid_prices
            FROM {database}.{table}
            """
            
            # Start query execution
            response = athena_client.start_query_execution(
                QueryString=query,
                ResultConfiguration={'OutputLocation': output_location},
                QueryExecutionContext={'Database': database}
            )
            
            query_execution_id = response['QueryExecutionId']
            
            # Wait for query to complete
            while True:
                query_status = athena_client.get_query_execution(
                    QueryExecutionId=query_execution_id
                )['QueryExecution']['Status']['State']
                
                if query_status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                    break
                    
            if query_status == 'SUCCEEDED':
                # Get results
                results = athena_client.get_query_results(
                    QueryExecutionId=query_execution_id
                )
                
                # Process results
                if len(results['ResultSet']['Rows']) > 1:
                    data = results['ResultSet']['Rows'][1]['Data']
                    return {
                        "total_rows": int(data[0]['VarCharValue']),
                        "extreme_returns": int(data[1]['VarCharValue']),
                        "high_volatility": int(data[2]['VarCharValue']),
                        "invalid_prices": int(data[3]['VarCharValue'])
                    }
            
            return {"error": f"Query failed with status: {query_status}"}
            
        except Exception as e:
            logger.error(f"Error running Athena validation: {str(e)}")
            return {"error": str(e)} 