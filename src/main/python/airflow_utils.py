from airflow.api.client.local_client import Client
from airflow.models import DagRun
from airflow.utils.state import State
import json
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)

class AirflowManager:
    def __init__(self):
        self.client = Client()
    
    def list_dag_runs(self, dag_id: str) -> List[Dict[str, str]]:
        """List all runs for a specific DAG."""
        try:
            dag_runs = self.client.get_dag_runs(dag_id)
            return [
                {
                    'dag_id': run.dag_id,
                    'execution_date': run.execution_date.isoformat(),
                    'state': run.state,
                    'start_date': run.start_date.isoformat() if run.start_date else None,
                    'end_date': run.end_date.isoformat() if run.end_date else None
                }
                for run in dag_runs
            ]
        except Exception as e:
            logger.error(f"Error listing DAG runs: {str(e)}")
            return []

    def get_task_status(self, dag_id: str, task_id: str, execution_date: str) -> str:
        """Get the status of a specific task in a DAG run."""
        try:
            task_instance = self.client.get_task_instance(
                dag_id=dag_id,
                task_id=task_id,
                execution_date=execution_date
            )
            return task_instance.state
        except Exception as e:
            logger.error(f"Error getting task status: {str(e)}")
            return "ERROR"

    def trigger_dag(self, dag_id: str, conf: Optional[Dict[str, Any]] = None) -> bool:
        """Trigger a DAG run with optional configuration."""
        try:
            self.client.trigger_dag(
                dag_id=dag_id,
                conf=conf
            )
            return True
        except Exception as e:
            logger.error(f"Error triggering DAG: {str(e)}")
            return False

    def retry_failed_tasks(self, dag_id: str, execution_date: str) -> bool:
        """Retry failed tasks in a DAG run."""
        try:
            self.client.retry_dag_run(
                dag_id=dag_id,
                execution_date=execution_date
            )
            return True
        except Exception as e:
            logger.error(f"Error retrying failed tasks: {str(e)}")
            return False

    def handle_kafka_feedback(self, dag_id: str, feedback: Dict[str, Any]) -> None:
        """Handle feedback from Kafka to manage DAG runs."""
        status = feedback.get('status')
        if status == 'FAILED':
            execution_date = feedback.get('execution_date')
            if execution_date:
                logger.info(f"Retrying failed tasks for DAG {dag_id} at {execution_date}")
                self.retry_failed_tasks(dag_id, execution_date)
            else:
                logger.warning("No execution date provided in feedback")
        elif status == 'SUCCESS':
            logger.info(f"DAG {dag_id} completed successfully")
        else:
            logger.warning(f"Unknown status in feedback: {status}") 