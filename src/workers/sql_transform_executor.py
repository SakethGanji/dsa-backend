"""Executor for SQL transformation jobs using service abstraction."""

import json
from typing import Dict, Any

from .job_worker import JobExecutor
from ..infrastructure.postgres.database import DatabasePool
from ..infrastructure.postgres.event_store import PostgresEventStore
from ..core.events.registry import InMemoryEventBus
from ..infrastructure.services.sql_execution import (
    SqlValidationService, SqlExecutionService, QueryOptimizationService,
    SqlSource, SqlTarget
)
from dataclasses import dataclass
from typing import Optional
from uuid import UUID
from ..infrastructure.postgres.table_reader import PostgresTableReader


# Job event classes
@dataclass
class JobStartedEvent:
    """Event raised when a job starts."""
    job_id: str
    job_type: str
    dataset_id: int
    user_id: int


@dataclass
class JobCompletedEvent:
    """Event raised when a job completes successfully."""
    job_id: str
    job_type: str
    dataset_id: int
    user_id: int
    result: Dict[str, Any]


@dataclass
class JobFailedEvent:
    """Event raised when a job fails."""
    job_id: str
    job_type: str
    dataset_id: Optional[int]
    user_id: int
    error_message: str


class SqlTransformExecutor(JobExecutor):
    """Executes SQL transformation jobs using SQL execution services."""
    
    async def execute(self, job_id: str, parameters: Dict[str, Any], db_pool: DatabasePool) -> Dict[str, Any]:
        """Execute SQL transformation job using services."""
        import logging
        logger = logging.getLogger(__name__)
        
        logger.info(f"SQL transform job {job_id} starting with parameters: {parameters}")
        
        # Create event bus and store for publishing events
        event_store = PostgresEventStore(db_pool)
        event_bus = InMemoryEventBus()
        event_bus.set_event_store(event_store)
        
        # Handle case where parameters come as string
        if isinstance(parameters, str):
            parameters = json.loads(parameters)
        
        # Get job details from database
        async with db_pool.acquire() as conn:
            job = await conn.fetchrow(
                "SELECT dataset_id, user_id FROM dsa_jobs.analysis_runs WHERE id = $1",
                job_id
            )
            
            user_id = job['user_id']
        
        # Create services
        validation_service = SqlValidationService()
        table_reader = PostgresTableReader(db_pool)
        execution_service = SqlExecutionService(
            db_pool=db_pool,
            validation_service=validation_service,
            table_reader=table_reader
        )
        
        # Convert parameters to service models
        sources = [
            SqlSource(
                dataset_id=src['dataset_id'],
                ref=src['ref'],
                table_key=src['table_key'],
                alias=src['alias']
            )
            for src in parameters['sources']
        ]
        
        target = SqlTarget(
            dataset_id=parameters['target']['dataset_id'],
            ref=parameters['target']['ref'],
            table_key=parameters['target']['table_key'],
            message=parameters['target']['message'],
            output_branch_name=parameters['target'].get('output_branch_name')
        )
        
        try:
            # Publish job started event
            await event_bus.publish(JobStartedEvent(
                job_id=job_id,
                job_type='sql_transform',
                dataset_id=target.dataset_id,
                user_id=user_id
            ))
            
            # Create execution plan
            plan = await execution_service.create_execution_plan(
                sources=sources,
                sql=parameters['sql'],
                target=target
            )
            
            logger.info(f"Created execution plan with estimated {plan.estimated_rows} rows")
            
            # Execute transformation
            result = await execution_service.execute_transformation(
                plan=plan,
                job_id=job_id,
                user_id=user_id
            )
            
            logger.info(f"SQL transform job {job_id} completed successfully")
            
            # Publish job completed event
            await event_bus.publish(JobCompletedEvent(
                job_id=job_id,
                job_type='sql_transform',
                dataset_id=target.dataset_id,
                user_id=user_id,
                result={
                    "rows_processed": result.rows_processed,
                    "new_commit_id": result.new_commit_id,
                    "output_branch_name": result.output_branch_name
                }
            ))
            
            return {
                "rows_processed": result.rows_processed,
                "new_commit_id": result.new_commit_id,
                "output_branch_name": result.output_branch_name,
                "execution_time_ms": result.execution_time_ms,
                "target_ref": target.ref,
                "table_key": result.table_key
            }
            
        except Exception as e:
            logger.error(f"SQL transform job {job_id} failed: {str(e)}", exc_info=True)
            
            # Publish job failed event
            await event_bus.publish(JobFailedEvent(
                job_id=job_id,
                job_type='sql_transform',
                dataset_id=target.dataset_id if 'target' in locals() else None,
                user_id=user_id,
                error_message=str(e)
            ))
            
            raise