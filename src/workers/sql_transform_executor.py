"""Executor for SQL transformation jobs using service abstraction."""

import json
from typing import Dict, Any
from uuid import UUID

from .job_worker import JobExecutor
from ..infrastructure.postgres.database import DatabasePool
from ..infrastructure.postgres.event_store import PostgresEventStore
from ..core.events.registry import InMemoryEventBus
from src.features.sql_workbench.services.sql_execution import (
    SqlValidationService, SqlExecutionService,
    SqlSource, SqlTarget
)
from ..core.events.publisher import JobStartedEvent, JobCompletedEvent, JobFailedEvent


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
        
        # Create services - table reader needs a connection, not pool
        validation_service = SqlValidationService()
        # We'll create table reader with proper connection in the execution service
        execution_service = SqlExecutionService(
            db_pool=db_pool,
            validation_service=validation_service,
            table_reader=None  # Will be created with connection in service
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
            output_branch_name=parameters['target'].get('output_branch_name'),
            expected_head_commit_id=parameters['target'].get('expected_head_commit_id')
        )
        
        try:
            # Publish job started event
            await event_bus.publish(JobStartedEvent(
                job_id=UUID(job_id),
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
                job_id=UUID(job_id),
                status='completed',
                dataset_id=target.dataset_id,
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
                job_id=UUID(job_id),
                error_message=str(e),
                dataset_id=target.dataset_id if 'target' in locals() else None
            ))
            
            raise