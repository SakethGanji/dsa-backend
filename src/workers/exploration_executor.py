"""Executor for exploration/profiling jobs."""

import json
import logging
from typing import Dict, Any
import pandas as pd
from ydata_profiling import ProfileReport
import asyncio
from concurrent.futures import ThreadPoolExecutor

from ..infrastructure.postgres.database import DatabasePool
from ..infrastructure.postgres.table_reader import PostgresTableReader
from ..infrastructure.postgres.event_store import PostgresEventStore
from ..core.events.registry import InMemoryEventBus
from .job_worker import JobExecutor
from src.core.domain_exceptions import EntityNotFoundException
from dataclasses import dataclass
from typing import Optional


logger = logging.getLogger(__name__)


# Import event classes from core
from src.core.events.publisher import JobStartedEvent, JobCompletedEvent, JobFailedEvent
from uuid import UUID


class ExplorationExecutor(JobExecutor):
    """Executor for running pandas profiling exploration jobs."""
    
    def __init__(self, pool: DatabasePool):
        self.pool = pool
        self.executor = ThreadPoolExecutor(max_workers=2)
    
    async def execute(self, job_id: str, parameters: Dict[str, Any], db_pool: DatabasePool) -> Dict[str, Any]:
        """Execute pandas profiling on dataset."""
        # Create event bus and store for publishing events
        event_store = PostgresEventStore(db_pool)
        event_bus = InMemoryEventBus()
        event_bus.set_event_store(event_store)
        
        # Get job details from database
        async with db_pool.acquire() as conn:
            job = await conn.fetchrow(
                "SELECT dataset_id, source_commit_id, user_id FROM dsa_jobs.analysis_runs WHERE id = $1::uuid",
                job_id
            )
            
            if not job:
                raise EntityNotFoundException("Job", job_id)
            
            dataset_id = job["dataset_id"]
            source_commit_id = job["source_commit_id"]
            user_id = job["user_id"]
        
        # Extract parameters
        table_key = parameters.get("table_key", "primary")
        profile_config = parameters.get("profile_config", {})
        
        logger.info(f"Starting exploration job {job_id} for dataset {dataset_id}, table {table_key}")
        
        try:
            # Publish job started event
            await event_bus.publish(JobStartedEvent(
                job_id=UUID(job_id),
                job_type='exploration',
                dataset_id=dataset_id,
                user_id=user_id
            ))
            
            # Read data using existing infrastructure
            async with db_pool.acquire() as conn:
                from ..infrastructure.postgres.uow import PostgresUnitOfWork
                async with PostgresUnitOfWork(db_pool) as uow:
                    table_reader = PostgresTableReader(uow.connection)
                    # Get table data as list of dicts
                    table_data = await table_reader.get_table_data(source_commit_id, table_key)
                    
                    # Convert to DataFrame
                    if not table_data:
                        df = pd.DataFrame()
                    else:
                        df = pd.DataFrame(table_data)
            
            # Check if DataFrame is empty
            if df.empty:
                # Return minimal results for empty DataFrame
                dataset_info = {
                    "rows": 0,
                    "columns": 0,
                    "memory_usage": 0.0,
                    "table_key": table_key,
                    "is_empty": True
                }
                
                # Create a simple HTML report for empty dataset
                profile_html = f"""
                <html>
                <head><title>Dataset Profile - Empty</title></head>
                <body>
                    <h1>Dataset Profile</h1>
                    <p>The dataset is empty (no data available).</p>
                    <ul>
                        <li>Table: {table_key}</li>
                        <li>Rows: 0</li>
                        <li>Columns: 0</li>
                    </ul>
                </body>
                </html>
                """
                
                profile_json = json.dumps({
                    "dataset_info": dataset_info,
                    "message": "Dataset is empty"
                })
                
                result = {
                    "profile_html": profile_html,
                    "profile_json": profile_json,
                    "dataset_info": dataset_info
                }
                
                # Publish job completed event for empty dataset
                await event_bus.publish(JobCompletedEvent(
                    job_id=UUID(job_id),
                    status='completed',
                    dataset_id=dataset_id,
                    result=dataset_info
                ))
                
                return result
            
            # Run profiling in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            profile_html, profile_json = await loop.run_in_executor(
                self.executor,
                self._generate_profile,
                df,
                profile_config
            )
            
            # Return results
            result = {
                "profile_html": profile_html,
                "profile_json": profile_json,
                "dataset_info": {
                    "rows": len(df),
                    "columns": len(df.columns),
                    "memory_usage": float(df.memory_usage(deep=True).sum()),
                    "table_key": table_key
                }
            }
            
            # Publish job completed event
            await event_bus.publish(JobCompletedEvent(
                job_id=UUID(job_id),
                status='completed',
                dataset_id=dataset_id,
                result=result["dataset_info"]
            ))
            
            return result
            
        except Exception as e:
            logger.error(f"Exploration job {job_id} failed: {str(e)}", exc_info=True)
            
            # Publish job failed event
            await event_bus.publish(JobFailedEvent(
                job_id=UUID(job_id),
                error_message=str(e),
                dataset_id=dataset_id
            ))
            
            raise
    
    def _generate_profile(self, df: pd.DataFrame, config: Dict[str, Any]) -> tuple[str, str]:
        """Generate pandas profiling report."""
        # Default minimal config for performance
        default_config = {
            "samples": {"head": 10, "tail": 10},
            "duplicates": {"head": 10},
            "interactions": {"continuous": False},
            "correlations": {
                "pearson": {"calculate": True},
                "spearman": {"calculate": False},
                "kendall": {"calculate": False}
            }
        }
        
        # Merge with user config
        if config:
            default_config.update(config)
        
        # Generate profile
        profile = ProfileReport(df, **default_config)
        
        # Return HTML and JSON
        return profile.to_html(), profile.to_json()