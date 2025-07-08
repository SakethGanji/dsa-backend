"""Background job worker that polls database for pending jobs."""

import asyncio
import logging
import json
from datetime import datetime
from typing import Dict, Any, Optional
from abc import ABC, abstractmethod

from src.core.database import DatabasePool
from src.core.config import get_settings

logger = logging.getLogger(__name__)


class JobExecutor(ABC):
    """Base class for job executors."""
    
    @abstractmethod
    async def execute(self, job_id: str, parameters: Dict[str, Any], db_pool: DatabasePool) -> Dict[str, Any]:
        """Execute the job and return results."""
        pass


class JobWorker:
    """Worker that polls database for pending jobs and executes them."""
    
    def __init__(self, db_pool: DatabasePool):
        self.db_pool = db_pool
        self.executors: Dict[str, JobExecutor] = {}
        self.running = False
        self.poll_interval = 5  # seconds
        
    def register_executor(self, job_type: str, executor: JobExecutor):
        """Register an executor for a job type."""
        self.executors[job_type] = executor
        logger.info(f"Registered executor for job type: {job_type}")
        
    async def process_job(self, job: Dict[str, Any]):
        """Process a single job."""
        job_id = str(job['id'])
        job_type = job['run_type']
        
        logger.info(f"Processing job {job_id} of type {job_type}")
        
        # Update status to running
        await self._update_job_status(job_id, 'running')
        
        try:
            # Get parameters first
            parameters = job.get('run_parameters', {})
            
            # Parse JSON if it's a string
            if isinstance(parameters, str):
                try:
                    parameters = json.loads(parameters)
                except json.JSONDecodeError:
                    logger.error(f"Failed to parse run_parameters JSON: {parameters}")
                    parameters = {}
            
            # Get executor - check if parameters specify a different job_type
            actual_job_type = job_type
            if isinstance(parameters, dict) and 'job_type' in parameters:
                actual_job_type = parameters['job_type']
                logger.info(f"Using job_type from parameters: {actual_job_type}")
            
            executor = self.executors.get(actual_job_type)
            if not executor:
                raise ValueError(f"No executor registered for job type: {actual_job_type}")
            
            logger.info(f"Job parameters type: {type(parameters)}, value: {parameters}")
            result = await executor.execute(job_id, parameters, self.db_pool)
            
            # Update job as completed
            await self._update_job_status(
                job_id, 
                'completed',
                output_summary=result,
                completed_at=datetime.utcnow()
            )
            
            logger.info(f"Job {job_id} completed successfully")
            
        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            logger.error(f"Job {job_id} failed: {str(e)}\n{error_details}")
            await self._update_job_status(
                job_id,
                'failed',
                error_message=str(e),
                completed_at=datetime.utcnow()
            )
    
    async def _update_job_status(
        self, 
        job_id: str, 
        status: str,
        output_summary: Optional[Dict[str, Any]] = None,
        error_message: Optional[str] = None,
        completed_at: Optional[datetime] = None
    ):
        """Update job status in database."""
        async with self.db_pool.acquire() as conn:
            query = """
                UPDATE dsa_jobs.analysis_runs
                SET status = $1,
                    output_summary = $2,
                    error_message = $3,
                    completed_at = $4
                WHERE id = $5
            """
            
            output_json = json.dumps(output_summary) if output_summary else None
            
            await conn.execute(
                query,
                status,
                output_json,
                error_message,
                completed_at,
                job_id
            )
    
    async def poll_for_jobs(self):
        """Poll database for pending jobs."""
        while self.running:
            try:
                async with self.db_pool.acquire() as conn:
                    # Get pending jobs
                    query = """
                        SELECT id, run_type, dataset_id, source_commit_id, 
                               user_id, run_parameters
                        FROM dsa_jobs.analysis_runs
                        WHERE status = 'pending'
                        ORDER BY created_at
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                    """
                    
                    job = await conn.fetchrow(query)
                    
                    if job:
                        await self.process_job(dict(job))
                    else:
                        # No jobs, wait before polling again
                        await asyncio.sleep(self.poll_interval)
                        
            except Exception as e:
                logger.error(f"Error polling for jobs: {str(e)}")
                await asyncio.sleep(self.poll_interval)
    
    async def start(self):
        """Start the worker."""
        logger.info("Starting job worker...")
        self.running = True
        await self.poll_for_jobs()
    
    async def stop(self):
        """Stop the worker."""
        logger.info("Stopping job worker...")
        self.running = False