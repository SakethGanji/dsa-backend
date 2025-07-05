"""PostgreSQL implementation of IJobRepository."""

from typing import Optional, Dict, Any, List
from uuid import UUID, uuid4
import json
from datetime import datetime
from asyncpg import Connection
from ...services.interfaces import IJobRepository


class PostgresJobRepository(IJobRepository):
    """PostgreSQL implementation for job queue management."""
    
    def __init__(self, connection: Connection):
        self._conn = connection
    
    async def create_job(
        self,
        run_type: str,
        dataset_id: int,
        user_id: int,
        source_commit_id: Optional[str] = None,
        run_parameters: Optional[Dict[str, Any]] = None
    ) -> UUID:
        """Create a new job."""
        job_id = uuid4()
        
        query = """
            INSERT INTO dsa_jobs.analysis_runs (
                id, run_type, status, dataset_id, user_id, 
                source_commit_id, run_parameters
            )
            VALUES ($1, $2::dsa_jobs.analysis_run_type, 'pending'::dsa_jobs.analysis_run_status, $3, $4, $5, $6)
            RETURNING id
        """
        
        await self._conn.execute(
            query,
            job_id,
            run_type,
            dataset_id,
            user_id,
            source_commit_id,
            json.dumps(run_parameters) if run_parameters else None
        )
        
        return job_id
    
    async def acquire_next_pending_job(self, job_type: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Atomically acquire the next pending job for processing."""
        if job_type:
            query = """
                UPDATE dsa_jobs.analysis_runs
                SET status = 'running'::dsa_jobs.analysis_run_status
                WHERE id = (
                    SELECT id
                    FROM dsa_jobs.analysis_runs
                    WHERE status = 'pending' AND run_type = $1::dsa_jobs.analysis_run_type
                    ORDER BY created_at
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                RETURNING id, run_type::text, dataset_id, user_id, 
                         source_commit_id, run_parameters, created_at
            """
            row = await self._conn.fetchrow(query, job_type)
        else:
            query = """
                UPDATE dsa_jobs.analysis_runs
                SET status = 'running'::dsa_jobs.analysis_run_status
                WHERE id = (
                    SELECT id
                    FROM dsa_jobs.analysis_runs
                    WHERE status = 'pending'
                    ORDER BY created_at
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                RETURNING id, run_type::text, dataset_id, user_id, 
                         source_commit_id, run_parameters, created_at
            """
            row = await self._conn.fetchrow(query)
        
        if row:
            result = dict(row)
            # Rename id to run_id for compatibility
            result['run_id'] = result.pop('id')
            # Parse JSON parameters
            if result.get('run_parameters'):
                result['run_parameters'] = json.loads(result['run_parameters'])
            return result
        
        return None
    
    async def update_job_status(
        self,
        job_id: UUID,
        status: str,
        output_summary: Optional[Dict[str, Any]] = None,
        error_message: Optional[str] = None
    ) -> None:
        """Update job status."""
        query = """
            UPDATE dsa_jobs.analysis_runs
            SET status = $2::dsa_jobs.analysis_run_status,
                output_summary = $3,
                error_message = $4,
                completed_at = CASE WHEN $2 IN ('completed', 'failed') THEN NOW() ELSE NULL END
            WHERE id = $1
        """
        
        await self._conn.execute(
            query,
            job_id,
            status,
            json.dumps(output_summary) if output_summary else None,
            error_message
        )
    
    async def get_job_by_id(self, job_id: UUID) -> Optional[Dict[str, Any]]:
        """Get job by ID."""
        query = """
            SELECT id, run_type::text, status::text, dataset_id, user_id,
                   source_commit_id, run_parameters, output_summary,
                   error_message, created_at, completed_at
            FROM dsa_jobs.analysis_runs
            WHERE id = $1
        """
        
        row = await self._conn.fetchrow(query, job_id)
        if row:
            result = dict(row)
            # Rename id to run_id for compatibility
            result['run_id'] = result.pop('id')
            # Parse JSON fields
            if result.get('run_parameters'):
                result['run_parameters'] = json.loads(result['run_parameters'])
            if result.get('output_summary'):
                result['output_summary'] = json.loads(result['output_summary'])
            return result
        
        return None
    
    async def list_dataset_jobs(self, dataset_id: int, limit: int = 50) -> List[Dict[str, Any]]:
        """List jobs for a dataset."""
        query = """
            SELECT id, run_type::text, status::text, dataset_id, user_id,
                   source_commit_id, error_message, created_at, completed_at
            FROM dsa_jobs.analysis_runs
            WHERE dataset_id = $1
            ORDER BY created_at DESC
            LIMIT $2
        """
        
        rows = await self._conn.fetch(query, dataset_id, limit)
        return [{
            'run_id': row['id'],
            **{k: v for k, v in dict(row).items() if k != 'id'}
        } for row in rows]