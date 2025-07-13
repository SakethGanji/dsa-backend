"""PostgreSQL implementation of IJobRepository."""

from typing import Optional, Dict, Any, List, Tuple
from uuid import UUID, uuid4
import json
from datetime import datetime
from asyncpg import Connection
from src.core.abstractions.repositories import IJobRepository


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
    
    async def get_sampling_jobs_by_dataset(
        self,
        dataset_id: int,
        ref_name: Optional[str] = None,
        status: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        offset: int = 0,
        limit: int = 20
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Get sampling jobs for a dataset with optional filters."""
        
        # Build the query with dynamic conditions
        conditions = ["ar.dataset_id = $1", "ar.run_type = 'sampling'"]
        params = [dataset_id]
        param_count = 1
        
        if ref_name:
            param_count += 1
            conditions.append(f"ar.run_parameters->>'source_ref' = ${param_count}")
            params.append(ref_name)
        
        if status:
            param_count += 1
            conditions.append(f"ar.status = ${param_count}::dsa_jobs.analysis_run_status")
            params.append(status)
        
        if start_date:
            param_count += 1
            conditions.append(f"ar.created_at >= ${param_count}")
            params.append(start_date)
        
        if end_date:
            param_count += 1
            conditions.append(f"ar.created_at <= ${param_count}")
            params.append(end_date)
        
        where_clause = " AND ".join(conditions)
        
        # Count query
        count_query = f"""
            SELECT COUNT(*) as total
            FROM dsa_jobs.analysis_runs ar
            WHERE {where_clause}
        """
        
        total_count = await self._conn.fetchval(count_query, *params)
        
        # Data query with user info join
        param_count += 1
        params.append(limit)
        param_count += 1
        params.append(offset)
        
        data_query = f"""
            SELECT 
                ar.id as job_id,
                ar.status::text,
                ar.created_at,
                ar.completed_at,
                ar.user_id,
                u.soeid,
                u.soeid as user_name,
                ar.source_commit_id,
                ar.run_parameters,
                ar.output_summary,
                ar.error_message,
                EXTRACT(EPOCH FROM (ar.completed_at - ar.created_at))::int as duration_seconds
            FROM dsa_jobs.analysis_runs ar
            JOIN dsa_auth.users u ON ar.user_id = u.id
            WHERE {where_clause}
            ORDER BY ar.created_at DESC
            LIMIT ${param_count - 1} OFFSET ${param_count}
        """
        
        rows = await self._conn.fetch(data_query, *params)
        
        # Process results
        jobs = []
        for row in rows:
            job = dict(row)
            
            # Parse JSON fields
            if job.get('run_parameters'):
                params_dict = json.loads(job['run_parameters'])
                job['source_ref'] = params_dict.get('source_ref', 'main')
                job['commit_message'] = params_dict.get('commit_message', '')
            
            if job.get('output_summary'):
                output = json.loads(job['output_summary'])
                job['output_commit_id'] = output.get('output_commit_id')
                job['sampling_summary'] = output.get('sampling_summary', {})
            
            # Format user info
            job['created_by'] = {
                'user_id': job.pop('user_id'),
                'soeid': job.pop('soeid'),
                'name': job.pop('user_name')
            }
            
            jobs.append(job)
        
        return jobs, total_count
    
    async def get_sampling_jobs_by_user(
        self,
        user_id: int,
        dataset_id: Optional[int] = None,
        status: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        offset: int = 0,
        limit: int = 20
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Get sampling jobs created by a user with optional filters."""
        
        # Build the query with dynamic conditions
        conditions = ["ar.user_id = $1", "ar.run_type = 'sampling'"]
        params = [user_id]
        param_count = 1
        
        if dataset_id:
            param_count += 1
            conditions.append(f"ar.dataset_id = ${param_count}")
            params.append(dataset_id)
        
        if status:
            param_count += 1
            conditions.append(f"ar.status = ${param_count}::dsa_jobs.analysis_run_status")
            params.append(status)
        
        if start_date:
            param_count += 1
            conditions.append(f"ar.created_at >= ${param_count}")
            params.append(start_date)
        
        if end_date:
            param_count += 1
            conditions.append(f"ar.created_at <= ${param_count}")
            params.append(end_date)
        
        where_clause = " AND ".join(conditions)
        
        # Count query
        count_query = f"""
            SELECT COUNT(*) as total
            FROM dsa_jobs.analysis_runs ar
            WHERE {where_clause}
        """
        
        total_count = await self._conn.fetchval(count_query, *params)
        
        # Data query with dataset info join
        param_count += 1
        params.append(limit)
        param_count += 1
        params.append(offset)
        
        data_query = f"""
            SELECT 
                ar.id as job_id,
                ar.dataset_id,
                d.name as dataset_name,
                ar.status::text,
                ar.created_at,
                ar.completed_at,
                ar.source_commit_id,
                ar.run_parameters,
                ar.output_summary,
                ar.error_message,
                EXTRACT(EPOCH FROM (ar.completed_at - ar.created_at))::int as duration_seconds
            FROM dsa_jobs.analysis_runs ar
            JOIN dsa_core.datasets d ON ar.dataset_id = d.id
            WHERE {where_clause}
            ORDER BY ar.created_at DESC
            LIMIT ${param_count - 1} OFFSET ${param_count}
        """
        
        rows = await self._conn.fetch(data_query, *params)
        
        # Process results
        jobs = []
        for row in rows:
            job = dict(row)
            
            # Parse JSON fields
            if job.get('run_parameters'):
                params_dict = json.loads(job['run_parameters'])
                job['source_ref'] = params_dict.get('source_ref', 'main')
            
            if job.get('output_summary'):
                output = json.loads(job['output_summary'])
                job['sampling_summary'] = output.get('sampling_summary', {})
            
            jobs.append(job)
        
        return jobs, total_count
    
    async def get_latest_import_job(self, dataset_id: int) -> Optional[Dict[str, Any]]:
        """Get the most recent import job for a dataset."""
        query = """
            SELECT 
                ar.id as job_id,
                ar.run_type,
                ar.status,
                ar.dataset_id,
                ar.user_id,
                ar.source_commit_id,
                ar.run_parameters,
                ar.output_summary,
                ar.created_at,
                ar.error_message
            FROM dsa_jobs.analysis_runs ar
            WHERE ar.dataset_id = $1 
                AND ar.run_type = 'import'
            ORDER BY ar.created_at DESC
            LIMIT 1
        """
        
        row = await self._conn.fetchrow(query, dataset_id)
        
        if not row:
            return None
        
        job = dict(row)
        
        # Parse JSON fields
        if job.get('run_parameters'):
            job['run_parameters'] = json.loads(job['run_parameters'])
        
        if job.get('output_summary'):
            job['output_summary'] = json.loads(job['output_summary'])
        
        return job