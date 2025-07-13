"""Handler for fetching a single job by ID."""

from typing import Optional, Dict, Any
from uuid import UUID
import json

from src.core.abstractions import IUnitOfWork


class GetJobByIdHandler:
    """Handler for fetching job details by ID."""
    
    def __init__(self, uow: IUnitOfWork):
        self._uow = uow
    
    async def handle(
        self,
        job_id: UUID,
        current_user_id: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch a single job by ID.
        
        Args:
            job_id: The job ID to fetch
            current_user_id: ID of the user making the request
            
        Returns:
            Job details or None if not found
        """
        conn = self._uow.connection
        query = """
            SELECT 
                ar.id,
                ar.run_type,
                ar.status,
                ar.dataset_id,
                d.name as dataset_name,
                ar.source_commit_id,
                ar.user_id,
                u.soeid as user_soeid,
                ar.run_parameters,
                ar.output_summary,
                ar.error_message,
                ar.created_at,
                ar.completed_at,
                CASE 
                    WHEN ar.completed_at IS NOT NULL 
                    THEN EXTRACT(EPOCH FROM (ar.completed_at - ar.created_at))
                    ELSE NULL 
                END as duration_seconds
            FROM dsa_jobs.analysis_runs ar
            LEFT JOIN dsa_core.datasets d ON ar.dataset_id = d.id
            LEFT JOIN dsa_auth.users u ON ar.user_id = u.id
            WHERE ar.id = $1
        """
        
        row = await conn.fetchrow(query, job_id)
        
        if not row:
            # Log for debugging
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Job not found with ID: {job_id}")
            
            # Try to see if the job exists with a simpler query
            exists = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM dsa_jobs.analysis_runs WHERE id = $1)",
                job_id
            )
            if exists:
                logger.error(f"Job {job_id} exists but failed to fetch with joins")
            
            return None
        
        # Format result
        try:
            # Parse JSON fields safely
            run_parameters = row['run_parameters']
            if isinstance(run_parameters, str):
                try:
                    run_parameters = json.loads(run_parameters)
                except json.JSONDecodeError:
                    import logging
                    logger = logging.getLogger(__name__)
                    logger.error(f"Failed to parse run_parameters for job {job_id}: {run_parameters[:100]}")
                    run_parameters = None
            
            output_summary = row['output_summary']
            if isinstance(output_summary, str):
                try:
                    output_summary = json.loads(output_summary)
                except json.JSONDecodeError:
                    import logging
                    logger = logging.getLogger(__name__)
                    logger.error(f"Failed to parse output_summary for job {job_id}: {output_summary[:100]}")
                    output_summary = None
            
            job = {
                "id": str(row['id']),
                "run_type": row['run_type'],
                "status": row['status'],
                "dataset_id": row['dataset_id'],
                "dataset_name": row['dataset_name'],
                "source_commit_id": row['source_commit_id'].strip() if row['source_commit_id'] else None,
                "user_id": row['user_id'],
                "user_soeid": row['user_soeid'],
                "run_parameters": run_parameters,
                "output_summary": output_summary,
                "error_message": row['error_message'],
                "created_at": row['created_at'].isoformat() if row['created_at'] else None,
                "completed_at": row['completed_at'].isoformat() if row['completed_at'] else None,
                "duration_seconds": float(row['duration_seconds']) if row['duration_seconds'] else None
            }
            
            return job
            
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Error formatting job {job_id}: {str(e)}")
            raise