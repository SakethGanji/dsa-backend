"""Handler for fetching jobs with filters."""

from typing import List, Optional, Dict, Any
from datetime import datetime
from uuid import UUID

from src.infrastructure.postgres.uow import PostgresUnitOfWork


class GetJobsHandler:
    """Handler for fetching jobs with various filters."""
    
    def __init__(self, uow: PostgresUnitOfWork):
        self._uow = uow
    
    async def handle(
        self,
        user_id: Optional[int] = None,
        dataset_id: Optional[int] = None,
        status: Optional[str] = None,
        run_type: Optional[str] = None,
        offset: int = 0,
        limit: int = 100,
        current_user_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Fetch jobs with optional filters.
        
        Args:
            user_id: Filter by user who created the job
            dataset_id: Filter by dataset
            status: Filter by job status (pending, running, completed, failed)
            run_type: Filter by job type (import, analysis, etc.)
            offset: Pagination offset
            limit: Number of results to return
            current_user_id: ID of the user making the request
            
        Returns:
            Dictionary with jobs and pagination info
        """
        conn = self._uow.connection
        # Build query with filters
        query = """
            SELECT 
                ar.id,
                ar.run_type,
                ar.status,
                ar.dataset_id,
                d.name as dataset_name,
                ar.user_id,
                u.soeid as user_soeid,
                ar.created_at,
                ar.completed_at,
                ar.error_message,
                ar.output_summary
            FROM dsa_jobs.analysis_runs ar
            LEFT JOIN dsa_core.datasets d ON ar.dataset_id = d.id
            LEFT JOIN dsa_auth.users u ON ar.user_id = u.id
            WHERE 1=1
        """
        
        params = []
        param_count = 0
        
        # Add filters
        if user_id is not None:
            param_count += 1
            query += f" AND ar.user_id = ${param_count}"
            params.append(user_id)
            
        if dataset_id is not None:
            param_count += 1
            query += f" AND ar.dataset_id = ${param_count}"
            params.append(dataset_id)
            
        if status is not None:
            param_count += 1
            query += f" AND ar.status = ${param_count}"
            params.append(status)
            
        if run_type is not None:
            param_count += 1
            query += f" AND ar.run_type = ${param_count}"
            params.append(run_type)
        
        # Add permission filter if current_user_id is provided
        # Only show jobs for datasets the user has permission to read
        if current_user_id is not None:
            param_count += 1
            query += f"""
                AND (
                    ar.dataset_id IS NULL  -- Jobs without datasets
                    OR EXISTS (
                        SELECT 1 FROM dsa_auth.dataset_permissions dp
                        WHERE dp.dataset_id = ar.dataset_id
                        AND dp.user_id = ${param_count}
                    )
                )
            """
            params.append(current_user_id)
        
        # Add ordering
        query += " ORDER BY ar.created_at DESC"
        
        # Add pagination
        param_count += 1
        query += f" LIMIT ${param_count}"
        params.append(limit)
        
        param_count += 1
        query += f" OFFSET ${param_count}"
        params.append(offset)
        
        # Execute query
        rows = await conn.fetch(query, *params)
        
        # Get total count
        count_query = """
            SELECT COUNT(*) as total
            FROM dsa_jobs.analysis_runs ar
            WHERE 1=1
        """
        
        count_params = []
        count_param_num = 0
        
        if user_id is not None:
            count_param_num += 1
            count_query += f" AND ar.user_id = ${count_param_num}"
            count_params.append(user_id)
            
        if dataset_id is not None:
            count_param_num += 1
            count_query += f" AND ar.dataset_id = ${count_param_num}"
            count_params.append(dataset_id)
            
        if status is not None:
            count_param_num += 1
            count_query += f" AND ar.status = ${count_param_num}"
            count_params.append(status)
            
        if run_type is not None:
            count_param_num += 1
            count_query += f" AND ar.run_type = ${count_param_num}"
            count_params.append(run_type)
        
        total_row = await conn.fetchrow(count_query, *count_params)
        total = total_row['total'] if total_row else 0
        
        # Format results
        jobs = []
        for row in rows:
            job = {
                "id": str(row['id']),
                "run_type": row['run_type'],
                "status": row['status'],
                "dataset_id": row['dataset_id'],
                "dataset_name": row['dataset_name'],
                "user_id": row['user_id'],
                "user_soeid": row['user_soeid'],
                "created_at": row['created_at'].isoformat() if row['created_at'] else None,
                "updated_at": row['created_at'].isoformat() if row['created_at'] else None,  # Use created_at as updated_at
                "completed_at": row['completed_at'].isoformat() if row['completed_at'] else None,
                "error_message": row['error_message'],
                "output_summary": row['output_summary']
            }
            jobs.append(job)
        
        return {
            "jobs": jobs,
            "total": total,
            "offset": offset,
            "limit": limit
        }