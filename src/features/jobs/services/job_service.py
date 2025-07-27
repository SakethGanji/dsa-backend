"""Consolidated service for all job operations."""

from typing import Dict, Any, List, Optional
from uuid import UUID
from datetime import datetime
import json
import logging

from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.core.permissions import PermissionService
from src.core.events.publisher import EventBus
from src.core.domain_exceptions import EntityNotFoundException, ForbiddenException, ValidationException, BusinessRuleViolation
from ...base_handler import with_transaction, with_error_handling
from fastapi import HTTPException
from ..models import (
    CreateJobCommand, CancelJobCommand,
    Job, JobParameters, JobType, JobStatus
)

logger = logging.getLogger(__name__)


class JobService:
    """Consolidated service for all job operations."""
    
    def __init__(
        self,
        uow: PostgresUnitOfWork,
        permissions: PermissionService,
        event_bus: Optional[EventBus] = None
    ):
        self._uow = uow
        self._permissions = permissions
        self._event_bus = event_bus
        self._job_repo = uow.jobs if hasattr(uow, 'jobs') else None
    
    @with_transaction
    @with_error_handling
    async def create_job(
        self,
        command: CreateJobCommand
    ) -> Dict[str, Any]:
        """Create a new job for processing."""
        # Check write permission on the dataset
        await self._permissions.require("dataset", command.dataset_id, command.user_id, "write")
        
        # Validate run type
        valid_run_types = ['import', 'sampling', 'exploration', 'sql_transform']
        if command.run_type not in valid_run_types:
            raise ValueError(f"Invalid run_type. Must be one of: {valid_run_types}")
        
        # Create job
        job_id = await self._job_repo.create_job(
            run_type=command.run_type,
            user_id=command.user_id,
            dataset_id=command.dataset_id,
            source_commit_id=command.source_commit_id,
            run_parameters=command.run_parameters,
            description=command.description
        )
        
        # Publish event if event bus available
        if self._event_bus:
            # Import here to avoid circular dependency
            from ..handlers.create_job import JobCreatedEvent
            await self._event_bus.publish(JobCreatedEvent(
                job_id=job_id,
                run_type=command.run_type,
                user_id=command.user_id,
                dataset_id=command.dataset_id
            ))
        
        return {
            'job_id': job_id,
            'status': 'pending',
            'created_at': datetime.utcnow()
        }
    
    @with_error_handling
    async def get_jobs(
        self,
        user_id: Optional[int] = None,
        dataset_id: Optional[int] = None,
        status: Optional[str] = None,
        run_type: Optional[str] = None,
        offset: int = 0,
        limit: int = 100,
        current_user_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """Get list of jobs with optional filters."""
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
        
        # Add permission filter for count
        if current_user_id is not None:
            count_param_num += 1
            count_query += f"""
                AND (
                    ar.dataset_id IS NULL
                    OR EXISTS (
                        SELECT 1 FROM dsa_auth.dataset_permissions dp
                        WHERE dp.dataset_id = ar.dataset_id
                        AND dp.user_id = ${count_param_num}
                    )
                )
            """
            count_params.append(current_user_id)
        
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
                "updated_at": row['created_at'].isoformat() if row['created_at'] else None,
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
    
    @with_error_handling
    async def get_job_by_id(
        self,
        job_id: UUID,
        current_user_id: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """Get detailed information about a specific job."""
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
            logger.warning(f"Job not found with ID: {job_id}")
            
            # Try to see if the job exists with a simpler query
            exists = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM dsa_jobs.analysis_runs WHERE id = $1)",
                job_id
            )
            if exists:
                logger.error(f"Job {job_id} exists but failed to fetch with joins")
            
            return None
        
        # Check if user has permission to view this job
        if current_user_id:
            is_job_owner = row['user_id'] == current_user_id
            if not is_job_owner and row['dataset_id']:
                has_permission = await self._permissions.has_permission(
                    "dataset", row['dataset_id'], current_user_id, "read"
                )
                if not has_permission:
                    return None
        
        # Format result
        try:
            # Parse JSON fields safely
            run_parameters = row['run_parameters']
            if isinstance(run_parameters, str):
                try:
                    run_parameters = json.loads(run_parameters)
                except json.JSONDecodeError:
                    logger.error(f"Failed to parse run_parameters for job {job_id}: {run_parameters[:100]}")
                    run_parameters = None
            
            output_summary = row['output_summary']
            if isinstance(output_summary, str):
                try:
                    output_summary = json.loads(output_summary)
                except json.JSONDecodeError:
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
            logger.error(f"Error formatting job {job_id}: {str(e)}")
            raise
    
    @with_error_handling
    async def get_job_status(
        self,
        job_id: UUID,
        user_id: int
    ) -> Dict[str, Any]:
        """Get status of a job."""
        # Get job details
        if self._job_repo:
            job = await self._job_repo.get_job_by_id(job_id)
        else:
            # Fallback to direct query
            job = await self.get_job_by_id(job_id, user_id)
        
        if not job:
            raise EntityNotFoundException("Job", job_id)
        
        # Check if user owns the job OR has read permission on the dataset
        is_job_owner = job.get('user_id') == user_id
        if not is_job_owner and job.get('dataset_id'):
            has_permission = await self._permissions.has_permission(
                "dataset", job['dataset_id'], user_id, "read"
            )
            if not has_permission:
                raise ForbiddenException()
        
        return {
            'job_id': job.get('id', job.get('job_id')),
            'run_type': job['run_type'],
            'status': job['status'],
            'dataset_id': job.get('dataset_id'),
            'created_at': job.get('created_at'),
            'completed_at': job.get('completed_at'),
            'error_message': job.get('error_message'),
            'output_summary': job.get('output_summary')
        }
    
    @with_transaction
    @with_error_handling
    async def cancel_job(
        self,
        job_id: UUID,
        user_id: int,
        dataset_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """Cancel a running or pending job."""
        # Get job details
        if self._job_repo:
            job_data = await self._job_repo.get_job_by_id(job_id)
        else:
            # Fallback to direct query
            job_data = await self.get_job_by_id(job_id, user_id)
            
        if not job_data:
            raise EntityNotFoundException("Job", job_id)
        
        # If dataset_id provided, verify job belongs to the dataset
        if dataset_id and job_data.get('dataset_id') != dataset_id:
            raise ValueError("Job does not belong to the specified dataset")
        
        # Extract dataset_id from job if not provided
        job_dataset_id = dataset_id or job_data.get('dataset_id')
        
        # Check if user owns the job OR has write permission on the dataset
        is_job_owner = job_data.get('user_id') == user_id
        if not is_job_owner and job_dataset_id:
            await self._permissions.require("dataset", job_dataset_id, user_id, "write")
        
        # Check if job can be cancelled (only pending or running jobs)
        current_status = job_data.get('status')
        if current_status not in ['pending', 'running']:
            raise BusinessRuleViolation(
                f"Cannot cancel job in {current_status} status",
                "job_not_cancellable"
            )
        
        # Update job status to cancelled
        if self._job_repo:
            await self._job_repo.update_job_status(
                job_id=job_id,
                status='cancelled',
                error_message="Job cancelled by user"
            )
        else:
            # Fallback to direct update
            await self._uow.connection.execute(
                """
                UPDATE dsa_jobs.analysis_runs 
                SET status = 'cancelled', 
                    error_message = 'Job cancelled by user',
                    completed_at = NOW()
                WHERE id = $1
                """,
                job_id
            )
        
        # Publish event if event bus available
        if self._event_bus:
            # Import here to avoid circular dependency
            from ..handlers.cancel_job import JobCancelledEvent
            await self._event_bus.publish(JobCancelledEvent(
                job_id=job_id,
                cancelled_by=user_id,
                reason="User requested cancellation"
            ))
        
        return {
            'job_id': job_id,
            'status': 'cancelled',
            'message': 'Job has been cancelled successfully'
        }