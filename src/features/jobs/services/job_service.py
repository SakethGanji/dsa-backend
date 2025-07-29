"""Consolidated service for all job operations."""

from typing import Dict, Any, List, Optional
from uuid import UUID
from datetime import datetime
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
        # Use repository method
        jobs, total = await self._job_repo.get_jobs(
            user_id=user_id,
            dataset_id=dataset_id,
            status=status,
            run_type=run_type,
            offset=offset,
            limit=limit,
            current_user_id=current_user_id
        )
        
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
        # Use repository method
        job = await self._job_repo.get_job_detail(job_id, current_user_id)
        
        if not job:
            logger.warning(f"Job not found with ID: {job_id}")
            return None
        
        # Check if user has permission to view this job
        if current_user_id:
            is_job_owner = job['user_id'] == current_user_id
            if not is_job_owner and job['dataset_id']:
                has_permission = await self._permissions.has_permission(
                    "dataset", job['dataset_id'], current_user_id, "read"
                )
                if not has_permission:
                    return None
        
        return job
    
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
            # Use repository method
            await self._job_repo.cancel_job(job_id)
        
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