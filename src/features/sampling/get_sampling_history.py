"""Handlers for retrieving sampling job history."""

from typing import Dict, Any, Optional
from datetime import datetime

from src.core.abstractions.uow import IUnitOfWork
from src.features.base_handler import BaseHandler, PaginationMixin, with_error_handling
from fastapi import HTTPException


class GetDatasetSamplingHistoryHandler(BaseHandler[Dict[str, Any]], PaginationMixin):
    """Handler for retrieving sampling job history for a dataset."""
    
    @with_error_handling
    async def handle(
        self,
        dataset_id: int,
        user_id: int,
        ref_name: Optional[str] = None,
        status: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        offset: int = 0,
        limit: int = 20
    ) -> Dict[str, Any]:
        """
        Get sampling job history for a dataset.
        
        Args:
            dataset_id: Dataset ID
            user_id: Current user ID for permission checks
            ref_name: Filter by source ref name
            status: Filter by job status
            start_date: Filter jobs created after this date
            end_date: Filter jobs created before this date
            offset: Pagination offset
            limit: Number of items per page
            
        Returns:
            Paginated list of sampling jobs with metadata
        """
        # Check dataset exists and user has permission
        dataset = await self._uow.datasets.get_dataset_by_id(dataset_id)
        if not dataset:
            raise HTTPException(status_code=404, detail=f"Dataset {dataset_id} not found")
        
        has_permission = await self._uow.datasets.user_has_permission(
            dataset_id, user_id, "read"
        )
        if not has_permission:
            raise HTTPException(status_code=404, detail=f"Dataset {dataset_id} not found")  # Don't reveal existence
        
        # Get sampling jobs
        jobs, total_count = await self._uow.jobs.get_sampling_jobs_by_dataset(
            dataset_id=dataset_id,
            ref_name=ref_name,
            status=status,
            start_date=start_date,
            end_date=end_date,
            offset=offset,
            limit=limit
        )
        
        # Build response
        return {
            'dataset_id': dataset_id,
            'dataset_name': dataset['name'],
            'jobs': jobs,
            'pagination': {
                'total': total_count,
                'offset': offset,
                'limit': limit,
                'has_more': offset + len(jobs) < total_count
            }
        }


class GetUserSamplingHistoryHandler(BaseHandler[Dict[str, Any]], PaginationMixin):
    """Handler for retrieving sampling job history for a user."""
    
    @with_error_handling
    async def handle(
        self,
        target_user_id: int,
        current_user_id: int,
        is_admin: bool = False,
        dataset_id: Optional[int] = None,
        status: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        offset: int = 0,
        limit: int = 20
    ) -> Dict[str, Any]:
        """
        Get sampling job history for a user.
        
        Args:
            target_user_id: User whose history to retrieve
            current_user_id: Current user ID for permission checks
            is_admin: Whether current user is admin
            dataset_id: Filter by dataset
            status: Filter by job status
            start_date: Filter jobs created after this date
            end_date: Filter jobs created before this date
            offset: Pagination offset
            limit: Number of items per page
            
        Returns:
            Paginated list of sampling jobs with summary
        """
        # Check permissions - users can only see their own history unless admin
        if target_user_id != current_user_id and not is_admin:
            raise HTTPException(status_code=404, detail=f"User {target_user_id} not found")
        
        # Get user info
        user = await self._uow.users.get_by_id(target_user_id)
        if not user:
            raise HTTPException(status_code=404, detail=f"User {target_user_id} not found")
        
        # Get sampling jobs
        jobs, total_count = await self._uow.jobs.get_sampling_jobs_by_user(
            user_id=target_user_id,
            dataset_id=dataset_id,
            status=status,
            start_date=start_date,
            end_date=end_date,
            offset=offset,
            limit=limit
        )
        
        # Calculate summary statistics
        summary = await self._calculate_user_summary(target_user_id)
        
        # Build response
        return {
            'user_id': target_user_id,
            'user_soeid': user.get('soeid'),
            'user_name': user.get('soeid'),  # Using soeid as name since first_name/last_name don't exist
            'jobs': jobs,
            'pagination': {
                'total': total_count,
                'offset': offset,
                'limit': limit,
                'has_more': offset + len(jobs) < total_count
            },
            'summary': summary
        }
    
    async def _calculate_user_summary(self, user_id: int) -> Dict[str, Any]:
        """Calculate summary statistics for user's sampling jobs."""
        # Get all user's sampling jobs for summary (without pagination)
        all_jobs, total = await self._uow.jobs.get_sampling_jobs_by_user(
            user_id=user_id,
            offset=0,
            limit=1000  # Reasonable limit for summary calculation
        )
        
        # Calculate statistics
        successful_jobs = sum(1 for job in all_jobs if job['status'] == 'completed')
        failed_jobs = sum(1 for job in all_jobs if job['status'] == 'failed')
        total_rows_sampled = 0
        datasets_sampled = set()
        
        for job in all_jobs:
            if job['status'] == 'completed' and job.get('sampling_summary'):
                total_rows_sampled += job['sampling_summary'].get('total_samples', 0)
            datasets_sampled.add(job['dataset_id'])
        
        return {
            'total_sampling_jobs': total,
            'successful_jobs': successful_jobs,
            'failed_jobs': failed_jobs,
            'total_rows_sampled': total_rows_sampled,
            'datasets_sampled': len(datasets_sampled)
        }