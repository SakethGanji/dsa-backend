from typing import Dict, List, Any, Optional
from fastapi import HTTPException, status
import logging

from app.sampling.service import SamplingService
from app.sampling.models import (
    SamplingRequest, SamplingJobResponse, 
    SamplingJobDetails, JobStatus
)

logger = logging.getLogger(__name__)

class SamplingController:
    def __init__(self, service: SamplingService):
        self.service = service
    
    async def create_sampling_job(
        self,
        dataset_id: int,
        version_id: int,
        request: SamplingRequest,
        user_id: int
    ) -> SamplingJobResponse:
        """
        Create a new sampling job
        
        Args:
            dataset_id: The ID of the dataset
            version_id: The ID of the version
            request: The sampling request with method and parameters
            user_id: The ID of the user making the request
            
        Returns:
            A response with the job ID and status
            
        Raises:
            HTTPException: If an error occurs during job creation
        """
        try:
            logger.info(f"User {user_id} creating sampling job for dataset {dataset_id}, version {version_id}")
            
            # Call service method to create job
            job = await self.service.create_sampling_job(
                dataset_id=dataset_id,
                version_id=version_id,
                request=request,
                user_id=user_id
            )
            
            logger.info(f"Created sampling job {job.id} for dataset {dataset_id}, version {version_id}")
            
            # Return job response
            return SamplingJobResponse(
                run_id=job.id,
                status=job.status,
                message="Sampling job enqueued"
            )
            
        except ValueError as e:
            # Handle validation and not found errors
            logger.warning(f"Resource not found: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=str(e)
            )
        except Exception as e:
            # Handle all other errors
            logger.error(f"Error creating sampling job: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error creating sampling job: {str(e)}"
            )
    
    async def get_job_details(self, job_id: str) -> SamplingJobDetails:
        """
        Get details for a sampling job
        
        Args:
            job_id: The ID of the job
            
        Returns:
            Detailed information about the job
            
        Raises:
            HTTPException: If the job is not found
        """
        try:
            logger.info(f"Getting details for job {job_id}")
            
            # Call service method
            job = await self.service.get_job(job_id)
            
            if not job:
                logger.warning(f"Job {job_id} not found")
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Job with ID {job_id} not found"
                )
            
            # Return job details
            return SamplingJobDetails(
                run_id=job.id,
                status=job.status,
                message=self._get_status_message(job.status),
                started_at=job.started_at,
                completed_at=job.completed_at,
                output_preview=job.output_preview,
                output_uri=job.output_uri,
                error_message=job.error_message
            )
            
        except HTTPException:
            # Re-raise HTTP exceptions
            raise
        except Exception as e:
            # Handle all other errors
            logger.error(f"Error getting job details: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error getting job details: {str(e)}"
            )
    
    async def get_job_preview(self, job_id: str) -> List[Dict[str, Any]]:
        """
        Get preview data for a sampling job
        
        Args:
            job_id: The ID of the job
            
        Returns:
            A list of records as preview data
            
        Raises:
            HTTPException: If an error occurs
        """
        try:
            logger.info(f"Getting preview for job {job_id}")
            
            # Call service method
            preview = await self.service.get_job_preview(job_id)
            
            return preview
            
        except Exception as e:
            # Handle all errors
            logger.error(f"Error getting job preview: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error getting job preview: {str(e)}"
            )
    
    def _get_status_message(self, status: JobStatus) -> str:
        """Get a human-readable message for a job status"""
        if status == JobStatus.PENDING:
            return "Sampling job is queued"
        elif status == JobStatus.RUNNING:
            return "Worker is processing the full sample"
        elif status == JobStatus.COMPLETED:
            return "Sampling job completed"
        elif status == JobStatus.FAILED:
            return "Sampling job failed"
        else:
            return "Unknown job status"