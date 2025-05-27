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
                error_message=job.error_message,
                data_summary=job.data_summary,
                sample_summary=job.sample_summary
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
    
    async def get_job_preview(self, job_id: str, page: int = 1, page_size: int = 100) -> Dict[str, Any]:
        """
        Get preview data for a sampling job with pagination
        
        Args:
            job_id: The ID of the job
            page: Page number (1-indexed)
            page_size: Number of items per page
            
        Returns:
            A dictionary with paginated data and metadata
            
        Raises:
            HTTPException: If an error occurs
        """
        try:
            logger.info(f"Getting preview for job {job_id}, page {page}, page_size {page_size}")
            
            # Call service method
            preview = await self.service.get_job_preview(job_id)
            
            # Apply pagination
            total_items = len(preview)
            total_pages = (total_items + page_size - 1) // page_size
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            
            paginated_data = preview[start_idx:end_idx]
            
            return {
                "data": paginated_data,
                "pagination": {
                    "page": page,
                    "page_size": page_size,
                    "total_items": total_items,
                    "total_pages": total_pages,
                    "has_next": page < total_pages,
                    "has_previous": page > 1
                }
            }
            
        except Exception as e:
            # Handle all errors
            logger.error(f"Error getting job preview: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error getting job preview: {str(e)}"
            )
    
    async def get_dataset_columns(self, dataset_id: int, version_id: int) -> Dict[str, Any]:
        """
        Get column information for a dataset version
        
        Args:
            dataset_id: The ID of the dataset
            version_id: The ID of the version
            
        Returns:
            Dictionary with column information
            
        Raises:
            HTTPException: If the dataset/version is not found
        """
        try:
            logger.info(f"Getting column info for dataset {dataset_id}, version {version_id}")
            
            # Call service method
            columns_info = await self.service.get_dataset_columns(dataset_id, version_id)
            
            return columns_info
            
        except ValueError as e:
            # Handle validation and not found errors
            logger.warning(f"Resource not found: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=str(e)
            )
        except Exception as e:
            # Handle all other errors
            logger.error(f"Error getting dataset columns: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error getting dataset columns: {str(e)}"
            )

    async def execute_sampling_sync(
        self,
        dataset_id: int,
        version_id: int,
        request: SamplingRequest,
        user_id: int, # Included for consistency, can be used for logging/auditing
        page: int = 1,
        page_size: int = 100
    ) -> Dict[str, Any]:
        """
        Execute sampling synchronously and return the data with pagination.
        """
        try:
            logger.info(f"User {user_id} executing sampling synchronously for dataset {dataset_id}, version {version_id}, page {page}, page_size {page_size}")

            # Call service method
            sampled_df = await self.service.execute_sampling_synchronously(
                dataset_id=dataset_id,
                version_id=version_id,
                request=request
            )

            logger.info(f"Synchronous sampling completed for dataset {dataset_id}, version {version_id}. Rows: {len(sampled_df)}")

            # Convert DataFrame to List[Dict]
            data = sampled_df.to_dict(orient="records")
            
            # Apply pagination
            total_items = len(data)
            total_pages = (total_items + page_size - 1) // page_size
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            
            paginated_data = data[start_idx:end_idx]
            
            return {
                "data": paginated_data,
                "pagination": {
                    "page": page,
                    "page_size": page_size,
                    "total_items": total_items,
                    "total_pages": total_pages,
                    "has_next": page < total_pages,
                    "has_previous": page > 1
                }
            }

        except ValueError as e:
            # Handle validation and not found errors from service
            logger.warning(f"Error during synchronous sampling for user {user_id}, dataset {dataset_id}, version {version_id}: {str(e)}")
            # Determine if it's a 404 or 400 based on error message if possible
            if "not found" in str(e).lower():
                 raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=str(e)
                )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=str(e)
            )
        except Exception as e:
            # Handle all other errors
            logger.error(f"Unexpected error during synchronous sampling for user {user_id}, dataset {dataset_id}, version {version_id}: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error executing sampling: {str(e)}"
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

