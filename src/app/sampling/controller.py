from typing import Dict, List, Any, Optional
from fastapi import HTTPException, status
import logging

from app.sampling.service import SamplingService
from app.sampling.models import (
    SamplingRequest, SamplingJobResponse, 
    SamplingJobDetails, JobStatus,
    MultiRoundSamplingRequest, MultiRoundSamplingJobResponse
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

            # Call service method - now returns List[Dict] directly
            data = await self.service.execute_sampling_synchronously(
                dataset_id=dataset_id,
                version_id=version_id,
                request=request
            )

            logger.info(f"Synchronous sampling completed for dataset {dataset_id}, version {version_id}. Rows: {len(data)}")
            
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
    
    # Multi-round sampling methods
    async def create_multi_round_sampling_job(
        self,
        dataset_id: int,
        version_id: int,
        request: MultiRoundSamplingRequest,
        user_id: int
    ) -> MultiRoundSamplingJobResponse:
        """
        Create a new multi-round sampling job
        
        Args:
            dataset_id: The ID of the dataset
            version_id: The ID of the version
            request: The multi-round sampling request with rounds configuration
            user_id: The ID of the user making the request
            
        Returns:
            A response with the job ID and status
            
        Raises:
            HTTPException: If an error occurs during job creation
        """
        try:
            logger.info(f"User {user_id} creating multi-round sampling job for dataset {dataset_id}, version {version_id}")
            
            # Call service method to create job
            job = await self.service.create_multi_round_sampling_job(
                dataset_id=dataset_id,
                version_id=version_id,
                request=request,
                user_id=user_id
            )
            
            logger.info(f"Created multi-round sampling job {job.id} for dataset {dataset_id}, version {version_id}")
            
            # Return response
            return MultiRoundSamplingJobResponse(
                run_id=job.id,
                status=job.status,
                message=f"Multi-round sampling job created with {len(request.rounds)} rounds",
                total_rounds=job.total_rounds,
                completed_rounds=job.completed_rounds,
                current_round=job.current_round,
                round_results=job.round_results,
                residual_uri=job.residual_uri,
                residual_size=job.residual_size,
                residual_summary=job.residual_summary,
                error_message=job.error_message,
                created_at=job.created_at,
                started_at=job.started_at,
                completed_at=job.completed_at
            )
            
        except ValueError as e:
            # Handle validation errors from service
            logger.warning(f"Validation error: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=str(e)
            )
        except Exception as e:
            # Handle all other errors
            logger.error(f"Error creating multi-round sampling job: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error creating multi-round sampling job: {str(e)}"
            )
    
    async def get_multi_round_job(self, job_id: str) -> MultiRoundSamplingJobResponse:
        """
        Get details of a multi-round sampling job
        
        Args:
            job_id: The ID of the job
            
        Returns:
            Job details including status and results
            
        Raises:
            HTTPException: If the job is not found
        """
        try:
            logger.info(f"Getting multi-round job details for job {job_id}")
            
            # Call service method
            job = await self.service.get_multi_round_job(job_id)
            
            if not job:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Multi-round sampling job with ID {job_id} not found"
                )
            
            # Build status message
            if job.status == JobStatus.COMPLETED:
                message = f"Multi-round sampling completed. {job.completed_rounds} rounds processed."
            elif job.status == JobStatus.RUNNING:
                message = f"Processing round {job.current_round} of {job.total_rounds}"
            elif job.status == JobStatus.FAILED:
                message = f"Multi-round sampling failed: {job.error_message}"
            else:
                message = "Multi-round sampling job is queued"
            
            # Return response
            return MultiRoundSamplingJobResponse(
                run_id=job.id,
                status=job.status,
                message=message,
                total_rounds=job.total_rounds,
                completed_rounds=job.completed_rounds,
                current_round=job.current_round,
                round_results=job.round_results,
                residual_uri=job.residual_uri,
                residual_size=job.residual_size,
                residual_summary=job.residual_summary,
                error_message=job.error_message,
                created_at=job.created_at,
                started_at=job.started_at,
                completed_at=job.completed_at
            )
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting multi-round job details: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error getting multi-round job details: {str(e)}"
            )
    
    async def get_round_preview(
        self,
        job_id: str,
        round_number: int,
        page: int = 1,
        page_size: int = 100
    ) -> Dict[str, Any]:
        """Get preview data from a specific sampling round"""
        try:
            # Get job details
            job = await self.service.get_multi_round_job(job_id)
            
            if not job:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Multi-round sampling job with ID {job_id} not found"
                )
            
            # Find the round result
            round_result = None
            for result in job.round_results:
                if result.round_number == round_number:
                    round_result = result
                    break
            
            if not round_result:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Round {round_number} not found or not yet completed"
                )
            
            # For now, return the preview data from the round result
            # In a full implementation, you would load the actual file and paginate
            preview_data = round_result.preview or []
            
            # Apply pagination
            total_items = len(preview_data)
            total_pages = (total_items + page_size - 1) // page_size
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            
            paginated_data = preview_data[start_idx:end_idx]
            
            return {
                "data": paginated_data,
                "round_info": {
                    "round_number": round_result.round_number,
                    "method": round_result.method,
                    "sample_size": round_result.sample_size,
                    "output_uri": round_result.output_uri
                },
                "pagination": {
                    "page": page,
                    "page_size": page_size,
                    "total_items": total_items,
                    "total_pages": total_pages,
                    "has_next": page < total_pages,
                    "has_previous": page > 1
                }
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting round preview: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error getting round preview: {str(e)}"
            )
    
    async def get_residual_preview(
        self,
        job_id: str,
        page: int = 1,
        page_size: int = 100
    ) -> Dict[str, Any]:
        """Get preview data from the residual dataset"""
        try:
            # Get job details
            job = await self.service.get_multi_round_job(job_id)
            
            if not job:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Multi-round sampling job with ID {job_id} not found"
                )
            
            if job.status != JobStatus.COMPLETED:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Job must be completed to view residual data"
                )
            
            if not job.residual_uri:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="No residual dataset available (export_residual was false or all data was sampled)"
                )
            
            # For now, return empty data as we don't have actual residual preview
            # In a full implementation, you would load the residual file and paginate
            return {
                "data": [],
                "residual_info": {
                    "size": job.residual_size,
                    "uri": job.residual_uri,
                    "summary": job.residual_summary
                },
                "pagination": {
                    "page": page,
                    "page_size": page_size,
                    "total_items": 0,
                    "total_pages": 0,
                    "has_next": False,
                    "has_previous": False
                }
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting residual preview: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error getting residual preview: {str(e)}"
            )
    
    async def execute_multi_round_sampling_sync(
        self,
        dataset_id: int,
        version_id: int,
        request: MultiRoundSamplingRequest,
        user_id: int,
        page: int = 1,
        page_size: int = 100
    ) -> Dict[str, Any]:
        """
        Execute multi-round sampling synchronously and return paginated results.
        
        Args:
            dataset_id: The ID of the dataset
            version_id: The ID of the version
            request: The multi-round sampling request with rounds configuration
            user_id: The ID of the user making the request
            page: Page number for pagination (1-indexed)
            page_size: Number of items per page
            
        Returns:
            Dictionary with paginated round results and residual data
            
        Raises:
            HTTPException: If an error occurs during execution
        """
        try:
            logger.info(f"User {user_id} executing multi-round sampling synchronously for dataset {dataset_id}, version {version_id}")
            
            # Call service method
            results = await self.service.execute_multi_round_sampling_synchronously(
                dataset_id=dataset_id,
                version_id=version_id,
                request=request
            )
            
            logger.info(f"Synchronous multi-round sampling completed for dataset {dataset_id}, version {version_id}. Rounds: {len(request.rounds)}")
            
            # Apply pagination to each round's data
            paginated_rounds = []
            for round_result in results["rounds"]:
                round_data = round_result["data"]
                total_items = len(round_data)
                total_pages = (total_items + page_size - 1) // page_size
                start_idx = (page - 1) * page_size
                end_idx = start_idx + page_size
                
                paginated_round = {
                    "round_number": round_result["round_number"],
                    "method": round_result["method"],
                    "sample_size": round_result["sample_size"],
                    "data": round_data[start_idx:end_idx],
                    "summary": round_result["summary"],
                    "pagination": {
                        "page": page,
                        "page_size": page_size,
                        "total_items": total_items,
                        "total_pages": total_pages,
                        "has_next": page < total_pages,
                        "has_previous": page > 1
                    }
                }
                paginated_rounds.append(paginated_round)
            
            # Apply pagination to residual data if present
            paginated_residual = None
            if results["residual"]:
                residual_data = results["residual"]["data"]
                if residual_data:
                    total_items = len(residual_data)
                    total_pages = (total_items + page_size - 1) // page_size
                    start_idx = (page - 1) * page_size
                    end_idx = start_idx + page_size
                    
                    paginated_residual = {
                        "size": results["residual"]["size"],
                        "data": residual_data[start_idx:end_idx],
                        "summary": results["residual"]["summary"],
                        "pagination": {
                            "page": page,
                            "page_size": page_size,
                            "total_items": total_items,
                            "total_pages": total_pages,
                            "has_next": page < total_pages,
                            "has_previous": page > 1
                        }
                    }
                else:
                    paginated_residual = results["residual"]
            
            return {
                "rounds": paginated_rounds,
                "residual": paginated_residual
            }
            
        except ValueError as e:
            # Handle validation errors from service
            logger.warning(f"Validation error during synchronous multi-round sampling: {str(e)}")
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
            logger.error(f"Error executing multi-round sampling synchronously: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error executing multi-round sampling: {str(e)}"
            )

