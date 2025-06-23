from typing import Dict, List, Any, Optional
from fastapi import HTTPException, status
import logging
from datetime import datetime

from app.sampling.service import SamplingService
from app.sampling.models import (
    JobStatus,
    MultiRoundSamplingRequest, MultiRoundSamplingJobResponse
)

logger = logging.getLogger(__name__)

class SamplingController:
    def __init__(self, service: SamplingService):
        self.service = service
    
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
            result = await self.service.create_multi_round_sampling_job(
                dataset_id=dataset_id,
                version_id=version_id,
                request=request,
                user_id=user_id
            )
            
            logger.info(f"Created multi-round sampling job {result['run_id']} for dataset {dataset_id}, version {version_id}")
            
            # Return response
            return MultiRoundSamplingJobResponse(
                run_id=str(result["run_id"]),  # Convert to string for consistency
                status=JobStatus(result["status"]),
                message=result.get("message", f"Multi-round sampling job created with {len(request.rounds)} rounds"),
                total_rounds=len(request.rounds),
                completed_rounds=0,
                current_round=None,
                round_results=[],
                residual_uri=None,
                residual_size=None,
                residual_summary=None,
                error_message=None,
                created_at=datetime.now()
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
            job_data = await self.service.get_multi_round_job(job_id)
            
            if not job_data:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Multi-round sampling job with ID {job_id} not found"
                )
            
            # Extract data from dict response
            status_str = job_data.get("status", "pending")
            status_enum = JobStatus(status_str)
            completed_rounds = job_data.get("completed_rounds", 0)
            total_rounds = job_data.get("total_rounds", 0)
            current_round = job_data.get("current_round")
            error_message = job_data.get("error_message")
            
            # Build status message
            if status_enum == JobStatus.COMPLETED:
                message = f"Multi-round sampling completed. {completed_rounds} rounds processed."
            elif status_enum == JobStatus.RUNNING:
                message = f"Processing round {current_round} of {total_rounds}"
            elif status_enum == JobStatus.FAILED:
                message = f"Multi-round sampling failed: {error_message}"
            else:
                message = "Multi-round sampling job is queued"
            
            # Return response
            return MultiRoundSamplingJobResponse(
                run_id=job_data.get("id", job_id),
                status=status_enum,
                message=message,
                total_rounds=total_rounds,
                completed_rounds=completed_rounds,
                current_round=current_round,
                round_results=job_data.get("round_results", []),
                residual_uri=job_data.get("residual_uri"),
                residual_size=job_data.get("residual_size"),
                residual_summary=job_data.get("residual_summary"),
                error_message=error_message,
                created_at=job_data.get("created_at", datetime.now()),
                started_at=job_data.get("started_at"),
                completed_at=job_data.get("completed_at")
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
            job_data = await self.service.get_multi_round_job(job_id)
            
            if not job_data:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Multi-round sampling job with ID {job_id} not found"
                )
            
            # Find the round result
            round_result = None
            for result in job_data.get("round_results", []):
                if result.get("round_number") == round_number:
                    round_result = result
                    break
            
            if not round_result:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Round {round_number} not found or not yet completed"
                )
            
            # For now, return the preview data from the round result
            # In a full implementation, you would load the actual file and paginate
            preview_data = round_result.get("preview", [])
            
            # Apply pagination
            total_items = len(preview_data)
            total_pages = (total_items + page_size - 1) // page_size
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            
            paginated_data = preview_data[start_idx:end_idx]
            
            return {
                "data": paginated_data,
                "round_info": {
                    "round_number": round_result.get("round_number"),
                    "method": round_result.get("method"),
                    "sample_size": round_result.get("sample_size"),
                    "output_uri": round_result.get("output_uri")
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
            job_data = await self.service.get_multi_round_job(job_id)
            
            if not job_data:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Multi-round sampling job with ID {job_id} not found"
                )
            
            status_str = job_data.get("status", "pending")
            if JobStatus(status_str) != JobStatus.COMPLETED:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Job must be completed to view residual data"
                )
            
            residual_uri = job_data.get("residual_uri")
            if not residual_uri:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="No residual dataset available (export_residual was false or all data was sampled)"
                )
            
            # For now, return empty data as we don't have actual residual preview
            # In a full implementation, you would load the residual file and paginate
            return {
                "data": [],
                "residual_info": {
                    "size": job_data.get("residual_size"),
                    "uri": residual_uri,
                    "summary": job_data.get("residual_summary")
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
    
    async def get_merged_sample_data(
        self,
        job_id: str,
        page: int = 1,
        page_size: int = 100,
        columns: Optional[List[str]] = None,
        export_format: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get paginated data from the merged sample file
        
        Args:
            job_id: The multi-round sampling job ID
            page: Page number (1-indexed)
            page_size: Number of items per page
            columns: Optional list of columns to return
            export_format: Optional export format (csv, json)
            
        Returns:
            Dictionary containing paginated data and metadata
            
        Raises:
            HTTPException: If job not found or data retrieval fails
        """
        try:
            logger.info(f"Getting merged sample data for job {job_id}, page {page}")
            
            # Call service method to get the paginated data
            result = await self.service.get_merged_sample_data(
                job_id=job_id,
                page=page,
                page_size=page_size,
                columns=columns,
                export_format=export_format
            )
            
            return result
            
        except ValueError as e:
            # Handle validation errors
            logger.warning(f"Validation error getting merged sample: {str(e)}")
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
            logger.error(f"Error getting merged sample data: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error retrieving merged sample data: {str(e)}"
            )
    
    async def get_samplings_by_user(
        self,
        user_id: int,
        page: int = 1,
        page_size: int = 10
    ) -> Dict[str, Any]:
        """
        Get all sampling runs created by a specific user
        
        Args:
            user_id: The user ID to filter by
            page: Page number (1-indexed)
            page_size: Number of items per page
            
        Returns:
            Dictionary with sampling runs and pagination info
            
        Raises:
            HTTPException: If an error occurs
        """
        try:
            logger.info(f"Getting sampling runs for user {user_id}, page {page}")
            
            # Call service method
            runs, total_count = await self.service.get_samplings_by_user(
                user_id=user_id,
                page=page,
                page_size=page_size
            )
            
            # Calculate pagination info
            total_pages = (total_count + page_size - 1) // page_size
            
            return {
                "runs": runs,
                "total_count": total_count,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_previous": page > 1
            }
            
        except ValueError as e:
            logger.warning(f"Validation error: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=str(e)
            )
        except Exception as e:
            logger.error(f"Error getting samplings by user: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error retrieving sampling runs: {str(e)}"
            )
    
    async def get_samplings_by_dataset_version(
        self,
        dataset_version_id: int,
        page: int = 1,
        page_size: int = 10
    ) -> Dict[str, Any]:
        """
        Get all sampling runs for a specific dataset version
        
        Args:
            dataset_version_id: The dataset version ID to filter by
            page: Page number (1-indexed)
            page_size: Number of items per page
            
        Returns:
            Dictionary with sampling runs and pagination info
            
        Raises:
            HTTPException: If an error occurs
        """
        try:
            logger.info(f"Getting sampling runs for dataset version {dataset_version_id}, page {page}")
            
            # Call service method
            runs, total_count = await self.service.get_samplings_by_dataset_version(
                dataset_version_id=dataset_version_id,
                page=page,
                page_size=page_size
            )
            
            # Calculate pagination info
            total_pages = (total_count + page_size - 1) // page_size
            
            return {
                "runs": runs,
                "total_count": total_count,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_previous": page > 1
            }
            
        except ValueError as e:
            logger.warning(f"Validation error: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=str(e)
            )
        except Exception as e:
            logger.error(f"Error getting samplings by dataset version: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error retrieving sampling runs: {str(e)}"
            )
    
    async def get_samplings_by_dataset(
        self,
        dataset_id: int,
        page: int = 1,
        page_size: int = 10
    ) -> Dict[str, Any]:
        """
        Get all sampling runs for a specific dataset (across all versions)
        
        Args:
            dataset_id: The dataset ID to filter by
            page: Page number (1-indexed)
            page_size: Number of items per page
            
        Returns:
            Dictionary with sampling runs and pagination info
            
        Raises:
            HTTPException: If an error occurs
        """
        try:
            logger.info(f"Getting sampling runs for dataset {dataset_id}, page {page}")
            
            # Call service method
            runs, total_count = await self.service.get_samplings_by_dataset(
                dataset_id=dataset_id,
                page=page,
                page_size=page_size
            )
            
            # Calculate pagination info
            total_pages = (total_count + page_size - 1) // page_size
            
            return {
                "runs": runs,
                "total_count": total_count,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_previous": page > 1
            }
            
        except ValueError as e:
            logger.warning(f"Validation error: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=str(e)
            )
        except Exception as e:
            logger.error(f"Error getting samplings by dataset: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error retrieving sampling runs: {str(e)}"
            )

