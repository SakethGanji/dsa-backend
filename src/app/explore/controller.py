from typing import Dict, Any
from fastapi import HTTPException, status
import logging

from app.explore.service import ExploreService
from app.explore.models import ExploreRequest

logger = logging.getLogger(__name__)

class ExploreController:
    def __init__(self, service: ExploreService):
        self.service = service
        
    async def explore_dataset(
        self, 
        dataset_id: int, 
        version_id: int, 
        request: ExploreRequest,
        user_id: int
    ) -> Dict[str, Any]:
        """
        Apply operations and generate a profile for a dataset
        
        Args:
            dataset_id: The ID of the dataset
            version_id: The ID of the version
            request: The explore request with operations
            user_id: The ID of the user making the request
            
        Returns:
            A dictionary with the exploration results
            
        Raises:
            HTTPException: If an error occurs during exploration
        """
        try:
            logger.info(f"User {user_id} exploring dataset {dataset_id}, version {version_id}")
            
            # Validate inputs
            self._validate_request(request)
            
            # Call service method
            result = await self.service.explore_dataset(
                dataset_id=dataset_id,
                version_id=version_id,
                request=request,
                user_id=user_id
            )
            
            logger.info(f"Successfully explored dataset {dataset_id}, version {version_id}")
            return result
            
        except ValueError as e:
            # Handle validation and not found errors
            logger.warning(f"Resource not found: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=str(e)
            )
        except Exception as e:
            # Handle all other errors
            logger.error(f"Error exploring dataset: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error exploring dataset: {str(e)}"
            )
            
    def _validate_request(self, request: ExploreRequest) -> None:
        """Validate the explore request"""
        # Validate that operations is a list
        if not isinstance(request.operations, list):
            raise ValueError("Operations must be a list")
        
        # Validate that each operation has a type
        for op in request.operations:
            if not isinstance(op, dict) or "type" not in op:
                raise ValueError("Each operation must be a dictionary with a 'type' field")
                
        # Note: more detailed validation can be added here if needed