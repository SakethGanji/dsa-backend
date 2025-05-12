from typing import Dict, Any
from fastapi import HTTPException, status

from app.explore.service import ExploreService
from app.explore.models import ExploreRequest

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
        """Apply basic operations and return a sample of data"""
        try:
            result = await self.service.explore_dataset(
                dataset_id=dataset_id,
                version_id=version_id,
                request=request,
                user_id=user_id
            )
            return result
        except ValueError as e:
            # Handle validation and not found errors
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=str(e)
            )
        except Exception as e:
            # Handle all other errors
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error exploring dataset: {str(e)}"
            )