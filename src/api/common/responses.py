"""Common response models and builders for API endpoints."""

from typing import Optional, Any, Dict, List
from pydantic import BaseModel


class SuccessResponse(BaseModel):
    """Standard success response format."""
    success: bool = True
    data: Any
    message: Optional[str] = None


class ErrorResponse(BaseModel):
    """Standard error response format."""
    success: bool = False
    error: str
    detail: Optional[str] = None
    code: Optional[str] = None


class ResponseBuilder:
    """Helper class for building consistent API responses."""
    
    @staticmethod
    def success(
        data: Any,
        message: Optional[str] = None
    ) -> SuccessResponse:
        """
        Build a success response.
        
        Args:
            data: The response data
            message: Optional success message
            
        Returns:
            SuccessResponse object
        """
        return SuccessResponse(
            data=data,
            message=message
        )
    
    @staticmethod
    def error(
        error: str,
        detail: Optional[str] = None,
        code: Optional[str] = None
    ) -> ErrorResponse:
        """
        Build an error response.
        
        Args:
            error: Error type/category
            detail: Detailed error message
            code: Optional error code
            
        Returns:
            ErrorResponse object
        """
        return ErrorResponse(
            error=error,
            detail=detail,
            code=code
        )
    
    @staticmethod
    def created(
        data: Any,
        message: str = "Resource created successfully"
    ) -> SuccessResponse:
        """Build a response for successful creation."""
        return SuccessResponse(
            data=data,
            message=message
        )
    
    @staticmethod
    def updated(
        data: Any,
        message: str = "Resource updated successfully"
    ) -> SuccessResponse:
        """Build a response for successful update."""
        return SuccessResponse(
            data=data,
            message=message
        )
    
    @staticmethod
    def deleted(
        message: str = "Resource deleted successfully"
    ) -> SuccessResponse:
        """Build a response for successful deletion."""
        return SuccessResponse(
            data=None,
            message=message
        )


class BatchResponse(BaseModel):
    """Response for batch operations."""
    success_count: int
    failure_count: int
    total: int
    results: List[Dict[str, Any]]
    
    @property
    def all_successful(self) -> bool:
        """Check if all operations were successful."""
        return self.failure_count == 0