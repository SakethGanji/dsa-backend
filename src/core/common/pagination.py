"""Unified pagination module for the entire application."""

from typing import TypeVar, Generic, List, Tuple, Optional
from pydantic import BaseModel, Field
from dataclasses import dataclass

T = TypeVar('T')


@dataclass
class PaginationConfig:
    """Central configuration for pagination"""
    DEFAULT_LIMIT: int = 100
    MAX_LIMIT: int = 1000  # Standardized limit across application
    MIN_LIMIT: int = 1


# Single source of truth for pagination configuration
pagination_config = PaginationConfig()


class PaginationParams(BaseModel):
    """Standard pagination parameters for API endpoints"""
    offset: int = Field(
        default=0, 
        ge=0, 
        description="Number of items to skip"
    )
    limit: int = Field(
        default=pagination_config.DEFAULT_LIMIT,
        ge=pagination_config.MIN_LIMIT,
        le=pagination_config.MAX_LIMIT,
        description="Number of items to return"
    )


class PaginatedResponse(BaseModel, Generic[T]):
    """Standard paginated response format"""
    items: List[T]
    total: int
    offset: int
    limit: int
    has_more: bool
    
    class Config:
        """Pydantic config to allow generic types"""
        arbitrary_types_allowed = True


class PaginationMixin:
    """Unified pagination mixin for all handlers"""
    
    @staticmethod
    def validate_pagination(
        offset: int = 0, 
        limit: int = pagination_config.DEFAULT_LIMIT
    ) -> Tuple[int, int]:
        """
        Validate and normalize pagination parameters.
        
        Args:
            offset: Number of items to skip
            limit: Number of items to return
            
        Returns:
            Tuple of (offset, limit) with validated values
        """
        offset = max(0, offset)
        limit = max(
            pagination_config.MIN_LIMIT, 
            min(pagination_config.MAX_LIMIT, limit)
        )
        return offset, limit
    
    @staticmethod
    def create_paginated_response(
        items: List[T],
        total: int,
        offset: int,
        limit: int
    ) -> PaginatedResponse[T]:
        """
        Create a standardized paginated response.
        
        Args:
            items: List of items for current page
            total: Total number of items across all pages
            offset: Number of items skipped
            limit: Maximum items per page
            
        Returns:
            PaginatedResponse with pagination metadata
        """
        return PaginatedResponse(
            items=items,
            total=total,
            offset=offset,
            limit=limit,
            has_more=total > offset + limit
        )
    
    def paginate_response(
        self,
        items: List[T],
        total: int,
        offset: int,
        limit: int
    ) -> PaginatedResponse[T]:
        """
        Alias for create_paginated_response for backward compatibility.
        
        This method exists to maintain compatibility with existing code
        that uses self.paginate_response().
        """
        return self.create_paginated_response(items, total, offset, limit)


# Re-export common types for convenience
__all__ = [
    'PaginationConfig',
    'pagination_config',
    'PaginationParams',
    'PaginatedResponse',
    'PaginationMixin'
]