"""Pagination utilities for API endpoints."""

from typing import TypeVar, Generic, List, Callable, Tuple
from pydantic import BaseModel, Field
from fastapi import Query, Depends
import functools

T = TypeVar('T')


class PaginationParams(BaseModel):
    """Standard pagination parameters for list endpoints."""
    offset: int = Field(
        Query(default=0, ge=0),
        description="Number of items to skip"
    )
    limit: int = Field(
        Query(default=100, ge=1, le=1000),
        description="Number of items to return"
    )


class PaginatedResponse(BaseModel, Generic[T]):
    """Standard paginated response format."""
    items: List[T]
    total: int
    offset: int
    limit: int
    has_more: bool
    
    class Config:
        """Pydantic config to allow generic types."""
        arbitrary_types_allowed = True


def paginate(func: Callable) -> Callable:
    """
    Decorator for automatic pagination handling.
    
    The decorated function should return a tuple of (items, total).
    This decorator will automatically wrap the response in PaginatedResponse.
    
    Example:
        @router.get("/items")
        @paginate
        async def list_items(
            pagination: PaginationParams = Depends()
        ):
            items, total = await get_items(
                offset=pagination.offset,
                limit=pagination.limit
            )
            return items, total
    """
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        # Extract pagination params
        pagination = None
        for key, value in kwargs.items():
            if isinstance(value, PaginationParams):
                pagination = value
                break
        
        if not pagination:
            # If no pagination params provided, use defaults
            pagination = PaginationParams()
        
        # Call the original function
        result = await func(*args, **kwargs)
        
        # Handle the response
        if isinstance(result, tuple) and len(result) == 2:
            items, total = result
            return PaginatedResponse(
                items=items,
                total=total,
                offset=pagination.offset,
                limit=pagination.limit,
                has_more=total > pagination.offset + pagination.limit
            )
        else:
            # If not a tuple, assume it's already a PaginatedResponse
            return result
    
    return wrapper


class PaginationMixin:
    """
    Mixin class for handlers that need pagination support.
    
    Usage:
        class ListDatasetsHandler(BaseHandler, PaginationMixin):
            async def handle(self, offset: int, limit: int):
                items = await self._repo.list(offset=offset, limit=limit)
                total = await self._repo.count()
                return self.paginate_response(items, total, offset, limit)
    """
    
    @staticmethod
    def paginate_response(
        items: List[T],
        total: int,
        offset: int,
        limit: int
    ) -> PaginatedResponse[T]:
        """Create a paginated response."""
        return PaginatedResponse(
            items=items,
            total=total,
            offset=offset,
            limit=limit,
            has_more=total > offset + limit
        )
    
    @staticmethod
    def validate_pagination(offset: int, limit: int) -> Tuple[int, int]:
        """
        Validate and normalize pagination parameters.
        
        Returns:
            Tuple of (offset, limit) with validated values
        """
        offset = max(0, offset)
        limit = max(1, min(1000, limit))
        return offset, limit