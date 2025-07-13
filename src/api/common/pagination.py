"""
Pagination utilities for API endpoints.

DEPRECATED: This module is deprecated in favor of src.core.common.pagination.
It is maintained for backward compatibility only.
"""

import warnings
from typing import Callable
import functools
from fastapi import Query
from pydantic import Field

# Import everything from the new unified pagination module
from src.core.common.pagination import (
    PaginationConfig,
    pagination_config,
    PaginationParams as _BasePaginationParams,
    PaginatedResponse,
    PaginationMixin
)

# Show deprecation warning
warnings.warn(
    "Importing from api.common.pagination is deprecated. "
    "Use src.core.common.pagination instead.",
    DeprecationWarning,
    stacklevel=2
)


# Extend PaginationParams to work with FastAPI Query
class PaginationParams(_BasePaginationParams):
    """Standard pagination parameters for list endpoints."""
    offset: int = Field(
        Query(default=0, ge=0),
        description="Number of items to skip"
    )
    limit: int = Field(
        Query(default=pagination_config.DEFAULT_LIMIT, 
              ge=pagination_config.MIN_LIMIT, 
              le=pagination_config.MAX_LIMIT),
        description="Number of items to return"
    )


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


# Re-export everything for backward compatibility
__all__ = [
    'PaginationConfig',
    'pagination_config',
    'PaginationParams',
    'PaginatedResponse',
    'PaginationMixin',
    'paginate'
]