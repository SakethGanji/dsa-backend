"""Base handler class to reduce duplication across feature handlers."""

from typing import TypeVar, Generic, Optional, Callable, Any
from functools import wraps
import logging
from src.core.abstractions import IUnitOfWork

# Type variable for handler return types
TResult = TypeVar('TResult')

logger = logging.getLogger(__name__)


class BaseHandler(Generic[TResult]):
    """Base handler class with common initialization and error handling."""
    
    def __init__(self, uow: IUnitOfWork):
        """Initialize handler with unit of work."""
        self._uow = uow
    
    async def handle(self, *args, **kwargs) -> TResult:
        """Abstract method to be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement handle method")


def with_error_handling(func: Callable) -> Callable:
    """Decorator for consistent error handling across handlers."""
    @wraps(func)
    async def wrapper(self, *args, **kwargs) -> Any:
        try:
            return await func(self, *args, **kwargs)
        except ValueError as e:
            logger.warning(f"Validation error in {self.__class__.__name__}: {str(e)}")
            raise
        except PermissionError as e:
            logger.warning(f"Permission denied in {self.__class__.__name__}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in {self.__class__.__name__}: {str(e)}", exc_info=True)
            raise
    return wrapper


def with_transaction(func: Callable) -> Callable:
    """Decorator for automatic transaction management."""
    @wraps(func)
    async def wrapper(self, *args, **kwargs) -> Any:
        if hasattr(self, '_uow'):
            await self._uow.begin()
            try:
                result = await func(self, *args, **kwargs)
                await self._uow.commit()
                return result
            except Exception:
                await self._uow.rollback()
                raise
        else:
            # If no UoW, just execute the function
            return await func(self, *args, **kwargs)
    return wrapper


class PaginationMixin:
    """Mixin for pagination support in handlers."""
    
    def validate_pagination(self, offset: int = 0, limit: int = 100) -> tuple[int, int]:
        """Validate and normalize pagination parameters."""
        if offset < 0:
            raise ValueError("Offset must be non-negative")
        if limit < 1:
            raise ValueError("Limit must be at least 1")
        if limit > 10000:
            raise ValueError("Limit cannot exceed 10000")
        return offset, limit
    
    def create_paginated_response(
        self, 
        items: list,
        total: int,
        offset: int,
        limit: int,
        response_class: type
    ) -> Any:
        """Create a paginated response object."""
        return response_class(
            items=items,
            total=total,
            offset=offset,
            limit=limit,
            has_more=offset + len(items) < total
        )