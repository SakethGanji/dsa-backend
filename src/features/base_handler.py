"""Base handler class to reduce duplication across feature handlers."""

from typing import TypeVar, Generic, Optional, Callable, Any
from functools import wraps
import logging
import asyncpg
from ..infrastructure.postgres.uow import PostgresUnitOfWork
from ..core.common.pagination import PaginationMixin
from ..core.domain_exceptions import (
    DomainException, 
    ValidationException, 
    ForbiddenException,
    ConflictException
)

# Type variable for handler return types
TResult = TypeVar('TResult')

logger = logging.getLogger(__name__)


class BaseHandler(Generic[TResult]):
    """Base handler class with common initialization and error handling."""
    
    def __init__(self, uow: PostgresUnitOfWork):
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
        except DomainException as e:
            # Domain exceptions are expected - log at INFO level
            logger.info(f"Domain exception in {self.__class__.__name__}: {e}")
            raise  # Re-raise as-is, will be handled by API layer
        except ValueError as e:
            # Convert to ValidationException
            logger.warning(f"Validation error in {self.__class__.__name__}: {str(e)}")
            raise ValidationException(str(e))
        except PermissionError as e:
            # Convert to ForbiddenException
            logger.warning(f"Permission denied in {self.__class__.__name__}: {str(e)}")
            raise ForbiddenException(str(e))
        except asyncpg.UniqueViolationError as e:
            # Convert to ConflictException for duplicate key violations
            logger.warning(f"Unique constraint violation in {self.__class__.__name__}: {str(e)}")
            # Extract the constraint name and field info from the error message
            error_msg = str(e)
            if "datasets_name_created_by_key" in error_msg:
                raise ConflictException("A dataset with this name already exists")
            else:
                raise ConflictException(f"Duplicate value violates unique constraint: {error_msg}")
        except asyncpg.PostgresError as e:
            # Handle other PostgreSQL errors
            logger.error(f"Database error in {self.__class__.__name__}: {str(e)}")
            raise DomainException(
                "Database operation failed",
                details={"original_error": str(e), "error_type": type(e).__name__}
            )
        except Exception as e:
            # Unexpected exceptions - log at ERROR level
            logger.error(f"Unexpected error in {self.__class__.__name__}: {str(e)}", exc_info=True)
            raise DomainException(
                "An unexpected error occurred",
                details={"original_error": str(e), "error_type": type(e).__name__}
            )
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


# Export public API
__all__ = ['BaseHandler', 'with_error_handling', 'with_transaction', 'PaginationMixin']