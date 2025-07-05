"""Dependency injection configuration for the application."""

from typing import AsyncGenerator, Optional
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer

from .database import DatabasePool
from .abstractions import IUnitOfWork
from .infrastructure.postgres import PostgresUnitOfWork
from .infrastructure.services import FileParserFactory, DefaultStatisticsCalculator

# OAuth2 scheme for token authentication
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")

# Global instances (these would be initialized in main.py)
_db_pool: Optional[DatabasePool] = None
_parser_factory: Optional[FileParserFactory] = None
_stats_calculator: Optional[DefaultStatisticsCalculator] = None


def set_database_pool(pool: DatabasePool) -> None:
    """Set the global database pool instance."""
    global _db_pool
    _db_pool = pool


def set_parser_factory(factory: FileParserFactory) -> None:
    """Set the global parser factory instance."""
    global _parser_factory
    _parser_factory = factory


def set_stats_calculator(calculator: DefaultStatisticsCalculator) -> None:
    """Set the global statistics calculator instance."""
    global _stats_calculator
    _stats_calculator = calculator


async def get_db_pool() -> DatabasePool:
    """Get database pool dependency."""
    if _db_pool is None:
        raise RuntimeError("Database pool not initialized")
    return _db_pool


async def get_uow() -> AsyncGenerator[IUnitOfWork, None]:
    """Get unit of work dependency."""
    pool = await get_db_pool()
    uow = PostgresUnitOfWork(pool)
    async with uow:
        yield uow


async def get_parser_factory() -> FileParserFactory:
    """Get file parser factory dependency."""
    if _parser_factory is None:
        raise RuntimeError("Parser factory not initialized")
    return _parser_factory


async def get_stats_calculator() -> DefaultStatisticsCalculator:
    """Get statistics calculator dependency."""
    if _stats_calculator is None:
        raise RuntimeError("Statistics calculator not initialized")
    return _stats_calculator


async def get_current_user(
    token: str = Depends(oauth2_scheme),
    uow: IUnitOfWork = Depends(get_uow)
) -> dict:
    """
    Get current authenticated user from JWT token.
    
    This is a placeholder implementation. In production, you would:
    1. Decode and validate the JWT token
    2. Extract user ID from token claims
    3. Load user from database
    4. Return user dict
    """
    # TODO: Implement proper JWT validation
    # For now, return a mock user
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    # Mock implementation - replace with real JWT validation
    if not token:
        raise credentials_exception
    
    # In real implementation:
    # - Decode JWT token
    # - Extract user_id from claims
    # - Load user from database using uow.users.get_by_id(user_id)
    
    return {
        "id": 1,
        "soeid": "test_user",
        "role": "user"
    }


async def require_admin(
    current_user: dict = Depends(get_current_user)
) -> dict:
    """Require admin role for the current user."""
    if current_user.get("role") != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required"
        )
    return current_user