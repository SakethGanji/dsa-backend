"""Dependency injection configuration for the FastAPI application."""

from typing import AsyncGenerator, Optional
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer

from ..infrastructure.postgres.database import DatabasePool
from ..core.events.publisher import EventBus
from ..infrastructure.postgres.uow import PostgresUnitOfWork
from src.features.file_processing.services.factory import FileParserFactory
from ..core.permissions import PermissionService

# OAuth2 scheme for token authentication
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")

# Global instances (these would be initialized in main.py)
_db_pool: Optional[DatabasePool] = None
_parser_factory: Optional[FileParserFactory] = None
_event_bus: Optional[EventBus] = None


def set_database_pool(pool: DatabasePool) -> None:
    """Set the global database pool instance."""
    global _db_pool
    _db_pool = pool


def set_parser_factory(factory: FileParserFactory) -> None:
    """Set the global parser factory instance."""
    global _parser_factory
    _parser_factory = factory


def set_event_bus(event_bus: EventBus) -> None:
    """Set the global event bus instance."""
    global _event_bus
    _event_bus = event_bus


async def get_db_pool() -> DatabasePool:
    """Get database pool dependency."""
    if _db_pool is None:
        raise RuntimeError("Database pool not initialized")
    return _db_pool


async def get_uow() -> AsyncGenerator[PostgresUnitOfWork, None]:
    """Get unit of work dependency."""
    pool = await get_db_pool()
    uow = PostgresUnitOfWork(pool)
    async with uow:
        yield uow


async def get_permission_service(
    uow: PostgresUnitOfWork = Depends(get_uow)
) -> PermissionService:
    """Get permission service dependency with request-scoped caching."""
    return PermissionService(uow)


async def get_parser_factory() -> FileParserFactory:
    """Get file parser factory dependency."""
    if _parser_factory is None:
        raise RuntimeError("Parser factory not initialized")
    return _parser_factory


async def get_event_bus() -> Optional[EventBus]:
    """Get event bus dependency."""
    return _event_bus  # Can be None if events not configured


async def get_current_user(
    token: str = Depends(oauth2_scheme),
    uow: PostgresUnitOfWork = Depends(get_uow)
) -> dict:
    """
    Get current authenticated user from JWT token.
    
    This is a placeholder implementation. In production, you would:
    1. Decode and validate the JWT token
    2. Extract user ID from token claims
    3. Load user from database
    4. Return user dict
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    # Mock implementation - replace with real JWT validation
    # For testing, accept any token (even empty) and return admin user
    # This is ONLY for POC testing - remove in production!
    return {
        "sub": "bg54677",  # This should be soeid
        "soeid": "bg54677",
        "role_id": 1,  # Admin role
        "role_name": "admin"
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


