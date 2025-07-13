"""Centralized exception handling for consistent error responses."""

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse
from typing import Optional


class PermissionDeniedError(Exception):
    """Custom exception for permission denied scenarios."""
    
    def __init__(self, resource_type: str, permission_level: str, user_id: int):
        self.resource_type = resource_type
        self.permission_level = permission_level
        self.user_id = user_id
        super().__init__(f"Permission denied: {resource_type} {permission_level} access required")


async def permission_exception_handler(request: Request, exc: PermissionDeniedError) -> JSONResponse:
    """Handle PermissionDeniedError with consistent format."""
    return JSONResponse(
        status_code=403,
        content={
            "detail": str(exc),
            "error_type": "permission_denied",
            "resource": exc.resource_type,
            "required_permission": exc.permission_level
        }
    )


def permission_denied(resource_type: str, permission_level: str) -> HTTPException:
    """Create standardized permission denied exception."""
    return HTTPException(
        status_code=403,
        detail=f"Permission denied: {resource_type} {permission_level} access required"
    )


def unauthorized() -> HTTPException:
    """Create standardized unauthorized exception."""
    return HTTPException(
        status_code=401,
        detail="Authentication required"
    )


def resource_not_found(resource_type: str, resource_id: Optional[int] = None) -> HTTPException:
    """Create standardized resource not found exception."""
    detail = f"{resource_type} not found"
    if resource_id:
        detail = f"{resource_type} with id {resource_id} not found"
    
    return HTTPException(
        status_code=404,
        detail=detail
    )