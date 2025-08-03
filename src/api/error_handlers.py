"""API error handlers for converting domain exceptions to HTTP responses."""

from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from src.core.domain_exceptions import DomainException
import logging
import uuid

logger = logging.getLogger(__name__)


async def domain_exception_handler(request: Request, exc: DomainException):
    """
    Convert domain exceptions to HTTP responses.
    
    Args:
        request: The FastAPI request
        exc: The domain exception
        
    Returns:
        JSON response with error details
    """
    # Generate request ID if not present
    request_id = getattr(request.state, 'request_id', None)
    if not request_id:
        request_id = str(uuid.uuid4())
    
    return JSONResponse(
        status_code=exc.http_status,
        content={
            "error": exc.error_code,
            "message": str(exc),
            "details": exc.details,
            "request_id": request_id
        }
    )


async def value_error_handler(request: Request, exc: ValueError):
    """
    Handle ValueError as validation errors.
    
    Args:
        request: The FastAPI request
        exc: The ValueError
        
    Returns:
        JSON response with validation error
    """
    request_id = getattr(request.state, 'request_id', str(uuid.uuid4()))
    
    return JSONResponse(
        status_code=400,
        content={
            "error": "VALIDATION_ERROR",
            "message": str(exc),
            "request_id": request_id
        }
    )


async def permission_error_handler(request: Request, exc: PermissionError):
    """
    Handle PermissionError as forbidden errors.
    
    Args:
        request: The FastAPI request
        exc: The PermissionError
        
    Returns:
        JSON response with forbidden error
    """
    request_id = getattr(request.state, 'request_id', str(uuid.uuid4()))
    
    return JSONResponse(
        status_code=403,
        content={
            "error": "FORBIDDEN",
            "message": str(exc),
            "request_id": request_id
        }
    )


async def request_validation_error_handler(request: Request, exc: RequestValidationError):
    """
    Handle Pydantic request validation errors.
    
    Args:
        request: The FastAPI request
        exc: The RequestValidationError
        
    Returns:
        JSON response with validation error details
    """
    request_id = getattr(request.state, 'request_id', str(uuid.uuid4()))
    
    # Extract the first validation error for a cleaner message
    errors = exc.errors()
    if errors:
        first_error = errors[0]
        field_path = " -> ".join(str(loc) for loc in first_error['loc'])
        message = f"{first_error['msg']} (field: {field_path})"
    else:
        message = "Request validation failed"
    
    return JSONResponse(
        status_code=422,
        content={
            "error": "VALIDATION_ERROR",
            "message": message,
            "details": {"validation_errors": errors},
            "request_id": request_id
        }
    )


async def generic_exception_handler(request: Request, exc: Exception):
    """
    Handle unexpected exceptions.
    
    Args:
        request: The FastAPI request
        exc: The exception
        
    Returns:
        JSON response with generic error
    """
    request_id = getattr(request.state, 'request_id', str(uuid.uuid4()))
    
    # Log the full exception
    logger.error(
        f"Unexpected error handling request {request_id}: {exc}",
        exc_info=True,
        extra={
            "request_id": request_id,
            "path": request.url.path,
            "method": request.method
        }
    )
    
    return JSONResponse(
        status_code=500,
        content={
            "error": "INTERNAL_ERROR",
            "message": "An unexpected error occurred",
            "request_id": request_id
        }
    )


def register_error_handlers(app):
    """
    Register all error handlers with the FastAPI app.
    
    Args:
        app: The FastAPI application instance
    """
    # FastAPI/Pydantic exceptions
    app.add_exception_handler(RequestValidationError, request_validation_error_handler)
    
    # Domain exceptions
    app.add_exception_handler(DomainException, domain_exception_handler)
    
    # Common Python exceptions
    app.add_exception_handler(ValueError, value_error_handler)
    app.add_exception_handler(PermissionError, permission_error_handler)
    
    # Generic exception handler (catches all)
    app.add_exception_handler(Exception, generic_exception_handler)
    
    logger.info("Error handlers registered successfully")