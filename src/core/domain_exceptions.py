"""Domain exceptions hierarchy for consistent error handling."""

from typing import Any, Optional, Dict
from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse


class DomainException(Exception):
    """Base domain exception."""
    http_status: int = 500
    error_code: str = "INTERNAL_ERROR"
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        """
        Initialize domain exception.
        
        Args:
            message: Error message
            details: Additional error details
        """
        super().__init__(message)
        self.message = message
        self.details = details or {}
    
    def to_http_exception(self) -> HTTPException:
        """Convert to HTTP exception for API layer."""
        return HTTPException(
            status_code=self.http_status,
            detail={
                "error": self.error_code,
                "message": self.message,
                "details": self.details
            }
        )


class EntityNotFoundException(DomainException):
    """Entity not found exception."""
    http_status = 404
    error_code = "NOT_FOUND"
    
    def __init__(self, entity_type: str, entity_id: Any, details: Optional[Dict[str, Any]] = None):
        """
        Initialize entity not found exception.
        
        Args:
            entity_type: Type of entity (e.g., "Dataset", "User")
            entity_id: ID of the entity
            details: Additional details
        """
        super().__init__(f"{entity_type} {entity_id} not found", details)
        self.entity_type = entity_type
        self.entity_id = entity_id


class ValidationException(DomainException):
    """Validation error exception."""
    http_status = 400
    error_code = "VALIDATION_ERROR"
    
    def __init__(self, message: str, field: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        """
        Initialize validation exception.
        
        Args:
            message: Validation error message
            field: Field that failed validation
            details: Additional validation details
        """
        super().__init__(message, details)
        self.field = field
        if field and 'field' not in self.details:
            self.details['field'] = field


class UnauthorizedException(DomainException):
    """Unauthorized access exception."""
    http_status = 401
    error_code = "UNAUTHORIZED"
    
    def __init__(self, message: str = "Unauthorized access", details: Optional[Dict[str, Any]] = None):
        """Initialize unauthorized exception."""
        super().__init__(message, details)


class ForbiddenException(DomainException):
    """Forbidden access exception."""
    http_status = 403
    error_code = "FORBIDDEN"
    
    def __init__(
        self, 
        message: str = "Access forbidden", 
        resource_type: Optional[str] = None,
        resource_id: Optional[Any] = None,
        required_permission: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize forbidden exception.
        
        Args:
            message: Error message
            resource_type: Type of resource access was denied to
            resource_id: ID of the resource
            required_permission: Permission that was required
            details: Additional details
        """
        super().__init__(message, details)
        self.resource_type = resource_type
        self.resource_id = resource_id
        self.required_permission = required_permission
        
        # Add to details if not already present
        if resource_type and 'resource_type' not in self.details:
            self.details['resource_type'] = resource_type
        if resource_id and 'resource_id' not in self.details:
            self.details['resource_id'] = resource_id
        if required_permission and 'required_permission' not in self.details:
            self.details['required_permission'] = required_permission


class ConflictException(DomainException):
    """Resource conflict exception."""
    http_status = 409
    error_code = "CONFLICT"
    
    def __init__(
        self, 
        message: str, 
        conflicting_field: Optional[str] = None,
        existing_value: Optional[Any] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize conflict exception.
        
        Args:
            message: Error message
            conflicting_field: Field causing the conflict
            existing_value: Existing value that conflicts
            details: Additional details
        """
        super().__init__(message, details)
        self.conflicting_field = conflicting_field
        self.existing_value = existing_value
        
        if conflicting_field and 'conflicting_field' not in self.details:
            self.details['conflicting_field'] = conflicting_field
        if existing_value is not None and 'existing_value' not in self.details:
            self.details['existing_value'] = existing_value


class ResourceExhaustedException(DomainException):
    """Resource exhausted exception (e.g., rate limits, quotas)."""
    http_status = 429
    error_code = "RESOURCE_EXHAUSTED"
    
    def __init__(
        self, 
        message: str = "Resource exhausted",
        resource_type: Optional[str] = None,
        limit: Optional[int] = None,
        retry_after: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize resource exhausted exception.
        
        Args:
            message: Error message
            resource_type: Type of exhausted resource
            limit: The limit that was exceeded
            retry_after: Seconds until retry is allowed
            details: Additional details
        """
        super().__init__(message, details)
        self.resource_type = resource_type
        self.limit = limit
        self.retry_after = retry_after
        
        if resource_type:
            self.details['resource_type'] = resource_type
        if limit is not None:
            self.details['limit'] = limit
        if retry_after is not None:
            self.details['retry_after'] = retry_after


class BusinessRuleViolation(DomainException):
    """Business rule violation exception."""
    http_status = 422
    error_code = "BUSINESS_RULE_VIOLATION"
    
    def __init__(
        self, 
        message: str,
        rule: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize business rule violation.
        
        Args:
            message: Error message
            rule: Name or ID of the violated rule
            details: Additional details
        """
        super().__init__(message, details)
        self.rule = rule
        if rule and 'rule' not in self.details:
            self.details['rule'] = rule


class ExternalServiceException(DomainException):
    """External service error."""
    http_status = 502
    error_code = "EXTERNAL_SERVICE_ERROR"
    
    def __init__(
        self, 
        message: str,
        service: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize external service exception.
        
        Args:
            message: Error message
            service: Name of the external service
            details: Additional details
        """
        super().__init__(message, details)
        self.service = service
        if service and 'service' not in self.details:
            self.details['service'] = service


class ErrorMessages:
    """Common error messages as constants to avoid duplication."""
    
    # Entity not found messages
    DATASET_NOT_FOUND = "Dataset not found"
    USER_NOT_FOUND = "User not found"
    JOB_NOT_FOUND = "Job not found"
    COMMIT_NOT_FOUND = "Commit not found"
    BRANCH_NOT_FOUND = "Branch not found"
    
    # Permission messages
    PERMISSION_DENIED = "Permission denied"
    INVALID_CREDENTIALS = "Invalid credentials"
    AUTHENTICATION_REQUIRED = "Authentication required"
    INSUFFICIENT_PERMISSIONS = "Insufficient permissions"
    
    # Validation messages
    INVALID_INPUT = "Invalid input"
    REQUIRED_FIELD_MISSING = "Required field missing"
    INVALID_FORMAT = "Invalid format"
    VALUE_OUT_OF_RANGE = "Value out of range"
    
    # Conflict messages
    DUPLICATE_ENTRY = "Duplicate entry"
    RESOURCE_ALREADY_EXISTS = "Resource already exists"
    CONCURRENT_MODIFICATION = "Resource was modified by another process"
    
    # Business rule messages
    INVALID_STATE_TRANSITION = "Invalid state transition"
    QUOTA_EXCEEDED = "Quota exceeded"
    OPERATION_NOT_ALLOWED = "Operation not allowed"
    
    # System messages
    INTERNAL_ERROR = "An internal error occurred"
    SERVICE_UNAVAILABLE = "Service temporarily unavailable"
    DATABASE_ERROR = "Database operation failed"


def convert_to_domain_exception(exc: Exception) -> DomainException:
    """
    Convert common exceptions to domain exceptions.
    
    Args:
        exc: The exception to convert
        
    Returns:
        A domain exception
    """
    if isinstance(exc, DomainException):
        return exc
    elif isinstance(exc, ValueError):
        return ValidationException(str(exc))
    elif isinstance(exc, PermissionError):
        return ForbiddenException(str(exc))
    elif isinstance(exc, KeyError):
        return ValidationException(f"Missing required field: {exc}")
    elif isinstance(exc, TypeError):
        return ValidationException(f"Invalid type: {exc}")
    else:
        # For unexpected exceptions, wrap in base DomainException
        return DomainException(
            ErrorMessages.INTERNAL_ERROR,
            details={"original_error": str(exc), "error_type": type(exc).__name__}
        )


# Legacy compatibility - content from exceptions.py
class PermissionDeniedError(ForbiddenException):
    """Custom exception for permission denied scenarios - extends ForbiddenException for compatibility."""
    
    def __init__(self, resource_type: str, permission_level: str, user_id: int):
        super().__init__(
            message=f"Permission denied: {resource_type} {permission_level} access required",
            resource_type=resource_type,
            required_permission=permission_level
        )
        self.permission_level = permission_level
        self.user_id = user_id


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