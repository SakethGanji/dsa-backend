from typing import Any, Dict, Optional


class DomainException(Exception):
    """Base exception for domain errors."""
    
    def __init__(self, message: str, code: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.code = code or self.__class__.__name__
        self.details = details or {}


class ValidationError(DomainException):
    """Raised when validation fails."""
    pass


class NotFoundError(DomainException):
    """Raised when an entity is not found."""
    
    def __init__(self, entity_type: str, entity_id: Any):
        super().__init__(
            f"{entity_type} with id {entity_id} not found",
            code="NOT_FOUND",
            details={"entity_type": entity_type, "entity_id": entity_id}
        )
        self.entity_type = entity_type
        self.entity_id = entity_id


class DuplicateError(DomainException):
    """Raised when trying to create a duplicate entity."""
    
    def __init__(self, entity_type: str, field: str, value: Any):
        super().__init__(
            f"{entity_type} with {field}='{value}' already exists",
            code="DUPLICATE",
            details={"entity_type": entity_type, "field": field, "value": value}
        )


class PermissionDeniedError(DomainException):
    """Raised when permission is denied."""
    
    def __init__(self, resource_type: str, resource_id: Any, required_permission: str):
        super().__init__(
            f"Permission denied: {required_permission} on {resource_type} {resource_id}",
            code="PERMISSION_DENIED",
            details={
                "resource_type": resource_type,
                "resource_id": resource_id,
                "required_permission": required_permission
            }
        )


class InvalidStateError(DomainException):
    """Raised when an operation is invalid for the current state."""
    
    def __init__(self, entity_type: str, current_state: str, operation: str):
        super().__init__(
            f"Cannot {operation} {entity_type} in state '{current_state}'",
            code="INVALID_STATE",
            details={
                "entity_type": entity_type,
                "current_state": current_state,
                "operation": operation
            }
        )


class ConcurrencyError(DomainException):
    """Raised when a concurrency conflict occurs."""
    
    def __init__(self, entity_type: str, entity_id: Any):
        super().__init__(
            f"Concurrency conflict updating {entity_type} {entity_id}",
            code="CONCURRENCY_CONFLICT",
            details={"entity_type": entity_type, "entity_id": entity_id}
        )