"""Core constants and enums used across the system."""

from enum import Enum


class PermissionLevel:
    """Permission level constants."""
    READ = "read"
    WRITE = "write"
    ADMIN = "admin"
    OWNER = "owner"


class JobStatus:
    """Job status constants."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ImportStatus:
    """Import status constants."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class PermissionType(str, Enum):
    """Permission types for datasets."""
    READ = "read"
    WRITE = "write"
    ADMIN = "admin"
    OWNER = "owner"