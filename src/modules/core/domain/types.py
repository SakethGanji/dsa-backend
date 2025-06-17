from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import NewType

from .base import ValueObject


# Type aliases
UserId = NewType('UserId', int)


@dataclass(frozen=True)
class ContentHash(ValueObject):
    """Value object for content hash (SHA256)."""
    value: str
    
    def __post_init__(self):
        if not self.value or len(self.value) != 64:
            raise ValueError("Content hash must be a 64-character SHA256 hash")
        if not all(c in '0123456789abcdef' for c in self.value.lower()):
            raise ValueError("Content hash must be a valid hexadecimal string")
    
    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True)
class FilePath(ValueObject):
    """Value object for file paths."""
    value: str
    
    def __post_init__(self):
        if not self.value:
            raise ValueError("File path cannot be empty")
    
    @property
    def path(self) -> Path:
        return Path(self.value)
    
    @property
    def name(self) -> str:
        return self.path.name
    
    @property
    def suffix(self) -> str:
        return self.path.suffix
    
    def __str__(self) -> str:
        return self.value


class Permission(str, Enum):
    """Permission types."""
    READ = "read"
    WRITE = "write"
    ADMIN = "admin"


class FileType(str, Enum):
    """File types supported by the system."""
    CSV = "csv"
    PARQUET = "parquet"
    EXCEL = "excel"
    JSON = "json"
    TEXT = "text"
    BINARY = "binary"


class StorageType(str, Enum):
    """Storage backend types."""
    LOCAL = "local"
    S3 = "s3"
    AZURE = "azure"
    GCS = "gcs"


class DatasetStatus(str, Enum):
    """Dataset status."""
    DRAFT = "draft"
    ACTIVE = "active"
    ARCHIVED = "archived"
    DELETED = "deleted"


class VersionStatus(str, Enum):
    """Version status."""
    CREATING = "creating"
    READY = "ready"
    FAILED = "failed"
    ARCHIVED = "archived"


class SamplingMethod(str, Enum):
    """Sampling methods."""
    RANDOM = "random"
    STRATIFIED = "stratified"
    SYSTEMATIC = "systematic"
    CLUSTER = "cluster"


class CompressionType(str, Enum):
    """Compression types."""
    NONE = "none"
    GZIP = "gzip"
    SNAPPY = "snappy"
    LZ4 = "lz4"
    ZSTD = "zstd"