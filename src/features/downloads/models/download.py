"""Domain entity for download operations."""
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any, List, Set
from enum import Enum

from src.core.domain_exceptions import BusinessRuleViolation


class ExportFormat(Enum):
    """Supported export formats."""
    CSV = "csv"
    EXCEL = "excel"
    JSON = "json"
    PARQUET = "parquet"


class DownloadStatus(Enum):
    """Status of a download operation."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    EXPIRED = "expired"


@dataclass
class DownloadOptions:
    """Options for customizing downloads."""
    include_headers: bool = True
    compression: Optional[str] = None
    filters: Optional[Dict[str, Any]] = None
    columns: Optional[List[str]] = None
    batch_size: int = 10000
    
    def validate(self) -> None:
        """Validate download options."""
        if self.compression and self.compression not in ['gzip', 'zip', 'bz2']:
            raise BusinessRuleViolation(
                f"Unsupported compression format: {self.compression}",
                rule="valid_compression"
            )
        
        if self.batch_size < 100 or self.batch_size > 100000:
            raise BusinessRuleViolation(
                "Batch size must be between 100 and 100,000",
                rule="valid_batch_size"
            )


@dataclass
class Download:
    """Domain entity representing a data download operation."""
    id: str
    dataset_id: str
    format: ExportFormat
    requested_by: str
    requested_at: datetime
    status: DownloadStatus
    options: DownloadOptions
    file_path: Optional[str] = None
    file_size: Optional[int] = None
    row_count: Optional[int] = None
    completed_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    error_message: Optional[str] = None
    
    def __post_init__(self):
        """Validate download on creation."""
        self.validate_format()
        self.options.validate()
    
    def validate_format(self) -> None:
        """Validate the requested format is supported."""
        if not isinstance(self.format, ExportFormat):
            raise BusinessRuleViolation(
                f"Invalid format: {self.format}",
                rule="supported_format_required"
            )
    
    def can_download(self, user_permissions: Set[str]) -> bool:
        """Check if user has permission to download."""
        return "read" in user_permissions or "download" in user_permissions
    
    def start_processing(self) -> None:
        """Start processing the download."""
        if self.status != DownloadStatus.PENDING:
            raise BusinessRuleViolation(
                f"Cannot start processing download in {self.status.value} status",
                rule="pending_status_required"
            )
        self.status = DownloadStatus.PROCESSING
    
    def mark_completed(self, file_path: str, file_size: int, row_count: int) -> None:
        """Mark download as completed."""
        if self.status != DownloadStatus.PROCESSING:
            raise BusinessRuleViolation(
                f"Cannot complete download in {self.status.value} status",
                rule="processing_status_required"
            )
        
        self.status = DownloadStatus.COMPLETED
        self.file_path = file_path
        self.file_size = file_size
        self.row_count = row_count
        self.completed_at = datetime.utcnow()
        
        # Set expiration (24 hours from completion)
        from datetime import timedelta
        self.expires_at = self.completed_at + timedelta(hours=24)
    
    def mark_failed(self, error: str) -> None:
        """Mark download as failed."""
        if self.status not in [DownloadStatus.PENDING, DownloadStatus.PROCESSING]:
            raise BusinessRuleViolation(
                f"Cannot fail download in {self.status.value} status",
                rule="valid_fail_status"
            )
        
        self.status = DownloadStatus.FAILED
        self.error_message = error
        self.completed_at = datetime.utcnow()
    
    def mark_expired(self) -> None:
        """Mark download as expired."""
        if self.status != DownloadStatus.COMPLETED:
            raise BusinessRuleViolation(
                "Only completed downloads can expire",
                rule="completed_status_required"
            )
        self.status = DownloadStatus.EXPIRED
    
    def is_expired(self) -> bool:
        """Check if the download has expired."""
        if self.status == DownloadStatus.EXPIRED:
            return True
        
        if self.expires_at and datetime.utcnow() > self.expires_at:
            return True
        
        return False
    
    def get_content_type(self) -> str:
        """Get the MIME content type for the download format."""
        content_types = {
            ExportFormat.CSV: "text/csv",
            ExportFormat.EXCEL: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ExportFormat.JSON: "application/json",
            ExportFormat.PARQUET: "application/octet-stream"
        }
        return content_types.get(self.format, "application/octet-stream")
    
    def get_filename(self, dataset_name: str) -> str:
        """Generate filename for the download."""
        timestamp = self.requested_at.strftime("%Y%m%d_%H%M%S")
        return f"{dataset_name}_{timestamp}.{self.format.value}"
    
    def get_summary(self) -> str:
        """Get a summary of the download."""
        if self.status == DownloadStatus.COMPLETED:
            size_mb = self.file_size / (1024 * 1024) if self.file_size else 0
            return (
                f"Downloaded {self.row_count:,} rows as {self.format.value.upper()} "
                f"({size_mb:.1f} MB)"
            )
        elif self.status == DownloadStatus.FAILED:
            return f"Download failed: {self.error_message}"
        else:
            return f"Download is {self.status.value}"