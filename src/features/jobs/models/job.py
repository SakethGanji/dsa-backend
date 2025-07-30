"""Job domain models."""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional
from enum import Enum
from datetime import datetime
from uuid import UUID

from src.core.domain_exceptions import ValidationException, BusinessRuleViolation


class JobStatus(Enum):
    """Job execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobType(Enum):
    """Type of job."""
    IMPORT = "import"
    EXPORT = "export"
    SAMPLING = "sampling"
    EXPLORATION = "exploration"
    SQL_TRANSFORM = "sql_transform"
    ANALYSIS = "analysis"
    
    @classmethod
    def from_string(cls, value: str) -> 'JobType':
        """Convert string to JobType."""
        try:
            return cls(value.lower())
        except ValueError:
            raise ValidationException(f"Invalid job type: {value}", field="job_type")


@dataclass
class JobParameters:
    """Value object for job parameters."""
    dataset_id: int
    user_id: int
    job_type: JobType
    parameters: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate job parameters."""
        if self.dataset_id <= 0:
            raise ValidationException("Invalid dataset ID", field="dataset_id")
        if self.user_id <= 0:
            raise ValidationException("Invalid user ID", field="user_id")
        
        # Validate type-specific parameters
        self._validate_type_specific_params()
    
    def _validate_type_specific_params(self) -> None:
        """Validate parameters based on job type."""
        if self.job_type == JobType.IMPORT:
            if 'file_path' not in self.parameters:
                raise ValidationException(
                    "Import job requires 'file_path' parameter",
                    field="parameters"
                )
            if 'branch_name' not in self.parameters:
                self.parameters['branch_name'] = 'main'
                
        elif self.job_type == JobType.SAMPLING:
            if 'sample_size' not in self.parameters:
                self.parameters['sample_size'] = 1000
            if 'method' not in self.parameters:
                self.parameters['method'] = 'random'
                
        elif self.job_type == JobType.EXPLORATION:
            if 'table_key' not in self.parameters:
                self.parameters['table_key'] = 'primary'
                
        elif self.job_type == JobType.SQL_TRANSFORM:
            if 'sql_query' not in self.parameters:
                raise ValidationException(
                    "SQL transform job requires 'sql_query' parameter",
                    field="parameters"
                )


@dataclass
class JobResult:
    """Value object for job execution result."""
    output_summary: Dict[str, Any] = field(default_factory=dict)
    error_message: Optional[str] = None
    execution_time_ms: Optional[int] = None
    rows_processed: Optional[int] = None
    
    def is_successful(self) -> bool:
        """Check if job completed successfully."""
        return self.error_message is None
    
    def add_output(self, key: str, value: Any) -> None:
        """Add output to the summary."""
        self.output_summary[key] = value
    
    def set_error(self, error_message: str) -> None:
        """Set error message for failed job."""
        self.error_message = error_message[:1000]  # Limit error message length


@dataclass
class Job:
    """Job aggregate root entity."""
    id: UUID
    parameters: JobParameters
    status: JobStatus = JobStatus.PENDING
    result: Optional[JobResult] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    source_commit_id: Optional[str] = None
    output_commit_id: Optional[str] = None
    progress_percentage: int = 0
    cancelled_by: Optional[int] = None
    
    def can_be_cancelled(self) -> bool:
        """Check if job can be cancelled."""
        return self.status in [JobStatus.PENDING, JobStatus.RUNNING]
    
    def cancel(self, user_id: int) -> None:
        """Cancel the job."""
        if not self.can_be_cancelled():
            raise BusinessRuleViolation(
                f"Cannot cancel job in {self.status.value} status",
                rule="job_must_be_cancellable"
            )
        
        self.status = JobStatus.CANCELLED
        self.cancelled_by = user_id
        self.completed_at = datetime.utcnow()
        
        if not self.result:
            self.result = JobResult()
        self.result.set_error("Job cancelled by user")
    
    def start(self) -> None:
        """Mark job as started."""
        if self.status != JobStatus.PENDING:
            raise BusinessRuleViolation(
                f"Cannot start job in {self.status.value} status",
                rule="job_must_be_pending"
            )
        
        self.status = JobStatus.RUNNING
        self.started_at = datetime.utcnow()
    
    def complete(self, result: JobResult, output_commit_id: Optional[str] = None) -> None:
        """Mark job as completed."""
        if self.status != JobStatus.RUNNING:
            raise BusinessRuleViolation(
                f"Cannot complete job in {self.status.value} status",
                rule="job_must_be_running"
            )
        
        self.status = JobStatus.COMPLETED
        self.completed_at = datetime.utcnow()
        self.result = result
        self.output_commit_id = output_commit_id
        self.progress_percentage = 100
        
        # Calculate execution time
        if self.started_at:
            execution_time = (self.completed_at - self.started_at).total_seconds() * 1000
            self.result.execution_time_ms = int(execution_time)
    
    def fail(self, error_message: str) -> None:
        """Mark job as failed."""
        if self.status not in [JobStatus.PENDING, JobStatus.RUNNING]:
            raise BusinessRuleViolation(
                f"Cannot fail job in {self.status.value} status",
                rule="job_must_be_active"
            )
        
        self.status = JobStatus.FAILED
        self.completed_at = datetime.utcnow()
        
        if not self.result:
            self.result = JobResult()
        self.result.set_error(error_message)
        
        # Calculate execution time if started
        if self.started_at:
            execution_time = (self.completed_at - self.started_at).total_seconds() * 1000
            self.result.execution_time_ms = int(execution_time)
    
    def update_progress(self, percentage: int) -> None:
        """Update job progress percentage."""
        if self.status != JobStatus.RUNNING:
            raise BusinessRuleViolation(
                "Can only update progress for running jobs",
                rule="job_must_be_running"
            )
        
        if percentage < 0 or percentage > 100:
            raise ValidationException(
                "Progress percentage must be between 0 and 100",
                field="progress_percentage"
            )
        
        self.progress_percentage = percentage
    
    def get_duration_seconds(self) -> Optional[float]:
        """Get job duration in seconds."""
        if not self.started_at:
            return None
        
        end_time = self.completed_at or datetime.utcnow()
        return (end_time - self.started_at).total_seconds()
    
    def is_active(self) -> bool:
        """Check if job is actively running."""
        return self.status == JobStatus.RUNNING
    
    def is_complete(self) -> bool:
        """Check if job has completed (successfully or not)."""
        return self.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]
    
    def was_successful(self) -> bool:
        """Check if job completed successfully."""
        return self.status == JobStatus.COMPLETED and self.result and self.result.is_successful()