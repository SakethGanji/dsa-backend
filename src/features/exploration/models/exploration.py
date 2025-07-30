"""Domain entity for exploration operations."""
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Any, Dict
from enum import Enum

from src.core.domain_exceptions import BusinessRuleViolation


class ExplorationStatus(Enum):
    """Status of an exploration operation."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class Insight:
    """A discovered insight from exploration."""
    type: str  # correlation, trend, outlier, pattern
    description: str
    confidence: float
    details: Dict[str, Any]


@dataclass
class Anomaly:
    """A detected anomaly in the data."""
    column_name: str
    anomaly_type: str
    severity: str  # low, medium, high
    affected_rows: int
    examples: List[Any]


@dataclass
class ExplorationResult:
    """Result of an exploration operation."""
    row_count: int
    column_count: int
    data_quality_score: float
    insights: List[Insight]
    anomalies: List[Anomaly]
    execution_time_ms: int
    generated_at: datetime


@dataclass
class Exploration:
    """Domain entity representing a data exploration operation."""
    id: str
    dataset_id: str
    query: str
    created_by: str
    created_at: datetime
    status: ExplorationStatus
    result: Optional[ExplorationResult] = None
    error_message: Optional[str] = None
    
    def __post_init__(self):
        """Validate exploration on creation."""
        self._validate_query()
    
    def _validate_query(self) -> None:
        """Validate the exploration query."""
        if not self.query or not self.query.strip():
            raise BusinessRuleViolation(
                "Exploration query cannot be empty",
                rule="valid_query_required"
            )
        
        # Check for dangerous operations
        dangerous_keywords = ['drop', 'delete', 'truncate', 'alter', 'update', 'insert']
        query_lower = self.query.lower()
        for keyword in dangerous_keywords:
            if keyword in query_lower.split():
                raise BusinessRuleViolation(
                    f"Dangerous operation '{keyword}' not allowed in exploration queries",
                    rule="safe_queries_only"
                )
    
    def execute(self) -> None:
        """Start execution of the exploration."""
        if self.status != ExplorationStatus.PENDING:
            raise BusinessRuleViolation(
                f"Cannot execute exploration in {self.status.value} status",
                rule="pending_status_required"
            )
        self.status = ExplorationStatus.RUNNING
    
    def complete(self, result: ExplorationResult) -> None:
        """Mark exploration as completed with results."""
        if self.status != ExplorationStatus.RUNNING:
            raise BusinessRuleViolation(
                f"Cannot complete exploration in {self.status.value} status",
                rule="running_status_required"
            )
        self.result = result
        self.status = ExplorationStatus.COMPLETED
    
    def fail(self, error: str) -> None:
        """Mark exploration as failed with error."""
        if self.status not in [ExplorationStatus.PENDING, ExplorationStatus.RUNNING]:
            raise BusinessRuleViolation(
                f"Cannot fail exploration in {self.status.value} status",
                rule="valid_fail_status"
            )
        self.status = ExplorationStatus.FAILED
        self.error_message = error
    
    def can_view_results(self, user_id: str) -> bool:
        """Check if a user can view the exploration results."""
        # For now, only the creator can view results
        # In production, this would check dataset permissions
        return user_id == self.created_by
    
    def get_summary(self) -> str:
        """Get a summary of the exploration."""
        if self.status == ExplorationStatus.COMPLETED and self.result:
            return (
                f"Explored {self.result.row_count:,} rows with "
                f"{self.result.column_count} columns. "
                f"Quality score: {self.result.data_quality_score:.1%}. "
                f"Found {len(self.result.insights)} insights and "
                f"{len(self.result.anomalies)} anomalies."
            )
        elif self.status == ExplorationStatus.FAILED:
            return f"Exploration failed: {self.error_message}"
        else:
            return f"Exploration is {self.status.value}"