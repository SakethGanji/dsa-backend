"""Sampling domain models."""

from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional
from enum import Enum
from datetime import datetime
from uuid import UUID

from src.core.domain_exceptions import ValidationException, BusinessRuleViolation


class SamplingMethod(Enum):
    """Sampling method enumeration."""
    RANDOM = "random"
    STRATIFIED = "stratified"
    SYSTEMATIC = "systematic"
    CLUSTER = "cluster"
    RESERVOIR = "reservoir"
    
    @classmethod
    def from_string(cls, value: str) -> 'SamplingMethod':
        """Convert string to SamplingMethod."""
        try:
            return cls(value.lower())
        except ValueError:
            raise ValidationException(
                f"Invalid sampling method: {value}. Valid methods are: {[m.value for m in cls]}",
                field="sampling_method"
            )
    
    def get_description(self) -> str:
        """Get description of sampling method."""
        descriptions = {
            SamplingMethod.RANDOM: "Random sampling - each row has equal probability of selection",
            SamplingMethod.STRATIFIED: "Stratified sampling - samples proportionally from groups",
            SamplingMethod.SYSTEMATIC: "Systematic sampling - selects every nth row",
            SamplingMethod.CLUSTER: "Cluster sampling - randomly selects groups of rows",
            SamplingMethod.RESERVOIR: "Reservoir sampling - memory-efficient sampling for large datasets"
        }
        return descriptions.get(self, "Unknown sampling method")


@dataclass
class SamplingConfiguration:
    """Value object for sampling configuration."""
    method: SamplingMethod
    sample_size: int
    parameters: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate sampling configuration."""
        if self.sample_size <= 0:
            raise ValidationException(
                "Sample size must be positive",
                field="sample_size"
            )
        
        if self.sample_size > 1000000:
            raise ValidationException(
                "Sample size cannot exceed 1,000,000 rows",
                field="sample_size"
            )
        
        # Validate method-specific parameters
        self._validate_method_parameters()
    
    def _validate_method_parameters(self) -> None:
        """Validate parameters based on sampling method."""
        if self.method == SamplingMethod.STRATIFIED:
            if 'stratify_column' not in self.parameters:
                raise ValidationException(
                    "Stratified sampling requires 'stratify_column' parameter",
                    field="parameters"
                )
                
        elif self.method == SamplingMethod.SYSTEMATIC:
            if 'interval' in self.parameters:
                interval = self.parameters['interval']
                if not isinstance(interval, int) or interval <= 0:
                    raise ValidationException(
                        "Systematic sampling interval must be a positive integer",
                        field="parameters.interval"
                    )
                    
        elif self.method == SamplingMethod.CLUSTER:
            if 'cluster_column' not in self.parameters:
                raise ValidationException(
                    "Cluster sampling requires 'cluster_column' parameter",
                    field="parameters"
                )
            if 'cluster_count' in self.parameters:
                count = self.parameters['cluster_count']
                if not isinstance(count, int) or count <= 0:
                    raise ValidationException(
                        "Cluster count must be a positive integer",
                        field="parameters.cluster_count"
                    )
    
    def get_seed(self) -> Optional[int]:
        """Get random seed for reproducible sampling."""
        return self.parameters.get('seed')
    
    def is_deterministic(self) -> bool:
        """Check if sampling will produce deterministic results."""
        return 'seed' in self.parameters and self.parameters['seed'] is not None


@dataclass
class SampleResult:
    """Value object for sampling result."""
    total_rows_scanned: int
    rows_sampled: int
    sample_data: List[Dict[str, Any]]
    execution_time_ms: int
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate sample result."""
        if self.total_rows_scanned < 0:
            raise ValidationException(
                "Total rows scanned cannot be negative",
                field="total_rows_scanned"
            )
        
        if self.rows_sampled < 0:
            raise ValidationException(
                "Rows sampled cannot be negative",
                field="rows_sampled"
            )
        
        if self.rows_sampled > self.total_rows_scanned:
            raise ValidationException(
                "Rows sampled cannot exceed total rows scanned",
                field="rows_sampled"
            )
        
        if len(self.sample_data) != self.rows_sampled:
            raise ValidationException(
                f"Sample data length ({len(self.sample_data)}) doesn't match rows_sampled ({self.rows_sampled})",
                field="sample_data"
            )
    
    def get_sampling_ratio(self) -> float:
        """Get the sampling ratio."""
        if self.total_rows_scanned == 0:
            return 0.0
        return self.rows_sampled / self.total_rows_scanned


@dataclass
class SamplingJob:
    """Sampling job entity."""
    id: UUID
    dataset_id: int
    user_id: int
    ref_name: str
    table_key: str
    configuration: SamplingConfiguration
    commit_id: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    result: Optional[SampleResult] = None
    
    def is_complete(self) -> bool:
        """Check if sampling job is complete."""
        return self.result is not None
    
    def set_result(self, result: SampleResult) -> None:
        """Set the sampling result."""
        if self.result is not None:
            raise BusinessRuleViolation(
                "Sampling result has already been set",
                rule="immutable_result"
            )
        self.result = result
    
    def get_output_summary(self) -> Dict[str, Any]:
        """Get summary of sampling job output."""
        if not self.result:
            return {
                "status": "pending",
                "created_at": self.created_at.isoformat()
            }
        
        return {
            "status": "completed",
            "created_at": self.created_at.isoformat(),
            "method": self.configuration.method.value,
            "requested_sample_size": self.configuration.sample_size,
            "actual_sample_size": self.result.rows_sampled,
            "total_rows": self.result.total_rows_scanned,
            "sampling_ratio": self.result.get_sampling_ratio(),
            "execution_time_ms": self.result.execution_time_ms,
            "metadata": self.result.metadata
        }