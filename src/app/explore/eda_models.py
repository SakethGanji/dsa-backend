from typing import Dict, List, Optional, Any, Union, Literal
from pydantic import BaseModel, Field
from enum import Enum
from datetime import datetime


class RenderType(str, Enum):
    """Enumeration of supported visualization/display types."""
    KEY_VALUE_PAIRS = "KEY_VALUE_PAIRS"
    TABLE = "TABLE"
    HISTOGRAM = "HISTOGRAM"
    BAR_CHART = "BAR_CHART"
    HEATMAP = "HEATMAP"
    SCATTER_PLOT = "SCATTER_PLOT"
    BOX_PLOT = "BOX_PLOT"
    DENDROGRAM = "DENDROGRAM"
    ALERT_LIST = "ALERT_LIST"
    MARKDOWN = "MARKDOWN"
    TEXT_BLOCK = "TEXT_BLOCK"
    MATRIX = "MATRIX"


class AnalysisBlock(BaseModel):
    """Self-describing analysis block for frontend rendering."""
    title: str = Field(..., description="Human-readable title for the analysis")
    render_as: RenderType = Field(..., description="Explicit instruction for UI rendering")
    data: Any = Field(..., description="The payload data structured for the render type")
    description: Optional[str] = Field(None, description="Optional tooltip or helper text")


class VariableType(str, Enum):
    """Data type categories for variables."""
    NUMERIC = "NUMERIC"
    CATEGORICAL = "CATEGORICAL"
    DATETIME = "DATETIME"
    TEXT = "TEXT"
    BOOLEAN = "BOOLEAN"
    UNKNOWN = "UNKNOWN"


class VariableInfo(BaseModel):
    """Common information about a variable/column."""
    name: str
    type: VariableType
    dtype: str = Field(..., description="DuckDB data type")
    
    
class VariableAnalysis(BaseModel):
    """Complete analysis for a single variable."""
    common_info: VariableInfo
    analyses: List[AnalysisBlock]


class AlertSeverity(str, Enum):
    """Severity levels for data quality alerts."""
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


class Alert(BaseModel):
    """Data quality alert."""
    column: Optional[str] = Field(None, description="Column name if alert is column-specific")
    alert_type: str = Field(..., description="Type of alert (e.g., 'high_cardinality', 'missing_values')")
    severity: AlertSeverity
    message: str
    details: Optional[Dict[str, Any]] = None


class EDAMetadata(BaseModel):
    """Metadata about the EDA analysis run."""
    dataset_id: int
    version_id: int
    analysis_timestamp: datetime
    sample_size_used: Optional[int] = Field(None, description="Number of rows sampled if sampling was used")
    total_rows: int
    total_columns: int
    analysis_duration_seconds: Optional[float] = None


class VariableConfig(BaseModel):
    """Configuration for variable-level analysis."""
    enabled: bool = True
    limit: int = Field(50, description="Maximum number of variables to analyze")
    types: List[Literal["numeric", "categorical", "datetime", "text"]] = Field(
        default=["numeric", "categorical", "datetime", "text"],
        description="Variable types to include in analysis"
    )


class InteractionConfig(BaseModel):
    """Configuration for interaction/bivariate analysis."""
    enabled: bool = True
    correlation_threshold: float = Field(0.5, ge=0, le=1, description="Minimum correlation to include")
    max_pairs: int = Field(20, description="Maximum number of variable pairs to analyze")


class AlertConfig(BaseModel):
    """Configuration for alert thresholds."""
    enabled: bool = True
    high_correlation_threshold: float = Field(0.9, ge=0, le=1, description="Threshold for high correlation alerts")
    high_cardinality_threshold: float = Field(0.9, ge=0, le=1, description="Threshold for high cardinality alerts (ratio)")
    high_missing_threshold: float = Field(20.0, ge=0, le=100, description="Threshold for high missing values alerts (%)")
    error_missing_threshold: float = Field(50.0, ge=0, le=100, description="Threshold for error-level missing values alerts (%)")
    high_zeros_threshold: float = Field(90.0, ge=0, le=100, description="Threshold for high zeros alerts (%)")
    high_skewness_threshold: float = Field(5.0, ge=0, description="Threshold for high skewness alerts (absolute value)")
    nearly_constant_threshold: float = Field(0.01, ge=0, le=1, description="Threshold for nearly constant alerts (distinct ratio)")
    duplicate_rows_threshold: float = Field(10.0, ge=0, le=100, description="Threshold for duplicate rows alerts (%)")
    frequency_table_limit: int = Field(20, ge=1, description="Maximum number of rows in frequency tables")


class AnalysisConfig(BaseModel):
    """Configuration for EDA analysis."""
    global_summary: bool = True
    variables: VariableConfig = Field(default_factory=VariableConfig)
    interactions: InteractionConfig = Field(default_factory=InteractionConfig)
    missing_values: bool = True
    alerts: AlertConfig = Field(default_factory=AlertConfig)
    sample_size: Optional[int] = Field(
        None, 
        description="Number of rows to sample for analysis. None means use full dataset"
    )


class EDARequest(BaseModel):
    """Request body for EDA analysis."""
    analysis_config: AnalysisConfig = Field(default_factory=AnalysisConfig)


class EDAResponse(BaseModel):
    """Complete EDA analysis response."""
    metadata: EDAMetadata
    global_summary: List[AnalysisBlock]
    variables: Dict[str, VariableAnalysis]
    interactions: List[AnalysisBlock]
    alerts: List[AnalysisBlock]


# Data structures for specific render types

class KeyValueData(BaseModel):
    """Data structure for KEY_VALUE_PAIRS render type."""
    items: Dict[str, Union[str, int, float, bool, None]]


class TableData(BaseModel):
    """Data structure for TABLE render type."""
    columns: List[str]
    rows: List[List[Any]]


class HistogramBin(BaseModel):
    """Single bin in a histogram."""
    min: float
    max: float
    count: int


class HistogramData(BaseModel):
    """Data structure for HISTOGRAM render type."""
    bins: List[HistogramBin]
    total_count: int


class BarChartData(BaseModel):
    """Data structure for BAR_CHART render type."""
    categories: List[str]
    values: List[float]
    labels: Optional[Dict[str, str]] = None


class HeatmapData(BaseModel):
    """Data structure for HEATMAP render type."""
    row_labels: List[str]
    col_labels: List[str]
    values: List[List[float]]
    min_value: float
    max_value: float


class ScatterPlotData(BaseModel):
    """Data structure for SCATTER_PLOT render type."""
    x_label: str
    y_label: str
    points: List[Dict[str, float]]
    sample_info: Optional[Dict[str, Any]] = None


class BoxPlotData(BaseModel):
    """Data structure for BOX_PLOT render type."""
    categories: List[str]
    data: List[Dict[str, float]]  # Each dict has: min, q1, median, q3, max, outliers


class MatrixData(BaseModel):
    """Data structure for MATRIX render type (missing values matrix)."""
    columns: List[str]
    rows: List[List[bool]]  # True indicates missing value
    row_indices: List[int]  # Original row indices if sampled