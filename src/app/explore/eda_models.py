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
    # New render types for enhanced visualizations
    DONUT_CHART = "DONUT_CHART"
    PIE_CHART = "PIE_CHART"
    GAUGE_CHART = "GAUGE_CHART"
    PROGRESS_BAR = "PROGRESS_BAR"
    SPARKLINE = "SPARKLINE"
    MINI_BAR_CHART = "MINI_BAR_CHART"
    BULLET_CHART = "BULLET_CHART"
    VIOLIN_PLOT = "VIOLIN_PLOT"
    DENSITY_PLOT = "DENSITY_PLOT"
    QQ_PLOT = "QQ_PLOT"
    RANGE_PLOT = "RANGE_PLOT"
    TREEMAP = "TREEMAP"
    SUNBURST = "SUNBURST"
    HORIZONTAL_BAR_CHART = "HORIZONTAL_BAR_CHART"
    CALENDAR_HEATMAP = "CALENDAR_HEATMAP"
    LINE_CHART = "LINE_CHART"
    AREA_CHART = "AREA_CHART"
    STACKED_BAR_CHART = "STACKED_BAR_CHART"
    RISK_MATRIX = "RISK_MATRIX"
    RADAR_CHART = "RADAR_CHART"
    NETWORK_GRAPH = "NETWORK_GRAPH"
    CHORD_DIAGRAM = "CHORD_DIAGRAM"
    PARALLEL_COORDINATES = "PARALLEL_COORDINATES"


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
    text_avg_length_threshold: float = Field(50.0, ge=0, description="Average length threshold for text classification")
    text_max_length_threshold: float = Field(200.0, ge=0, description="Max length threshold for text classification")
    text_distinct_ratio_threshold: float = Field(0.5, ge=0, le=1, description="Distinct ratio threshold for text classification")


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
    data: List[Dict[str, Union[float, List[float]]]]  # Each dict has: min, q1, median, q3, max, outliers


class MatrixData(BaseModel):
    """Data structure for MATRIX render type (missing values matrix)."""
    columns: List[str]
    rows: List[List[bool]]  # True indicates missing value
    row_indices: List[int]  # Original row indices if sampled


# New data structures for enhanced render types

class DonutChartData(BaseModel):
    """Data structure for DONUT_CHART render type."""
    labels: List[str]
    values: List[float]
    center_text: Optional[str] = None  # Text to display in center


class PieChartData(BaseModel):
    """Data structure for PIE_CHART render type."""
    labels: List[str]
    values: List[float]
    show_percentages: bool = True


class GaugeChartData(BaseModel):
    """Data structure for GAUGE_CHART render type."""
    value: float
    min_value: float = 0
    max_value: float = 100
    thresholds: Optional[List[Dict[str, Any]]] = None  # e.g., [{"value": 70, "color": "yellow"}, {"value": 90, "color": "red"}]
    label: Optional[str] = None


class ProgressBarData(BaseModel):
    """Data structure for PROGRESS_BAR render type."""
    value: float
    max_value: float = 100
    label: Optional[str] = None
    color: Optional[str] = None
    show_percentage: bool = True


class SparklineData(BaseModel):
    """Data structure for SPARKLINE render type."""
    values: List[float]
    show_dots: bool = False
    show_area: bool = False


class MiniBarChartData(BaseModel):
    """Data structure for MINI_BAR_CHART render type."""
    values: List[float]
    labels: Optional[List[str]] = None
    max_bars: int = 10


class BulletChartData(BaseModel):
    """Data structure for BULLET_CHART render type."""
    value: float
    target: float
    ranges: List[Dict[str, Union[str, float]]]  # e.g., [{"min": 0, "max": 50, "label": "Poor"}, ...]
    label: Optional[str] = None


class ViolinPlotData(BaseModel):
    """Data structure for VIOLIN_PLOT render type."""
    categories: List[str]
    data: List[Dict[str, Any]]  # Each dict contains distribution data


class DensityPlotData(BaseModel):
    """Data structure for DENSITY_PLOT render type."""
    x_values: List[float]
    y_values: List[float]
    label: Optional[str] = None


class QQPlotData(BaseModel):
    """Data structure for QQ_PLOT render type."""
    theoretical_quantiles: List[float]
    sample_quantiles: List[float]
    reference_line: Optional[Dict[str, float]] = None  # slope and intercept


class RangePlotData(BaseModel):
    """Data structure for RANGE_PLOT render type."""
    categories: List[str]
    ranges: List[Dict[str, float]]  # Each dict has min, max, mean, median


class TreemapData(BaseModel):
    """Data structure for TREEMAP render type."""
    data: List[Dict[str, Any]]  # Hierarchical data with name, value, children


class SunburstData(BaseModel):
    """Data structure for SUNBURST render type."""
    data: List[Dict[str, Any]]  # Hierarchical data similar to treemap


class CalendarHeatmapData(BaseModel):
    """Data structure for CALENDAR_HEATMAP render type."""
    data: List[Dict[str, Any]]  # Each dict has date and value
    start_date: str
    end_date: str


class LineChartData(BaseModel):
    """Data structure for LINE_CHART render type."""
    x_values: List[Any]
    series: List[Dict[str, Any]]  # Each series has name and y_values


class AreaChartData(BaseModel):
    """Data structure for AREA_CHART render type."""
    x_values: List[Any]
    series: List[Dict[str, Any]]  # Each series has name and y_values
    stacked: bool = False


class StackedBarChartData(BaseModel):
    """Data structure for STACKED_BAR_CHART render type."""
    categories: List[str]
    series: List[Dict[str, Any]]  # Each series has name and values


class RiskMatrixData(BaseModel):
    """Data structure for RISK_MATRIX render type."""
    items: List[Dict[str, Any]]  # Each item has likelihood, impact, label
    likelihood_labels: List[str]
    impact_labels: List[str]


class RadarChartData(BaseModel):
    """Data structure for RADAR_CHART render type."""
    categories: List[str]
    series: List[Dict[str, Any]]  # Each series has name and values


class NetworkGraphData(BaseModel):
    """Data structure for NETWORK_GRAPH render type."""
    nodes: List[Dict[str, Any]]  # Each node has id, label, etc.
    edges: List[Dict[str, Any]]  # Each edge has source, target, weight


class ChordDiagramData(BaseModel):
    """Data structure for CHORD_DIAGRAM render type."""
    labels: List[str]
    matrix: List[List[float]]  # Connection matrix


class ParallelCoordinatesData(BaseModel):
    """Data structure for PARALLEL_COORDINATES render type."""
    dimensions: List[Dict[str, Any]]  # Each dimension has label, values
    data: List[Dict[str, Any]]  # Each row of data

