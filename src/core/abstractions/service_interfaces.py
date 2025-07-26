"""Service interfaces for domain services."""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Union, Tuple, Set, AsyncGenerator
from dataclasses import dataclass
from enum import Enum
import pandas as pd


# File Processing Service Interfaces
@dataclass
class TableData:
    """Represents data from a single table/sheet."""
    table_key: str  # 'primary' for single-table formats, sheet name for Excel
    dataframe: pd.DataFrame
    
    
@dataclass
class ParsedData:
    """Result of parsing a file containing one or more tables."""
    tables: List[TableData]
    file_type: str
    filename: str


class IFileParser(ABC):
    """Abstract interface for parsing different file formats."""
    
    @abstractmethod
    def can_parse(self, filename: str) -> bool:
        """Check if this parser can handle the given filename."""
        pass
    
    @abstractmethod
    async def parse(self, file_path: str, filename: str) -> ParsedData:
        """Parse the file and return structured data."""
        pass
    
    @abstractmethod
    def get_supported_extensions(self) -> List[str]:
        """Return list of file extensions this parser supports."""
        pass


class IFileProcessingService(ABC):
    """Service interface for file processing operations."""
    
    @abstractmethod
    def get_parser(self, filename: str) -> IFileParser:
        """Get the appropriate parser for the given filename."""
        pass
    
    @abstractmethod
    def register_parser(self, parser: IFileParser) -> None:
        """Register a new parser with the factory."""
        pass
    
    @abstractmethod
    def list_supported_formats(self) -> Dict[str, List[str]]:
        """List all supported formats and their extensions."""
        pass


# Statistics Service Interfaces
@dataclass
class ColumnStatistics:
    """Statistics for a single column."""
    name: str
    dtype: str
    null_count: int
    null_percentage: float
    unique_count: int
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    mean_value: Optional[float] = None
    median_value: Optional[float] = None
    std_dev: Optional[float] = None


@dataclass 
class TableStatistics:
    """Statistics for an entire table."""
    row_count: int
    column_count: int
    columns: Dict[str, ColumnStatistics]
    memory_usage_bytes: int
    unique_row_count: Optional[int] = None
    duplicate_row_count: Optional[int] = None


class IStatisticsService(ABC):
    """Service interface for statistics calculations."""
    
    @abstractmethod
    async def calculate_table_statistics(
        self,
        dataframe: pd.DataFrame,
        table_key: str
    ) -> TableStatistics:
        """Calculate comprehensive statistics for a table."""
        pass
    
    @abstractmethod
    async def calculate_column_statistics(
        self,
        series: pd.Series,
        column_name: str
    ) -> ColumnStatistics:
        """Calculate statistics for a single column."""
        pass
    
    @abstractmethod
    def get_summary_dict(self, stats: TableStatistics) -> Dict[str, Any]:
        """Convert TableStatistics to a dictionary suitable for storage."""
        pass


# Exploration Service Interfaces
class VisualizationType(Enum):
    """Supported visualization types."""
    HISTOGRAM = "histogram"
    SCATTER = "scatter"
    LINE = "line"
    BAR = "bar"
    BOX = "box"
    HEATMAP = "heatmap"
    CORRELATION_MATRIX = "correlation_matrix"
    DISTRIBUTION = "distribution"
    PAIR_PLOT = "pair_plot"


@dataclass
class DataQualityIssue:
    """Represents a data quality issue found during profiling."""
    issue_type: str
    severity: str
    affected_columns: List[str]
    row_count: int
    description: str
    recommendation: Optional[str] = None


@dataclass
class DataProfile:
    """Comprehensive profile of a dataset/table."""
    table_key: str
    row_count: int
    column_count: int
    memory_usage_mb: float
    column_profiles: Dict[str, Dict[str, Any]]
    quality_score: float
    quality_issues: List[DataQualityIssue]
    numeric_summary: Optional[pd.DataFrame] = None
    categorical_summary: Optional[Dict[str, pd.DataFrame]] = None
    correlation_matrix: Optional[pd.DataFrame] = None
    detected_patterns: List[Dict[str, Any]] = None
    key_insights: List[str] = None


class IExplorationService(ABC):
    """Service interface for data exploration and visualization."""
    
    @abstractmethod
    async def profile_table(
        self,
        table_reader: 'ITableReader',
        commit_id: str,
        table_key: str,
        sample_size: Optional[int] = None
    ) -> DataProfile:
        """Generate comprehensive profile of a table."""
        pass
    
    @abstractmethod
    async def detect_quality_issues(
        self,
        dataframe: pd.DataFrame,
        table_key: str
    ) -> List[DataQualityIssue]:
        """Detect data quality issues in the dataset."""
        pass
    
    @abstractmethod
    async def generate_insights(
        self,
        profile: DataProfile
    ) -> List[str]:
        """Generate human-readable insights from the profile."""
        pass


# Sampling Service Interfaces
class SamplingMethod(Enum):
    """Supported sampling methods."""
    RANDOM = "random"
    STRATIFIED = "stratified"
    CLUSTER = "cluster"
    SYSTEMATIC = "systematic"
    LLM_BASED = "llm_based"
    MULTI_ROUND = "multi_round"


@dataclass
class SampleConfig:
    """Configuration for sampling operations."""
    method: SamplingMethod
    sample_size: Union[int, float]
    random_seed: Optional[int] = None
    stratify_columns: Optional[List[str]] = None
    proportional: bool = True
    cluster_column: Optional[str] = None
    num_clusters: Optional[int] = None
    llm_prompt: Optional[str] = None
    relevance_threshold: Optional[float] = None
    num_rounds: Optional[int] = None
    round_configs: Optional[List['SampleConfig']] = None


@dataclass
class SampleResult:
    """Result of a sampling operation."""
    sampled_data: List[Dict[str, Any]]
    sample_size: int
    method_used: SamplingMethod
    metadata: Dict[str, Any]
    strata_counts: Optional[Dict[str, int]] = None
    selected_clusters: Optional[List[Any]] = None
    relevance_scores: Optional[List[float]] = None
    round_results: Optional[List['SampleResult']] = None


class ISamplingService(ABC):
    """Service interface for data sampling operations."""
    
    @abstractmethod
    async def sample(
        self,
        table_reader: 'ITableReader',
        commit_id: str,
        table_key: str,
        config: SampleConfig
    ) -> SampleResult:
        """Perform sampling on table data."""
        pass
    
    @abstractmethod
    def create_strategy(self, method: SamplingMethod) -> Any:
        """Create a sampling strategy for the given method."""
        pass
    
    @abstractmethod
    def list_available_methods(self) -> List[SamplingMethod]:
        """List all available sampling methods."""
        pass


# Workbench Service Interfaces
class OperationType(Enum):
    """Supported workbench operations."""
    CREATE_DERIVED = "create_derived"
    MERGE_DATASETS = "merge_datasets"
    TRANSFORM = "transform"
    FILTER = "filter"
    AGGREGATE = "aggregate"
    JOIN = "join"
    UNION = "union"
    PIVOT = "pivot"
    SPLIT = "split"
    ENRICH = "enrich"
    SQL_TRANSFORM = "sql_transform"


@dataclass
class WorkbenchContext:
    """Context for workbench operations."""
    user_id: int
    source_datasets: List[int]
    source_refs: List[str]
    operation_type: OperationType
    parameters: Dict[str, Any]
    operation_name: Optional[str] = None
    description: Optional[str] = None
    tags: Optional[List[str]] = None


@dataclass
class OperationResult:
    """Result of a workbench operation."""
    success: bool
    operation_id: str
    output_dataset_id: Optional[int] = None
    output_commit_id: Optional[str] = None
    rows_affected: Optional[int] = None
    execution_time_ms: Optional[int] = None
    memory_used_mb: Optional[float] = None
    error_message: Optional[str] = None
    error_details: Optional[Dict[str, Any]] = None
    log_entries: List[Dict[str, Any]] = None


class IWorkbenchService(ABC):
    """Service interface for workbench operations."""
    
    @abstractmethod
    async def validate_operation(
        self,
        context: WorkbenchContext,
        uow: 'IUnitOfWork'
    ) -> List[str]:
        """Validate the operation can be performed."""
        pass
    
    @abstractmethod
    async def preview_operation(
        self,
        context: WorkbenchContext,
        uow: 'IUnitOfWork',
        sample_size: int = 100
    ) -> Dict[str, Any]:
        """Preview the operation results without executing."""
        pass
    
    @abstractmethod
    async def execute_operation(
        self,
        context: WorkbenchContext,
        uow: 'IUnitOfWork'
    ) -> OperationResult:
        """Execute the workbench operation."""
        pass
    
    @abstractmethod
    def list_available_operations(self) -> List[OperationType]:
        """List all available operation types."""
        pass

# Table Analysis Service Interfaces
@dataclass
class ColumnStatistics:
    """Statistics for a single column."""
    column_name: str
    data_type: str
    non_null_count: int
    null_count: int
    unique_count: int
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    mean_value: Optional[float] = None
    median_value: Optional[float] = None
    mode_value: Optional[Any] = None
    std_dev: Optional[float] = None
    percentiles: Optional[Dict[str, float]] = None


@dataclass
class TableSchema:
    """Schema information for a table."""
    columns: List[Dict[str, Any]]
    primary_key: Optional[List[str]] = None
    row_count: int = 0
    size_bytes: Optional[int] = None


@dataclass 
class TableAnalysis:
    """Complete analysis results for a table."""
    schema: TableSchema
    statistics: List[ColumnStatistics]
    sample_values: Dict[str, List[Any]]
    data_quality_issues: List[Dict[str, Any]]
    profiling_metadata: Dict[str, Any]


class ITableAnalysisService(ABC):
    """Service for comprehensive table analysis."""
    
    @abstractmethod
    async def analyze_table(
        self,
        commit_id: str,
        table_key: str,
        sample_size: int = 100,
        compute_statistics: bool = True,
        infer_types: bool = True
    ) -> TableAnalysis:
        """Perform comprehensive analysis on a table."""
        pass
    
    @abstractmethod
    async def get_column_profile(
        self,
        commit_id: str,
        table_key: str,
        column_name: str
    ) -> ColumnStatistics:
        """Get detailed profile for a single column."""
        pass


class IDataTypeInferenceService(ABC):
    """Service for inferring data types from values."""
    
    @abstractmethod
    def infer_column_type(self, values: List[Any]) -> str:
        """Infer the data type of a column based on sample values."""
        pass
    
    @abstractmethod
    def validate_type_consistency(
        self,
        values: List[Any],
        expected_type: str
    ) -> Dict[str, Any]:
        """Validate that values are consistent with expected type."""
        pass
    
    @abstractmethod
    def get_type_hierarchy(self) -> Dict[str, List[str]]:
        """Get the type hierarchy for type coercion."""
        pass


class IColumnStatisticsService(ABC):
    """Service for computing column statistics."""
    
    @abstractmethod
    async def compute_numeric_statistics(
        self,
        values: List[float]
    ) -> Dict[str, float]:
        """Compute statistics for numeric columns."""
        pass
    
    @abstractmethod
    async def compute_string_statistics(
        self,
        values: List[str]
    ) -> Dict[str, Any]:
        """Compute statistics for string columns."""
        pass
    
    @abstractmethod
    async def compute_date_statistics(
        self,
        values: List[Any]
    ) -> Dict[str, Any]:
        """Compute statistics for date/time columns."""
        pass
    
    @abstractmethod
    async def detect_outliers(
        self,
        values: List[float],
        method: str = "iqr"
    ) -> List[int]:
        """Detect outlier indices in numeric data."""
        pass
    
    @abstractmethod
    async def compute_correlations(
        self,
        columns: Dict[str, List[float]]
    ) -> Dict[str, Dict[str, float]]:
        """Compute correlations between numeric columns."""
        pass


# SQL Execution Service Interfaces
@dataclass
class SqlSource:
    """Represents a source table for SQL execution."""
    dataset_id: int
    ref: str
    table_key: str
    alias: str


@dataclass
class SqlTarget:
    """Represents the target for SQL results."""
    dataset_id: int
    ref: str
    table_key: str
    message: str
    output_branch_name: Optional[str] = None


@dataclass
class SqlExecutionPlan:
    """Execution plan for SQL transformation."""
    sources: List[SqlSource]
    sql_query: str
    target: SqlTarget
    estimated_rows: Optional[int] = None
    estimated_memory_mb: Optional[float] = None
    optimization_hints: Optional[List[str]] = None


@dataclass
class SqlExecutionResult:
    """Result of SQL execution."""
    new_commit_id: str
    rows_processed: int
    execution_time_ms: int
    table_key: str
    output_branch_name: str
    memory_used_mb: Optional[float] = None
    optimization_applied: Optional[List[str]] = None


class ISqlValidationService(ABC):
    """Service for validating SQL queries."""
    
    @abstractmethod
    async def validate_query(
        self,
        sql: str,
        sources: List[SqlSource]
    ) -> Tuple[bool, List[str]]:
        """
        Validate SQL syntax and semantic correctness.
        Returns (is_valid, error_messages).
        """
        pass
    
    @abstractmethod
    async def estimate_resource_usage(
        self,
        sql: str,
        sources: List[SqlSource],
        table_reader: 'ITableReader'
    ) -> Dict[str, Any]:
        """Estimate memory and time requirements for the query."""
        pass
    
    @abstractmethod
    def sanitize_query(self, sql: str) -> str:
        """Sanitize SQL query for safe execution."""
        pass


class ISqlExecutionService(ABC):
    """Service for executing SQL transformations."""
    
    @abstractmethod
    async def create_execution_plan(
        self,
        sources: List[SqlSource],
        sql: str,
        target: SqlTarget
    ) -> SqlExecutionPlan:
        """Create an optimized execution plan."""
        pass
    
    @abstractmethod
    async def execute_transformation(
        self,
        plan: SqlExecutionPlan,
        job_id: str,
        user_id: int
    ) -> SqlExecutionResult:
        """Execute the SQL transformation according to the plan."""
        pass
    
    @abstractmethod
    async def preview_results(
        self,
        sources: List[SqlSource],
        sql: str,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Preview transformation results without committing."""
        pass


class IQueryOptimizationService(ABC):
    """Service for optimizing SQL queries."""
    
    @abstractmethod
    def optimize_query(
        self,
        sql: str,
        table_schemas: Dict[str, TableSchema]
    ) -> str:
        """Optimize SQL query based on table schemas."""
        pass
    
    @abstractmethod
    def suggest_indexes(
        self,
        sql: str,
        table_schemas: Dict[str, TableSchema]
    ) -> List[Dict[str, Any]]:
        """Suggest indexes that would improve query performance."""
        pass
    
    @abstractmethod
    def analyze_query_plan(self, sql: str) -> Dict[str, Any]:
        """Analyze the query execution plan."""
        pass


# Data Export Service
@dataclass
class ExportOptions:
    """Options for data export operations."""
    batch_size: int = 10000
    include_headers: bool = True
    compression: Optional[str] = None
    filters: Optional[Dict[str, Any]] = None
    columns: Optional[List[str]] = None
    limit: Optional[int] = None


@dataclass
class ExportResult:
    """Result of a data export operation."""
    file_path: str
    format: str
    row_count: int
    file_size: int
    export_time_ms: int
    metadata: Dict[str, Any]


class IDataExportService(ABC):
    """Service interface for exporting data in various formats."""
    
    @abstractmethod
    async def export_to_csv(
        self,
        dataset_id: str,
        commit_id: str,
        table_name: str,
        options: Optional[ExportOptions] = None
    ) -> ExportResult:
        """Export data to CSV format."""
        pass
    
    @abstractmethod
    async def export_to_excel(
        self,
        dataset_id: str,
        commit_id: str,
        table_name: str,
        options: Optional[ExportOptions] = None
    ) -> ExportResult:
        """Export data to Excel format."""
        pass
    
    @abstractmethod
    async def export_to_json(
        self,
        dataset_id: str,
        commit_id: str,
        table_name: str,
        options: Optional[ExportOptions] = None
    ) -> ExportResult:
        """Export data to JSON format."""
        pass
    
    @abstractmethod
    async def export_to_parquet(
        self,
        dataset_id: str,
        commit_id: str,
        table_name: str,
        options: Optional[ExportOptions] = None
    ) -> ExportResult:
        """Export data to Parquet format."""
        pass
    
    @abstractmethod
    def get_supported_formats(self) -> List[str]:
        """Get list of supported export formats."""
        pass


# Commit Preparation Service
@dataclass
class CommitData:
    """Data needed to create a commit."""
    commit_id: str
    parent_commit_id: Optional[str]
    message: str
    author: str
    table_changes: Dict[str, Any]
    row_hashes: List[str]
    metadata: Dict[str, Any]


class ICommitPreparationService(ABC):
    """Service interface for preparing commit data."""
    
    @abstractmethod
    async def prepare_commit_data(
        self,
        dataset_id: str,
        parent_commit_id: str,
        changes: Dict[str, Any],
        message: str,
        author: str
    ) -> CommitData:
        """Prepare all data needed for creating a commit."""
        pass
    
    @abstractmethod
    async def canonicalize_data(
        self,
        data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Canonicalize data for consistent hashing."""
        pass
    
    @abstractmethod
    def compute_row_hash(
        self,
        row_data: Dict[str, Any]
    ) -> str:
        """Compute hash for a single row."""
        pass
    
    @abstractmethod
    async def extract_schema(
        self,
        table_data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Extract schema from table data."""
        pass
