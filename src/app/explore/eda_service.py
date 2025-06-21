import asyncio
import json
import logging
from typing import Dict, List, Optional, Any, Tuple, AsyncGenerator
from datetime import datetime
import time
from collections import defaultdict
import duckdb
from sqlalchemy.ext.asyncio import AsyncSession
import math

from app.datasets.exceptions import DatasetNotFound, DatasetVersionNotFound
from app.datasets.repository import DatasetsRepository
from app.explore.eda_models import (
    AnalysisBlock, RenderType, VariableType, VariableInfo, VariableAnalysis,
    Alert, AlertSeverity, EDAMetadata, AnalysisConfig, EDAResponse,
    KeyValueData, TableData, HistogramData, BarChartData, HeatmapData,
    ScatterPlotData, BoxPlotData, MatrixData, HistogramBin, AlertConfig,
    DonutChartData, PieChartData, GaugeChartData, ProgressBarData,
    SparklineData, MiniBarChartData, BulletChartData, ViolinPlotData,
    DensityPlotData, QQPlotData, RangePlotData, TreemapData, SunburstData,
    CalendarHeatmapData, LineChartData, AreaChartData, StackedBarChartData,
    RiskMatrixData, RadarChartData, NetworkGraphData, ChordDiagramData,
    ParallelCoordinatesData
)
from app.storage.backend import StorageBackend

logger = logging.getLogger(__name__)


def quote_identifier(name: Any) -> str:
    """Properly quote a DuckDB identifier (column name)."""
    # Convert to string first to handle numeric column names
    name_str = str(name)
    # DuckDB uses double quotes for identifiers
    # Escape any existing double quotes
    escaped_name = name_str.replace('"', '""')
    return f'"{escaped_name}"'


class EDAService:
    """Service for performing Exploratory Data Analysis on datasets."""

    def __init__(
            self,
            db: AsyncSession,
            repository: DatasetsRepository,
            storage_backend: StorageBackend
    ):
        self.db = db
        self.repository = repository
        self.storage_backend = storage_backend

    async def analyze_dataset(
            self,
            dataset_id: str,
            version_id: str,
            config: AnalysisConfig
    ) -> EDAResponse:
        """Perform comprehensive EDA analysis on a dataset version."""
        start_time = time.time()

        # Get dataset and version
        dataset = await self.repository.get_dataset(dataset_id)
        if not dataset:
            raise DatasetNotFound(dataset_id)

        version = await self.repository.get_dataset_version(version_id)
        if not version:
            raise DatasetVersionNotFound(version_id)

        # Verify dataset ID matches
        if version.dataset_id != dataset_id:
            raise ValueError(f"Version {version_id} does not belong to dataset {dataset_id}")

        # Get file path
        file_id = version.materialized_file_id or version.overlay_file_id
        if not file_id:
            raise ValueError("No file associated with this version")

        file_info = await self.repository.get_file(file_id)
        if not file_info:
            raise ValueError(f"File {file_id} not found")

        file_path = file_info.file_path

        # OPTIMIZATION: Run blocking DuckDB analysis in a separate thread
        eda_results = await asyncio.to_thread(
            self._run_analysis_sync, file_path, config
        )

        # Create metadata
        metadata = EDAMetadata(
            dataset_id=dataset_id,
            version_id=version_id,
            analysis_timestamp=datetime.utcnow(),
            sample_size_used=eda_results.pop("sample_size_used"),
            total_rows=eda_results.pop("total_rows"),
            total_columns=eda_results.pop("total_columns"),
            analysis_duration_seconds=time.time() - start_time
        )

        # Format alerts as analysis blocks with enhanced visualizations
        alert_blocks = []
        if eda_results["alerts"]:
            alerts = eda_results["alerts"]
            
            # Group alerts by severity for risk matrix
            severity_counts = defaultdict(int)
            alert_types = defaultdict(int)
            for alert in alerts:
                severity_counts[alert.severity.value] += 1
                alert_types[alert.alert_type] += 1
            
            # Risk overview gauge
            # Calculate risk score (0-100) based on severity distribution
            risk_score = (
                severity_counts.get("ERROR", 0) * 30 +
                severity_counts.get("WARNING", 0) * 10 +
                severity_counts.get("INFO", 0) * 2
            )
            risk_score = min(100, risk_score)  # Cap at 100
            
            alert_blocks.append(AnalysisBlock(
                title="Data Quality Risk Score",
                render_as=RenderType.GAUGE_CHART,
                data=GaugeChartData(
                    value=risk_score,
                    min_value=0,
                    max_value=100,
                    thresholds=[
                        {"value": 30, "color": "yellow"},
                        {"value": 70, "color": "red"}
                    ],
                    label=f"{risk_score:.0f}/100"
                ),
                description=f"Overall risk based on {len(alerts)} quality issues"
            ))
            
            # Risk Matrix visualization
            risk_matrix_data = defaultdict(lambda: defaultdict(int))
            alert_type_labels = set()
            severity_labels = ["INFO", "WARNING", "ERROR"]  # Ordered
            
            for alert in alerts:
                alert_type_str = alert.alert_type.replace("_", " ").title()
                risk_matrix_data[alert_type_str][alert.severity.value] += 1
                alert_type_labels.add(alert_type_str)
            
            # Build the matrix in the correct structure
            sorted_alert_types = sorted(list(alert_type_labels))
            risk_items = []
            
            for i, alert_type in enumerate(sorted_alert_types):
                for j, severity in enumerate(severity_labels):
                    count = risk_matrix_data[alert_type].get(severity, 0)
                    if count > 0:
                        risk_items.append({
                            "x": j,  # severity index
                            "y": i,  # alert type index
                            "value": count,
                            "label": f"{alert_type}: {count}"
                        })
            
            if risk_items:
                alert_blocks.append(AnalysisBlock(
                    title="Data Quality Risk Matrix",
                    render_as=RenderType.RISK_MATRIX,
                    data=RiskMatrixData(
                        items=risk_items,
                        likelihood_labels=severity_labels,
                        impact_labels=sorted_alert_types
                    ),
                    description="Alert distribution by type and severity"
                ))
            
            # Alert distribution by severity
            if severity_counts:
                alert_blocks.append(AnalysisBlock(
                    title="Alerts by Severity",
                    render_as=RenderType.DONUT_CHART,
                    data=DonutChartData(
                        labels=list(severity_counts.keys()),
                        values=list(severity_counts.values()),
                        center_text=f"{len(alerts)} total"
                    ),
                    description="Distribution of alerts by severity level"
                ))
            
            # Alert types horizontal bar chart
            if alert_types:
                alert_blocks.append(AnalysisBlock(
                    title="Alert Types",
                    render_as=RenderType.HORIZONTAL_BAR_CHART,
                    data=BarChartData(
                        categories=[k.replace("_", " ").title() for k in alert_types.keys()],
                        values=list(alert_types.values())
                    ),
                    description="Frequency of different alert types"
                ))
            
            # Original alert list for details
            alert_blocks.append(AnalysisBlock(
                title="Data Quality Alert Details",
                render_as=RenderType.ALERT_LIST,
                data={"alerts": [alert.dict() for alert in alerts]},
                description="Detailed list of all data quality issues"
            ))

        return EDAResponse(
            metadata=metadata,
            global_summary=eda_results["global_summary"],
            variables=eda_results["variables"],
            interactions=eda_results["interactions"],
            alerts=alert_blocks
        )

    async def analyze_dataset_stream(
            self,
            dataset_id: str,
            version_id: str,
            config: AnalysisConfig
    ) -> AsyncGenerator[str, None]:
        """Stream EDA analysis results as Server-Sent Events."""
        start_time = time.time()
        
        try:
            # Get dataset and version
            dataset = await self.repository.get_dataset(dataset_id)
            if not dataset:
                yield self._format_sse_event("error", {"message": f"Dataset {dataset_id} not found"})
                return

            version = await self.repository.get_dataset_version(version_id)
            if not version:
                yield self._format_sse_event("error", {"message": f"Version {version_id} not found"})
                return

            # Verify dataset ID matches
            if version.dataset_id != dataset_id:
                yield self._format_sse_event("error", {"message": f"Version {version_id} does not belong to dataset {dataset_id}"})
                return

            # Get file path
            file_id = version.materialized_file_id or version.overlay_file_id
            if not file_id:
                yield self._format_sse_event("error", {"message": "No file associated with this version"})
                return

            file_info = await self.repository.get_file(file_id)
            if not file_info:
                yield self._format_sse_event("error", {"message": f"File {file_id} not found"})
                return

            file_path = file_info.file_path

            # Stream analysis results
            async for event in self._stream_analysis(file_path, config, dataset_id, version_id, start_time):
                yield event
                
        except Exception as e:
            logger.error(f"Error in EDA stream: {str(e)}")
            yield self._format_sse_event("error", {"message": f"Analysis error: {str(e)}"})

    async def _stream_analysis(
            self,
            file_path: str,
            config: AnalysisConfig,
            dataset_id: str,
            version_id: str,
            start_time: float
    ) -> AsyncGenerator[str, None]:
        """Stream the actual analysis results."""
        # Use asyncio.to_thread to run blocking operations
        analysis_generator = await asyncio.to_thread(
            self._run_streaming_analysis_sync, file_path, config
        )
        
        # First, send metadata
        metadata_sent = False
        
        # Stream results as they come
        for result_type, result_data in analysis_generator:
            if result_type == "metadata" and not metadata_sent:
                # Enhance metadata with additional info
                result_data.update({
                    "dataset_id": dataset_id,
                    "version_id": version_id,
                    "analysis_timestamp": datetime.utcnow().isoformat(),
                    "stream_start_time": start_time
                })
                yield self._format_sse_event("metadata", result_data)
                metadata_sent = True
            elif result_type == "analysis_block":
                yield self._format_sse_event("analysis_block", result_data)
            elif result_type == "alert":
                yield self._format_sse_event("alert", result_data)
            elif result_type == "progress":
                yield self._format_sse_event("progress", result_data)
        
        # Send completion event
        yield self._format_sse_event("complete", {
            "analysis_duration_seconds": time.time() - start_time,
            "timestamp": datetime.utcnow().isoformat()
        })

    def _format_sse_event(self, event_type: str, data: Any) -> str:
        """Format data as SSE event with robust JSON encoding."""
        from datetime import date
        from decimal import Decimal
        
        def json_encoder(obj):
            # Handle floats with special values
            if isinstance(obj, float):
                if math.isnan(obj) or math.isinf(obj):
                    return None
            # Handle datetime and date objects
            elif isinstance(obj, (datetime, date)):
                return obj.isoformat()
            # Handle Decimal (common from DuckDB)
            elif isinstance(obj, Decimal):
                # Convert to float and check for special values
                val = float(obj)
                if math.isnan(val) or math.isinf(val):
                    return None
                return val
            # Try default encoder first
            try:
                return json.JSONEncoder().default(obj)
            except TypeError:
                # Fallback: convert unknown types to string and log warning
                logger.warning(f"Unsupported type for JSON serialization: {type(obj).__name__}. Converting to string.")
                return str(obj)
        
        try:
            json_data = json.dumps(data, default=json_encoder)
            return f"event: {event_type}\ndata: {json_data}\n\n"
        except Exception as e:
            # Final fallback to prevent stream crash
            logger.error(f"Failed to serialize SSE event '{event_type}': {e}")
            error_data = json.dumps({"error": "Serialization failed", "event_type": event_type})
            return f"event: error\ndata: {error_data}\n\n"

    def _run_analysis_sync(self, file_path: str, config: AnalysisConfig) -> Dict[str, Any]:
        """
        Synchronous helper function to run the entire DuckDB analysis.
        This function is designed to be run in a thread pool.
        """
        with duckdb.connect(':memory:') as conn:
            # Create view with optional sampling
            if config.sample_size and config.sample_size > 0:
                conn.execute(f"""
                    CREATE VIEW dataset AS 
                    SELECT * FROM read_parquet('{file_path}') 
                    USING SAMPLE {config.sample_size} ROWS
                """)
                sample_size_used = config.sample_size
            else:
                conn.execute(f"CREATE VIEW dataset AS SELECT * FROM read_parquet('{file_path}')")
                sample_size_used = None

            # Get total rows and columns
            total_rows = conn.execute("SELECT COUNT(*) FROM dataset").fetchone()[0]
            if total_rows == 0:
                # Handle empty dataset gracefully
                return {
                    "global_summary": [], "variables": {}, "interactions": [], "alerts": [],
                    "sample_size_used": sample_size_used, "total_rows": 0, "total_columns": 0,
                }
                
            column_info_raw = conn.execute("PRAGMA table_info('dataset')").fetchall()
            column_info = [{"name": col[1], "type": col[2]} for col in column_info_raw]
            total_columns = len(column_info)

            # Initialize response components
            global_summary = []
            variables = {}
            interactions = []
            alerts = []
            correlation_pairs = []
            duplicate_rows = 0

            # Perform analyses based on config
            if config.global_summary:
                global_summary, duplicate_rows = self._analyze_global_summary(conn, column_info, total_rows)

            # Get common stats for all columns (needed for variables and missing values)
            common_stats_results = {}
            if config.variables.enabled or config.missing_values:
                common_stats_results = self._execute_batch_common_stats(conn, column_info, total_rows)

            if config.variables.enabled:
                variables = self._batch_analyze_columns(
                    conn,
                    column_info,
                    total_rows,
                    config,
                    common_stats_results
                )

            if config.interactions.enabled:
                interactions, correlation_pairs = self._analyze_interactions(
                    conn,
                    variables,
                    config.interactions.correlation_threshold,
                    config.interactions.max_pairs
                )

            if config.missing_values:
                missing_summary, _ = self._analyze_missing_values(column_info, total_rows, common_stats_results)
                if missing_summary:
                    global_summary.extend(missing_summary)

            if config.alerts.enabled:
                alerts = self._detect_alerts(
                    variables,
                    total_rows,
                    correlation_pairs,
                    duplicate_rows,
                    config.alerts
                )

            return {
                "global_summary": global_summary,
                "variables": variables,
                "interactions": interactions,
                "alerts": alerts,
                "sample_size_used": sample_size_used,
                "total_rows": total_rows,
                "total_columns": total_columns,
            }

    def _run_streaming_analysis_sync(self, file_path: str, config: AnalysisConfig):
        """
        Generator function to stream analysis results.
        Yields tuples of (event_type, data).
        """
        with duckdb.connect(':memory:') as conn:
            # Create view with optional sampling
            if config.sample_size and config.sample_size > 0:
                conn.execute(f"""
                    CREATE VIEW dataset AS 
                    SELECT * FROM read_parquet('{file_path}') 
                    USING SAMPLE {config.sample_size} ROWS
                """)
                sample_size_used = config.sample_size
            else:
                conn.execute(f"CREATE VIEW dataset AS SELECT * FROM read_parquet('{file_path}')")
                sample_size_used = None

            # Get basic info
            column_info = conn.execute("DESCRIBE dataset").fetchall()
            column_info = [{"name": col[0], "type": col[1]} for col in column_info]
            total_rows = conn.execute("SELECT COUNT(*) FROM dataset").fetchone()[0]
            total_columns = len(column_info)

            # Yield metadata first
            yield ("metadata", {
                "sample_size_used": sample_size_used,
                "total_rows": total_rows,
                "total_columns": total_columns
            })
            
            # Handle empty dataset
            if total_rows == 0:
                yield ("analysis_block", AnalysisBlock(
                    title="Empty Dataset",
                    render_as=RenderType.KEY_VALUE_PAIRS,
                    data={"Message": "This dataset contains no rows"},
                    description="No data available for analysis"
                ).dict())
                return

            # 1. Global Summary Analysis
            yield ("progress", {"stage": "global_summary", "message": "Analyzing dataset statistics..."})
            global_blocks, duplicate_rows = self._analyze_global_summary(conn, column_info, total_rows)
            for block in global_blocks:
                yield ("analysis_block", block.dict())

            # 2. Variable Analysis (streaming)
            yield ("progress", {"stage": "variables", "message": f"Analyzing {len(column_info)} variables..."})
            
            # First get common stats for all columns
            common_stats_results = self._execute_batch_common_stats(conn, column_info, total_rows)
            
            # Then analyze in smaller batches to enable streaming
            batch_size = 5  # Process 5 variables at a time
            all_variables = {}
            correlation_pairs = []  # Initialize for alerts analysis
            
            for i in range(0, len(column_info), batch_size):
                batch_columns = column_info[i:i + batch_size]
                if i + batch_size <= config.variables.limit:
                    batch_vars = self._batch_analyze_columns(
                        conn, batch_columns, total_rows, config, common_stats_results
                    )
                    all_variables.update(batch_vars)
                    
                    # Stream each variable's analysis blocks
                    for var_name, var_analysis in batch_vars.items():
                        for block in var_analysis.analyses:
                            block_data = block.dict()
                            block_data["variable_name"] = var_name  # Add variable context
                            yield ("analysis_block", block_data)
                    
                    yield ("progress", {
                        "stage": "variables", 
                        "message": f"Analyzed {min(i + batch_size, len(column_info))} of {len(column_info)} variables"
                    })

            # 3. Missing Value Analysis
            if config.missing_values:
                yield ("progress", {"stage": "missing_values", "message": "Analyzing missing value patterns..."})
                # REUSE the stats. Don't re-calculate.
                missing_blocks, _ = self._analyze_missing_values(column_info, total_rows, common_stats_results)
                for block in missing_blocks:
                    yield ("analysis_block", block.dict())

            # 4. Interaction Analysis
            if config.interactions.enabled and len(all_variables) >= 2:
                yield ("progress", {"stage": "interactions", "message": "Analyzing variable interactions..."})
                interaction_blocks, correlation_pairs = self._analyze_interactions(
                    conn, all_variables, 
                    config.interactions.correlation_threshold,
                    config.interactions.max_pairs
                )
                for block in interaction_blocks:
                    yield ("analysis_block", block.dict())

            # 5. Alerts Analysis
            yield ("progress", {"stage": "alerts", "message": "Checking for data quality issues..."})
            alerts = self._analyze_alerts(
                conn, column_info, total_rows, all_variables,
                duplicate_rows, correlation_pairs, config.alerts
            )
            
            # Stream alerts as they're found
            if alerts:
                alert_block = AnalysisBlock(
                    title="Data Quality Alerts",
                    render_as=RenderType.ALERT_LIST,
                    data={"alerts": [alert.dict() for alert in alerts]},
                    description="Potential data quality issues detected"
                )
                yield ("analysis_block", alert_block.dict())

    def _analyze_global_summary(
            self,
            conn: duckdb.DuckDBPyConnection,
            column_info: List[Dict],
            total_rows: int
    ) -> Tuple[List[AnalysisBlock], int]:
        """Analyze dataset-level statistics."""
        blocks = []

        # Duplicate row calculation using efficient HASH method
        try:
            duplicate_query = "SELECT COUNT(*) - COUNT(DISTINCT hash) FROM (SELECT HASH(*) as hash FROM dataset) t"
            duplicate_rows = conn.execute(duplicate_query).fetchone()[0] or 0
        except Exception as e:
            logger.warning(f"Could not calculate duplicate rows: {e}")
            duplicate_rows = 0

        # Calculate data completeness
        non_duplicate_rows = total_rows - duplicate_rows
        duplicate_percent = self._safe_percentage(duplicate_rows, total_rows)
        completeness_percent = self._safe_percentage(non_duplicate_rows, total_rows)

        # 1. Data Completeness - Progress Bar
        blocks.append(AnalysisBlock(
            title="Data Completeness",
            render_as=RenderType.PROGRESS_BAR,
            data=ProgressBarData(
                value=completeness_percent,
                max_value=100,
                label=f"{non_duplicate_rows:,} unique rows out of {total_rows:,}",
                color="green" if completeness_percent >= 90 else "yellow" if completeness_percent >= 70 else "red",
                show_percentage=True
            ),
            description="Percentage of unique (non-duplicate) rows"
        ))

        # 2. Duplicate Rows - Donut Chart
        if duplicate_rows > 0:
            blocks.append(AnalysisBlock(
                title="Duplicate Rows Analysis",
                render_as=RenderType.DONUT_CHART,
                data=DonutChartData(
                    labels=["Unique Rows", "Duplicate Rows"],
                    values=[non_duplicate_rows, duplicate_rows],
                    center_text=f"{duplicate_percent:.1f}%"
                ),
                description=f"Dataset contains {duplicate_rows:,} duplicate rows"
            ))

        # 3. Data Quality Score - Gauge Chart
        # Calculate a simple quality score based on duplicates and completeness
        quality_score = 100 - duplicate_percent
        blocks.append(AnalysisBlock(
            title="Data Quality Score",
            render_as=RenderType.GAUGE_CHART,
            data=GaugeChartData(
                value=quality_score,
                min_value=0,
                max_value=100,
                thresholds=[
                    {"value": 70, "color": "yellow"},
                    {"value": 90, "color": "green"}
                ],
                label=f"{quality_score:.1f}%"
            ),
            description="Overall data quality based on uniqueness"
        ))

        # 4. Dataset Overview - Mini Stats with Sparklines
        blocks.append(AnalysisBlock(
            title="Dataset Dimensions",
            render_as=RenderType.MINI_BAR_CHART,
            data=MiniBarChartData(
                values=[total_rows, len(column_info)],
                labels=["Rows", "Columns"],
                max_bars=2
            ),
            description=f"{total_rows:,} rows × {len(column_info)} columns"
        ))

        # 5. Data type distribution - Enhanced Bar Chart
        type_counts = defaultdict(int)
        for col in column_info:
            var_type = self._categorize_dtype(col["type"])
            if var_type != VariableType.UNKNOWN:
                type_counts[var_type.value] += 1
        
        if type_counts:
            blocks.append(AnalysisBlock(
                title="Data Type Distribution",
                render_as=RenderType.HORIZONTAL_BAR_CHART,
                data=BarChartData(
                    categories=list(type_counts.keys()),
                    values=list(type_counts.values())
                ),
                description="Distribution of column data types"
            ))

        return blocks, duplicate_rows

    def _batch_analyze_columns(
            self,
            conn: duckdb.DuckDBPyConnection,
            all_columns: List[Dict],
            total_rows: int,
            config: AnalysisConfig,
            common_stats_results: Dict[str, Dict] = None
    ) -> Dict[str, VariableAnalysis]:
        """
        Analyzes all variables in batches using consolidated queries. This is the main performance gain.
        """
        # 1. Classify columns and respect limits
        columns_to_analyze = []
        numeric_cols, categorical_cols, datetime_cols, text_cols, boolean_cols = [], [], [], [], []

        for col in all_columns:
            if len(columns_to_analyze) >= config.variables.limit:
                break
            
            var_type = self._categorize_dtype(col['type'])
            if var_type.lower() not in [t.lower() for t in config.variables.types]:
                continue
            
            col['var_type'] = var_type
            columns_to_analyze.append(col)

            if var_type == VariableType.NUMERIC: numeric_cols.append(col)
            elif var_type == VariableType.CATEGORICAL: categorical_cols.append(col)
            elif var_type == VariableType.DATETIME: datetime_cols.append(col)
            elif var_type == VariableType.TEXT: text_cols.append(col)
            elif var_type == VariableType.BOOLEAN: boolean_cols.append(col)

        # 2. Build and execute consolidated queries
        # Use provided common_stats_results if available, otherwise compute
        if common_stats_results is None:
            common_stats_results = self._execute_batch_common_stats(conn, columns_to_analyze, total_rows)
        else:
            # Filter common_stats_results to only include columns we're analyzing
            filtered_stats = {}
            for col in columns_to_analyze:
                if col['name'] in common_stats_results:
                    filtered_stats[col['name']] = common_stats_results[col['name']]
            common_stats_results = filtered_stats
            
        # Improve text vs categorical distinction for string columns
        text_threshold_query_parts = []
        for col in columns_to_analyze:
            if col['var_type'] == VariableType.CATEGORICAL and col['type'].upper() in ['VARCHAR', 'CHAR', 'TEXT', 'STRING']:
                q_col = quote_identifier(col['name'])
                text_threshold_query_parts.append(f"""
                    SELECT 
                        '{col['name']}' as col_name,
                        AVG(LENGTH({q_col})) as avg_length,
                        MAX(LENGTH({q_col})) as max_length
                    FROM dataset
                    WHERE {q_col} IS NOT NULL
                """)
        
        if text_threshold_query_parts:
            text_threshold_query = "\nUNION ALL\n".join(text_threshold_query_parts)
            text_threshold_results = conn.execute(text_threshold_query).fetchall()
            
            # Re-categorize based on heuristics
            for row in text_threshold_results:
                col_name = row[0]
                avg_length = self._clean_numeric_value(row[1]) or 0
                max_length = self._clean_numeric_value(row[2]) or 0
                distinct_ratio = common_stats_results.get(col_name, {}).get("Distinct (%)", 0) / 100.0
                
                # Heuristics for text classification using configurable thresholds
                if (avg_length > config.variables.text_avg_length_threshold or 
                    max_length > config.variables.text_max_length_threshold or 
                    distinct_ratio > config.variables.text_distinct_ratio_threshold):
                    # Find and update the column type
                    for col in columns_to_analyze:
                        if col['name'] == col_name:
                            col['var_type'] = VariableType.TEXT
                            # Move from categorical to text list
                            categorical_cols[:] = [c for c in categorical_cols if c['name'] != col_name]
                            text_cols.append(col)
                            break
            
        numeric_stats_results = self._execute_batch_numeric_stats(conn, numeric_cols, total_rows)
        categorical_freq_results = self._execute_batch_categorical_freq(conn, categorical_cols, total_rows, config.alerts.frequency_table_limit)
        datetime_stats_results = self._execute_batch_datetime_stats(conn, datetime_cols, total_rows)
        boolean_stats_results = self._execute_batch_boolean_stats(conn, boolean_cols, total_rows)
        text_stats_results = self._execute_batch_text_stats(conn, text_cols, total_rows)

        # 3. Assemble results into VariableAnalysis objects
        variables = {}
        for col in columns_to_analyze:
            col_name = col['name']
            var_type = col['var_type']
            
            var_info = VariableInfo(name=col_name, type=var_type, dtype=col['type'])
            analyses = []

            # Add common stats from the batch results with enhanced visualizations
            if col_name in common_stats_results:
                stats = common_stats_results[col_name]
                
                # Mini bar chart for missing vs non-missing
                missing_count = stats.get("Missing Count", 0)
                non_missing_count = total_rows - missing_count
                
                analyses.append(AnalysisBlock(
                    title="Data Completeness",
                    render_as=RenderType.MINI_BAR_CHART,
                    data=MiniBarChartData(
                        values=[non_missing_count, missing_count],
                        labels=["Present", "Missing"],
                        max_bars=2
                    ),
                    description=f"{stats.get('Missing (%)', 0):.1f}% missing values"
                ))
                
                # Progress bar for distinct ratio
                distinct_percent = stats.get("Distinct (%)", 0)
                analyses.append(AnalysisBlock(
                    title="Cardinality",
                    render_as=RenderType.PROGRESS_BAR,
                    data=ProgressBarData(
                        value=distinct_percent,
                        max_value=100,
                        label=f"{stats.get('Distinct Count', 0):,} unique values",
                        color="blue" if distinct_percent < 50 else "orange" if distinct_percent < 90 else "red",
                        show_percentage=True
                    ),
                    description="Percentage of distinct values"
                ))

            # Add type-specific stats
            if var_type == VariableType.NUMERIC and col_name in numeric_stats_results:
                stats = numeric_stats_results[col_name]
                
                # Range plot showing min/max/mean/median in one view
                desc_stats = stats.get('descriptive', {})
                range_data = {
                    "min": desc_stats.get("Min", 0),
                    "max": desc_stats.get("Max", 0),
                    "mean": desc_stats.get("Mean", 0),
                    "median": desc_stats.get("Median", 0)
                }
                
                analyses.append(AnalysisBlock(
                    title="Statistical Summary",
                    render_as=RenderType.RANGE_PLOT,
                    data=RangePlotData(
                        categories=[col_name],
                        ranges=[range_data]
                    ),
                    description="Min, Max, Mean, and Median values"
                ))
                
                # Bullet chart for quantiles
                quantile_stats = stats.get('quantile', {})
                if quantile_stats.get("Q1 (25th Percentile)") is not None:
                    analyses.append(AnalysisBlock(
                        title="Quartile Distribution",
                        render_as=RenderType.BULLET_CHART,
                        data=BulletChartData(
                            value=quantile_stats.get("Median (50th Percentile)", 0),
                            target=desc_stats.get("Mean", 0),
                            ranges=[
                                {"min": desc_stats.get("Min", 0), "max": quantile_stats.get("Q1 (25th Percentile)", 0), "label": "Q1"},
                                {"min": quantile_stats.get("Q1 (25th Percentile)", 0), "max": quantile_stats.get("Q3 (75th Percentile)", 0), "label": "IQR"},
                                {"min": quantile_stats.get("Q3 (75th Percentile)", 0), "max": desc_stats.get("Max", 0), "label": "Q3"}
                            ],
                            label="Median vs Mean"
                        ),
                        description="Quartile ranges with median (line) and mean (marker)"
                    ))
                
                # Gauge for zero values percentage
                zeros_stats = stats.get('zeros', {})
                zeros_percent = zeros_stats.get("Zeros (%)", 0)
                if zeros_percent > 0:
                    analyses.append(AnalysisBlock(
                        title="Zero Values",
                        render_as=RenderType.GAUGE_CHART,
                        data=GaugeChartData(
                            value=zeros_percent,
                            min_value=0,
                            max_value=100,
                            thresholds=[
                                {"value": 50, "color": "yellow"},
                                {"value": 90, "color": "red"}
                            ],
                            label=f"{zeros_stats.get('Zeros Count', 0):,} zeros"
                        ),
                        description="Percentage of zero values in the column"
                    ))
                
                # Enhanced histogram with density overlay option
                if stats.get('histogram'):
                    analyses.append(AnalysisBlock(
                        title="Distribution",
                        render_as=RenderType.HISTOGRAM,
                        data=stats['histogram'],
                        description="Value distribution with bin counts"
                    ))
                
                # Add Violin Plot for better distribution visualization
                if quantile_stats.get("Q1 (25th Percentile)") is not None:
                    # Create violin plot data from quantiles and histogram
                    violin_data = {
                        "min": desc_stats.get("Min", 0),
                        "q1": quantile_stats.get("Q1 (25th Percentile)", 0),
                        "median": quantile_stats.get("Median (50th Percentile)", 0),
                        "q3": quantile_stats.get("Q3 (75th Percentile)", 0),
                        "max": desc_stats.get("Max", 0),
                        "mean": desc_stats.get("Mean", 0)
                    }
                    
                    # Add histogram data for kernel density
                    if stats.get('histogram') and stats['histogram'].bins:
                        violin_data["density"] = [
                            {"x": (b.min + b.max) / 2, "y": b.count} 
                            for b in stats['histogram'].bins
                        ]
                    
                    analyses.append(AnalysisBlock(
                        title="Distribution (Violin Plot)",
                        render_as=RenderType.VIOLIN_PLOT,
                        data=ViolinPlotData(
                            categories=[col_name],
                            data=[violin_data]
                        ),
                        description="Distribution shape with quartiles and density"
                    ))
                
                # Add Density Plot as alternative to histogram
                density_data = self._calculate_density_data(conn, col_name)
                if density_data:
                    analyses.append(AnalysisBlock(
                        title="Probability Density",
                        render_as=RenderType.DENSITY_PLOT,
                        data=density_data,
                        description="Smooth probability density estimate"
                    ))
                
                # Add Q-Q Plot for normality testing
                qq_data = self._calculate_qq_plot_data(conn, col_name)
                if qq_data:
                    analyses.append(AnalysisBlock(
                        title="Normality Test (Q-Q Plot)",
                        render_as=RenderType.QQ_PLOT,
                        data=qq_data,
                        description="Compare distribution against normal distribution"
                    ))
                
                # Add sparkline for quick distribution view
                if stats.get('histogram') and stats['histogram'].bins:
                    sparkline_values = [bin.count for bin in stats['histogram'].bins[:20]]  # Limit to 20 points
                    analyses.append(AnalysisBlock(
                        title="Distribution Shape",
                        render_as=RenderType.SPARKLINE,
                        data=SparklineData(
                            values=sparkline_values,
                            show_dots=False,
                            show_area=True
                        ),
                        description="Quick view of distribution shape"
                    ))

            elif var_type == VariableType.CATEGORICAL and col_name in categorical_freq_results:
                freq_data = categorical_freq_results[col_name]
                
                # Treemap for many categories (more space-efficient than bar chart)
                if freq_data['table'] and freq_data['table'].rows and len(freq_data['table'].rows) > 10:
                    # Create treemap data for all categories
                    treemap_data = [
                        {
                            "name": str(row[0]) if row[0] is not None else "NULL",
                            "value": row[1],
                            "percentage": row[2]
                        }
                        for row in freq_data['table'].rows[:50]  # Limit to top 50 for performance
                    ]
                    
                    analyses.append(AnalysisBlock(
                        title="Category Distribution (Treemap)",
                        render_as=RenderType.TREEMAP,
                        data=TreemapData(data=treemap_data),
                        description=f"Space-efficient view of top {min(50, len(freq_data['table'].rows))} categories"
                    ))
                
                # Pie/Donut chart for top categories with "Others" group
                if freq_data['table'] and freq_data['table'].rows:
                    top_rows = freq_data['table'].rows[:5]  # Top 5 categories
                    other_rows = freq_data['table'].rows[5:]  # Remaining categories
                    
                    pie_labels = [str(row[0]) if row[0] is not None else "NULL" for row in top_rows]
                    pie_values = [row[1] for row in top_rows]
                    
                    # Add "Others" category if there are more than 5 categories
                    if other_rows:
                        others_count = sum(row[1] for row in other_rows)
                        pie_labels.append("Others")
                        pie_values.append(others_count)
                    
                    # Use donut chart for better visual appeal
                    analyses.append(AnalysisBlock(
                        title="Top Categories Overview",
                        render_as=RenderType.DONUT_CHART,
                        data=DonutChartData(
                            labels=pie_labels,
                            values=pie_values,
                            center_text=f"{len(freq_data['table'].rows)} categories"
                        ),
                        description="Top 5 categories with others grouped"
                    ))
                
                # Horizontal bar chart for long category names
                if freq_data['chart']:
                    # Use horizontal bars for better readability of category names
                    analyses.append(AnalysisBlock(
                        title="Top 10 Values",
                        render_as=RenderType.HORIZONTAL_BAR_CHART,
                        data=freq_data['chart'],
                        description="Most frequent values in the column"
                    ))
                
                # Add frequency table for detailed view
                if freq_data['table']:
                    analyses.append(AnalysisBlock(
                        title="Frequency Table",
                        render_as=RenderType.TABLE,
                        data=freq_data['table'],
                        description="Complete frequency distribution"
                    ))
            
            elif var_type == VariableType.DATETIME and col_name in datetime_stats_results:
                stats = datetime_stats_results[col_name]
                
                # Timeline visualization for date range
                date_range = stats.get('range', {})
                if date_range.get('Min Date') and date_range.get('Max Date'):
                    analyses.append(AnalysisBlock(
                        title="Date Range Timeline",
                        render_as=RenderType.PROGRESS_BAR,
                        data=ProgressBarData(
                            value=100,  # Full bar to show range
                            max_value=100,
                            label=f"{date_range['Min Date']} to {date_range['Max Date']}",
                            color="blue",
                            show_percentage=False
                        ),
                        description=f"Spanning {date_range.get('Unique Days', 0):,} unique days"
                    ))
                
                # Calendar Heatmap for daily patterns
                calendar_data = self._calculate_calendar_heatmap_data(conn, col_name, date_range)
                if calendar_data:
                    analyses.append(AnalysisBlock(
                        title="Daily Activity Calendar",
                        render_as=RenderType.CALENDAR_HEATMAP,
                        data=calendar_data,
                        description="Daily activity patterns over time"
                    ))
                
                # Line chart for temporal trends (if we have daily/weekly/monthly counts)
                if stats.get('temporal_patterns') and stats['temporal_patterns'].rows:
                    # Convert table data to line chart
                    x_values = [row[0] for row in stats['temporal_patterns'].rows]
                    y_values = [row[1] for row in stats['temporal_patterns'].rows]
                    
                    analyses.append(AnalysisBlock(
                        title="Day of Week Pattern",
                        render_as=RenderType.LINE_CHART,
                        data=LineChartData(
                            x_values=x_values,
                            series=[{"name": "Count", "y_values": y_values}]
                        ),
                        description="Distribution across days of the week"
                    ))
                
                # Area chart for monthly distribution
                if stats.get('monthly_distribution'):
                    monthly_data = stats['monthly_distribution']
                    analyses.append(AnalysisBlock(
                        title="Monthly Trend",
                        render_as=RenderType.AREA_CHART,
                        data=AreaChartData(
                            x_values=monthly_data.categories,
                            series=[{"name": "Count", "y_values": monthly_data.values}],
                            stacked=False
                        ),
                        description="Monthly distribution of values"
                    ))
                
                # Mini stats with sparkline for date patterns
                if date_range:
                    mini_stats = [
                        date_range.get('Unique Years', 0),
                        date_range.get('Unique Months', 0),
                        date_range.get('Unique Days', 0)
                    ]
                    analyses.append(AnalysisBlock(
                        title="Temporal Coverage",
                        render_as=RenderType.MINI_BAR_CHART,
                        data=MiniBarChartData(
                            values=mini_stats,
                            labels=["Years", "Months", "Days"],
                            max_bars=3
                        ),
                        description="Unique temporal units in the data"
                    ))
                    
            elif var_type == VariableType.BOOLEAN and col_name in boolean_stats_results:
                stats = boolean_stats_results[col_name]
                dist = stats.get('distribution', {})
                
                # Gauge chart for true ratio
                true_ratio = dist.get('True Ratio (excl. nulls)', 0)
                analyses.append(AnalysisBlock(
                    title="True Value Ratio",
                    render_as=RenderType.GAUGE_CHART,
                    data=GaugeChartData(
                        value=true_ratio,
                        min_value=0,
                        max_value=100,
                        thresholds=[
                            {"value": 30, "color": "orange"},
                            {"value": 70, "color": "green"}
                        ],
                        label=f"{true_ratio:.1f}% True"
                    ),
                    description="Percentage of True values (excluding nulls)"
                ))
                
                # Donut chart for true/false/null distribution
                if stats.get('chart'):
                    chart_data = stats['chart']
                    analyses.append(AnalysisBlock(
                        title="Boolean Value Distribution",
                        render_as=RenderType.DONUT_CHART,
                        data=DonutChartData(
                            labels=chart_data.categories,
                            values=chart_data.values,
                            center_text=f"{dist.get('True Count', 0):,} True"
                        ),
                        description="Distribution of True, False, and Null values"
                    ))
                    
            elif var_type == VariableType.TEXT and col_name in text_stats_results:
                stats = text_stats_results[col_name]
                text_stats = stats.get('statistics', {})
                
                # Range plot for text lengths
                analyses.append(AnalysisBlock(
                    title="Text Length Range",
                    render_as=RenderType.RANGE_PLOT,
                    data=RangePlotData(
                        categories=["Text Length"],
                        ranges=[{
                            "min": text_stats.get("Min Length", 0),
                            "max": text_stats.get("Max Length", 0),
                            "mean": text_stats.get("Avg Length", 0),
                            "median": text_stats.get("Median Length", 0)
                        }]
                    ),
                    description="Min, Max, Average, and Median text lengths"
                ))
                
                # Progress bar for empty strings
                empty_percent = text_stats.get("Empty Strings (%)", 0)
                if empty_percent > 0:
                    analyses.append(AnalysisBlock(
                        title="Empty Strings",
                        render_as=RenderType.PROGRESS_BAR,
                        data=ProgressBarData(
                            value=empty_percent,
                            max_value=100,
                            label=f"{text_stats.get('Empty Strings', 0):,} empty strings",
                            color="red" if empty_percent > 10 else "yellow" if empty_percent > 5 else "green",
                            show_percentage=True
                        ),
                        description="Percentage of empty string values"
                    ))
                
                # Enhanced histogram for length distribution
                if stats.get('length_histogram'):
                    analyses.append(AnalysisBlock(
                        title="Length Distribution",
                        render_as=RenderType.HISTOGRAM,
                        data=stats['length_histogram'],
                        description="Distribution of text lengths"
                    ))

            variables[col_name] = VariableAnalysis(common_info=var_info, analyses=analyses)
        
        return variables

    def _execute_batch_common_stats(self, conn: duckdb.DuckDBPyConnection, columns: List[Dict], total_rows: int) -> Dict:
        if not columns: return {}
        
        query_parts = []
        for col in columns:
            q_col = quote_identifier(col['name'])
            query_parts.append(f"""
                SELECT
                    '{col['name']}' as col_name,
                    COUNT({q_col}) as non_missing_count,
                    COUNT(DISTINCT {q_col}) as distinct_count
                FROM dataset
            """)
        
        query = "\nUNION ALL\n".join(query_parts)
        results = conn.execute(query).fetchall()

        stats_map = {}
        for row in results:
            col_name, non_missing_count, distinct_count = row
            missing_count = total_rows - non_missing_count
            stats_map[col_name] = {
                "Distinct Count": distinct_count,
                "Distinct (%)": self._safe_percentage(distinct_count, total_rows),
                "Missing Count": missing_count,
                "Missing (%)": self._safe_percentage(missing_count, total_rows),
                "Is Unique": distinct_count == total_rows
            }
        return stats_map

    def _calculate_density_data(self, conn: duckdb.DuckDBPyConnection, col_name: str, n_points: int = 100) -> Optional[DensityPlotData]:
        """Calculate kernel density estimation data for a numeric column."""
        try:
            q_col = quote_identifier(col_name)
            
            # Get min, max, and stddev for bandwidth calculation
            stats_query = f"""
                SELECT MIN({q_col}), MAX({q_col}), STDDEV_SAMP({q_col}), COUNT(*)
                FROM dataset 
                WHERE {q_col} IS NOT NULL
            """
            stats = conn.execute(stats_query).fetchone()
            min_val, max_val, std_dev, count = stats
            
            if count < 10 or min_val is None or max_val is None:
                return None
                
            # Calculate bandwidth using Silverman's rule of thumb
            bandwidth = 1.06 * std_dev * (count ** (-1/5))
            
            # Generate evaluation points
            x_range = max_val - min_val
            x_points = [min_val + (i * x_range / (n_points - 1)) for i in range(n_points)]
            
            # Calculate density at each point using Gaussian kernel
            density_query = f"""
                WITH points AS (
                    SELECT unnest(?::DOUBLE[]) as x
                )
                SELECT 
                    p.x,
                    AVG(EXP(-0.5 * POW((p.x - {q_col}) / ?, 2)) / (? * SQRT(2 * PI()))) as density
                FROM points p, dataset
                WHERE {q_col} IS NOT NULL
                GROUP BY p.x
                ORDER BY p.x
            """
            
            density_results = conn.execute(density_query, [x_points, bandwidth, bandwidth]).fetchall()
            
            x_values = [row[0] for row in density_results]
            y_values = [self._clean_numeric_value(row[1]) or 0 for row in density_results]
            
            return DensityPlotData(
                x_values=x_values,
                y_values=y_values,
                label=col_name
            )
            
        except Exception as e:
            logger.warning(f"Failed to calculate density for {col_name}: {e}")
            return None

    def _calculate_calendar_heatmap_data(self, conn: duckdb.DuckDBPyConnection, col_name: str, date_range: Dict) -> Optional[CalendarHeatmapData]:
        """Calculate calendar heatmap data for a datetime column."""
        try:
            q_col = quote_identifier(col_name)
            
            # Get daily counts
            daily_query = f"""
                SELECT 
                    DATE_TRUNC('day', {q_col}) as date,
                    COUNT(*) as count
                FROM dataset
                WHERE {q_col} IS NOT NULL
                GROUP BY 1
                ORDER BY 1
            """
            
            daily_results = conn.execute(daily_query).fetchall()
            
            if not daily_results:
                return None
            
            # Format data for calendar heatmap
            data = [
                {"date": str(row[0]), "value": row[1]}
                for row in daily_results
            ]
            
            # Use actual date range from data
            start_date = str(daily_results[0][0])
            end_date = str(daily_results[-1][0])
            
            return CalendarHeatmapData(
                data=data,
                start_date=start_date,
                end_date=end_date
            )
            
        except Exception as e:
            logger.warning(f"Failed to calculate calendar heatmap for {col_name}: {e}")
            return None

    def _calculate_qq_plot_data(self, conn: duckdb.DuckDBPyConnection, col_name: str) -> Optional[QQPlotData]:
        """Calculate Q-Q plot data for normality testing."""
        try:
            q_col = quote_identifier(col_name)
            
            # Get quantiles from the data
            quantile_points = [i/100.0 for i in range(1, 100)]  # 1% to 99%
            
            sample_quantiles_query = f"""
                SELECT unnest(quantile_disc({q_col}, ?::DOUBLE[]))
                FROM dataset
                WHERE {q_col} IS NOT NULL
            """
            
            sample_quantiles_result = conn.execute(sample_quantiles_query, [quantile_points]).fetchall()
            sample_quantiles = [self._clean_numeric_value(row[0]) for row in sample_quantiles_result if row[0] is not None]
            
            if len(sample_quantiles) < 10:
                return None
            
            # Calculate theoretical normal quantiles
            # Using inverse normal CDF approximation
            from math import sqrt, log, pi
            
            def norm_ppf(p):
                """Approximate inverse normal CDF."""
                if p <= 0 or p >= 1:
                    return None
                # Simplified approximation
                return sqrt(2) * sqrt(-log(4 * p * (1 - p))) * (1 if p > 0.5 else -1)
            
            theoretical_quantiles = [norm_ppf(p) for p in quantile_points[:len(sample_quantiles)]]
            
            # Calculate reference line (through Q1 and Q3)
            q1_idx = int(0.25 * len(sample_quantiles))
            q3_idx = int(0.75 * len(sample_quantiles))
            
            if q1_idx < len(sample_quantiles) and q3_idx < len(sample_quantiles):
                slope = (sample_quantiles[q3_idx] - sample_quantiles[q1_idx]) / (theoretical_quantiles[q3_idx] - theoretical_quantiles[q1_idx])
                intercept = sample_quantiles[q1_idx] - slope * theoretical_quantiles[q1_idx]
                reference_line = {"slope": slope, "intercept": intercept}
            else:
                reference_line = None
            
            return QQPlotData(
                theoretical_quantiles=theoretical_quantiles,
                sample_quantiles=sample_quantiles,
                reference_line=reference_line
            )
            
        except Exception as e:
            logger.warning(f"Failed to calculate Q-Q plot for {col_name}: {e}")
            return None

    def _execute_batch_numeric_stats(self, conn: duckdb.DuckDBPyConnection, columns: List[Dict], total_rows: int) -> Dict:
        if not columns: return {}

        query_parts = []
        for col in columns:
            q_col = quote_identifier(col['name'])
            query_parts.append(f"""
                SELECT
                    '{col['name']}' as col_name,
                    AVG({q_col}), MEDIAN({q_col}), STDDEV_SAMP({q_col}), MIN({q_col}), MAX({q_col}),
                    SKEWNESS({q_col}), KURTOSIS({q_col}),
                    QUANTILE_DISC({q_col}, 0.05), QUANTILE_DISC({q_col}, 0.25), QUANTILE_DISC({q_col}, 0.75), QUANTILE_DISC({q_col}, 0.95),
                    SUM(CASE WHEN {q_col} = 0 THEN 1 ELSE 0 END),
                    HISTOGRAM({q_col})
                FROM dataset
            """)
        
        query = "\nUNION ALL\n".join(query_parts)
        results = conn.execute(query).fetchall()

        stats_map = {}
        for row in results:
            col_name = row[0]
            zeros_count = row[12] or 0

            histogram_data = None
            try:
                hist_raw = row[13]
                if hist_raw:
                    bins = []
                    for bin_range, count in hist_raw.items():
                        min_v, max_v = self._parse_histogram_bin(bin_range)
                        # Skip invalid bins
                        if min_v is None or max_v is None:
                            continue
                        # Ensure count is valid
                        try:
                            count = int(count) if count is not None else 0
                        except (ValueError, TypeError):
                            count = 0
                        bins.append(HistogramBin(min=min_v, max=max_v, count=count))
                    if bins:  # Only create histogram if we have valid bins
                        histogram_data = HistogramData(bins=bins, total_count=total_rows)
            except Exception as e:
                logger.warning(f"Failed to parse histogram for {col_name}: {e}")

            # Clean all numeric values FIRST
            mean_val = self._clean_numeric_value(row[1])
            median_val = self._clean_numeric_value(row[2])
            std_val = self._clean_numeric_value(row[3])
            min_val = self._clean_numeric_value(row[4])
            max_val = self._clean_numeric_value(row[5])
            skew_val = self._clean_numeric_value(row[6])
            kurt_val = self._clean_numeric_value(row[7])
            p5 = self._clean_numeric_value(row[8])
            q1 = self._clean_numeric_value(row[9])
            q3 = self._clean_numeric_value(row[10])
            p95 = self._clean_numeric_value(row[11])
            
            # Safely calculate derived stats AFTER cleaning
            iqr = None
            range_val = None
            
            if q1 is not None and q3 is not None:
                iqr = q3 - q1
                
            if min_val is not None and max_val is not None:
                range_val = max_val - min_val
            
            stats_map[col_name] = {
                "descriptive": {
                    "Mean": round(mean_val, 4) if mean_val is not None else None,
                    "Median": round(median_val, 4) if median_val is not None else None,
                    "Std Dev": round(std_val, 4) if std_val is not None else None,
                    "Min": min_val, "Max": max_val, "Range": round(range_val, 4) if range_val is not None else None,
                    "Skewness": round(skew_val, 4) if skew_val is not None else None,
                    "Kurtosis": round(kurt_val, 4) if kurt_val is not None else None,
                },
                "quantile": {
                    "5th Percentile": p5,
                    "Q1 (25th Percentile)": q1,
                    "Median (50th Percentile)": median_val,
                    "Q3 (75th Percentile)": q3,
                    "95th Percentile": p95,
                    "IQR": round(iqr, 4) if iqr is not None else None
                },
                "zeros": {
                    "Zeros Count": zeros_count,
                    "Zeros (%)": self._safe_percentage(zeros_count, total_rows),
                },
                "histogram": histogram_data
            }
        return stats_map
    
    def _execute_batch_categorical_freq(self, conn: duckdb.DuckDBPyConnection, columns: List[Dict], total_rows: int, limit: int) -> Dict:
        if not columns: return {}
        
        query_parts = []
        for col in columns:
            q_col = quote_identifier(col['name'])
            query_parts.append(f"SELECT '{col['name']}' as col_name, CAST({q_col} AS VARCHAR) as value, COUNT(*) as frequency FROM dataset GROUP BY 1, 2")
        
        full_query = f"""
            WITH all_freqs AS ({' UNION ALL '.join(query_parts)}),
            ranked_freqs AS (
                SELECT *, ROW_NUMBER() OVER(PARTITION BY col_name ORDER BY frequency DESC) as rn
                FROM all_freqs
            )
            SELECT col_name, value, frequency
            FROM ranked_freqs WHERE rn <= {limit}
        """
        results = conn.execute(full_query).fetchall()

        freq_map = defaultdict(lambda: {'rows': []})
        for col_name, value, frequency in results:
            freq_map[col_name]['rows'].append([value, frequency, self._safe_percentage(frequency, total_rows)])

        processed_map = {}
        for col_name, data in freq_map.items():
            table = TableData(columns=["Value", "Frequency", "Percentage (%)"], rows=data['rows'])
            
            top_10_rows = data['rows'][:10]
            categories = [str(row[0]) if row[0] is not None else "NULL" for row in top_10_rows]
            values = [row[1] for row in top_10_rows]
            chart = BarChartData(categories=categories, values=values)
            processed_map[col_name] = {'table': table, 'chart': chart}

        return processed_map
    
    def _execute_batch_datetime_stats(self, conn: duckdb.DuckDBPyConnection, columns: List[Dict], total_rows: int) -> Dict:
        if not columns: return {}
        
        query_parts = []
        for col in columns:
            q_col = quote_identifier(col['name'])
            query_parts.append(f"""
                SELECT
                    '{col['name']}' as col_name,
                    MIN({q_col}) as min_date,
                    MAX({q_col}) as max_date,
                    COUNT(DISTINCT DATE_TRUNC('day', {q_col})) as unique_days,
                    COUNT(DISTINCT DATE_TRUNC('month', {q_col})) as unique_months,
                    COUNT(DISTINCT DATE_TRUNC('year', {q_col})) as unique_years
                FROM dataset
                WHERE {q_col} IS NOT NULL
            """)
        
        query = "\nUNION ALL\n".join(query_parts)
        results = conn.execute(query).fetchall()
        
        stats_map = {}
        for row in results:
            col_name = row[0]
            min_date = row[1]
            max_date = row[2]
            
            # Calculate date range statistics
            range_stats = {
                "Min Date": str(min_date) if min_date else None,
                "Max Date": str(max_date) if max_date else None,
                "Unique Days": row[3],
                "Unique Months": row[4],
                "Unique Years": row[5]
            }
            
            # Get temporal patterns (day of week, month distribution)
            temporal_data = None
            monthly_data = None
            
            try:
                q_col = quote_identifier(col_name)
                
                # Day of week distribution
                dow_query = f"""
                    SELECT 
                        DAYNAME({q_col}) as day_name,
                        DAYOFWEEK({q_col}) as day_num,
                        COUNT(*) as count
                    FROM dataset
                    WHERE {q_col} IS NOT NULL
                    GROUP BY 1, 2
                    ORDER BY 2
                """
                dow_results = conn.execute(dow_query).fetchall()
                
                if dow_results:
                    temporal_data = TableData(
                        columns=["Day of Week", "Count", "Percentage (%)"],
                        rows=[[day, count, self._safe_percentage(count, total_rows)] 
                              for day, _, count in dow_results]
                    )
                
                # Monthly distribution (for recent data)
                month_query = f"""
                    SELECT 
                        MONTHNAME({q_col}) as month_name,
                        MONTH({q_col}) as month_num,
                        COUNT(*) as count
                    FROM dataset
                    WHERE {q_col} IS NOT NULL
                    GROUP BY 1, 2
                    ORDER BY 2
                    LIMIT 12
                """
                month_results = conn.execute(month_query).fetchall()
                
                if month_results:
                    categories = [row[0] for row in month_results]
                    values = [row[2] for row in month_results]
                    monthly_data = BarChartData(categories=categories, values=values)
                    
            except Exception as e:
                logger.warning(f"Failed to get temporal patterns for {col_name}: {e}")
            
            stats_map[col_name] = {
                'range': range_stats,
                'temporal_patterns': temporal_data,
                'monthly_distribution': monthly_data
            }
            
        return stats_map
    
    def _execute_batch_boolean_stats(self, conn: duckdb.DuckDBPyConnection, columns: List[Dict], total_rows: int) -> Dict:
        if not columns: return {}
        
        query_parts = []
        for col in columns:
            q_col = quote_identifier(col['name'])
            query_parts.append(f"""
                SELECT
                    '{col['name']}' as col_name,
                    SUM(CASE WHEN {q_col} = true THEN 1 ELSE 0 END) as true_count,
                    SUM(CASE WHEN {q_col} = false THEN 1 ELSE 0 END) as false_count,
                    SUM(CASE WHEN {q_col} IS NULL THEN 1 ELSE 0 END) as null_count
                FROM dataset
            """)
        
        query = "\nUNION ALL\n".join(query_parts)
        results = conn.execute(query).fetchall()
        
        stats_map = {}
        for row in results:
            col_name = row[0]
            true_count = row[1] or 0
            false_count = row[2] or 0
            null_count = row[3] or 0
            
            non_null_total = true_count + false_count
            
            distribution = {
                "True Count": true_count,
                "True (%)": self._safe_percentage(true_count, total_rows),
                "False Count": false_count,
                "False (%)": self._safe_percentage(false_count, total_rows),
                "Null Count": null_count,
                "Null (%)": self._safe_percentage(null_count, total_rows),
                "True Ratio (excl. nulls)": self._safe_percentage(true_count, non_null_total)
            }
            
            # Create bar chart data
            chart = BarChartData(
                categories=["True", "False", "Null"],
                values=[true_count, false_count, null_count]
            )
            
            stats_map[col_name] = {
                'distribution': distribution,
                'chart': chart
            }
            
        return stats_map
    
    def _execute_batch_text_stats(self, conn: duckdb.DuckDBPyConnection, columns: List[Dict], total_rows: int) -> Dict:
        if not columns: return {}
        
        query_parts = []
        for col in columns:
            q_col = quote_identifier(col['name'])
            query_parts.append(f"""
                SELECT
                    '{col['name']}' as col_name,
                    MIN(LENGTH({q_col})) as min_length,
                    MAX(LENGTH({q_col})) as max_length,
                    AVG(LENGTH({q_col})) as avg_length,
                    MEDIAN(LENGTH({q_col})) as median_length,
                    COUNT(CASE WHEN {q_col} = '' THEN 1 END) as empty_count,
                    COUNT(CASE WHEN {q_col} IS NULL THEN 1 END) as null_count,
                    COUNT(DISTINCT {q_col}) as distinct_count
                FROM dataset
            """)
        
        query = "\nUNION ALL\n".join(query_parts)
        results = conn.execute(query).fetchall()
        
        stats_map = {}
        for row in results:
            col_name = row[0]
            
            # Clean numeric values
            avg_length = self._clean_numeric_value(row[3])
            
            statistics = {
                "Min Length": int(row[1]) if row[1] is not None else 0,
                "Max Length": int(row[2]) if row[2] is not None else 0,
                "Avg Length": round(avg_length, 2) if avg_length is not None else 0,
                "Median Length": int(row[4]) if row[4] is not None else 0,
                "Empty Strings": row[5] or 0,
                "Empty Strings (%)": self._safe_percentage(row[5] or 0, total_rows),
                "Null Count": row[6] or 0,
                "Null (%)": self._safe_percentage(row[6] or 0, total_rows),
                "Distinct Count": row[7] or 0
            }
            
            # Get length histogram
            length_histogram = None
            try:
                q_col = quote_identifier(col_name)
                hist_query = f"""
                    SELECT HISTOGRAM(LENGTH({q_col}))
                    FROM dataset
                    WHERE {q_col} IS NOT NULL
                """
                hist_result = conn.execute(hist_query).fetchone()[0]
                
                if hist_result:
                    bins = []
                    for bin_range, count in hist_result.items():
                        min_v, max_v = self._parse_histogram_bin(bin_range)
                        bins.append(HistogramBin(min=min_v, max=max_v, count=count))
                    length_histogram = HistogramData(bins=bins, total_count=total_rows - (row[6] or 0))
                    
            except Exception as e:
                logger.warning(f"Failed to get length histogram for {col_name}: {e}")
            
            stats_map[col_name] = {
                'statistics': statistics,
                'length_histogram': length_histogram
            }
            
        return stats_map


    def _analyze_interactions(
            self,
            conn: duckdb.DuckDBPyConnection,
            variables: Dict[str, VariableAnalysis],
            correlation_threshold: float,
            max_pairs: int
    ) -> Tuple[List[AnalysisBlock], List[Tuple[str, str, float]]]:
        """Analyze interactions between variables."""
        blocks = []
        correlation_pairs = []  # Initialize correlation_pairs

        # Get numeric variables
        numeric_vars = [
            name for name, var in variables.items()
            if var.common_info.type == VariableType.NUMERIC
        ]

        if len(numeric_vars) >= 2:
            # Correlation matrix
            correlations = {}

            for i, var1 in enumerate(numeric_vars):
                for j, var2 in enumerate(numeric_vars):
                    if i < j:  # Only calculate upper triangle
                        try:
                            quoted_var1 = quote_identifier(var1)
                            quoted_var2 = quote_identifier(var2)
                            # Add WHERE clause to exclude NULL pairs
                            corr = conn.execute(f"""
                                SELECT CORR({quoted_var1}, {quoted_var2}) 
                                FROM dataset
                                WHERE {quoted_var1} IS NOT NULL AND {quoted_var2} IS NOT NULL
                            """).fetchone()[0]

                            if corr is not None:
                                # Clean the correlation value
                                corr = self._clean_numeric_value(corr)
                                if corr is not None:
                                    correlations[(var1, var2)] = corr
                                    if abs(corr) >= correlation_threshold:
                                        correlation_pairs.append((var1, var2, corr))
                        except Exception as e:
                            logger.warning(f"Failed to calculate correlation between {var1} and {var2}: {e}")

            # Sort by absolute correlation
            correlation_pairs.sort(key=lambda x: abs(x[2]), reverse=True)

            # Create correlation heatmap data
            if correlations:
                # Build full matrix
                matrix_size = len(numeric_vars)
                matrix = [[1.0 if i == j else 0.0 for j in range(matrix_size)] for i in range(matrix_size)]

                for (var1, var2), corr in correlations.items():
                    i = numeric_vars.index(var1)
                    j = numeric_vars.index(var2)
                    matrix[i][j] = corr
                    matrix[j][i] = corr  # Symmetric

                # Clean matrix values
                cleaned_matrix = []
                for row in matrix:
                    cleaned_row = [self._clean_numeric_value(val) if val != 1.0 and val != 0.0 else val for val in row]
                    cleaned_matrix.append(cleaned_row)
                
                blocks.append(AnalysisBlock(
                    title="Correlation Matrix",
                    render_as=RenderType.HEATMAP,
                    data=HeatmapData(
                        row_labels=numeric_vars,
                        col_labels=numeric_vars,
                        values=cleaned_matrix,
                        min_value=-1.0,
                        max_value=1.0
                    ),
                    description="Pearson correlation coefficients between numeric variables"
                ))

            # Network Graph for correlation visualization
            if correlations and len(numeric_vars) > 2:
                # Create network graph data
                nodes = [{"id": var, "label": var, "group": "numeric"} for var in numeric_vars]
                edges = []
                
                for (var1, var2), corr in correlations.items():
                    if abs(corr) >= 0.3:  # Only show meaningful correlations
                        edges.append({
                            "source": var1,
                            "target": var2,
                            "weight": abs(corr),
                            "value": round(corr, 3),
                            "color": "red" if corr < 0 else "green"
                        })
                
                if edges:
                    blocks.append(AnalysisBlock(
                        title="Correlation Network",
                        render_as=RenderType.NETWORK_GRAPH,
                        data=NetworkGraphData(nodes=nodes, edges=edges),
                        description="Visual network of correlations (|r| >= 0.3)"
                    ))
            
            # Parallel Coordinates for multivariate analysis
            if len(numeric_vars) >= 3:
                # Sample data for parallel coordinates (limit to prevent overwhelming the visualization)
                sample_size = min(1000, conn.execute("SELECT COUNT(*) FROM dataset").fetchone()[0])
                pc_vars = numeric_vars[:8]  # Limit to 8 dimensions for readability
                
                # Build query to get sampled data
                quoted_vars = [quote_identifier(var) for var in pc_vars]
                select_cols = ", ".join(quoted_vars)
                
                # Get min/max for each variable for normalization
                stats_query = " UNION ALL ".join([
                    f"SELECT '{var}' as var_name, MIN({quoted}) as min_val, MAX({quoted}) as max_val FROM dataset"
                    for var, quoted in zip(pc_vars, quoted_vars)
                ])
                
                stats_results = conn.execute(stats_query).fetchall()
                var_stats = {row[0]: {"min": row[1], "max": row[2]} for row in stats_results}
                
                # Sample data
                sample_query = f"""
                    SELECT {select_cols}
                    FROM dataset
                    WHERE {' AND '.join([f'{col} IS NOT NULL' for col in quoted_vars])}
                    ORDER BY RANDOM()
                    LIMIT {sample_size}
                """
                
                sample_data = conn.execute(sample_query).fetchall()
                
                if sample_data:
                    # Prepare dimensions info
                    dimensions = []
                    for i, var in enumerate(pc_vars):
                        min_val = var_stats[var]["min"]
                        max_val = var_stats[var]["max"]
                        values = [row[i] for row in sample_data if row[i] is not None]
                        
                        dimensions.append({
                            "label": var,
                            "values": values,
                            "min": self._clean_numeric_value(min_val),
                            "max": self._clean_numeric_value(max_val)
                        })
                    
                    # Prepare data rows
                    data_rows = []
                    for row in sample_data[:500]:  # Limit rows for performance
                        row_data = {}
                        for i, var in enumerate(pc_vars):
                            if row[i] is not None:
                                row_data[var] = self._clean_numeric_value(row[i])
                        if len(row_data) == len(pc_vars):  # Only include complete rows
                            data_rows.append(row_data)
                    
                    if data_rows:
                        blocks.append(AnalysisBlock(
                            title="Parallel Coordinates",
                            render_as=RenderType.PARALLEL_COORDINATES,
                            data=ParallelCoordinatesData(
                                dimensions=dimensions,
                                data=data_rows
                            ),
                            description=f"Multivariate patterns across {len(pc_vars)} numeric variables (sample of {len(data_rows)} rows)"
                        ))
            
            # High correlations table
            if correlation_pairs:
                high_corr_data = []
                for var1, var2, corr in correlation_pairs[:max_pairs]:
                    high_corr_data.append([var1, var2, round(corr, 4)])

                blocks.append(AnalysisBlock(
                    title="High Correlations",
                    render_as=RenderType.TABLE,
                    data=TableData(
                        columns=["Variable 1", "Variable 2", "Correlation"],
                        rows=high_corr_data
                    ),
                    description=f"Variable pairs with |correlation| >= {correlation_threshold}"
                ))

        # Box plots for numeric by categorical
        categorical_vars = [
            name for name, var in variables.items()
            if var.common_info.type == VariableType.CATEGORICAL
        ]

        # Limit the number of box plots to avoid overwhelming output
        box_plot_count = 0
        max_box_plots = 10

        for num_var in numeric_vars[:5]:  # Limit numeric vars
            for cat_var in categorical_vars[:5]:  # Limit categorical vars
                if box_plot_count >= max_box_plots:
                    break

                try:
                    quoted_num = quote_identifier(num_var)
                    quoted_cat = quote_identifier(cat_var)

                    # First check cardinality of categorical variable
                    cardinality_check = conn.execute(f"""
                        SELECT COUNT(DISTINCT {quoted_cat}) as cardinality
                        FROM dataset
                    """).fetchone()[0]

                    # Skip if too many categories
                    if cardinality_check > 20:
                        continue

                    # Get box plot statistics for each category
                    box_plot_query = f"""
                        SELECT
                            {quoted_cat} as category,
                            MIN({quoted_num}) as min_val,
                            QUANTILE_DISC({quoted_num}, 0.25) as q1,
                            QUANTILE_DISC({quoted_num}, 0.5) as median,
                            QUANTILE_DISC({quoted_num}, 0.75) as q3,
                            MAX({quoted_num}) as max_val,
                            COUNT(*) as count
                        FROM dataset
                        WHERE {quoted_num} IS NOT NULL AND {quoted_cat} IS NOT NULL
                        GROUP BY {quoted_cat}
                        HAVING COUNT(*) >= 5  -- Need at least 5 values for meaningful box plot
                        ORDER BY COUNT(*) DESC
                        LIMIT 10  -- Limit number of categories shown
                    """

                    box_plot_result = conn.execute(box_plot_query).fetchall()

                    if box_plot_result and len(box_plot_result) >= 2:  # Need at least 2 categories
                        categories = []
                        data = []

                        for row in box_plot_result:
                            category = str(row[0]) if row[0] is not None else "NULL"
                            categories.append(category)

                            # Calculate outlier bounds
                            q1 = row[2]
                            q3 = row[4]
                            iqr = q3 - q1 if q1 is not None and q3 is not None else 0
                            lower_bound = q1 - 1.5 * iqr if q1 is not None else row[1]
                            upper_bound = q3 + 1.5 * iqr if q3 is not None else row[5]

                            # Get outliers
                            outliers = []
                            if lower_bound is not None and upper_bound is not None:
                                try:
                                    # Handle NULL categories properly
                                    if row[0] is None:
                                        outlier_query = f"""
                                            SELECT {quoted_num}
                                            FROM dataset
                                            WHERE {quoted_cat} IS NULL
                                            AND {quoted_num} IS NOT NULL
                                            AND ({quoted_num} < ? OR {quoted_num} > ?)
                                            LIMIT 100  -- Limit outliers for performance
                                        """
                                        params = [lower_bound, upper_bound]
                                    else:
                                        outlier_query = f"""
                                            SELECT {quoted_num}
                                            FROM dataset
                                            WHERE {quoted_cat} = ?
                                            AND {quoted_num} IS NOT NULL
                                            AND ({quoted_num} < ? OR {quoted_num} > ?)
                                            LIMIT 100  -- Limit outliers for performance
                                        """
                                        params = [row[0], lower_bound, upper_bound]
                                    
                                    outlier_results = conn.execute(outlier_query, params).fetchall()
                                    
                                    # Convert results to list of floats and clean them
                                    outliers = [self._clean_numeric_value(float(item[0])) for item in outlier_results if item[0] is not None]
                                except Exception as e:
                                    logger.warning(f"Could not calculate outliers for {num_var} by {cat_var}: {e}")
                                    outliers = []

                            # Clean all box plot values
                            data.append({
                                "min": self._clean_numeric_value(row[1]) if row[1] is not None else 0,
                                "q1": self._clean_numeric_value(row[2]) if row[2] is not None else 0,
                                "median": self._clean_numeric_value(row[3]) if row[3] is not None else 0,
                                "q3": self._clean_numeric_value(row[4]) if row[4] is not None else 0,
                                "max": self._clean_numeric_value(row[5]) if row[5] is not None else 0,
                                "outliers": [self._clean_numeric_value(v) for v in outliers]
                            })

                        blocks.append(AnalysisBlock(
                            title=f"{num_var} by {cat_var}",
                            render_as=RenderType.BOX_PLOT,
                            data=BoxPlotData(
                                categories=categories,
                                data=data
                            ),
                            description=f"Distribution of {num_var} across {cat_var} categories"
                        ))

                        box_plot_count += 1

                except Exception as e:
                    logger.warning(f"Failed to generate box plot for {num_var} by {cat_var}: {e}")

            if box_plot_count >= max_box_plots:
                break

        # Stacked Bar Charts for Categorical vs Categorical interactions
        if len(categorical_vars) >= 2:
            stacked_bar_count = 0
            max_stacked_bars = 5
            
            for i, cat1 in enumerate(categorical_vars[:5]):
                for j, cat2 in enumerate(categorical_vars[:5]):
                    if i >= j or stacked_bar_count >= max_stacked_bars:
                        continue
                    
                    try:
                        quoted_cat1 = quote_identifier(cat1)
                        quoted_cat2 = quote_identifier(cat2)
                        
                        # Check cardinality of both variables
                        cardinality_query = f"""
                            SELECT 
                                COUNT(DISTINCT {quoted_cat1}) as card1,
                                COUNT(DISTINCT {quoted_cat2}) as card2
                            FROM dataset
                        """
                        card1, card2 = conn.execute(cardinality_query).fetchone()
                        
                        # Skip if too many categories
                        if card1 > 10 or card2 > 10:
                            continue
                        
                        # Get cross-tabulation data
                        crosstab_query = f"""
                            SELECT 
                                {quoted_cat1} as cat1,
                                {quoted_cat2} as cat2,
                                COUNT(*) as count
                            FROM dataset
                            WHERE {quoted_cat1} IS NOT NULL AND {quoted_cat2} IS NOT NULL
                            GROUP BY 1, 2
                            ORDER BY 1, 2
                        """
                        
                        crosstab_result = conn.execute(crosstab_query).fetchall()
                        
                        if crosstab_result:
                            # Organize data for stacked bar chart
                            categories_set = set()
                            series_dict = defaultdict(dict)
                            
                            for row in crosstab_result:
                                cat1_val = str(row[0]) if row[0] is not None else "NULL"
                                cat2_val = str(row[1]) if row[1] is not None else "NULL"
                                count = row[2]
                                
                                categories_set.add(cat1_val)
                                series_dict[cat2_val][cat1_val] = count
                            
                            categories = sorted(list(categories_set))
                            series = []
                            
                            for cat2_val, cat1_counts in series_dict.items():
                                values = [cat1_counts.get(cat1, 0) for cat1 in categories]
                                series.append({
                                    "name": cat2_val,
                                    "values": values
                                })
                            
                            blocks.append(AnalysisBlock(
                                title=f"{cat1} by {cat2}",
                                render_as=RenderType.STACKED_BAR_CHART,
                                data=StackedBarChartData(
                                    categories=categories,
                                    series=series
                                ),
                                description=f"Distribution of {cat2} within {cat1} categories"
                            ))
                            
                            stacked_bar_count += 1
                            
                    except Exception as e:
                        logger.warning(f"Failed to generate stacked bar chart for {cat1} by {cat2}: {e}")

        return blocks, correlation_pairs

    def _analyze_missing_values(
            self,
            column_info: List[Dict],
            total_rows: int,
            common_stats_results: Dict[str, Dict]
    ) -> Tuple[List[AnalysisBlock], int]:
        """Analyze missing value patterns using already-computed common stats."""
        blocks = []
        
        total_cells = total_rows * len(column_info)
        total_missing = 0
        missing_stats = []
        missing_by_type = defaultdict(int)

        # Use missing counts from common_stats_results instead of re-querying
        for col in column_info:
            col_name = col['name']
            if col_name in common_stats_results:
                missing_count = common_stats_results[col_name].get("Missing Count", 0)
                missing_percent = common_stats_results[col_name].get("Missing (%)", 0)
                total_missing += missing_count

                if missing_count > 0:
                    missing_stats.append({
                        "column": col_name,
                        "missing_count": missing_count,
                        "missing_percent": missing_percent,
                        "type": self._categorize_dtype(col["type"])
                    })
                    # Track missing by variable type
                    var_type = self._categorize_dtype(col["type"])
                    if var_type != VariableType.UNKNOWN:
                        missing_by_type[var_type.value] += missing_count

        # Overall data completeness gauge
        completeness_percent = self._safe_percentage(total_cells - total_missing, total_cells)
        blocks.append(AnalysisBlock(
            title="Overall Data Completeness",
            render_as=RenderType.GAUGE_CHART,
            data=GaugeChartData(
                value=completeness_percent,
                min_value=0,
                max_value=100,
                thresholds=[
                    {"value": 70, "color": "yellow"},
                    {"value": 90, "color": "green"}
                ],
                label=f"{completeness_percent:.1f}% Complete"
            ),
            description=f"{total_missing:,} missing values out of {total_cells:,} total cells"
        ))

        # Missing values by variable type - Stacked bar chart
        if missing_by_type:
            blocks.append(AnalysisBlock(
                title="Missing Values by Data Type",
                render_as=RenderType.HORIZONTAL_BAR_CHART,
                data=BarChartData(
                    categories=list(missing_by_type.keys()),
                    values=list(missing_by_type.values())
                ),
                description="Distribution of missing values across variable types"
            ))

        # Top columns with missing - Enhanced visualization
        if missing_stats:
            missing_stats.sort(key=lambda x: x["missing_percent"], reverse=True)
            
            # Horizontal bar chart for top 10 columns with most missing
            top_missing = missing_stats[:10]
            blocks.append(AnalysisBlock(
                title="Top Columns with Missing Data",
                render_as=RenderType.HORIZONTAL_BAR_CHART,
                data=BarChartData(
                    categories=[item["column"] for item in top_missing],
                    values=[item["missing_percent"] for item in top_missing]
                ),
                description="Top 10 columns by missing percentage"
            ))

            # Missing data matrix visualization (if not too many columns)
            if len(column_info) <= 50 and total_rows <= 1000:
                # Create a sample matrix showing missing patterns
                matrix_cols = [col["name"] for col in column_info[:20]]  # Limit columns
                sample_size = min(100, total_rows)  # Sample rows
                
                blocks.append(AnalysisBlock(
                    title="Missing Data Pattern",
                    render_as=RenderType.MATRIX,
                    data=MatrixData(
                        columns=matrix_cols,
                        rows=[],  # Would need actual data sampling
                        row_indices=list(range(sample_size))
                    ),
                    description=f"Missing data patterns (sample of {sample_size} rows)"
                ))

            # Detailed table for reference
            columns = ["Column", "Type", "Missing Count", "Missing %"]
            rows = [[item["column"], item["type"].value if item["type"] != VariableType.UNKNOWN else "Unknown", 
                     item["missing_count"], item["missing_percent"]]
                    for item in missing_stats[:20]]

            blocks.append(AnalysisBlock(
                title="Missing Values Details",
                render_as=RenderType.TABLE,
                data=TableData(columns=columns, rows=rows),
                description="Detailed missing value statistics by column"
            ))

        return blocks, total_missing

    def _detect_alerts(
            self,
            variables: Dict[str, VariableAnalysis],
            total_rows: int,
            correlation_pairs: List[Tuple[str, str, float]],
            duplicate_rows: int,
            alert_config: AlertConfig
    ) -> List[Alert]:
        """Detect data quality issues and anomalies."""
        alerts = []

        # Check each variable for issues
        for var_name, var_analysis in variables.items():
            # Extract statistics from analysis blocks
            stats = {}
            for analysis in var_analysis.analyses:
                if analysis.render_as == RenderType.KEY_VALUE_PAIRS and isinstance(analysis.data, dict):
                    stats.update(analysis.data)

            # High cardinality check (for non-numeric)
            if var_analysis.common_info.type == VariableType.CATEGORICAL:
                distinct_ratio = stats.get("Distinct (%)", 0) / 100.0
                if distinct_ratio > alert_config.high_cardinality_threshold:
                    alerts.append(Alert(
                        column=var_name,
                        alert_type="high_cardinality",
                        severity=AlertSeverity.WARNING,
                        message=f"High cardinality detected: {stats.get('Distinct Count', 0)} unique values ({stats.get('Distinct (%)', 0)}%)",
                        details={"distinct_ratio": distinct_ratio}
                    ))

            # Missing values check
            missing_percent = stats.get("Missing (%)", 0)
            if missing_percent > alert_config.high_missing_threshold:
                severity = AlertSeverity.ERROR if missing_percent > alert_config.error_missing_threshold else AlertSeverity.WARNING
                alerts.append(Alert(
                    column=var_name,
                    alert_type="missing_values",
                    severity=severity,
                    message=f"High percentage of missing values: {missing_percent}%",
                    details={"missing_percent": missing_percent}
                ))

            # Constant and nearly-constant value check
            distinct_count = stats.get("Distinct Count", 0)
            distinct_ratio = stats.get("Distinct (%)", 0) / 100.0

            if distinct_count == 1:
                alerts.append(Alert(
                    column=var_name,
                    alert_type="constant_value",
                    severity=AlertSeverity.INFO,
                    message="Column contains only one unique value",
                    details={"is_constant": True, "distinct_count": distinct_count}
                ))
            elif distinct_ratio < alert_config.nearly_constant_threshold and distinct_count > 1:
                alerts.append(Alert(
                    column=var_name,
                    alert_type="nearly_constant",
                    severity=AlertSeverity.WARNING,
                    message=f"Column is nearly constant: only {distinct_count} unique values ({stats.get('Distinct (%)', 0)}%)",
                    details={"distinct_ratio": distinct_ratio, "distinct_count": distinct_count}
                ))

            # Numeric-specific checks
            if var_analysis.common_info.type == VariableType.NUMERIC:
                # High zeros check
                zeros_percent = stats.get("Zeros (%)", 0)
                if zeros_percent > alert_config.high_zeros_threshold:
                    alerts.append(Alert(
                        column=var_name,
                        alert_type="high_zeros",
                        severity=AlertSeverity.WARNING,
                        message=f"High percentage of zero values: {zeros_percent}%",
                        details={"zeros_percent": zeros_percent}
                    ))

                # Skewness check
                skewness = stats.get("Skewness")
                if skewness is not None and abs(skewness) > alert_config.high_skewness_threshold:
                    alerts.append(Alert(
                        column=var_name,
                        alert_type="high_skewness",
                        severity=AlertSeverity.INFO,
                        message=f"Highly skewed distribution: skewness = {skewness}",
                        details={"skewness": skewness}
                    ))

        # High correlation alerts
        for var1, var2, corr in correlation_pairs:
            if abs(corr) > alert_config.high_correlation_threshold:
                alerts.append(Alert(
                    column=f"{var1}, {var2}",
                    alert_type="high_correlation",
                    severity=AlertSeverity.WARNING,
                    message=f"High correlation between '{var1}' and '{var2}': {corr:.2f}",
                    details={"correlation": corr, "var1": var1, "var2": var2}
                ))

        # Dataset-level alerts - check for duplicate rows
        duplicate_percent = self._safe_percentage(duplicate_rows, total_rows)

        if duplicate_percent > alert_config.duplicate_rows_threshold:
            alerts.append(Alert(
                column=None,
                alert_type="duplicate_rows",
                severity=AlertSeverity.WARNING,
                message=f"High percentage of duplicate rows: {duplicate_rows} ({duplicate_percent:.2f}%)",
                details={
                    "duplicate_rows": duplicate_rows,
                    "duplicate_percent": duplicate_percent
                }
            ))

        return alerts

    def _categorize_dtype(self, dtype: str) -> VariableType:
        """Categorize DuckDB data type into variable type."""
        dtype_upper = dtype.upper()

        # Check for complex/nested types first - these should be skipped
        if any(t in dtype_upper for t in ['STRUCT', 'MAP', 'LIST', 'ARRAY', 'UNION', 'JSON']):
            logger.warning(f"Complex/nested type '{dtype}' detected. These types are not currently supported for analysis.")
            return VariableType.UNKNOWN
        elif any(t in dtype_upper for t in
               ['INT', 'FLOAT', 'DOUBLE', 'DECIMAL', 'NUMERIC', 'BIGINT', 'SMALLINT', 'TINYINT', 
                'REAL', 'HUGEINT', 'UBIGINT', 'UINTEGER', 'UTINYINT', 'USMALLINT']):
            return VariableType.NUMERIC
        elif any(t in dtype_upper for t in ['DATE', 'TIME', 'TIMESTAMP', 'INTERVAL']):
            return VariableType.DATETIME
        elif 'BOOL' in dtype_upper:
            return VariableType.BOOLEAN
        elif any(t in dtype_upper for t in ['VARCHAR', 'CHAR', 'TEXT', 'STRING', 'BLOB']):
            # We'll improve this in _batch_analyze_columns to distinguish text vs categorical
            return VariableType.CATEGORICAL
        elif dtype_upper in ['UUID', 'BIT', 'BITSTRING']:
            # Special string-like types
            return VariableType.CATEGORICAL
        else:
            logger.info(f"Unknown data type '{dtype}' encountered. Skipping analysis for this column.")
            return VariableType.UNKNOWN

    def _parse_histogram_bin(self, bin_str: str, is_datetime: bool = False) -> Tuple[Any, Any]:
        """Parse DuckDB histogram bin string."""
        # DuckDB histogram bins are typically in format "[min, max)"
        # Remove brackets and split
        bin_str = bin_str.strip('[]() ')
        parts = bin_str.split(',')

        if len(parts) >= 2:
            min_val_str = parts[0].strip()
            max_val_str = parts[1].strip()

            if is_datetime:
                # For datetimes, return the strings directly. The frontend will handle parsing.
                return min_val_str, max_val_str

            try:
                # For numeric, convert to float
                min_val = float(min_val_str)
                max_val = float(max_val_str)
                # Handle infinity and NaN
                min_val = self._clean_numeric_value(min_val)
                max_val = self._clean_numeric_value(max_val)
                return min_val, max_val
            except ValueError:
                # Fallback for numeric parsing failure
                return 0.0, 0.0

        # Fallback if split fails
        return (None, None) if is_datetime else (0.0, 0.0)
    
    def _analyze_missing_patterns(self, conn: duckdb.DuckDBPyConnection, column_info: List[Dict], total_rows: int, common_stats_results: Dict[str, Dict]) -> List[AnalysisBlock]:
        """Analyze missing value patterns."""
        blocks = []
        
        # Analyze missing values using provided common_stats_results
        missing_blocks, _ = self._analyze_missing_values(column_info, total_rows, common_stats_results)
        blocks.extend(missing_blocks)
        
        return blocks
    
    def _analyze_alerts(
            self,
            conn: duckdb.DuckDBPyConnection,
            column_info: List[Dict],
            total_rows: int,
            variables: Dict[str, VariableAnalysis],
            duplicate_rows: int,
            correlation_pairs: List[Tuple[str, str, float]],
            alert_config: AlertConfig
    ) -> List[Alert]:
        """Wrapper for _detect_alerts to match the streaming interface."""
        return self._detect_alerts(variables, total_rows, correlation_pairs, duplicate_rows, alert_config)
    
    def _clean_numeric_value(self, value: Any) -> Optional[float]:
        """
        Cleans a numeric value to be JSON compliant.
        - Converts None to None.
        - Converts NaN and Infinity to None.
        """
        if value is None:
            return None
        
        try:
            val = float(value)
        except (ValueError, TypeError):
            return None  # Can't be a float, so it's not a numeric value we can clean
        
        if math.isnan(val) or math.isinf(val):
            return None  # Unambiguously represent non-finite numbers as null
        
        return val
    
    def _safe_percentage(self, numerator: Optional[float], denominator: Optional[float]) -> float:
        """Calculate percentage safely, handling None, zero, and negative inputs."""
        # Defensively handle None inputs
        if numerator is None or denominator is None or denominator <= 0 or numerator < 0:
            return 0.0
        
        try:
            result = (numerator / denominator) * 100.0
        except ZeroDivisionError:
            return 0.0
        
        # The result itself could be non-finite (e.g., inf/inf), so clean it
        cleaned_result = self._clean_numeric_value(result)
        
        return round(cleaned_result, 2) if cleaned_result is not None else 0.0
