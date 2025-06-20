import json
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
import time
import duckdb
from sqlalchemy.ext.asyncio import AsyncSession

from app.datasets.exceptions import DatasetNotFound, DatasetVersionNotFound
from app.datasets.repository import DatasetsRepository
from app.explore.eda_models import (
    AnalysisBlock, RenderType, VariableType, VariableInfo, VariableAnalysis,
    Alert, AlertSeverity, EDAMetadata, AnalysisConfig, EDAResponse,
    KeyValueData, TableData, HistogramData, BarChartData, HeatmapData,
    ScatterPlotData, BoxPlotData, MatrixData, HistogramBin, AlertConfig
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

        # Initialize response components
        global_summary = []
        variables = {}
        interactions = []
        alerts = []
        correlation_pairs = []

        # Create DuckDB connection and analyze
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
            column_info = conn.execute("PRAGMA table_info('dataset')").fetchall()
            total_columns = len(column_info)

            # Perform analyses based on config
            duplicate_rows = 0
            if config.global_summary:
                global_summary, duplicate_rows = await self._analyze_global_summary(conn, column_info)

            if config.variables.enabled:
                variables = await self._analyze_variables(
                    conn,
                    column_info,
                    config.variables.limit,
                    config.variables.types,
                    config
                )

            if config.interactions.enabled:
                interactions, correlation_pairs = await self._analyze_interactions(
                    conn,
                    variables,
                    config.interactions.correlation_threshold,
                    config.interactions.max_pairs
                )

            if config.missing_values:
                missing_analysis = await self._analyze_missing_values(conn, column_info)
                if missing_analysis:
                    global_summary.extend(missing_analysis)

            if config.alerts.enabled:
                # Get correlation pairs if interactions were analyzed
                corr_pairs = correlation_pairs if config.interactions.enabled else []
                alerts = await self._detect_alerts(
                    conn,
                    variables,
                    total_rows,
                    corr_pairs,
                    config.interactions.correlation_threshold if config.interactions.enabled else 0.5,
                    duplicate_rows,
                    config.alerts
                )

        # Create metadata
        metadata = EDAMetadata(
            dataset_id=dataset_id,
            version_id=version_id,
            analysis_timestamp=datetime.utcnow(),
            sample_size_used=sample_size_used,
            total_rows=total_rows,
            total_columns=total_columns,
            analysis_duration_seconds=time.time() - start_time
        )

        # Format alerts as analysis blocks
        alert_blocks = []
        if alerts:
            alert_blocks.append(AnalysisBlock(
                title="Data Quality Alerts",
                render_as=RenderType.ALERT_LIST,
                data={"alerts": [alert.dict() for alert in alerts]},
                description="Potential data quality issues detected"
            ))

        return EDAResponse(
            metadata=metadata,
            global_summary=global_summary,
            variables=variables,
            interactions=interactions,
            alerts=alert_blocks
        )

    async def _analyze_global_summary(
            self,
            conn: duckdb.DuckDBPyConnection,
            column_info: List[Tuple]
    ) -> Tuple[List[AnalysisBlock], int]:
        """Analyze dataset-level statistics."""
        blocks = []

        # Dataset statistics
        # Note: COUNT(DISTINCT *) is not supported in DuckDB, so we'll calculate duplicates differently
        row_count = conn.execute("SELECT COUNT(*) FROM dataset").fetchone()[0]

        # Get duplicate count using SHA256 hash for efficiency
        try:
            # Use SHA256 to hash entire rows and count duplicates
            duplicate_query = """
                SELECT COUNT(*) - COUNT(DISTINCT row_hash) as duplicate_count
                FROM (
                    SELECT SHA256(CAST(row(*) AS VARCHAR)) as row_hash
                    FROM dataset
                ) t
            """
            duplicate_result = conn.execute(duplicate_query).fetchone()
            duplicate_rows = duplicate_result[0] if duplicate_result and duplicate_result[0] is not None else 0
        except Exception as e:
            logger.warning(f"Could not calculate duplicate rows using SHA256: {e}")
            # Fallback to the original method if SHA256 fails
            try:
                columns = [col[1] for col in column_info]
                if columns:
                    column_list = ', '.join([quote_identifier(col) for col in columns])
                    duplicate_query = f"""
                        SELECT SUM(cnt - 1)
                        FROM (
                            SELECT COUNT(*) as cnt
                            FROM dataset
                            GROUP BY {column_list}
                            HAVING COUNT(*) > 1
                        ) t
                    """
                    duplicate_result = conn.execute(duplicate_query).fetchone()
                    duplicate_rows = duplicate_result[0] if duplicate_result and duplicate_result[0] is not None else 0
                else:
                    duplicate_rows = 0
            except Exception as e2:
                logger.warning(f"Fallback duplicate calculation also failed: {e2}")
                duplicate_rows = 0

        dataset_stats = {
            "Number of Rows": row_count,
            "Number of Variables": len(column_info),
            "Duplicate Rows": duplicate_rows,
            "Duplicate Rows (%)": round(100.0 * duplicate_rows / row_count, 2) if row_count > 0 else 0
        }

        blocks.append(AnalysisBlock(
            title="Dataset Statistics",
            render_as=RenderType.KEY_VALUE_PAIRS,
            data=dataset_stats
        ))

        # Data type distribution
        type_counts = {}
        for col in column_info:
            dtype = col[1]
            type_category = self._categorize_dtype(dtype)
            type_counts[type_category] = type_counts.get(type_category, 0) + 1

        blocks.append(AnalysisBlock(
            title="Data Type Distribution",
            render_as=RenderType.BAR_CHART,
            data={
                "categories": list(type_counts.keys()),
                "values": list(type_counts.values())
            }
        ))

        # DuckDB SUMMARIZE output - parsed into structured format
        try:
            summarize_result = conn.execute("SUMMARIZE dataset").fetchall()

            # Parse SUMMARIZE output into structured data
            # SUMMARIZE returns rows like: (column_name, column_type, min, max, unique_count, avg, std_dev, q25, q50, q75, count, null_count)
            summarize_data = []
            for row in summarize_result:
                if len(row) >= 2:
                    # Extract column name and type
                    col_name = row[0]
                    col_type = row[1]

                    # Build a dictionary of available statistics
                    col_stats = {
                        "Column": col_name,
                        "Type": col_type
                    }

                    # Add numeric statistics if available (for numeric columns)
                    if len(row) >= 12 and col_type.upper() in ['INTEGER', 'BIGINT', 'DOUBLE', 'FLOAT', 'DECIMAL',
                                                               'NUMERIC']:
                        col_stats.update({
                            "Min": row[2],
                            "Max": row[3],
                            "Unique": row[4],
                            "Avg": round(row[5], 4) if row[5] is not None else None,
                            "Std Dev": round(row[6], 4) if row[6] is not None else None,
                            "Q25": row[7],
                            "Q50": row[8],
                            "Q75": row[9],
                            "Count": row[10],
                            "Nulls": row[11]
                        })
                    # For other types, include what's available
                    elif len(row) >= 6:
                        col_stats.update({
                            "Min": row[2] if len(row) > 2 else None,
                            "Max": row[3] if len(row) > 3 else None,
                            "Unique": row[4] if len(row) > 4 else None,
                            "Count": row[10] if len(row) > 10 else None,
                            "Nulls": row[11] if len(row) > 11 else None
                        })

                    summarize_data.append(col_stats)

            if summarize_data:
                # Create a table with the parsed data
                columns = list(summarize_data[0].keys())
                rows = [[item.get(col) for col in columns] for item in summarize_data]

                blocks.append(AnalysisBlock(
                    title="Quick Summary (DuckDB SUMMARIZE)",
                    render_as=RenderType.TABLE,
                    data=TableData(columns=columns, rows=rows),
                    description="DuckDB's built-in summary statistics"
                ))
            else:
                # Fallback to text if parsing fails
                summarize_text = "\n".join([str(row) for row in summarize_result])
                blocks.append(AnalysisBlock(
                    title="Quick Summary (DuckDB SUMMARIZE)",
                    render_as=RenderType.TEXT_BLOCK,
                    data={"text": summarize_text},
                    description="DuckDB's built-in summary statistics"
                ))
        except Exception as e:
            logger.warning(f"Failed to run SUMMARIZE: {e}")

        return blocks, duplicate_rows

    async def _analyze_variables(
            self,
            conn: duckdb.DuckDBPyConnection,
            column_info: List[Tuple],
            limit: int,
            types_to_include: List[str],
            config: AnalysisConfig
    ) -> Dict[str, VariableAnalysis]:
        """Analyze individual variables."""
        variables = {}
        analyzed_count = 0

        for col in column_info:
            if analyzed_count >= limit:
                break

            col_name = col[1]  # Column name is at index 1
            dtype = col[2]  # Data type is at index 2
            var_type = self._categorize_dtype(dtype)

            # Skip if type not requested
            if var_type.lower() not in [t.lower() for t in types_to_include]:
                continue

            # Create variable info
            var_info = VariableInfo(
                name=col_name,
                type=var_type,
                dtype=dtype
            )

            # Analyze based on type
            analyses = []

            # Common stats for all types
            common_stats = await self._analyze_common_stats(conn, col_name)
            analyses.extend(common_stats)

            # Type-specific analysis
            if var_type == VariableType.NUMERIC:
                numeric_analyses = await self._analyze_numeric_variable(conn, col_name)
                analyses.extend(numeric_analyses)
            elif var_type == VariableType.CATEGORICAL:
                categorical_analyses = await self._analyze_categorical_variable(
                    conn, col_name, config.alerts.frequency_table_limit
                )
                analyses.extend(categorical_analyses)
            elif var_type == VariableType.DATETIME:
                datetime_analyses = await self._analyze_datetime_variable(conn, col_name)
                analyses.extend(datetime_analyses)
            elif var_type == VariableType.TEXT:
                text_analyses = await self._analyze_text_variable(conn, col_name)
                analyses.extend(text_analyses)
            elif var_type == VariableType.BOOLEAN:
                boolean_analyses = await self._analyze_boolean_variable(conn, col_name)
                analyses.extend(boolean_analyses)

            variables[col_name] = VariableAnalysis(
                common_info=var_info,
                analyses=analyses
            )
            analyzed_count += 1

        return variables

    async def _analyze_common_stats(
            self,
            conn: duckdb.DuckDBPyConnection,
            col_name: str
    ) -> List[AnalysisBlock]:
        """Analyze common statistics for any column type."""
        blocks = []

        # Basic counts
        quoted_col = quote_identifier(col_name)
        stats = conn.execute(f"""
            SELECT 
                COUNT(DISTINCT {quoted_col}) as distinct_count,
                COUNT(*) as total_count,
                COUNT(*) - COUNT({quoted_col}) as missing_count
            FROM dataset
        """).fetchone()

        total_count = stats[1]
        distinct_count = stats[0]
        missing_count = stats[2]

        common_stats = {
            "Distinct Count": distinct_count,
            "Distinct (%)": round(100.0 * distinct_count / total_count, 2) if total_count > 0 else 0,
            "Missing Count": missing_count,
            "Missing (%)": round(100.0 * missing_count / total_count, 2) if total_count > 0 else 0,
            "Is Unique": distinct_count == total_count
        }

        blocks.append(AnalysisBlock(
            title="Common Statistics",
            render_as=RenderType.KEY_VALUE_PAIRS,
            data=common_stats
        ))

        return blocks

    async def _analyze_numeric_variable(
            self,
            conn: duckdb.DuckDBPyConnection,
            col_name: str
    ) -> List[AnalysisBlock]:
        """Analyze numeric variable."""
        blocks = []

        # Descriptive statistics
        quoted_col = quote_identifier(col_name)
        desc_stats = conn.execute(f"""
            SELECT 
                AVG({quoted_col}) as mean,
                MEDIAN({quoted_col}) as median,
                STDDEV_SAMP({quoted_col}) as std_dev,
                VAR_SAMP({quoted_col}) as variance,
                MIN({quoted_col}) as min_val,
                MAX({quoted_col}) as max_val,
                MAX({quoted_col}) - MIN({quoted_col}) as range_val,
                SKEWNESS({quoted_col}) as skewness,
                KURTOSIS({quoted_col}) as kurtosis
            FROM dataset
        """).fetchone()

        descriptive = {
            "Mean": round(desc_stats[0], 4) if desc_stats[0] is not None else None,
            "Median": round(desc_stats[1], 4) if desc_stats[1] is not None else None,
            "Std Dev": round(desc_stats[2], 4) if desc_stats[2] is not None else None,
            "Min": desc_stats[4],
            "Max": desc_stats[5],
            "Range": desc_stats[6],
            "Skewness": round(desc_stats[7], 4) if desc_stats[7] is not None else None,
            "Kurtosis": round(desc_stats[8], 4) if desc_stats[8] is not None else None
        }

        blocks.append(AnalysisBlock(
            title="Descriptive Statistics",
            render_as=RenderType.KEY_VALUE_PAIRS,
            data=descriptive
        ))

        # Quantiles
        quantiles = conn.execute(f"""
            SELECT 
                QUANTILE_DISC({quoted_col}, 0.05) as p5,
                QUANTILE_DISC({quoted_col}, 0.25) as q1,
                QUANTILE_DISC({quoted_col}, 0.5) as median,
                QUANTILE_DISC({quoted_col}, 0.75) as q3,
                QUANTILE_DISC({quoted_col}, 0.95) as p95
            FROM dataset
        """).fetchone()

        iqr = quantiles[3] - quantiles[1] if quantiles[3] is not None and quantiles[1] is not None else None

        quantile_stats = {
            "5th Percentile": quantiles[0],
            "Q1 (25th Percentile)": quantiles[1],
            "Median (50th Percentile)": quantiles[2],
            "Q3 (75th Percentile)": quantiles[3],
            "95th Percentile": quantiles[4],
            "IQR": iqr
        }

        blocks.append(AnalysisBlock(
            title="Quantile Statistics",
            render_as=RenderType.KEY_VALUE_PAIRS,
            data=quantile_stats
        ))

        # Zeros count
        zeros_stats = conn.execute(f"""
            SELECT 
                SUM(CASE WHEN {quoted_col} = 0 THEN 1 ELSE 0 END) as zeros_count,
                COUNT(*) as total_count
            FROM dataset
        """).fetchone()

        zeros_count = zeros_stats[0] or 0
        total_count = zeros_stats[1]

        blocks.append(AnalysisBlock(
            title="Zero Values",
            render_as=RenderType.KEY_VALUE_PAIRS,
            data={
                "Zeros Count": zeros_count,
                "Zeros (%)": round(100.0 * zeros_count / total_count, 2) if total_count > 0 else 0
            }
        ))

        # Outlier detection using IQR method
        if quantiles[1] is not None and quantiles[3] is not None and iqr is not None:
            lower_fence = quantiles[1] - 1.5 * iqr
            upper_fence = quantiles[3] + 1.5 * iqr

            outlier_stats = conn.execute(f"""
                SELECT 
                    COUNT(*) as outlier_count,
                    MIN({quoted_col}) as min_outlier,
                    MAX({quoted_col}) as max_outlier
                FROM dataset
                WHERE {quoted_col} < {lower_fence} OR {quoted_col} > {upper_fence}
            """).fetchone()

            outlier_count = outlier_stats[0] or 0
            outlier_percent = round(100.0 * outlier_count / total_count, 2) if total_count > 0 else 0

            outlier_data = {
                "Outlier Count": outlier_count,
                "Outliers (%)": outlier_percent,
                "Lower Fence": round(lower_fence, 4),
                "Upper Fence": round(upper_fence, 4)
            }

            if outlier_count > 0:
                outlier_data["Min Outlier"] = outlier_stats[1]
                outlier_data["Max Outlier"] = outlier_stats[2]

            blocks.append(AnalysisBlock(
                title="Outlier Detection (IQR Method)",
                render_as=RenderType.KEY_VALUE_PAIRS,
                data=outlier_data,
                description="Values outside Q1 - 1.5*IQR and Q3 + 1.5*IQR"
            ))

        # Histogram
        try:
            histogram_result = conn.execute(f'SELECT HISTOGRAM({quoted_col}) FROM dataset').fetchone()[0]

            # Parse DuckDB histogram format
            bins = []
            if histogram_result:
                # DuckDB returns histogram as a map/dict structure
                for bin_range, count in histogram_result.items():
                    # Parse bin range (format: "[min, max)")
                    min_val, max_val = self._parse_histogram_bin(bin_range)
                    bins.append(HistogramBin(min=min_val, max=max_val, count=count))

            blocks.append(AnalysisBlock(
                title="Distribution",
                render_as=RenderType.HISTOGRAM,
                data=HistogramData(
                    bins=bins,
                    total_count=total_count
                )
            ))
        except Exception as e:
            logger.warning(f"Failed to generate histogram for {col_name}: {e}")

        return blocks

    async def _analyze_categorical_variable(
            self,
            conn: duckdb.DuckDBPyConnection,
            col_name: str,
            frequency_limit: int = 20
    ) -> List[AnalysisBlock]:
        """Analyze categorical variable."""
        blocks = []

        # Frequency table
        quoted_col = quote_identifier(col_name)
        freq_result = conn.execute(f"""
            SELECT 
                {quoted_col} as value,
                COUNT(*) as frequency,
                100.0 * COUNT(*) / (SELECT COUNT(*) FROM dataset) as percentage
            FROM dataset
            GROUP BY {quoted_col}
            ORDER BY frequency DESC
            LIMIT {frequency_limit}
        """).fetchall()

        if freq_result:
            columns = ["Value", "Frequency", "Percentage"]
            rows = [[row[0], row[1], round(row[2], 2)] for row in freq_result]

            blocks.append(AnalysisBlock(
                title="Top Values",
                render_as=RenderType.TABLE,
                data=TableData(columns=columns, rows=rows)
            ))

            # Bar chart of top values
            categories = [str(row[0]) if row[0] is not None else "NULL" for row in freq_result[:10]]
            values = [row[1] for row in freq_result[:10]]

            blocks.append(AnalysisBlock(
                title="Top 10 Values Distribution",
                render_as=RenderType.BAR_CHART,
                data=BarChartData(categories=categories, values=values)
            ))

        # String length statistics (if applicable)
        try:
            length_stats = conn.execute(f"""
                SELECT 
                    MIN(LENGTH(CAST({quoted_col} AS VARCHAR))) as min_length,
                    MAX(LENGTH(CAST({quoted_col} AS VARCHAR))) as max_length,
                    AVG(LENGTH(CAST({quoted_col} AS VARCHAR))) as avg_length
                FROM dataset
                WHERE {quoted_col} IS NOT NULL
            """).fetchone()

            if all(v is not None for v in length_stats):
                blocks.append(AnalysisBlock(
                    title="String Length Statistics",
                    render_as=RenderType.KEY_VALUE_PAIRS,
                    data={
                        "Min Length": int(length_stats[0]),
                        "Max Length": int(length_stats[1]),
                        "Avg Length": round(length_stats[2], 2)
                    }
                ))
        except Exception as e:
            logger.debug(f"Could not calculate string length for {col_name}: {e}")

        return blocks

    async def _analyze_datetime_variable(
            self,
            conn: duckdb.DuckDBPyConnection,
            col_name: str
    ) -> List[AnalysisBlock]:
        """Analyze datetime variable."""
        blocks = []

        # Date range
        quoted_col = quote_identifier(col_name)
        date_range = conn.execute(f"""
            SELECT 
                MIN({quoted_col}) as min_date,
                MAX({quoted_col}) as max_date,
                MAX({quoted_col}) - MIN({quoted_col}) as date_range
            FROM dataset
        """).fetchone()

        blocks.append(AnalysisBlock(
            title="Date Range",
            render_as=RenderType.KEY_VALUE_PAIRS,
            data={
                "Earliest Date": str(date_range[0]) if date_range[0] else None,
                "Latest Date": str(date_range[1]) if date_range[1] else None,
                "Range": str(date_range[2]) if date_range[2] else None
            }
        ))

        # Histogram over time
        try:
            histogram_result = conn.execute(f'SELECT HISTOGRAM({quoted_col}) FROM dataset').fetchone()[0]

            bins = []
            if histogram_result:
                for bin_range, count in histogram_result.items():
                    # For datetime, bins are typically date ranges
                    min_val, max_val = self._parse_histogram_bin(bin_range, is_datetime=True)
                    bins.append(HistogramBin(min=min_val, max=max_val, count=count))

            blocks.append(AnalysisBlock(
                title="Distribution Over Time",
                render_as=RenderType.HISTOGRAM,
                data=HistogramData(
                    bins=bins,
                    total_count=conn.execute("SELECT COUNT(*) FROM dataset").fetchone()[0]
                ),
                description="Temporal distribution of values"
            ))
        except Exception as e:
            logger.warning(f"Failed to generate datetime histogram for {col_name}: {e}")

        return blocks

    async def _analyze_text_variable(
            self,
            conn: duckdb.DuckDBPyConnection,
            col_name: str
    ) -> List[AnalysisBlock]:
        """Analyze text variable."""
        blocks = []

        # Basic text statistics
        quoted_col = quote_identifier(col_name)
        text_stats = conn.execute(f"""
            SELECT 
                MIN(LENGTH({quoted_col})) as min_length,
                MAX(LENGTH({quoted_col})) as max_length,
                AVG(LENGTH({quoted_col})) as avg_length,
                COUNT(DISTINCT {quoted_col}) as unique_values,
                COUNT(*) as total_values
            FROM dataset
            WHERE {quoted_col} IS NOT NULL
        """).fetchone()

        blocks.append(AnalysisBlock(
            title="Text Statistics",
            render_as=RenderType.KEY_VALUE_PAIRS,
            data={
                "Min Length": int(text_stats[0]) if text_stats[0] is not None else 0,
                "Max Length": int(text_stats[1]) if text_stats[1] is not None else 0,
                "Avg Length": round(text_stats[2], 2) if text_stats[2] is not None else 0,
                "Unique Values": text_stats[3],
                "Total Values": text_stats[4]
            }
        ))

        # Word statistics (simplified)
        try:
            word_stats = conn.execute(f"""
                SELECT 
                    AVG(ARRAY_LENGTH(STR_SPLIT({quoted_col}, ' '))) as avg_words
                FROM dataset
                WHERE {quoted_col} IS NOT NULL
            """).fetchone()

            if word_stats[0] is not None:
                blocks.append(AnalysisBlock(
                    title="Word Statistics",
                    render_as=RenderType.KEY_VALUE_PAIRS,
                    data={
                        "Average Words": round(word_stats[0], 2)
                    }
                ))
        except Exception as e:
            logger.debug(f"Could not calculate word statistics for {col_name}: {e}")

        # Sample values
        samples = conn.execute(f"""
            SELECT DISTINCT {quoted_col}
            FROM dataset
            WHERE {quoted_col} IS NOT NULL
            ORDER BY RANDOM()
            LIMIT 5
        """).fetchall()

        if samples:
            sample_text = "\n\n".join([f"• {row[0][:200]}..." if len(str(row[0])) > 200 else f"• {row[0]}"
                                       for row in samples])
            blocks.append(AnalysisBlock(
                title="Sample Values",
                render_as=RenderType.TEXT_BLOCK,
                data={"text": sample_text}
            ))

        return blocks

    async def _analyze_boolean_variable(
            self,
            conn: duckdb.DuckDBPyConnection,
            col_name: str
    ) -> List[AnalysisBlock]:
        """Analyze boolean variable."""
        blocks = []

        # Boolean value counts
        quoted_col = quote_identifier(col_name)
        bool_stats = conn.execute(f"""
            SELECT 
                SUM(CASE WHEN {quoted_col} = TRUE THEN 1 ELSE 0 END) as true_count,
                SUM(CASE WHEN {quoted_col} = FALSE THEN 1 ELSE 0 END) as false_count,
                SUM(CASE WHEN {quoted_col} IS NULL THEN 1 ELSE 0 END) as null_count,
                COUNT(*) as total_count
            FROM dataset
        """).fetchone()

        true_count = bool_stats[0] or 0
        false_count = bool_stats[1] or 0
        null_count = bool_stats[2] or 0
        total_count = bool_stats[3]

        # Calculate percentages
        true_pct = round(100.0 * true_count / total_count, 2) if total_count > 0 else 0
        false_pct = round(100.0 * false_count / total_count, 2) if total_count > 0 else 0
        null_pct = round(100.0 * null_count / total_count, 2) if total_count > 0 else 0

        # Value distribution
        blocks.append(AnalysisBlock(
            title="Boolean Value Distribution",
            render_as=RenderType.KEY_VALUE_PAIRS,
            data={
                "TRUE Count": true_count,
                "TRUE (%)": true_pct,
                "FALSE Count": false_count,
                "FALSE (%)": false_pct,
                "NULL Count": null_count,
                "NULL (%)": null_pct,
                "Total Count": total_count
            }
        ))

        # Bar chart for visualization
        if true_count > 0 or false_count > 0:
            categories = []
            values = []

            if true_count > 0:
                categories.append("TRUE")
                values.append(true_count)
            if false_count > 0:
                categories.append("FALSE")
                values.append(false_count)
            if null_count > 0:
                categories.append("NULL")
                values.append(null_count)

            blocks.append(AnalysisBlock(
                title="Boolean Distribution Chart",
                render_as=RenderType.BAR_CHART,
                data=BarChartData(categories=categories, values=values),
                description="Distribution of boolean values"
            ))

        # True/False ratio
        if false_count > 0:
            true_false_ratio = round(true_count / false_count, 4)
            blocks.append(AnalysisBlock(
                title="Additional Statistics",
                render_as=RenderType.KEY_VALUE_PAIRS,
                data={
                    "TRUE/FALSE Ratio": true_false_ratio,
                    "Non-NULL Count": true_count + false_count,
                    "Non-NULL (%)": round(100.0 * (true_count + false_count) / total_count, 2) if total_count > 0 else 0
                }
            ))

        return blocks

    async def _analyze_interactions(
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
                            corr = conn.execute(f"""
                                SELECT CORR({quoted_var1}, {quoted_var2}) 
                                FROM dataset
                            """).fetchone()[0]

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

                blocks.append(AnalysisBlock(
                    title="Correlation Matrix",
                    render_as=RenderType.HEATMAP,
                    data=HeatmapData(
                        row_labels=numeric_vars,
                        col_labels=numeric_vars,
                        values=matrix,
                        min_value=-1.0,
                        max_value=1.0
                    ),
                    description="Pearson correlation coefficients between numeric variables"
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
                            outlier_query = f"""
                                SELECT {quoted_num}
                                FROM dataset
                                WHERE {quoted_cat} = $1 
                                AND {quoted_num} IS NOT NULL
                                AND ({quoted_num} < $2 OR {quoted_num} > $3)
                                LIMIT 100  -- Limit outliers for performance
                            """

                            # For now, we'll just use empty outliers list
                            # In a real implementation, you'd execute the outlier query
                            outliers = []

                            data.append({
                                "min": float(row[1]) if row[1] is not None else 0,
                                "q1": float(row[2]) if row[2] is not None else 0,
                                "median": float(row[3]) if row[3] is not None else 0,
                                "q3": float(row[4]) if row[4] is not None else 0,
                                "max": float(row[5]) if row[5] is not None else 0,
                                "outliers": outliers
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

        return blocks, correlation_pairs

    async def _analyze_missing_values(
            self,
            conn: duckdb.DuckDBPyConnection,
            column_info: List[Tuple]
    ) -> List[AnalysisBlock]:
        """Analyze missing value patterns."""
        blocks = []

        # --- EFFICIENT SINGLE-PASS QUERY ---
        select_clauses = []
        for col in column_info:
            col_name = col[1]
            quoted_col = quote_identifier(col_name)
            # Use SUM(CASE...) which is fast and clear
            select_clauses.append(
                f"SUM(CASE WHEN {quoted_col} IS NULL THEN 1 ELSE 0 END) AS {quote_identifier(col_name + '_missing')}")

        if not select_clauses:
            return []

        query = f"SELECT {', '.join(select_clauses)}, COUNT(*) as total_rows FROM dataset"
        all_missing_counts = conn.execute(query).fetchone()

        total_rows = all_missing_counts[-1]
        total_cells = total_rows * len(column_info)
        total_missing = 0
        missing_stats = []

        # The result `all_missing_counts` has N columns of missing counts + 1 for total_rows
        for i, col in enumerate(column_info):
            col_name = col[1]
            missing_count = all_missing_counts[i]
            total_missing += missing_count

            if missing_count > 0:
                missing_stats.append({
                    "column": col_name,
                    "missing_count": missing_count,
                    "missing_percent": round(100.0 * missing_count / total_rows, 2) if total_rows > 0 else 0
                })

        # Overall missing statistics
        blocks.append(AnalysisBlock(
            title="Missing Values Summary",
            render_as=RenderType.KEY_VALUE_PAIRS,
            data={
                "Total Cells": total_cells,
                "Total Missing": total_missing,
                "Missing (%)": round(100.0 * total_missing / total_cells, 2) if total_cells > 0 else 0,
                "Columns with Missing": len(missing_stats)
            }
        ))

        # Missing values by column
        if missing_stats:
            missing_stats.sort(key=lambda x: x["missing_percent"], reverse=True)

            columns = ["Column", "Missing Count", "Missing %"]
            rows = [[item["column"], item["missing_count"], item["missing_percent"]]
                    for item in missing_stats[:20]]  # Top 20

            blocks.append(AnalysisBlock(
                title="Missing Values by Column",
                render_as=RenderType.TABLE,
                data=TableData(columns=columns, rows=rows),
                description="Top columns with missing values"
            ))

        # Missing values matrix (sample)
        try:
            # Get a sample of rows to visualize missing patterns
            sample_size = min(100, conn.execute("SELECT COUNT(*) FROM dataset").fetchone()[0])

            if sample_size > 0:
                # Build query to get NULL indicators
                null_indicators = []
                for col in column_info[:20]:  # Limit to 20 columns for visualization
                    col_name = col[1]  # Column name is at index 1
                    quoted_col = quote_identifier(col_name)
                    null_indicators.append(f'{quoted_col} IS NULL AS {quote_identifier(col_name + "_is_null")}')

                query = f"""
                    SELECT {', '.join(null_indicators)}
                    FROM dataset
                    LIMIT {sample_size}
                """

                matrix_result = conn.execute(query).fetchall()

                if matrix_result:
                    columns = [col[1] for col in column_info[:20]]  # Column name is at index 1
                    rows = [[bool(val) for val in row] for row in matrix_result]

                    blocks.append(AnalysisBlock(
                        title="Missing Values Pattern",
                        render_as=RenderType.MATRIX,
                        data=MatrixData(
                            columns=columns,
                            rows=rows,
                            row_indices=list(range(sample_size))
                        ),
                        description=f"Missing value patterns in first {sample_size} rows"
                    ))
        except Exception as e:
            logger.warning(f"Failed to generate missing values matrix: {e}")

        return blocks

    async def _detect_alerts(
            self,
            conn: duckdb.DuckDBPyConnection,
            variables: Dict[str, VariableAnalysis],
            total_rows: int,
            correlation_pairs: List[Tuple[str, str, float]],
            correlation_threshold: float,
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
        duplicate_percent = (duplicate_rows / total_rows) * 100 if total_rows > 0 else 0

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

        if any(t in dtype_upper for t in
               ['INT', 'FLOAT', 'DOUBLE', 'DECIMAL', 'NUMERIC', 'BIGINT', 'SMALLINT', 'TINYINT']):
            return VariableType.NUMERIC
        elif any(t in dtype_upper for t in ['DATE', 'TIME', 'TIMESTAMP']):
            return VariableType.DATETIME
        elif 'BOOL' in dtype_upper:
            return VariableType.BOOLEAN
        elif any(t in dtype_upper for t in ['VARCHAR', 'CHAR', 'TEXT', 'STRING']):
            # Could be categorical or text - for now default to categorical
            # In production, might want to check cardinality
            return VariableType.CATEGORICAL
        else:
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
                return float(min_val_str), float(max_val_str)
            except ValueError:
                # Fallback for numeric parsing failure
                return 0.0, 0.0

        # Fallback if split fails
        return (None, None) if is_datetime else (0.0, 0.0)
