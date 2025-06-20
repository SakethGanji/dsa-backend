import asyncio
import json
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
import time
from collections import defaultdict
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

        # Format alerts as analysis blocks
        alert_blocks = []
        if eda_results["alerts"]:
            alert_blocks.append(AnalysisBlock(
                title="Data Quality Alerts",
                render_as=RenderType.ALERT_LIST,
                data={"alerts": [alert.dict() for alert in eda_results["alerts"]]},
                description="Potential data quality issues detected"
            ))

        return EDAResponse(
            metadata=metadata,
            global_summary=eda_results["global_summary"],
            variables=eda_results["variables"],
            interactions=eda_results["interactions"],
            alerts=alert_blocks
        )

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

        dataset_stats = {
            "Number of Rows": total_rows,
            "Number of Variables": len(column_info),
            "Duplicate Rows": duplicate_rows,
            "Duplicate Rows (%)": round(100.0 * duplicate_rows / total_rows, 2) if total_rows > 0 else 0
        }
        blocks.append(AnalysisBlock(title="Dataset Statistics", render_as=RenderType.KEY_VALUE_PAIRS, data=dataset_stats))

        # Data type distribution
        type_counts = defaultdict(int)
        for col in column_info:
            var_type = self._categorize_dtype(col["type"])
            type_counts[var_type] += 1
        
        blocks.append(AnalysisBlock(
            title="Data Type Distribution",
            render_as=RenderType.BAR_CHART,
            data=BarChartData(categories=list(type_counts.keys()), values=list(type_counts.values()))
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
            
        numeric_stats_results = self._execute_batch_numeric_stats(conn, numeric_cols, total_rows)
        categorical_freq_results = self._execute_batch_categorical_freq(conn, categorical_cols, total_rows, config.alerts.frequency_table_limit)
        # (Add other batch executions for datetime, text, etc. as needed here)

        # 3. Assemble results into VariableAnalysis objects
        variables = {}
        for col in columns_to_analyze:
            col_name = col['name']
            var_type = col['var_type']
            
            var_info = VariableInfo(name=col_name, type=var_type, dtype=col['type'])
            analyses = []

            # Add common stats from the batch results
            if col_name in common_stats_results:
                analyses.append(AnalysisBlock(
                    title="Common Statistics",
                    render_as=RenderType.KEY_VALUE_PAIRS,
                    data=common_stats_results[col_name]
                ))

            # Add type-specific stats
            if var_type == VariableType.NUMERIC and col_name in numeric_stats_results:
                stats = numeric_stats_results[col_name]
                analyses.append(AnalysisBlock(title="Descriptive Statistics", render_as=RenderType.KEY_VALUE_PAIRS, data=stats['descriptive']))
                analyses.append(AnalysisBlock(title="Quantile Statistics", render_as=RenderType.KEY_VALUE_PAIRS, data=stats['quantile']))
                analyses.append(AnalysisBlock(title="Zero Values", render_as=RenderType.KEY_VALUE_PAIRS, data=stats['zeros']))
                if stats.get('histogram'):
                    analyses.append(AnalysisBlock(title="Distribution", render_as=RenderType.HISTOGRAM, data=stats['histogram']))

            elif var_type == VariableType.CATEGORICAL and col_name in categorical_freq_results:
                freq_data = categorical_freq_results[col_name]
                if freq_data['table']:
                    analyses.append(AnalysisBlock(title="Top Values", render_as=RenderType.TABLE, data=freq_data['table']))
                if freq_data['chart']:
                    analyses.append(AnalysisBlock(title="Top 10 Values Distribution", render_as=RenderType.BAR_CHART, data=freq_data['chart']))
            
            # (Add similar blocks for other types: datetime, text, bool)

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
                "Distinct (%)": round(100.0 * distinct_count / total_rows, 2) if total_rows > 0 else 0,
                "Missing Count": missing_count,
                "Missing (%)": round(100.0 * missing_count / total_rows, 2) if total_rows > 0 else 0,
                "Is Unique": distinct_count == total_rows
            }
        return stats_map

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
            p5, q1, q3, p95 = row[8], row[9], row[10], row[11]
            iqr = q3 - q1 if q1 is not None and q3 is not None else None
            zeros_count = row[12] or 0

            histogram_data = None
            try:
                hist_raw = row[13]
                if hist_raw:
                    bins = []
                    for bin_range, count in hist_raw.items():
                        min_v, max_v = self._parse_histogram_bin(bin_range)
                        bins.append(HistogramBin(min=min_v, max=max_v, count=count))
                    histogram_data = HistogramData(bins=bins, total_count=total_rows)
            except Exception as e:
                logger.warning(f"Failed to parse histogram for {col_name}: {e}")

            stats_map[col_name] = {
                "descriptive": {
                    "Mean": round(row[1], 4) if row[1] is not None else None,
                    "Median": round(row[2], 4) if row[2] is not None else None,
                    "Std Dev": round(row[3], 4) if row[3] is not None else None,
                    "Min": row[4], "Max": row[5], "Range": row[5] - row[4] if row[4] is not None and row[5] is not None else None,
                    "Skewness": round(row[6], 4) if row[6] is not None else None,
                    "Kurtosis": round(row[7], 4) if row[7] is not None else None,
                },
                "quantile": {
                    "5th Percentile": p5, "Q1 (25th Percentile)": q1,
                    "Median (50th Percentile)": row[2],
                    "Q3 (75th Percentile)": q3, "95th Percentile": p95, "IQR": iqr
                },
                "zeros": {
                    "Zeros Count": zeros_count,
                    "Zeros (%)": round(100.0 * zeros_count / total_rows, 2) if total_rows > 0 else 0,
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
            freq_map[col_name]['rows'].append([value, frequency, round(100.0 * frequency / total_rows, 2) if total_rows > 0 else 0])

        processed_map = {}
        for col_name, data in freq_map.items():
            table = TableData(columns=["Value", "Frequency", "Percentage (%)"], rows=data['rows'])
            
            top_10_rows = data['rows'][:10]
            categories = [str(row[0]) if row[0] is not None else "NULL" for row in top_10_rows]
            values = [row[1] for row in top_10_rows]
            chart = BarChartData(categories=categories, values=values)
            processed_map[col_name] = {'table': table, 'chart': chart}

        return processed_map


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
                        "missing_percent": missing_percent
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
