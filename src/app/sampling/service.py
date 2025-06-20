import duckdb
import logging
import asyncio
import tempfile
import os
from io import BytesIO
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from app.storage.backend import StorageBackend
from app.sampling.models import (
    SamplingMethod, JobStatus, SamplingRequest, 
    RandomSamplingParams, StratifiedSamplingParams,
    SystematicSamplingParams, ClusterSamplingParams, CustomSamplingParams,
    WeightedSamplingParams, DataFilters, DataSelection, FilterCondition, DataSummary,
    MultiRoundSamplingRequest, MultiRoundSamplingJob,
    SamplingRoundConfig, RoundResult
)

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class SamplingService:
    def __init__(self, datasets_repository, sampling_repository, storage_backend: StorageBackend):
        self.datasets_repository = datasets_repository
        self.sampling_repository = sampling_repository
        self.storage = storage_backend
        
    
    async def get_dataset_columns(self, dataset_id: int, version_id: int) -> Dict[str, Any]:
        """Get column information for a dataset version"""
        try:
            # Create a new connection for this operation
            from app.db.connection import AsyncSessionLocal
            
            async with AsyncSessionLocal() as session:
                from app.datasets.repository import DatasetsRepository
                datasets_repo = DatasetsRepository(session)
                
                # Get dataset version
                version = await datasets_repo.get_dataset_version(version_id)
                if not version:
                    raise ValueError(f"Dataset version with ID {version_id} not found")
                
                # Verify dataset ID matches
                if version.dataset_id != dataset_id:
                    raise ValueError(f"Version {version_id} does not belong to dataset {dataset_id}")
                
                # Get file data
                file_info = await datasets_repo.get_file(version.file_id)
                if not file_info or not file_info.file_path:
                    raise ValueError("File path not found")
                
                # Use DuckDB to get metadata - no memory limits for optimal performance
                conn = duckdb.connect(':memory:')
                try:
                    # Create view from Parquet file - this doesn't load data into memory
                    conn.execute(f"CREATE VIEW dataset AS SELECT * FROM read_parquet('{file_info.file_path}')")
                    
                    # Get column information - use DESCRIBE for better performance
                    columns_info = conn.execute("DESCRIBE dataset").fetchall()
                    columns = []
                    column_types = {}
                    for col_info in columns_info:
                        col_name = col_info[0]
                        col_type = col_info[1]
                        columns.append(col_name)
                        column_types[col_name] = col_type
                    
                    # Get row count - for large datasets, use Parquet metadata
                    try:
                        # First check file size to decide approach
                        file_size_mb = os.path.getsize(file_info.file_path) / (1024 * 1024)
                        
                        if file_size_mb > 100:  # For files > 100MB, use COUNT for now
                            logger.info(f"Large file detected ({file_size_mb:.1f}MB), using efficient COUNT")
                            # DuckDB optimizes COUNT(*) on parquet files
                            total_rows = conn.execute("SELECT COUNT(*) FROM dataset").fetchone()[0]
                        else:
                            # For smaller files, get exact count
                            total_rows = conn.execute("SELECT COUNT(*) FROM dataset").fetchone()[0]
                    except Exception as e:
                        logger.warning(f"Error getting row count: {str(e)}, defaulting to 0")
                        total_rows = 0
                    
                    # Get null counts and sample values
                    null_counts = {}
                    sample_values = {}
                    
                    for col_name in columns:
                        # Skip null counting for large datasets - too expensive
                        null_counts[col_name] = None

                        # Get sample values using LIMIT for speed
                        try:
                            result = conn.execute(f'''
                                SELECT DISTINCT "{col_name}"
                                FROM (
                                    SELECT "{col_name}" 
                                    FROM dataset 
                                    WHERE "{col_name}" IS NOT NULL
                                    LIMIT 1000
                                )
                                LIMIT 10
                            ''').fetchall()
                            sample_values[col_name] = [row[0] for row in result]
                        except Exception as e:
                            logger.warning(f"Could not get sample values for column {col_name}: {str(e)}")
                            sample_values[col_name] = []
                    
                    return {
                        "columns": columns,
                        "column_types": column_types,
                        "total_rows": total_rows,
                        "null_counts": null_counts,
                        "sample_values": sample_values
                    }
                except Exception as e:
                    logger.error(f"Error reading dataset: {str(e)}")
                    raise ValueError(f"Error reading dataset: {str(e)}")
                finally:
                    conn.close()
                
        except Exception as e:
            logger.error(f"Error getting dataset columns: {str(e)}", exc_info=True)
            raise ValueError(f"Error getting dataset columns: {str(e)}")
    
    
    def _create_temp_file_from_bytes(self, file_data: bytes, file_type: str) -> str:
        """Create a temporary file from bytes data and return the path"""
        suffix = f".{file_type.lower()}"
        if file_type.lower() in ["xls", "xlsx", "xlsm"]:
            suffix = ".xlsx"  # Excel files
        elif file_type.lower() == "parquet":
            suffix = ".parquet"
        
        # Create temporary file
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        try:
            temp_file.write(file_data)
            temp_file.flush()
            temp_file.close()
            return temp_file.name
        except Exception as e:
            # Clean up on error
            temp_file.close()
            os.unlink(temp_file.name)
            raise e
    
    async def _apply_sampling_sql(self, conn: duckdb.DuckDBPyConnection, base_query: str, request: SamplingRequest) -> str:
        """
        Generate SQL query for the requested sampling method
        
        Args:
            conn: DuckDB connection
            base_query: Base SQL query to sample from
            request: Sampling request with method and parameters
            
        Returns:
            SQL query string for the sampled data
        """
        try:
            # Get typed parameters
            params = request.get_typed_parameters()
            
            # Create temporary view from base query
            try:
                logger.debug(f"Creating filtered_data view with base_query: {base_query}")
                conn.execute(f"CREATE OR REPLACE TEMPORARY VIEW filtered_data AS {base_query}")
            except Exception as e:
                logger.error(f"Failed to create filtered_data view")
                logger.error(f"Base query was: {base_query}")
                raise
            
            # Apply sampling method
            if request.method == SamplingMethod.RANDOM:
                return self._random_sampling_sql(conn, params)
            elif request.method == SamplingMethod.STRATIFIED:
                return self._stratified_sampling_sql(conn, params)
            elif request.method == SamplingMethod.SYSTEMATIC:
                return self._systematic_sampling_sql(conn, params)
            elif request.method == SamplingMethod.CLUSTER:
                return self._cluster_sampling_sql(conn, params)
            elif request.method == SamplingMethod.WEIGHTED:
                return self._weighted_sampling_sql(conn, params)
            elif request.method == SamplingMethod.CUSTOM:
                return self._custom_sampling_sql(conn, params)
            else:
                raise ValueError(f"Unknown sampling method: {request.method}")
        except Exception as e:
            logger.error(f"Error applying sampling: {str(e)}", exc_info=True)
            raise ValueError(f"Error applying sampling: {str(e)}")
    
    def _random_sampling_sql(self, conn: duckdb.DuckDBPyConnection, params: RandomSamplingParams) -> str:
        """Generate SQL for random sampling"""
        # Get total count - use approximate count for large datasets
        total_count = conn.execute("SELECT COUNT(*) FROM filtered_data").fetchone()[0]
        
        if params.sample_size >= total_count:
            return "SELECT * FROM filtered_data"
        
        # Use DuckDB's TABLESAMPLE for better performance on large datasets
        if params.seed is not None:
            # For exact sample size with seed, use SAMPLE clause with reservoir method
            return f"SELECT * FROM filtered_data USING SAMPLE reservoir({params.sample_size} ROWS) REPEATABLE({params.seed})"
        else:
            # Always use SAMPLE for consistent behavior
            return f"SELECT * FROM filtered_data USING SAMPLE {params.sample_size} ROWS"
    
    def _escape_sql_string(self, value: Any) -> str:
        """Escape a value for use in SQL string literal"""
        if value is None:
            return 'NULL'
        # Convert to string and escape single quotes
        str_value = str(value).replace("'", "''")
        return str_value
    
    def _stratified_sampling_sql(self, conn: duckdb.DuckDBPyConnection, params: StratifiedSamplingParams) -> str:
        """Generate SQL for stratified sampling using window functions for better performance"""
        # Validate that strata columns are provided
        if not params.strata_columns:
            raise ValueError("Stratified sampling requires at least one strata column")
        
        # Validate strata columns exist
        columns_result = conn.execute("PRAGMA table_info('filtered_data')").fetchall()
        available_columns = [col[1] for col in columns_result]
        
        for col in params.strata_columns:
            if col not in available_columns:
                raise ValueError(f"Strata column '{col}' not found in dataset")
        
        # Build strata expression
        strata_cols = ", ".join([f'"{col}"' for col in params.strata_columns])
        
        # Determine sampling approach based on parameters
        if params.sample_size is None and params.min_per_stratum is None:
            # Default to 10% per stratum using QUALIFY for better performance
            seed_expr = f"RANDOM({params.seed})" if params.seed else "RANDOM()"
            return f"""
                SELECT * FROM filtered_data
                QUALIFY ROW_NUMBER() OVER (PARTITION BY {strata_cols} ORDER BY {seed_expr}) 
                        <= CEIL(COUNT(*) OVER (PARTITION BY {strata_cols}) * 0.1)
                """
        
        elif isinstance(params.sample_size, float):
            # Sample by fraction using QUALIFY for better performance
            fraction = params.sample_size
            seed_expr = f"RANDOM({params.seed})" if params.seed else "RANDOM()"
            return f"""
                SELECT * FROM filtered_data
                QUALIFY ROW_NUMBER() OVER (PARTITION BY {strata_cols} ORDER BY {seed_expr}) 
                        <= CEIL(COUNT(*) OVER (PARTITION BY {strata_cols}) * {fraction})
                """
        
        else:
            # Proportional allocation - ensure exact sample size
            total_samples = params.sample_size if params.sample_size else 1000
            min_per_stratum = params.min_per_stratum or 0
            
            # First, calculate the number of strata and their sizes
            strata_info = conn.execute(f"""
                SELECT {strata_cols}, COUNT(*) as stratum_size
                FROM filtered_data
                GROUP BY {strata_cols}
            """).fetchall()
            
            if not strata_info:
                raise ValueError("No data found in filtered dataset")
            
            # Calculate proportional allocation
            total_rows = sum(row[-1] for row in strata_info)
            allocations = []
            allocated_samples = 0
            
            for stratum_row in strata_info:
                stratum_size = stratum_row[-1]
                # Proportional allocation
                proportion = stratum_size / total_rows
                ideal_allocation = total_samples * proportion
                
                # Apply minimum constraint if specified
                if min_per_stratum > 0:
                    allocation = max(min_per_stratum, int(ideal_allocation))
                else:
                    allocation = int(ideal_allocation)
                
                # Don't allocate more than available in stratum
                allocation = min(allocation, stratum_size)
                allocations.append((stratum_row[:-1], allocation))
                allocated_samples += allocation
            
            # Adjust allocations to match exact sample size
            if allocated_samples != total_samples:
                # Sort by remainder (fractional part) descending
                remainders = []
                for i, (stratum_row, _) in enumerate(allocations):
                    stratum_size = strata_info[i][-1]
                    proportion = stratum_size / total_rows
                    ideal = total_samples * proportion
                    remainder = ideal - int(ideal)
                    remainders.append((i, remainder, stratum_size))
                
                remainders.sort(key=lambda x: x[1], reverse=True)
                
                # Adjust allocations
                diff = total_samples - allocated_samples
                if diff > 0:
                    # Need to add more samples
                    for i, _, stratum_size in remainders:
                        if diff == 0:
                            break
                        stratum_values, current_alloc = allocations[i]
                        if current_alloc < stratum_size:
                            add = min(diff, stratum_size - current_alloc)
                            allocations[i] = (stratum_values, current_alloc + add)
                            diff -= add
                else:
                    # Need to remove samples (diff is negative)
                    for i, _, _ in reversed(remainders):
                        if diff == 0:
                            break
                        stratum_values, current_alloc = allocations[i]
                        if current_alloc > min_per_stratum:
                            remove = min(-diff, current_alloc - min_per_stratum)
                            allocations[i] = (stratum_values, current_alloc - remove)
                            diff += remove
            
            # Build the sampling query with exact allocations
            seed_expr = f"RANDOM({params.seed})" if params.seed else "RANDOM()"
            
            # Create UNION ALL query for each stratum
            union_parts = []
            for stratum_values, allocation in allocations:
                if allocation > 0:
                    # Build WHERE clause for this stratum
                    conditions = []
                    for i, col in enumerate(params.strata_columns):
                        val = stratum_values[i] if isinstance(stratum_values, tuple) else stratum_values
                        if val is None:
                            conditions.append(f'"{col}" IS NULL')
                        else:
                            # Escape value properly
                            escaped_val = str(val).replace("'", "''")
                            conditions.append(f'"{col}" = \'{escaped_val}\'')
                    
                    where_clause = " AND ".join(conditions)
                    
                    union_parts.append(f"""
                        (SELECT * FROM filtered_data
                         WHERE {where_clause}
                         ORDER BY {seed_expr}
                         LIMIT {allocation})
                    """)
            
            if not union_parts:
                return "SELECT * FROM filtered_data WHERE 1=0"  # Empty result
            
            return " UNION ALL ".join(union_parts)
    
    def _systematic_sampling_sql(self, conn: duckdb.DuckDBPyConnection, params: SystematicSamplingParams) -> str:
        """Generate SQL for systematic sampling"""
        if params.interval <= 0:
            raise ValueError("Interval must be greater than 0")
        
        start = params.start if params.start is not None else 0
        
        # Use ROW_NUMBER() to implement systematic sampling
        return f"""
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER () - 1 as rn 
            FROM filtered_data
        ) t 
        WHERE (rn - {start}) % {params.interval} = 0
        """
    
    def _cluster_sampling_sql(self, conn: duckdb.DuckDBPyConnection, params: ClusterSamplingParams) -> str:
        """Generate SQL for cluster sampling"""
        # Validate cluster column exists
        columns_result = conn.execute("PRAGMA table_info('filtered_data')").fetchall()
        available_columns = [col[1] for col in columns_result]
        
        if params.cluster_column not in available_columns:
            raise ValueError(f"Cluster column '{params.cluster_column}' not found in dataset")
        
        # Get unique cluster count first
        cluster_count_result = conn.execute(f'SELECT COUNT(DISTINCT "{params.cluster_column}") FROM filtered_data').fetchone()
        total_clusters = cluster_count_result[0] if cluster_count_result else 0
        
        if params.num_clusters >= total_clusters:
            # If we want more clusters than exist, return all
            return "SELECT * FROM filtered_data"
        
        # Use DuckDB's SAMPLE BY for efficient cluster sampling
        base_query = f"""
        WITH sampled_clusters AS (
            SELECT "{params.cluster_column}"
            FROM (SELECT DISTINCT "{params.cluster_column}" FROM filtered_data)
            USING SAMPLE {params.num_clusters} ROWS
        )
        SELECT f.* FROM filtered_data f
        WHERE f."{params.cluster_column}" IN (SELECT "{params.cluster_column}" FROM sampled_clusters)
        """
        
        # Optionally sample within clusters
        if params.sample_within_clusters:
            # Sample 50% within each cluster using QUALIFY
            return f"""
            SELECT * FROM ({base_query}) t
            QUALIFY ROW_NUMBER() OVER (PARTITION BY "{params.cluster_column}" ORDER BY RANDOM()) 
                    <= COUNT(*) OVER (PARTITION BY "{params.cluster_column}") / 2
            """
        
        return base_query
    
    def _custom_sampling_sql(self, conn: duckdb.DuckDBPyConnection, params: CustomSamplingParams) -> str:
        """Generate SQL for custom sampling"""
        # The custom query parameter should contain a WHERE clause condition
        # We'll wrap it in a proper SQL query
        return f"SELECT * FROM filtered_data WHERE {params.query}"
    
    def _weighted_sampling_sql(self, conn: duckdb.DuckDBPyConnection, params: WeightedSamplingParams) -> str:
        """Generate SQL for weighted sampling"""
        # Validate weight column exists
        columns_result = conn.execute("PRAGMA table_info('filtered_data')").fetchall()
        available_columns = [col[1] for col in columns_result]
        
        if params.weight_column not in available_columns:
            raise ValueError(f"Weight column '{params.weight_column}' not found in dataset")
        
        # Get total weight sum for normalization
        weight_sum_result = conn.execute(f'SELECT SUM("{params.weight_column}") FROM filtered_data WHERE "{params.weight_column}" > 0').fetchone()
        total_weight = weight_sum_result[0] if weight_sum_result and weight_sum_result[0] else 0
        
        if total_weight <= 0:
            raise ValueError(f"Sum of weights in column '{params.weight_column}' must be positive")
        
        # Use cumulative distribution approach for weighted sampling
        # This ensures exact sample size
        seed_clause = f"SETSEED({params.seed / 2**31 - 1})" if params.seed is not None else ""
        
        return f"""
        WITH weighted_data AS (
            SELECT *,
                   "{params.weight_column}" AS weight,
                   SUM("{params.weight_column}") OVER (ORDER BY RANDOM()) AS cumulative_weight,
                   {total_weight} AS total_weight
            FROM filtered_data
            WHERE "{params.weight_column}" > 0
        ),
        random_draws AS (
            SELECT (RANDOM() * {total_weight}) AS threshold
            FROM generate_series(1, {params.sample_size})
        )
        {seed_clause}
        SELECT DISTINCT ON (cumulative_weight) 
               * EXCLUDE (weight, cumulative_weight, total_weight)
        FROM weighted_data
        WHERE cumulative_weight >= ANY(SELECT threshold FROM random_draws)
        ORDER BY cumulative_weight
        LIMIT {params.sample_size}
        """
    
    def _get_data_summary(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> DataSummary:
        """Generate data summary statistics using DuckDB"""
        try:
            # Get basic info
            result = conn.execute(f"SELECT COUNT(*) as total_rows FROM {table_name}").fetchone()
            total_rows = result[0]
            
            # Get column info
            columns_result = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
            total_columns = len(columns_result)
            
            # Get column types
            column_types = {col_info[1]: col_info[2] for col_info in columns_result}
            
            # Skip null counting for performance - can be expensive on large datasets
            # Set to 0 as placeholder since the model expects integers
            null_counts = {col_name: 0 for col_name in column_types.keys()}
            
            # Estimate memory usage (rough calculation)
            memory_usage_mb = total_rows * total_columns * 8 / (1024 * 1024)  # Rough estimate
            
            return DataSummary(
                total_rows=total_rows,
                total_columns=total_columns,
                column_types=column_types,
                memory_usage_mb=round(memory_usage_mb, 2),
                null_counts=null_counts
            )
            
        except Exception as e:
            logger.error(f"Error getting data summary: {str(e)}")
            raise ValueError(f"Error getting data summary: {str(e)}")
    

    def _get_sample_summary_from_duckdb(self, conn: duckdb.DuckDBPyConnection, sample_query: str) -> DataSummary:
        """Generate summary statistics for the sampled data using DuckDB"""
        try:
            # Create temporary view for the sample
            conn.execute(f"CREATE OR REPLACE TEMPORARY VIEW sample_data AS {sample_query}")
            
            # Get basic info
            result = conn.execute("SELECT COUNT(*) as total_rows FROM sample_data").fetchone()
            total_rows = result[0]
            
            # Get column info
            columns_result = conn.execute("PRAGMA table_info('sample_data')").fetchall()
            total_columns = len(columns_result)
            
            # Get column types
            column_types = {col_info[1]: col_info[2] for col_info in columns_result}
            
            # Skip null counting for performance
            null_counts = {col_name: None for col_name in column_types.keys()}
            
            # Estimate memory usage (rough calculation)
            memory_usage_mb = total_rows * total_columns * 8 / (1024 * 1024)  # Rough estimate
            
            return DataSummary(
                total_rows=total_rows,
                total_columns=total_columns,
                column_types=column_types,
                memory_usage_mb=round(memory_usage_mb, 2),
                null_counts=null_counts
            )
            
        except Exception as e:
            logger.error(f"Error getting sample summary: {str(e)}")
            raise ValueError(f"Error getting sample summary: {str(e)}")
    
    def _build_filter_query_embedded(self, filters: Optional[DataFilters]) -> str:
        """Build SQL WHERE clause with values embedded directly (no parameters)"""
        if not filters:
            return ""
        
        # Build the WHERE clause from nested filter structure
        filter_expr = self._build_filter_expression_embedded(filters)
        if filter_expr:
            return f"WHERE {filter_expr}"
        return ""
    
    def _build_filter_expression_embedded(self, filters: DataFilters) -> str:
        """Build filter expression recursively for nested groups"""
        parts = []
        
        # Process direct conditions
        if filters.conditions:
            for condition in filters.conditions:
                col = f'"{condition.column}"'  # Quote column names

                if condition.operator in ['IS NULL', 'IS NOT NULL']:
                    parts.append(f"{col} {condition.operator}")
                elif condition.operator in ['IN', 'NOT IN']:
                    if isinstance(condition.value, list):
                        if not condition.value:  # Empty list
                            if condition.operator == 'IN':
                                parts.append("0=1")  # Always false for IN empty list
                            else:  # NOT IN
                                parts.append("1=1")  # Always true for NOT IN empty list
                        else:  # Non-empty list
                            # Escape each value
                            escaped_values = [f"'{self._escape_sql_string(v)}'" for v in condition.value]
                            parts.append(f"{col} {condition.operator} ({', '.join(escaped_values)})")
                    else:  # Single value, treat as = or !=
                        actual_operator = '=' if condition.operator == 'IN' else '!='
                        escaped_value = self._escape_sql_string(condition.value)
                        parts.append(f"{col} {actual_operator} '{escaped_value}'")
                elif condition.operator in ['=', '!=', '>', '<', '>=', '<=']:
                    # For numeric comparisons, don't quote the value if it's a number
                    if isinstance(condition.value, (int, float)):
                        parts.append(f"{col} {condition.operator} {condition.value}")
                    else:
                        escaped_value = self._escape_sql_string(condition.value)
                        parts.append(f"{col} {condition.operator} '{escaped_value}'")
                elif condition.operator in ['LIKE', 'ILIKE']:
                    escaped_value = self._escape_sql_string(condition.value)
                    parts.append(f"{col} {condition.operator} '{escaped_value}'")
                else:
                    # Default case - treat as string
                    escaped_value = self._escape_sql_string(condition.value)
                    parts.append(f"{col} {condition.operator} '{escaped_value}'")
        
        # Process nested groups recursively
        if filters.groups:
            for group in filters.groups:
                group_expr = self._build_filter_expression_embedded(group)
                if group_expr:
                    # Wrap nested groups in parentheses
                    parts.append(f"({group_expr})")
        
        if not parts:
            return ""
        
        # Join all parts with the specified logic
        return f' {filters.logic} '.join(parts)
    
    def _build_filter_query(self, filters: Optional[DataFilters]) -> Tuple[str, List[Any]]:
        """Build SQL WHERE clause from filter conditions, returning clause and parameters."""
        if not filters:
            return "", []
        
        # Build the WHERE clause from nested filter structure
        filter_expr, params = self._build_filter_expression(filters)
        if filter_expr:
            return f"WHERE {filter_expr}", params
        return "", []
    
    def _build_filter_expression(self, filters: DataFilters) -> Tuple[str, List[Any]]:
        """Build filter expression recursively for nested groups with parameters"""
        parts = []
        params: List[Any] = []
        
        # Process direct conditions
        if filters.conditions:
            for condition in filters.conditions:
                col = f'"{condition.column}"'  # Quote column names

                if condition.operator in ['IS NULL', 'IS NOT NULL']:
                    parts.append(f"{col} {condition.operator}")
                elif condition.operator in ['IN', 'NOT IN']:
                    if isinstance(condition.value, list):
                        if not condition.value:  # Empty list
                            if condition.operator == 'IN':
                                parts.append("0=1")  # Always false for IN empty list
                            else:  # NOT IN
                                parts.append("1=1")  # Always true for NOT IN empty list
                        else:  # Non-empty list
                            placeholders = ', '.join(['?'] * len(condition.value))
                            parts.append(f"{col} {condition.operator} ({placeholders})")
                            params.extend(condition.value)
                    else:  # Single value, treat as = or !=
                        actual_operator = '=' if condition.operator == 'IN' else '!='
                        parts.append(f"{col} {actual_operator} ?")
                        params.append(condition.value)
                else:  # For other operators like =, !=, >, <, LIKE, ILIKE
                    parts.append(f"{col} {condition.operator} ?")
                    params.append(condition.value)
        
        # Process nested groups recursively
        if filters.groups:
            for group in filters.groups:
                group_expr, group_params = self._build_filter_expression(group)
                if group_expr:
                    # Wrap nested groups in parentheses
                    parts.append(f"({group_expr})")
                    params.extend(group_params)
        
        if not parts:
            return "", []
        
        # Join all parts with the specified logic
        return f' {filters.logic} '.join(parts), params

    def _build_select_from_clause(self, conn: duckdb.DuckDBPyConnection, selection: Optional[DataSelection], table_name: str = 'main_data') -> str:
        """Build SQL SELECT ... FROM ... clause, handling column selection and exclusion."""
        columns_sql = "*"
        if selection:
            if selection.columns:
                columns_sql = ', '.join([f'"{col}"' for col in selection.columns])
            elif selection.exclude_columns:
                # Fetch all column names from the table
                all_columns_info = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
                all_column_names = [info[1] for info in all_columns_info]

                # Filter out excluded columns
                selected_columns = [col for col in all_column_names if col not in selection.exclude_columns]

                if not selected_columns:
                    # If all columns are excluded, this would result in an error.
                    # Default to selecting a literal to prevent SQL errors, or raise.
                    # For simplicity, let's raise if it results in no columns.
                    # A more sophisticated approach might select 'NULL' or similar if allowed.
                    raise ValueError("Excluding all columns or resulting in an empty column set is not allowed.")
                columns_sql = ', '.join([f'"{col}"' for col in selected_columns])

        return f"SELECT {columns_sql} FROM {table_name}"

    def _build_order_limit_offset_clause(self, selection: Optional[DataSelection]) -> str:
        """Build SQL ORDER BY, LIMIT, OFFSET clause."""
        parts = []
        if selection:
            if selection.order_by:
                order_direction = "DESC" if selection.order_desc else "ASC"
                parts.append(f'ORDER BY "{selection.order_by}" {order_direction}')
            if selection.limit is not None: # Allow 0 as a valid limit
                parts.append(f"LIMIT {selection.limit}")
                if selection.offset is not None: # Offset typically used with limit
                    parts.append(f"OFFSET {selection.offset}")
        return " ".join(parts)

    async def _apply_sampling_with_duckdb(self, conn: duckdb.DuckDBPyConnection, request: SamplingRequest) -> str:
        """Apply filtering, selection, and sampling using DuckDB and return the final SQL query."""
        try:
            # Validate filters and selection (checks if columns exist, etc.)
            if request.filters:
                self._validate_filters(conn, request.filters)
            if request.selection:
                self._validate_selection(conn, request.selection)
            
            # Build query parts
            select_from_clause = self._build_select_from_clause(conn, request.selection, 'main_data')
            filter_clause_str, filter_params = self._build_filter_query(request.filters)
            order_limit_offset_clause_str = self._build_order_limit_offset_clause(request.selection)

            query_parts = [select_from_clause]
            if filter_clause_str:
                query_parts.append(filter_clause_str)
            if order_limit_offset_clause_str:
                query_parts.append(order_limit_offset_clause_str)

            base_query = " ".join(query_parts).strip()

            logger.debug(f"Base DuckDB query: {base_query} with params: {filter_params}")

            # If we have filters, we need to embed them directly in the query
            # DuckDB doesn't support parameters in CREATE VIEW statements
            if filter_params and filter_clause_str:
                # Build the filter clause with embedded values
                embedded_filter_clause = self._build_filter_query_embedded(request.filters)
                query_parts = [select_from_clause]
                if embedded_filter_clause:
                    query_parts.append(embedded_filter_clause)
                if order_limit_offset_clause_str:
                    query_parts.append(order_limit_offset_clause_str)
                base_query = " ".join(query_parts).strip()

            # Now apply sampling method to get the final query
            return await self._apply_sampling_sql(conn, base_query, request)
            
        except Exception as e:
            logger.error(f"Error applying sampling with DuckDB: {str(e)}", exc_info=True)
            raise ValueError(f"Error applying sampling with DuckDB: {str(e)}")

    def _validate_filters(self, conn: duckdb.DuckDBPyConnection, filters: DataFilters) -> None:
        """Validate that filter columns exist and have appropriate types"""
        # Get available columns
        columns_result = conn.execute("PRAGMA table_info('main_data')").fetchall()
        available_columns = {col[1]: col[2] for col in columns_result}  # name: type
        
        self._validate_filter_group(filters, available_columns)
    
    def _validate_filter_group(self, filters: DataFilters, available_columns: Dict[str, str]) -> None:
        """Recursively validate filter groups"""
        # Validate direct conditions
        if filters.conditions:
            for condition in filters.conditions:
                # Check if column exists
                if condition.column not in available_columns:
                    raise ValueError(f"Filter column '{condition.column}' does not exist")
                
                # Basic type validation for certain operators
                col_type = available_columns[condition.column].lower()
                if condition.operator in ['>', '<', '>=', '<='] and 'text' in col_type:
                    logger.warning(f"Using numeric comparison operator on text column '{condition.column}'")
        
        # Validate nested groups
        if filters.groups:
            for group in filters.groups:
                self._validate_filter_group(group, available_columns)
    
    def _validate_selection(self, conn: duckdb.DuckDBPyConnection, selection: DataSelection) -> None:
        """Validate that selection columns exist"""
        # Get available columns
        columns_result = conn.execute("PRAGMA table_info('main_data')").fetchall()
        available_columns = [col[1] for col in columns_result]
        
        # Validate columns to include
        if selection.columns:
            for col in selection.columns:
                if col not in available_columns:
                    raise ValueError(f"Selection column '{col}' does not exist")
        
        # Validate columns to exclude
        if selection.exclude_columns:
            for col in selection.exclude_columns:
                if col not in available_columns:
                    raise ValueError(f"Exclude column '{col}' does not exist")
        
        # Validate order by column
        if selection.order_by and selection.order_by not in available_columns:
            raise ValueError(f"Order by column '{selection.order_by}' does not exist")
    
    
    
    # Multi-round sampling methods
    async def create_multi_round_sampling_job(
        self,
        dataset_id: int,
        version_id: int,
        request: MultiRoundSamplingRequest,
        user_id: int
    ) -> MultiRoundSamplingJob:
        """
        Create and enqueue a new multi-round sampling job
        
        Args:
            dataset_id: ID of the dataset to sample
            version_id: Version of the dataset to sample
            request: Multi-round sampling request with configuration
            user_id: ID of the user creating the job
            
        Returns:
            A MultiRoundSamplingJob object with a unique ID
        """
        # Create a new job
        job = MultiRoundSamplingJob(
            dataset_id=dataset_id,
            version_id=version_id,
            user_id=user_id,
            request=request,
            total_rounds=len(request.rounds)
        )
        
        # Store the job
        await self.sampling_repository.create_multi_round_job(job)
        
        # Start the job in the background
        asyncio.create_task(self._process_multi_round_job(job.id))
        
        return job
    
    async def get_multi_round_job(self, job_id: str) -> Optional[MultiRoundSamplingJob]:
        """Get multi-round job details by ID"""
        return await self.sampling_repository.get_multi_round_job(job_id)
    
    async def _process_multi_round_job(self, job_id: str) -> None:
        """
        Process a multi-round sampling job in the background
        
        This method executes each sampling round progressively,
        tracking residuals after each round.
        """
        job = await self.sampling_repository.get_multi_round_job(job_id)
        if not job:
            logger.error(f"Multi-round job {job_id} not found")
            return
        
        conn = None
        try:
            # Update job status
            job.status = JobStatus.RUNNING
            job.started_at = datetime.now()
            await self.sampling_repository.update_multi_round_job(job)
            
            # Get dataset information
            from app.db.connection import AsyncSessionLocal
            
            async with AsyncSessionLocal() as session:
                from app.datasets.repository import DatasetsRepository
                datasets_repo = DatasetsRepository(session)
                
                # Get dataset version
                version = await datasets_repo.get_dataset_version(job.version_id)
                if not version:
                    raise ValueError(f"Dataset version with ID {job.version_id} not found")
                
                # Verify dataset ID matches
                if version.dataset_id != job.dataset_id:
                    raise ValueError(f"Version {job.version_id} does not belong to dataset {job.dataset_id}")
                
                # Get file data
                file_info = await datasets_repo.get_file(version.file_id)
                if not file_info or not file_info.file_path:
                    raise ValueError("File path not found")
                
                # Create DuckDB connection
                conn = duckdb.connect(':memory:')
                
                # Create initial view from dataset
                conn.execute(f"CREATE VIEW original_data AS SELECT * FROM read_parquet('{file_info.file_path}')")
                
                # Add unique row identifier for tracking
                conn.execute("""
                    CREATE VIEW data_with_id AS 
                    SELECT *, ROW_NUMBER() OVER () AS __row_id__ 
                    FROM original_data
                """)
                
                # Initialize residual as full dataset
                conn.execute("CREATE TABLE residual_data AS SELECT * FROM data_with_id")
                
                # Process each round
                for round_config in job.request.rounds:
                    job.current_round = round_config.round_number
                    await self.sampling_repository.update_multi_round_job(job)
                    
                    # Execute sampling for this round
                    round_result = await self._execute_sampling_round(
                        conn, job, round_config, file_info.file_path
                    )
                    
                    # Update job with round result
                    job.round_results.append(round_result)
                    job.completed_rounds += 1
                    await self.sampling_repository.update_multi_round_job(job)
                
                # Export final residual if requested
                if job.request.export_residual:
                    await self._export_residual_dataset(conn, job)
                
                # Update job status
                job.status = JobStatus.COMPLETED
                job.completed_at = datetime.now()
                job.current_round = None
                await self.sampling_repository.update_multi_round_job(job)
                
        except Exception as e:
            # Handle job failure
            logger.error(f"Error processing multi-round job {job_id}: {str(e)}", exc_info=True)
            job.status = JobStatus.FAILED
            job.error_message = str(e)
            if not job.started_at:
                job.started_at = datetime.now()
            job.completed_at = datetime.now()
            await self.sampling_repository.update_multi_round_job(job)
        finally:
            if conn:
                conn.close()
    
    async def _execute_sampling_round(
        self,
        conn: duckdb.DuckDBPyConnection,
        job: MultiRoundSamplingJob,
        round_config: SamplingRoundConfig,
        original_file_path: str
    ) -> RoundResult:
        """Execute a single sampling round and update residual"""
        started_at = datetime.now()
        
        try:
            # Create view for current residual
            conn.execute("CREATE OR REPLACE VIEW current_residual AS SELECT * FROM residual_data")
            
            # Build base query from residual with filters
            base_query = self._build_base_query_for_round(conn, round_config)
            
            # Create sampling request for this round
            sampling_request = SamplingRequest(
                method=round_config.method,
                parameters=round_config.parameters,
                output_name=round_config.output_name,
                filters=round_config.filters,
                selection=round_config.selection
            )
            
            # Apply sampling to get sample query
            sample_query = await self._apply_sampling_sql(conn, base_query, sampling_request)
            
            # Create temporary table with sampled IDs
            conn.execute(f"""
                CREATE TEMPORARY TABLE round_{round_config.round_number}_sample AS
                SELECT __row_id__ FROM ({sample_query})
            """)
            
            # Get sample size
            sample_size = conn.execute(f"SELECT COUNT(*) FROM round_{round_config.round_number}_sample").fetchone()[0]
            
            # Save sample data (without __row_id__)
            sample_path = self.storage.get_multi_round_sample_path(
                dataset_id=job.dataset_id,
                version_id=job.version_id,
                job_id=job.id,
                round_number=round_config.round_number
            )
            
            # Export sample without internal row ID
            conn.execute(f"""
                COPY (
                    SELECT * EXCLUDE (__row_id__)
                    FROM ({sample_query})
                ) TO '{sample_path}' (FORMAT PARQUET)
            """)
            
            # Get preview data
            preview_result = conn.execute(f"""
                SELECT * EXCLUDE (__row_id__)
                FROM ({sample_query})
                LIMIT 10
            """).fetchall()
            columns = [desc[0] for desc in conn.description]
            preview_data = [dict(zip(columns, row)) for row in preview_result]
            
            # Get sample summary
            conn.execute(f"CREATE OR REPLACE TEMPORARY VIEW sample_view AS {sample_query}")
            sample_summary = self._get_data_summary(conn, 'sample_view')
            
            # Update residual by removing sampled rows
            conn.execute(f"""
                DELETE FROM residual_data
                WHERE __row_id__ IN (
                    SELECT __row_id__ FROM round_{round_config.round_number}_sample
                )
            """)
            
            # Clean up temporary table
            conn.execute(f"DROP TABLE round_{round_config.round_number}_sample")
            
            completed_at = datetime.now()
            
            return RoundResult(
                round_number=round_config.round_number,
                method=round_config.method,
                sample_size=sample_size,
                output_uri=f"file://{sample_path}",
                preview=preview_data,
                summary=sample_summary,
                started_at=started_at,
                completed_at=completed_at
            )
            
        except Exception as e:
            logger.error(f"Error in round {round_config.round_number}: {str(e)}")
            raise
    
    def _build_base_query_for_round(
        self,
        conn: duckdb.DuckDBPyConnection,
        round_config: SamplingRoundConfig
    ) -> str:
        """Build base query for a sampling round from residual data"""
        # Start with residual data
        base_query = "SELECT * FROM current_residual"
        
        # Apply any round-specific filters
        if round_config.filters:
            # Use embedded filter query for DuckDB compatibility
            filter_clause = self._build_filter_query_embedded(round_config.filters)
            if filter_clause:
                base_query = f"SELECT * FROM ({base_query}) AS filtered {filter_clause}"
        
        # Apply selection if specified
        if round_config.selection:
            # Build column selection
            columns_sql = "*"
            if round_config.selection.columns:
                # Create a list to ensure __row_id__ is included
                selected_cols = list(round_config.selection.columns)
                if "__row_id__" not in selected_cols:
                    selected_cols.append("__row_id__")
                columns_sql = ', '.join([f'"{col}"' for col in selected_cols])
            elif round_config.selection.exclude_columns:
                # Get all columns from current_residual
                all_columns_info = conn.execute("PRAGMA table_info('current_residual')").fetchall()
                all_column_names = [info[1] for info in all_columns_info]
                # Filter out excluded columns and __row_id__
                selected_columns = [col for col in all_column_names 
                                  if col not in round_config.selection.exclude_columns and col != '__row_id__']
                # Always include __row_id__ for tracking
                if '__row_id__' not in selected_columns:
                    selected_columns.append('__row_id__')
                columns_sql = ', '.join([f'"{col}"' for col in selected_columns])
            
            # Apply order/limit/offset
            order_clause = self._build_order_limit_offset_clause(round_config.selection)
            if order_clause:
                base_query = f"SELECT {columns_sql} FROM ({base_query}) AS filtered_data {order_clause}"
            else:
                base_query = f"SELECT {columns_sql} FROM ({base_query}) AS filtered_data"
        
        return base_query
    
    async def _export_residual_dataset(
        self,
        conn: duckdb.DuckDBPyConnection,
        job: MultiRoundSamplingJob
    ) -> None:
        """Export the final residual dataset"""
        try:
            # Get residual count
            residual_count = conn.execute("SELECT COUNT(*) FROM residual_data").fetchone()[0]
            job.residual_size = residual_count
            
            if residual_count > 0:
                # Save residual data
                residual_path = self.storage.get_multi_round_residual_path(
                    dataset_id=job.dataset_id,
                    version_id=job.version_id,
                    job_id=job.id
                )
                
                # Export without internal row ID
                conn.execute(f"""
                    COPY (
                        SELECT * EXCLUDE (__row_id__)
                        FROM residual_data
                    ) TO '{residual_path}' (FORMAT PARQUET)
                """)
                
                job.residual_uri = f"file://{residual_path}"
                
                # Get residual summary
                job.residual_summary = self._get_data_summary(conn, 'residual_data')
            else:
                job.residual_uri = None
                job.residual_summary = None
                
        except Exception as e:
            logger.error(f"Error exporting residual dataset: {str(e)}")
            raise
    
    async def execute_multi_round_sampling_synchronously(
        self,
        dataset_id: int,
        version_id: int,
        request: MultiRoundSamplingRequest
    ) -> Dict[str, Any]:
        """
        Execute multi-round sampling synchronously and return results directly.
        
        Args:
            dataset_id: ID of the dataset to sample
            version_id: Version of the dataset to sample
            request: Multi-round sampling request with configuration
            
        Returns:
            Dictionary containing round results and residual data
        """
        conn = None
        try:
            # Get dataset information
            from app.db.connection import AsyncSessionLocal
            
            async with AsyncSessionLocal() as session:
                from app.datasets.repository import DatasetsRepository
                datasets_repo = DatasetsRepository(session)
                
                # Get dataset version
                version = await datasets_repo.get_dataset_version(version_id)
                if not version:
                    raise ValueError(f"Dataset version with ID {version_id} not found")
                
                # Verify dataset ID matches
                if version.dataset_id != dataset_id:
                    raise ValueError(f"Version {version_id} does not belong to dataset {dataset_id}")
                
                # Get file data
                file_info = await datasets_repo.get_file(version.file_id)
                if not file_info or not file_info.file_path:
                    raise ValueError("File path not found")
                
                # Create DuckDB connection
                conn = duckdb.connect(':memory:')
                
                # Create initial view from dataset
                conn.execute(f"CREATE VIEW original_data AS SELECT * FROM read_parquet('{file_info.file_path}')")
                
                # Add unique row identifier for tracking
                conn.execute("""
                    CREATE VIEW data_with_id AS 
                    SELECT *, ROW_NUMBER() OVER () AS __row_id__ 
                    FROM original_data
                """)
                
                # Initialize residual as full dataset
                conn.execute("CREATE TABLE residual_data AS SELECT * FROM data_with_id")
                
                # Process each round and collect results
                round_results = []
                
                for round_config in request.rounds:
                    try:
                        # Create view for current residual
                        logger.debug(f"Processing round {round_config.round_number}")
                        conn.execute("CREATE OR REPLACE VIEW current_residual AS SELECT * FROM residual_data")
                        
                        # Build base query from residual with filters
                        base_query = self._build_base_query_for_round(conn, round_config)
                        logger.debug(f"Round {round_config.round_number} base_query: {base_query}")
                    except Exception as e:
                        logger.error(f"Error in round {round_config.round_number} query building: {str(e)}")
                        raise
                    
                    # Create sampling request for this round
                    sampling_request = SamplingRequest(
                        method=round_config.method,
                        parameters=round_config.parameters,
                        output_name=round_config.output_name,
                        filters=round_config.filters,
                        selection=round_config.selection
                    )
                    
                    # Apply sampling to get sample query
                    sample_query = await self._apply_sampling_sql(conn, base_query, sampling_request)
                    
                    # Log the queries for debugging
                    logger.debug(f"Round {round_config.round_number} base_query: {base_query}")
                    logger.debug(f"Round {round_config.round_number} sample_query: {sample_query}")
                    
                    # Create temporary table with sampled IDs
                    try:
                        conn.execute(f"""
                            CREATE TEMPORARY TABLE round_{round_config.round_number}_sample AS
                            SELECT __row_id__ FROM ({sample_query})
                        """)
                    except Exception as e:
                        logger.error(f"Error creating temp table for round {round_config.round_number}")
                        logger.error(f"Base query: {base_query}")
                        logger.error(f"Sample query: {sample_query}")
                        raise
                    
                    # Get sample size
                    sample_size = conn.execute(f"SELECT COUNT(*) FROM round_{round_config.round_number}_sample").fetchone()[0]
                    
                    # Get full sample data (not just preview)
                    sample_result = conn.execute(f"""
                        SELECT * EXCLUDE (__row_id__)
                        FROM ({sample_query})
                    """).fetchall()
                    columns = [desc[0] for desc in conn.description]
                    sample_data = [dict(zip(columns, row)) for row in sample_result]
                    
                    # Get sample summary
                    conn.execute(f"CREATE OR REPLACE TEMPORARY VIEW sample_view AS {sample_query}")
                    sample_summary = self._get_data_summary(conn, 'sample_view')
                    
                    # Update residual by removing sampled rows
                    conn.execute(f"""
                        DELETE FROM residual_data
                        WHERE __row_id__ IN (SELECT __row_id__ FROM round_{round_config.round_number}_sample)
                    """)
                    
                    # Add round result
                    round_results.append({
                        "round_number": round_config.round_number,
                        "method": round_config.method.value,
                        "sample_size": sample_size,
                        "data": sample_data,
                        "summary": sample_summary
                    })
                
                # Handle residual data if requested
                residual_data = None
                residual_summary = None
                residual_size = 0
                
                if request.export_residual:
                    # Get residual count
                    residual_size = conn.execute("SELECT COUNT(*) FROM residual_data").fetchone()[0]
                    
                    if residual_size > 0:
                        # Get residual data
                        residual_result = conn.execute("""
                            SELECT * EXCLUDE (__row_id__)
                            FROM residual_data
                        """).fetchall()
                        columns = [desc[0] for desc in conn.description]
                        residual_data = [dict(zip(columns, row)) for row in residual_result]
                        
                        # Get residual summary
                        residual_summary = self._get_data_summary(conn, 'residual_data')
                
                return {
                    "rounds": round_results,
                    "residual": {
                        "size": residual_size,
                        "data": residual_data,
                        "summary": residual_summary
                    } if request.export_residual else None
                }
                
        except Exception as e:
            logger.error(f"Error executing multi-round sampling synchronously: {str(e)}", exc_info=True)
            raise
        finally:
            if conn:
                conn.close()
