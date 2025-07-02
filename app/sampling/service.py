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
    SamplingRoundConfig, RoundResult,
    AnalysisRunResponse
)
from app.sampling.db_repository import SamplingDBRepository

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class SamplingService:
    def __init__(self, datasets_repository, sampling_repository, storage_backend: StorageBackend, db_session=None, artifact_producer=None):
        self.datasets_repository = datasets_repository
        self.sampling_repository = sampling_repository  # Keep for backward compatibility
        self.storage = storage_backend
        self.db_session = db_session  # For database operations
        self.artifact_producer = artifact_producer
        self.db_repository = None
        if db_session:
            self.db_repository = SamplingDBRepository(db_session)
    
    async def _save_sample_file(self, conn: duckdb.DuckDBPyConnection, query: str, output_path: str) -> int:
        """Save sample results to file using artifact producer if available.
        
        Args:
            conn: DuckDB connection
            query: SQL query to get sample data
            output_path: Target path for the file
            
        Returns:
            File ID if using artifact producer, 0 otherwise
        """
        if self.artifact_producer:
            # Export to temporary file first
            temp_fd, temp_path = tempfile.mkstemp(suffix=".parquet")
            os.close(temp_fd)
            
            try:
                # Export query results to temp file
                conn.execute(f"COPY ({query}) TO '{temp_path}' (FORMAT PARQUET)")
                
                # Create artifact using producer
                with open(temp_path, 'rb') as f:
                    file_id = await self.artifact_producer.create_artifact(
                        content_stream=f,
                        file_type="parquet",
                        mime_type="application/parquet",
                        metadata={
                            "source": "sampling_service",
                            "query": query[:500],  # Store first 500 chars of query
                            "target_path": output_path
                        }
                    )
                
                logger.info(f"Sample file created via artifact producer: id={file_id}")
                return file_id
                
            finally:
                # Clean up temp file
                if os.path.exists(temp_path):
                    os.unlink(temp_path)
        else:
            # Fallback to direct file creation
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            conn.execute(f"COPY ({query}) TO '{output_path}' (FORMAT PARQUET)")
            return 0
    
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
                
                # Get primary file info from version_files table
                primary_file = await datasets_repo.get_version_file_by_component(
                    version_id, "primary", "main"
                )
                
                if primary_file and primary_file.file:
                    file_info = primary_file.file
                else:
                    # Fallback to overlay file if no primary file in version_files
                    file_id = version.overlay_file_id
                    file_info = await datasets_repo.get_file(file_id)
                    
                if not file_info or not file_info.file_path:
                    raise ValueError("File path not found")
                
                # Convert relative path to absolute path if needed
                file_path = file_info.file_path
                if not os.path.isabs(file_path):
                    base_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))), "data")
                    file_path = os.path.join(base_path, file_path)
                
                # Ensure parquet extension
                if not file_path.endswith('.parquet'):
                    file_path = f"{file_path}.parquet"
                
                # Use file-backed DuckDB to prevent memory crashes
                temp_db_fd, temp_db_path = tempfile.mkstemp(suffix=".duckdb")
                os.close(temp_db_fd)  # Close the file descriptor
                os.unlink(temp_db_path)  # Remove the empty file so DuckDB can create it fresh
                
                try:
                    conn = duckdb.connect(database=temp_db_path, read_only=False)
                    try:
                        # Create view from Parquet file - this doesn't load data into memory
                        conn.execute(f"CREATE VIEW dataset AS SELECT * FROM read_parquet('{file_path}')")
                        
                        # Get column information - use DESCRIBE for better performance
                        columns_info = conn.execute("DESCRIBE dataset").fetchall()
                        columns = []
                        column_types = {}
                        for col_info in columns_info:
                            col_name = col_info[0]
                            col_type = col_info[1]
                            columns.append(col_name)
                            column_types[col_name] = col_type
                        
                        # Get row count using parquet_metadata for instant results
                        try:
                            # Use parquet_metadata for instantaneous row count
                            meta = conn.execute(f"SELECT num_rows FROM parquet_metadata('{file_path}')").fetchone()
                            total_rows = meta[0] if meta and meta[0] is not None else 0
                            logger.info(f"Got row count from parquet metadata: {total_rows}")
                        except Exception as e:
                            # Fallback to COUNT(*) if metadata is not available
                            logger.warning(f"Could not get row count from metadata: {str(e)}, falling back to COUNT(*)")
                            try:
                                total_rows = conn.execute("SELECT COUNT(*) FROM dataset").fetchone()[0]
                            except Exception as count_error:
                                logger.error(f"Error getting row count: {str(count_error)}, defaulting to 0")
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
                finally:
                    # Clean up temporary database file
                    if os.path.exists(temp_db_path):
                        try:
                            os.unlink(temp_db_path)
                        except Exception as cleanup_error:
                            logger.warning(f"Failed to delete temporary DB file: {cleanup_error}")
                
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
    
    async def _apply_sampling_sql(self, conn: duckdb.DuckDBPyConnection, base_query: str, query_params: List[Any], request: SamplingRequest) -> str:
        try:
            # Get typed parameters
            params = request.get_typed_parameters()
            
            # Create temporary table from base query with parameters
            # We can't use views with parameters, so we use a temp table instead
            try:
                logger.debug(f"Creating filtered_data table with base_query: {base_query} and params: {query_params}")
                if query_params:
                    conn.execute(f"CREATE OR REPLACE TEMPORARY TABLE filtered_data AS {base_query}", query_params)
                else:
                    conn.execute(f"CREATE OR REPLACE TEMPORARY TABLE filtered_data AS {base_query}")
            except Exception as e:
                logger.error(f"Failed to create filtered_data table")
                logger.error(f"Base query was: {base_query}")
                logger.error(f"Query params were: {query_params}")
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
        
        if params.seed is not None:
            # For exact sample size with seed, use SAMPLE clause with reservoir method
            return f"SELECT * FROM filtered_data USING SAMPLE reservoir({params.sample_size} ROWS) REPEATABLE({params.seed})"
        else:
            # Always use SAMPLE for consistent behavior
            return f"SELECT * FROM filtered_data USING SAMPLE {params.sample_size} ROWS"
    
    
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
            # Proportional allocation - ensure exact sample size using pure SQL
            total_samples = params.sample_size if params.sample_size else 1000
            min_per_stratum = params.min_per_stratum or 0
            seed_expr = f"RANDOM({params.seed})" if params.seed else "RANDOM()"
            
            # Use window functions for efficient proportional allocation
            return f"""
            WITH strata_stats AS (
                -- Calculate stratum sizes and total count
                SELECT 
                    {strata_cols},
                    COUNT(*) as stratum_size,
                    SUM(COUNT(*)) OVER () as total_size
                FROM filtered_data
                GROUP BY {strata_cols}
            ),
            allocations AS (
                -- Calculate proportional allocations with adjustments
                SELECT 
                    {strata_cols},
                    stratum_size,
                    total_size,
                    -- Base allocation (proportional)
                    GREATEST(
                        {min_per_stratum},
                        FLOOR({total_samples} * CAST(stratum_size AS DOUBLE) / total_size)
                    ) as base_allocation,
                    -- Remainder for adjustment
                    ({total_samples} * CAST(stratum_size AS DOUBLE) / total_size) - 
                    FLOOR({total_samples} * CAST(stratum_size AS DOUBLE) / total_size) as remainder
                FROM strata_stats
            ),
            adjusted_allocations AS (
                -- Adjust allocations to match exact sample size
                SELECT 
                    {strata_cols},
                    stratum_size,
                    CASE
                        -- Add 1 to strata with highest remainders until we reach target
                        WHEN ROW_NUMBER() OVER (ORDER BY remainder DESC) <= 
                            ({total_samples} - SUM(base_allocation) OVER ())
                        THEN LEAST(base_allocation + 1, stratum_size)
                        ELSE LEAST(base_allocation, stratum_size)
                    END as final_allocation
                FROM allocations
            ),
            stratified_sample AS (
                -- Sample from each stratum according to allocation
                SELECT 
                    f.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY {strata_cols} 
                        ORDER BY {seed_expr}
                    ) as row_num,
                    a.final_allocation
                FROM filtered_data f
                JOIN adjusted_allocations a USING ({strata_cols})
            )
            SELECT * EXCLUDE (row_num, final_allocation)
            FROM stratified_sample
            WHERE row_num <= final_allocation
            """
    
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
        """Generate SQL for weighted sampling using DuckDB's native weighted reservoir sampler"""
        # Validate weight column exists
        columns_result = conn.execute("PRAGMA table_info('filtered_data')").fetchall()
        available_columns = [col[1] for col in columns_result]
        
        if params.weight_column not in available_columns:
            raise ValueError(f"Weight column '{params.weight_column}' not found in dataset")
        
        # DuckDB's weighted reservoir sampler is highly optimized and simpler to use
        if params.seed is not None:
            return f"""
            SELECT *
            FROM filtered_data
            USING SAMPLE reservoir({params.sample_size} ROWS) REPEATABLE({params.seed})
            WITH WEIGHTS "{params.weight_column}"
            """
        else:
            return f"""
            SELECT *
            FROM filtered_data
            USING SAMPLE reservoir({params.sample_size} ROWS)
            WITH WEIGHTS "{params.weight_column}"
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

            # Now apply sampling method to get the final query
            return await self._apply_sampling_sql(conn, base_query, filter_params, request)
            
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
    ) -> Dict[str, Any]:
        """
        Create and enqueue a new multi-round sampling job
        
        Args:
            dataset_id: ID of the dataset to sample
            version_id: Version of the dataset to sample
            request: Multi-round sampling request with configuration
            user_id: ID of the user creating the job
            
        Returns:
            Dictionary with job ID and status information
        """
        # Use database repository if available, otherwise fall back to in-memory
        if self.db_repository:
            # Create analysis run in database
            run_id = await self.db_repository.create_analysis_run(
                dataset_version_id=version_id,
                user_id=user_id,
                request=request
            )
            
            # Start the job in the background
            asyncio.create_task(self._process_multi_round_job_db(run_id))
            
            return {
                "run_id": run_id,
                "status": "pending",
                "message": "Multi-round sampling job created successfully"
            }
        else:
            # Fallback to in-memory repository for backward compatibility
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
            
            return {
                "run_id": job.id,
                "status": "pending",
                "message": "Multi-round sampling job created successfully",
                "job": job  # Include for backward compatibility
            }
    
    async def get_multi_round_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get multi-round job details by ID"""
        if self.db_repository:
            # Try to parse job_id as integer for database
            try:
                run_id = int(job_id)
                logger.debug(f"Looking for job {run_id} in database")
                run_data = await self.db_repository.get_analysis_run(run_id)
                logger.debug(f"Database returned: {run_data is not None}")
                if run_data:
                    transformed = self._transform_db_run_to_job_response(run_data)
                    logger.debug(f"Transformed data: {transformed is not None}")
                    return transformed
            except ValueError:
                # Not an integer ID, might be a UUID from in-memory
                logger.debug(f"Job ID {job_id} is not an integer, skipping database lookup")
                pass
            except Exception as e:
                logger.error(f"Error getting job from database: {str(e)}", exc_info=True)
                raise
        
        # Fallback to in-memory repository
        job = await self.sampling_repository.get_multi_round_job(job_id)
        if job:
            return job.dict()
        
        return None
    
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
        temp_db_path = None
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
                
                # Get primary file info from version_files table
                primary_file = await datasets_repo.get_version_file_by_component(
                    version_id, "primary", "main"
                )
                
                if primary_file and primary_file.file:
                    file_info = primary_file.file
                else:
                    # Fallback to overlay file if no primary file in version_files
                    file_id = version.overlay_file_id
                    file_info = await datasets_repo.get_file(file_id)
                    
                if not file_info or not file_info.file_path:
                    raise ValueError("File path not found")
                
                # Convert relative path to absolute path if needed
                file_path = file_info.file_path
                if not os.path.isabs(file_path):
                    base_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))), "data")
                    file_path = os.path.join(base_path, file_path)
                
                # Ensure parquet extension
                if not file_path.endswith('.parquet'):
                    file_path = f"{file_path}.parquet"
                
                # Create file-backed DuckDB connection to prevent memory crashes
                temp_db_fd, temp_db_path = tempfile.mkstemp(suffix=".duckdb")
                os.close(temp_db_fd)  # Close the file descriptor
                os.unlink(temp_db_path)  # Remove the empty file so DuckDB can create it fresh
                
                conn = duckdb.connect(database=temp_db_path, read_only=False)
                
                # Create initial view from dataset
                conn.execute(f"CREATE VIEW original_data AS SELECT * FROM read_parquet('{file_path}')")
                
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
                        conn, job, round_config, file_path
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
            # Clean up temporary database file
            if temp_db_path and os.path.exists(temp_db_path):
                try:
                    os.unlink(temp_db_path)
                except Exception as e:
                    logger.warning(f"Failed to delete temporary DB file: {e}")
    
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
            base_query, query_params = self._build_base_query_for_round(conn, round_config)
            
            # Create sampling request for this round
            sampling_request = SamplingRequest(
                method=round_config.method,
                parameters=round_config.parameters,
                output_name=round_config.output_name,
                filters=round_config.filters,
                selection=round_config.selection
            )
            
            # Apply sampling to get sample query
            sample_query = await self._apply_sampling_sql(conn, base_query, query_params, sampling_request)
            
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
            sample_file_id = await self._save_sample_file(
                conn=conn,
                query=f"SELECT * EXCLUDE (__row_id__) FROM ({sample_query})",
                output_path=sample_path
            )
            
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
            
            # Update residual by creating new table without sampled rows
            # This is much faster than DELETE for large datasets
            conn.execute(f"""
                CREATE TABLE new_residual_data AS
                SELECT * FROM residual_data
                WHERE __row_id__ NOT IN (
                    SELECT __row_id__ FROM round_{round_config.round_number}_sample
                )
            """)
            conn.execute("DROP TABLE residual_data")
            conn.execute("ALTER TABLE new_residual_data RENAME TO residual_data")
            
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
    ) -> Tuple[str, List[Any]]:
        """Build base query for a sampling round from residual data"""
        # Start with residual data
        base_query = "SELECT * FROM current_residual"
        params = []
        
        # Apply any round-specific filters
        if round_config.filters:
            # Build filter with parameters
            filter_clause, filter_params = self._build_filter_query(round_config.filters)
            if filter_clause:
                base_query = f"SELECT * FROM ({base_query}) AS filtered {filter_clause}"
                params.extend(filter_params)
        
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
        
        return base_query, params
    
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
        
        **Warning:** This method loads all sampled data into memory. It is
        not suitable for large datasets. Use the asynchronous job-based
        method for production workloads.
        
        Args:
            dataset_id: ID of the dataset to sample
            version_id: Version of the dataset to sample
            request: Multi-round sampling request with configuration
            
        Returns:
            Dictionary containing round results and residual data
        """
        conn = None
        temp_db_path = None
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
                
                # Get primary file info from version_files table
                primary_file = await datasets_repo.get_version_file_by_component(
                    version_id, "primary", "main"
                )
                
                if primary_file and primary_file.file:
                    file_info = primary_file.file
                else:
                    # Fallback to overlay file if no primary file in version_files
                    file_id = version.overlay_file_id
                    file_info = await datasets_repo.get_file(file_id)
                    
                if not file_info or not file_info.file_path:
                    raise ValueError("File path not found")
                
                # Convert relative path to absolute path if needed
                file_path = file_info.file_path
                if not os.path.isabs(file_path):
                    base_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))), "data")
                    file_path = os.path.join(base_path, file_path)
                
                # Ensure parquet extension
                if not file_path.endswith('.parquet'):
                    file_path = f"{file_path}.parquet"
                
                # Create file-backed DuckDB connection to prevent memory crashes
                temp_db_fd, temp_db_path = tempfile.mkstemp(suffix=".duckdb")
                os.close(temp_db_fd)  # Close the file descriptor
                os.unlink(temp_db_path)  # Remove the empty file so DuckDB can create it fresh
                
                conn = duckdb.connect(database=temp_db_path, read_only=False)
                
                # Create initial view from dataset
                conn.execute(f"CREATE VIEW original_data AS SELECT * FROM read_parquet('{file_path}')")
                
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
                        base_query, query_params = self._build_base_query_for_round(conn, round_config)
                        logger.debug(f"Round {round_config.round_number} base_query: {base_query} params: {query_params}")
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
                    sample_query = await self._apply_sampling_sql(conn, base_query, query_params, sampling_request)
                    
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
                    
                    # Update residual by creating new table without sampled rows
                    # This is much faster than DELETE for large datasets
                    conn.execute(f"""
                        CREATE TABLE new_residual_data AS
                        SELECT * FROM residual_data
                        WHERE __row_id__ NOT IN (
                            SELECT __row_id__ FROM round_{round_config.round_number}_sample
                        )
                    """)
                    conn.execute("DROP TABLE residual_data")
                    conn.execute("ALTER TABLE new_residual_data RENAME TO residual_data")
                    
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
            # Clean up temporary database file
            if temp_db_path and os.path.exists(temp_db_path):
                try:
                    os.unlink(temp_db_path)
                except Exception as e:
                    logger.warning(f"Failed to delete temporary DB file: {e}")

    # Database-based multi-round sampling methods
    async def _process_multi_round_job_db(self, run_id: int) -> None:
        """
        Process a multi-round sampling job using database persistence
        
        This method executes each sampling round progressively,
        tracking residuals after each round and persisting state to database.
        """
        start_time = datetime.now()
        conn = None
        temp_db_path = None
        
        # Create a new, independent database session for this background task
        from app.db.connection import AsyncSessionLocal
        from app.datasets.repository import DatasetsRepository
        from app.datasets.models import FileCreate
        
        async with AsyncSessionLocal() as session:
            # Create repository instances with the new session
            db_repository = SamplingDBRepository(session)
            datasets_repo = DatasetsRepository(session)
            
            try:
                # Update job status to running using the new session
                await db_repository.update_analysis_run(
                    run_id=run_id,
                    status="running"
                )
                
                # Get job details from database
                run_data = await db_repository.get_analysis_run(run_id)
                if not run_data:
                    logger.error(f"Analysis run {run_id} not found after starting.")
                    return
            
                # Extract request from run_parameters
                try:
                    request_data = run_data["run_parameters"]["request"]
                    logger.debug(f"Request data type: {type(request_data)}")
                    logger.debug(f"Request data: {request_data}")
                    request = MultiRoundSamplingRequest(**request_data)
                except Exception as e:
                    logger.error(f"Error parsing request: {str(e)}")
                    logger.error(f"run_parameters type: {type(run_data.get('run_parameters'))}")
                    logger.error(f"run_parameters: {run_data.get('run_parameters')}")
                    raise ValueError(f"Failed to parse sampling request: {str(e)}")
                dataset_version_id = run_data["dataset_version_id"]
                
                # Get dataset version
                version = await datasets_repo.get_dataset_version(dataset_version_id)
                if not version:
                    raise ValueError(f"Dataset version with ID {dataset_version_id} not found")
                
                # Get primary file info from version_files table
                primary_file = await datasets_repo.get_version_file_by_component(
                    dataset_version_id, "primary", "main"
                )
                
                if primary_file and primary_file.file:
                    file_info = primary_file.file
                else:
                    # Fallback to overlay file if no primary file in version_files
                    file_id = version.overlay_file_id
                    file_info = await datasets_repo.get_file(file_id)
                    
                if not file_info or not file_info.file_path:
                    raise ValueError("File path not found")
                
                # Convert relative path to absolute path if needed
                file_path = file_info.file_path
                if not os.path.isabs(file_path):
                    base_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))), "data")
                    file_path = os.path.join(base_path, file_path)
                
                # Ensure parquet extension
                if not file_path.endswith('.parquet'):
                    file_path = f"{file_path}.parquet"
                
                # Create file-backed DuckDB connection
                temp_db_fd, temp_db_path = tempfile.mkstemp(suffix=".duckdb")
                os.close(temp_db_fd)
                os.unlink(temp_db_path)
                
                conn = duckdb.connect(database=temp_db_path, read_only=False)
                
                # Create initial view from dataset
                conn.execute(f"CREATE VIEW original_data AS SELECT * FROM read_parquet('{file_path}')")
                
                # Add unique row identifier for tracking
                conn.execute("""
                    CREATE VIEW data_with_id AS 
                    SELECT *, ROW_NUMBER() OVER () AS __row_id__ 
                    FROM original_data
                """)
                
                # Initialize residual as full dataset
                conn.execute("CREATE TABLE residual_data AS SELECT * FROM data_with_id")
                
                # SIMPLIFIED: Create ONE table to collect all sampled IDs
                conn.execute("CREATE TABLE all_sampled_ids (__row_id__ BIGINT)")
                
                # Process each round
                round_results = []
                for round_config in request.rounds:
                    # Update current round in database
                    await db_repository.update_round_progress(
                        run_id=run_id,
                        current_round=round_config.round_number,
                        completed_rounds=len(round_results),
                        round_results=round_results
                    )
                    
                    # Execute sampling for this round (simplified)
                    round_result = await self._execute_sampling_round_simplified(
                        conn, round_config
                    )
                    
                    # Add to results
                    round_results.append(round_result)
                
                # Export final residual if requested
                residual_info = None
                if request.export_residual:
                    residual_info = await self._export_residual_dataset_db(
                        conn, run_data, request.residual_output_name or "residual"
                    )
                
                # Create final merged sample file
                merged_info = await self._create_merged_sample_db(
                    conn, run_data
                )
                
                # Register the output file in the database
                file_create = FileCreate(
                    file_type="parquet",
                    mime_type="application/parquet",
                    file_path=merged_info["merged_path"],
                    file_size=os.path.getsize(merged_info["merged_path"])
                )
                
                output_file_id = await datasets_repo.create_file(file_create)
                
                # Calculate execution time
                execution_time_ms = int((datetime.now() - start_time).total_seconds() * 1000)
                
                # Prepare output summary
                output_summary = {
                    "total_rounds": len(request.rounds),
                    "completed_rounds": len(round_results),
                    "round_results": round_results,
                    "total_samples": merged_info["sample_count"],
                    "residual_info": residual_info
                }
                
                # Update job as completed
                await db_repository.update_analysis_run(
                    run_id=run_id,
                    status="completed",
                    output_file_id=output_file_id,
                    output_summary=output_summary,
                    execution_time_ms=execution_time_ms
                )
                
                logger.info(f"Multi-round sampling job {run_id} completed successfully")
                
            except Exception as e:
                logger.error(f"Error processing multi-round job {run_id}: {str(e)}", exc_info=True)
                
                # Use the same db_repository instance to update the status to failed
                await db_repository.update_analysis_run(
                    run_id=run_id,
                    status="failed",
                    notes=f"An unexpected error occurred: {str(e)}",
                    execution_time_ms=int((datetime.now() - start_time).total_seconds() * 1000)
                )
            
            finally:
                # Clean up
                if conn:
                    conn.close()
                if temp_db_path and os.path.exists(temp_db_path):
                    os.unlink(temp_db_path)
    
    async def _execute_sampling_round_db(
        self,
        conn: duckdb.DuckDBPyConnection,
        run_data: dict,
        round_config: SamplingRoundConfig,
        original_file_path: str
    ) -> RoundResult:
        """Execute a single sampling round for database-backed job"""
        started_at = datetime.now()
        
        try:
            # Create view for current residual
            conn.execute("CREATE OR REPLACE VIEW current_residual AS SELECT * FROM residual_data")
            
            # Build base query from residual with filters
            base_query, query_params = self._build_base_query_for_round(conn, round_config)
            
            # Create sampling request for this round
            sampling_request = SamplingRequest(
                method=round_config.method,
                parameters=round_config.parameters,
                output_name=round_config.output_name,
                filters=round_config.filters,
                selection=round_config.selection
            )
            
            # Apply sampling to get sample query
            sample_query = await self._apply_sampling_sql(conn, base_query, query_params, sampling_request)
            
            # Create temporary table with sampled IDs
            conn.execute(f"""
                CREATE TEMPORARY TABLE round_{round_config.round_number}_sample AS
                SELECT __row_id__ FROM ({sample_query})
            """)
            
            # Get sample size
            sample_size = conn.execute(f"SELECT COUNT(*) FROM round_{round_config.round_number}_sample").fetchone()[0]
            
            # Save sample data (without __row_id__)
            dataset_id = run_data["dataset_id"]
            version_id = run_data["dataset_version_id"]
            sample_path = self.storage.get_multi_round_sample_path(
                dataset_id=dataset_id,
                version_id=version_id,
                job_id=str(run_data["id"]),
                round_number=round_config.round_number
            )
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(sample_path), exist_ok=True)
            
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
            
            # Update residual by creating new table without sampled rows
            conn.execute(f"""
                CREATE TABLE new_residual_data AS
                SELECT * FROM residual_data
                WHERE __row_id__ NOT IN (
                    SELECT __row_id__ FROM round_{round_config.round_number}_sample
                )
            """)
            conn.execute("DROP TABLE residual_data")
            conn.execute("ALTER TABLE new_residual_data RENAME TO residual_data")
            
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
    
    async def _create_merged_sample_db(
        self, conn: duckdb.DuckDBPyConnection, 
        run_data: dict
    ) -> Dict[str, Any]:
        """Create a single merged sample file from all rounds for database-backed job"""
        # Check if we have any sampled IDs
        sample_count = conn.execute("SELECT COUNT(*) FROM all_sampled_ids").fetchone()[0]
        
        if sample_count > 0:
            # FIXED: Use correct join logic with data_with_id
            conn.execute("""
                CREATE VIEW final_merged_sample AS
                SELECT * EXCLUDE (__row_id__)
                FROM data_with_id
                WHERE __row_id__ IN (SELECT __row_id__ FROM all_sampled_ids)
            """)
        else:
            # No samples collected
            conn.execute("""
                CREATE VIEW final_merged_sample AS
                SELECT * FROM original_data LIMIT 0
            """)
        
        # Export merged sample
        dataset_id = run_data["dataset_id"]
        version_id = run_data["dataset_version_id"]
        merged_path = self.storage.get_multi_round_sample_path(
            dataset_id, version_id, str(run_data["id"]), 0  # Use 0 for merged sample
        )
        
        os.makedirs(os.path.dirname(merged_path), exist_ok=True)
        conn.execute(f"COPY final_merged_sample TO '{merged_path}' (FORMAT PARQUET)")
        
        return {"merged_path": merged_path, "sample_count": sample_count}
    
    async def _execute_sampling_round_simplified(
        self,
        conn: duckdb.DuckDBPyConnection,
        round_config: SamplingRoundConfig
    ) -> dict:
        """
        Execute a single sampling round and update residual (simplified version)
        
        This method:
        1. Samples from current residual
        2. Adds sampled IDs directly to all_sampled_ids table
        3. Updates residual data
        4. Returns summary information
        """
        started_at = datetime.now()
        
        try:
            # Create view for current residual
            conn.execute("CREATE OR REPLACE VIEW current_residual AS SELECT * FROM residual_data")
            
            # Build base query from residual with filters
            base_query, query_params = self._build_base_query_for_round(conn, round_config)
            
            # Create sampling request for this round
            sampling_request = SamplingRequest(
                method=round_config.method,
                parameters=round_config.parameters,
                output_name=round_config.output_name,
                filters=round_config.filters,
                selection=round_config.selection
            )
            
            # Apply sampling to get sample query
            sample_query = await self._apply_sampling_sql(conn, base_query, query_params, sampling_request)
            
            # SIMPLIFIED: Insert sampled IDs directly into accumulator table
            conn.execute(f"""
                INSERT INTO all_sampled_ids (__row_id__)
                SELECT __row_id__ FROM ({sample_query})
            """)
            
            # Get count of samples in this round
            round_sample_count = conn.execute(f"""
                SELECT COUNT(*) FROM ({sample_query})
            """).fetchone()[0]
            
            # Update residual by removing sampled rows
            conn.execute(f"""
                CREATE TABLE new_residual_data AS
                SELECT * FROM residual_data
                WHERE __row_id__ NOT IN (SELECT __row_id__ FROM ({sample_query}))
            """)
            conn.execute("DROP TABLE residual_data")
            conn.execute("ALTER TABLE new_residual_data RENAME TO residual_data")
            
            completed_at = datetime.now()
            
            # Return summary info for this round
            return {
                "round_number": round_config.round_number,
                "method": round_config.method.value,
                "sample_size": round_sample_count,
                "started_at": started_at.isoformat(),
                "completed_at": completed_at.isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error in round {round_config.round_number}: {str(e)}")
            raise
    
    async def _export_residual_dataset_db(
        self, conn: duckdb.DuckDBPyConnection, 
        run_data: dict, output_name: str
    ) -> Dict[str, Any]:
        """Export the final residual dataset for database-backed job"""
        # Get residual count
        residual_count = conn.execute("SELECT COUNT(*) FROM residual_data").fetchone()[0]
        
        # Export residual
        dataset_id = run_data["dataset_id"]
        version_id = run_data["dataset_version_id"]
        residual_path = self.storage.get_multi_round_residual_path(
            dataset_id, version_id, str(run_data["id"])
        )
        
        os.makedirs(os.path.dirname(residual_path), exist_ok=True)
        
        # Export without the __row_id__ column
        conn.execute(f"""
            COPY (SELECT * EXCLUDE (__row_id__) FROM residual_data) 
            TO '{residual_path}' (FORMAT PARQUET)
        """)
        
        # Calculate summary statistics
        residual_summary = self._get_data_summary(conn, 'residual_data')
        
        return {
            "path": residual_path,
            "size": residual_count,
            "summary": residual_summary.dict() if residual_summary else None
        }
    
    def _transform_db_run_to_job_response(self, run_data: dict) -> dict:
        """Transform database analysis run to job response format"""
        if not run_data:
            return None
        run_params = run_data.get("run_parameters", {})
        
        # Extract round results and other info from run_parameters
        round_results = run_params.get("round_results", [])
        completed_rounds = run_params.get("completed_rounds", 0)
        current_round = run_params.get("current_round")
        
        # Check if round results are in output_summary instead
        if run_data.get("output_summary") and "round_results" in run_data["output_summary"]:
            raw_round_results = run_data["output_summary"]["round_results"]
            completed_rounds = run_data["output_summary"].get("completed_rounds", len(raw_round_results))
        else:
            # Use round results from run_parameters
            raw_round_results = round_results
        
        # Transform round results to include required fields
        round_results = []
        for rr in raw_round_results:
            # Make a copy to avoid modifying original data
            round_result = dict(rr)
            
            # Add default output_uri if missing
            if "output_uri" not in round_result:
                dataset_id = run_data["dataset_id"]
                version_id = run_data["dataset_version_id"]
                run_id = run_data["id"]
                round_number = round_result.get("round_number", 1)
                # Construct expected path
                round_result["output_uri"] = f"file:///data/samples/{dataset_id}/{version_id}/multi_round/{run_id}/{round_number}.parquet"
            
            # Ensure all required fields are present
            if "preview" not in round_result:
                round_result["preview"] = None
            if "summary" not in round_result:
                round_result["summary"] = None
            
            # Ensure sample_size is present
            if "sample_size" not in round_result:
                round_result["sample_size"] = 0  # Default to 0 if not available
            
            # Ensure datetime fields are present
            if "started_at" not in round_result:
                round_result["started_at"] = run_data.get("run_timestamp", datetime.now())
            if "completed_at" not in round_result:
                round_result["completed_at"] = run_data.get("run_timestamp", datetime.now())
                
            round_results.append(round_result)
        
        # Build response
        response = {
            "id": str(run_data["id"]),  # Convert to string for consistency
            "dataset_id": run_data["dataset_id"],
            "version_id": run_data["dataset_version_id"],
            "user_id": run_data["user_id"],
            "status": run_data["status"],
            "created_at": run_data["run_timestamp"],
            "total_rounds": run_params.get("total_rounds", 0),
            "completed_rounds": completed_rounds,
            "current_round": current_round,
            "round_results": round_results,
            "request": run_params.get("request", {})
        }
        
        # Add optional fields if present
        if run_data.get("execution_time_ms"):
            response["execution_time_ms"] = run_data["execution_time_ms"]
            
        if run_data.get("notes"):
            response["error_message"] = run_data["notes"]
            
        if run_data.get("output_summary"):
            summary = run_data["output_summary"]
            if "residual_info" in summary and summary["residual_info"] is not None:
                residual_info = summary["residual_info"]
                response["residual_uri"] = residual_info.get("path")
                response["residual_size"] = residual_info.get("size")
                response["residual_summary"] = residual_info.get("summary")
        
        return response
    
    async def recover_running_jobs(self) -> None:
        """
        Recover jobs that were running when the server shut down.
        Called on startup to handle interrupted jobs.
        """
        if not self.db_repository:
            return
            
        try:
            running_jobs = await self.db_repository.get_running_jobs()
            
            for job_data in running_jobs:
                run_id = job_data["id"]
                logger.info(f"Recovering running job {run_id}")
                
                # Check how long the job has been running
                run_timestamp = job_data["run_timestamp"]
                time_since_start = datetime.now() - run_timestamp
                
                # If job has been running for more than 1 hour, mark as failed
                if time_since_start.total_seconds() > 3600:
                    await self.db_repository.update_analysis_run(
                        run_id=run_id,
                        status="failed",
                        notes="Job interrupted by server restart and exceeded recovery timeout"
                    )
                else:
                    # Restart the job
                    asyncio.create_task(self._process_multi_round_job_db(run_id))
        except Exception as e:
            logger.error(f"Error recovering running jobs: {str(e)}", exc_info=True)
    
    async def get_merged_sample_data(
        self,
        job_id: str,
        page: int = 1,
        page_size: int = 100,
        columns: Optional[List[str]] = None,
        export_format: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get paginated data from the merged sample file using DuckDB
        
        Args:
            job_id: The multi-round sampling job ID
            page: Page number (1-indexed)
            page_size: Number of items per page
            columns: Optional list of columns to return
            export_format: Optional export format (csv, json)
            
        Returns:
            Dictionary containing paginated data and metadata
        """
        # Get the job details from the database
        if not self.db_session:
            raise ValueError("Database session not available")
            
        db_repository = SamplingDBRepository(self.db_session)
        run_data = await db_repository.get_analysis_run(int(job_id))
        
        if not run_data:
            raise ValueError(f"Job {job_id} not found")
            
        if run_data["status"] != "completed":
            raise ValueError(f"Job {job_id} is not completed yet (status: {run_data['status']})")
            
        if not run_data["output_file_path"]:
            raise ValueError(f"Job {job_id} has no output file")
        
        # Get the merged sample file path
        merged_file_path = run_data["output_file_path"]
        
        if not os.path.exists(merged_file_path):
            raise ValueError(f"Merged sample file not found at {merged_file_path}")
        
        # Use DuckDB to efficiently query the parquet file
        conn = None
        try:
            conn = duckdb.connect()
            
            # Read the parquet file
            conn.execute(f"CREATE VIEW merged_sample AS SELECT * FROM '{merged_file_path}'")
            
            # Get total count
            total_count = conn.execute("SELECT COUNT(*) FROM merged_sample").fetchone()[0]
            
            # Calculate pagination
            total_pages = (total_count + page_size - 1) // page_size
            offset = (page - 1) * page_size
            
            # Build column selection
            if columns:
                # Validate columns exist
                available_columns = [col[0] for col in conn.execute("DESCRIBE merged_sample").fetchall()]
                invalid_columns = [col for col in columns if col not in available_columns]
                if invalid_columns:
                    raise ValueError(f"Invalid columns: {invalid_columns}")
                column_list = ", ".join(columns)
            else:
                column_list = "*"
            
            # Query with pagination
            query = f"""
                SELECT {column_list}
                FROM merged_sample
                LIMIT {page_size}
                OFFSET {offset}
            """
            
            result = conn.execute(query).fetchall()
            
            # Get column names
            if columns:
                column_names = columns
            else:
                column_names = [col[0] for col in conn.execute("DESCRIBE merged_sample").fetchall()]
            
            # Convert to list of dicts
            data = [dict(zip(column_names, row)) for row in result]
            
            # Handle export formats
            if export_format:
                if export_format.lower() == "csv":
                    import csv
                    import io
                    output = io.StringIO()
                    writer = csv.DictWriter(output, fieldnames=column_names)
                    writer.writeheader()
                    writer.writerows(data)
                    return {
                        "format": "csv",
                        "data": output.getvalue(),
                        "filename": f"job_{job_id}_page_{page}.csv"
                    }
                elif export_format.lower() == "json":
                    return {
                        "format": "json",
                        "data": data,
                        "filename": f"job_{job_id}_page_{page}.json"
                    }
                else:
                    raise ValueError(f"Unsupported export format: {export_format}")
            
            # Get summary statistics if on first page
            summary = None
            if page == 1:
                summary = run_data.get("output_summary", {})
            
            return {
                "data": data,
                "pagination": {
                    "page": page,
                    "page_size": page_size,
                    "total_items": total_count,
                    "total_pages": total_pages,
                    "has_next": page < total_pages,
                    "has_previous": page > 1
                },
                "columns": column_names,
                "summary": summary,
                "file_path": merged_file_path,
                "job_id": job_id
            }
            
        finally:
            if conn:
                conn.close()
    
    async def get_samplings_by_user(
        self,
        user_id: int,
        page: int = 1,
        page_size: int = 10
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        Get all sampling runs created by a specific user
        
        Args:
            user_id: The user ID to filter by
            page: Page number (1-indexed)
            page_size: Number of items per page
            
        Returns:
            Tuple of (list of sampling runs, total count)
        """
        if not self.db_repository:
            raise ValueError("Database repository not available")
            
        offset = (page - 1) * page_size
        return await self.db_repository.get_samplings_by_user(
            user_id=user_id,
            limit=page_size,
            offset=offset
        )
    
    async def get_samplings_by_dataset_version(
        self,
        dataset_version_id: int,
        page: int = 1,
        page_size: int = 10
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        Get all sampling runs for a specific dataset version
        
        Args:
            dataset_version_id: The dataset version ID to filter by
            page: Page number (1-indexed)
            page_size: Number of items per page
            
        Returns:
            Tuple of (list of sampling runs, total count)
        """
        if not self.db_repository:
            raise ValueError("Database repository not available")
            
        offset = (page - 1) * page_size
        return await self.db_repository.get_samplings_by_dataset_version(
            dataset_version_id=dataset_version_id,
            limit=page_size,
            offset=offset
        )
    
    async def get_samplings_by_dataset(
        self,
        dataset_id: int,
        page: int = 1,
        page_size: int = 10
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        Get all sampling runs for a specific dataset (across all versions)
        
        Args:
            dataset_id: The dataset ID to filter by
            page: Page number (1-indexed)
            page_size: Number of items per page
            
        Returns:
            Tuple of (list of sampling runs, total count)
        """
        if not self.db_repository:
            raise ValueError("Database repository not available")
            
        offset = (page - 1) * page_size
        return await self.db_repository.get_samplings_by_dataset(
            dataset_id=dataset_id,
            limit=page_size,
            offset=offset
        )