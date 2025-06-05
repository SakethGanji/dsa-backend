import duckdb
import logging
import asyncio
import tempfile
import os
from io import BytesIO
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from app.storage.local_storage import LocalFileStorage
from app.sampling.models import (
    SamplingMethod, JobStatus, SamplingRequest, 
    SamplingJob, RandomSamplingParams, StratifiedSamplingParams,
    SystematicSamplingParams, ClusterSamplingParams, CustomSamplingParams,
    DataFilters, DataSelection, FilterCondition, DataSummary,
    PipelineStep, PipelineStepConfig, PipelineFilterParams,
    ConsecutiveSamplingParams, PipelineRandomParams
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SamplingService:
    def __init__(self, datasets_repository, sampling_repository):
        self.datasets_repository = datasets_repository
        self.sampling_repository = sampling_repository
        self.storage = LocalFileStorage()
        
    async def create_sampling_job(
        self, 
        dataset_id: int,
        version_id: int,
        request: SamplingRequest,
        user_id: int
    ) -> SamplingJob:
        """
        Create and enqueue a new sampling job
        
        Args:
            dataset_id: ID of the dataset to sample
            version_id: Version of the dataset to sample
            request: Sampling request with method and parameters
            user_id: ID of the user creating the job
            
        Returns:
            A SamplingJob object with a unique ID
        """
        # Create a new job
        job = SamplingJob(
            dataset_id=dataset_id,
            version_id=version_id,
            user_id=user_id,
            request=request
        )
        
        # Store the job
        await self.sampling_repository.create_job(job)
        
        # Start the job in the background
        asyncio.create_task(self._process_job(job.id))
        
        return job
    
    async def get_job(self, job_id: str) -> Optional[SamplingJob]:
        """Get job details by ID"""
        return await self.sampling_repository.get_job(job_id)
    
    async def get_job_preview(self, job_id: str) -> List[Dict[str, Any]]:
        """Get preview data for a job"""
        job = await self.sampling_repository.get_job(job_id)
        if not job:
            return []
        
        return job.output_preview or []
    
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
                
                # Use DuckDB to get metadata
                conn = duckdb.connect(':memory:')
                try:
                    # Create view from Parquet file - this doesn't load data into memory
                    conn.execute(f"CREATE VIEW dataset AS SELECT * FROM read_parquet('{file_info.file_path}')")
                    
                    # Get column information
                    columns_info = conn.execute("PRAGMA table_info('dataset')").fetchall()
                    column_types = {}
                    columns = []
                    for col_info in columns_info:
                        col_name = col_info[1]
                        col_type = col_info[2]
                        columns.append(col_name)
                        column_types[col_name] = col_type
                    
                    # Get row count - for large datasets, use Parquet metadata
                    try:
                        # First check file size to decide approach
                        file_size_mb = os.path.getsize(file_info.file_path) / (1024 * 1024)
                        
                        if file_size_mb > 100:  # For files > 100MB, use metadata
                            logger.info(f"Large file detected ({file_size_mb:.1f}MB), using Parquet metadata for row count")
                            parquet_meta = conn.execute(f"SELECT num_rows FROM parquet_metadata('{file_info.file_path}')").fetchone()
                            total_rows = parquet_meta[0] if parquet_meta else 0
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
                        # Get null count
                        null_count = conn.execute(f'SELECT COUNT(*) FROM dataset WHERE "{col_name}" IS NULL').fetchone()[0]
                        null_counts[col_name] = null_count
                        
                        # Get sample unique values efficiently
                        try:
                            # For large datasets, use sampling to get distinct values quickly
                            if total_rows > 1000000:
                                # Use TABLESAMPLE for efficient sampling on large datasets
                                result = conn.execute(f'''
                                    WITH sampled AS (
                                        SELECT "{col_name}" 
                                        FROM dataset TABLESAMPLE(10000 ROWS)
                                        WHERE "{col_name}" IS NOT NULL
                                    )
                                    SELECT DISTINCT "{col_name}" 
                                    FROM sampled 
                                    LIMIT 10
                                ''').fetchall()
                            else:
                                # For smaller datasets, get distinct values directly
                                result = conn.execute(f'''
                                    SELECT DISTINCT "{col_name}" 
                                    FROM dataset 
                                    WHERE "{col_name}" IS NOT NULL 
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
    
    async def _process_job(self, job_id: str) -> None:
        """
        Process a sampling job in the background
        
        This method loads the dataset, applies the sampling method,
        and updates the job status.
        """
        job = await self.sampling_repository.get_job(job_id)
        if not job:
            logger.error(f"Job {job_id} not found")
            return
        
        try:
            # Update job status
            job.status = JobStatus.RUNNING
            job.started_at = datetime.now()
            await self.sampling_repository.update_job(job)
            
            # Validate dataset and version
            # Create a new connection for this background task
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
                
                # Check file size to ensure we can handle it
                if hasattr(file_info, 'file_size') and file_info.file_size:
                    max_file_size_gb = 50  # Maximum file size in GB
                    file_size_gb = file_info.file_size / (1024 * 1024 * 1024)
                    if file_size_gb > max_file_size_gb:
                        raise ValueError(f"File size ({file_size_gb:.2f} GB) exceeds maximum allowed size ({max_file_size_gb} GB)")
                
                # Create DuckDB connection
                conn = duckdb.connect(':memory:')
                
                try:
                    # Create a view instead of table to avoid loading entire file into memory
                    conn.execute(f"CREATE VIEW main_data AS SELECT * FROM read_parquet('{file_info.file_path}')")
                    
                    # Generate data summary
                    data_summary = self._get_data_summary(conn, 'main_data')
                    
                    # Apply filtering and sampling
                    sampled_data = await self._apply_sampling_with_duckdb(conn, job.request)
                    
                    # Generate sample summary
                    sample_summary = self._get_sample_summary_from_duckdb(conn, sampled_data)
                    
                    # Get preview data
                    preview_result = conn.execute(f"SELECT * FROM ({sampled_data}) LIMIT 10").fetchall()
                    columns = [desc[0] for desc in conn.description]
                    job.output_preview = [dict(zip(columns, row)) for row in preview_result]
                    
                    # Save sampled data as Parquet directly from DuckDB
                    sample_path, sample_size = await self.storage.save_sample_data_from_query(
                        conn=conn,
                        query=sampled_data,
                        dataset_id=job.dataset_id,
                        version_id=job.version_id,
                        job_id=job_id
                    )
                    
                    job.data_summary = data_summary
                    job.sample_summary = sample_summary
                    job.output_uri = f"file://{sample_path}"

                    # Update job status
                    job.status = JobStatus.COMPLETED
                    job.completed_at = datetime.now()
                    await self.sampling_repository.update_job(job)
                finally:
                    conn.close()
            
        except Exception as e:
            # Handle job failure
            logger.error(f"Error processing job {job_id}: {str(e)}", exc_info=True)
            job.status = JobStatus.FAILED
            job.error_message = str(e)
            if not job.started_at:
                job.started_at = datetime.now()
            job.completed_at = datetime.now()
            await self.sampling_repository.update_job(job)
    
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
            conn.execute(f"CREATE OR REPLACE TEMPORARY VIEW filtered_data AS {base_query}")
            
            # Apply sampling method
            if request.method == SamplingMethod.RANDOM:
                return self._random_sampling_sql(conn, params)
            elif request.method == SamplingMethod.STRATIFIED:
                return self._stratified_sampling_sql(conn, params)
            elif request.method == SamplingMethod.SYSTEMATIC:
                return self._systematic_sampling_sql(conn, params)
            elif request.method == SamplingMethod.CLUSTER:
                return self._cluster_sampling_sql(conn, params)
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
        
        # Set seed if provided
        if params.seed is not None:
            seed_value = (params.seed % 1000000) / 1000000.0
            conn.execute(f"SELECT setseed({seed_value})")
        
        # Use DuckDB's SAMPLE for sampling
        if params.sample_size < 10000:
            return f"SELECT * FROM filtered_data USING SAMPLE {params.sample_size}"
        else:
            # For larger samples, use percentage-based sampling
            sample_percent = min(100.0, (params.sample_size / max(1, total_count)) * 100 * 1.1)  # Add 10% buffer
            return f"SELECT * FROM filtered_data TABLESAMPLE {sample_percent} PERCENT"
    
    def _escape_sql_string(self, value: Any) -> str:
        """Escape a value for use in SQL string literal"""
        if value is None:
            return 'NULL'
        # Convert to string and escape single quotes
        str_value = str(value).replace("'", "''")
        return str_value
    
    def _stratified_sampling_sql(self, conn: duckdb.DuckDBPyConnection, params: StratifiedSamplingParams) -> str:
        """Generate SQL for stratified sampling using window functions for better performance"""
        # Validate strata columns exist
        columns_result = conn.execute("PRAGMA table_info('filtered_data')").fetchall()
        available_columns = [col[1] for col in columns_result]
        
        for col in params.strata_columns:
            if col not in available_columns:
                raise ValueError(f"Strata column '{col}' not found in dataset")
        
        # Build strata expression
        strata_cols = ", ".join([f'"{col}"' for col in params.strata_columns])
        
        # Set random seed if provided
        if params.seed:
            # DuckDB uses setseed with a value between -1 and 1
            seed_value = (params.seed % 1000000) / 1000000.0
            conn.execute(f"SELECT setseed({seed_value})")
        
        # Determine sampling approach based on parameters
        if params.sample_size is None and params.min_per_stratum is None:
            # Default to 10% per stratum
            return f"""
            SELECT * FROM (
                SELECT *, 
                       ROW_NUMBER() OVER (PARTITION BY {strata_cols} ORDER BY RANDOM()) as rn,
                       COUNT(*) OVER (PARTITION BY {strata_cols}) as stratum_count
                FROM filtered_data
            ) t
            WHERE rn <= CEIL(stratum_count * 0.1)
            """
        
        elif isinstance(params.sample_size, float):
            # Sample by fraction using window functions
            fraction = params.sample_size
            return f"""
            SELECT * FROM (
                SELECT *, 
                       ROW_NUMBER() OVER (PARTITION BY {strata_cols} ORDER BY RANDOM()) as rn,
                       COUNT(*) OVER (PARTITION BY {strata_cols}) as stratum_count
                FROM filtered_data
            ) t
            WHERE rn <= CEIL(stratum_count * {fraction})
            """
        
        else:
            # Proportional allocation with minimum - more complex but still single query
            total_samples = params.sample_size if params.sample_size else 1000
            min_per_stratum = params.min_per_stratum or 0
            
            # First get total count for proportion calculation
            total_count = conn.execute("SELECT COUNT(*) FROM filtered_data").fetchone()[0]
            
            return f"""
            SELECT * FROM (
                SELECT *, 
                       ROW_NUMBER() OVER (PARTITION BY {strata_cols} ORDER BY RANDOM()) as rn,
                       COUNT(*) OVER (PARTITION BY {strata_cols}) as stratum_count,
                       COUNT(*) OVER () as total_count
                FROM filtered_data
            ) t
            WHERE rn <= GREATEST(
                {min_per_stratum},
                LEAST(
                    stratum_count,
                    CEIL(({total_samples} * CAST(stratum_count AS FLOAT) / {total_count}))
                )
            )
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
        
        # Use DuckDB's window functions to efficiently sample clusters
        # This avoids loading all clusters into memory
        base_query = f"""
        WITH cluster_sample AS (
            SELECT DISTINCT "{params.cluster_column}",
                   ROW_NUMBER() OVER (ORDER BY RANDOM()) as rn
            FROM filtered_data
        )
        SELECT f.* FROM filtered_data f
        INNER JOIN cluster_sample cs ON f."{params.cluster_column}" = cs."{params.cluster_column}"
        WHERE cs.rn <= {params.num_clusters}
        """
        
        # Optionally sample within clusters
        if params.sample_within_clusters:
            # Sample 50% within each cluster using window functions
            return f"""
            WITH ranked AS (
                SELECT *, 
                       ROW_NUMBER() OVER (PARTITION BY "{params.cluster_column}" ORDER BY RANDOM()) as rn,
                       COUNT(*) OVER (PARTITION BY "{params.cluster_column}") as cluster_size
                FROM ({base_query}) t
            )
            SELECT * FROM ranked WHERE rn <= cluster_size / 2
            """
        
        return base_query
    
    def _custom_sampling_sql(self, conn: duckdb.DuckDBPyConnection, params: CustomSamplingParams) -> str:
        """Generate SQL for custom sampling"""
        # The custom query parameter should contain a WHERE clause condition
        # We'll wrap it in a proper SQL query
        return f"SELECT * FROM filtered_data WHERE {params.query}"
    
    
    def _get_data_summary(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> DataSummary:
        """Generate data summary statistics using DuckDB"""
        try:
            # Get basic info
            result = conn.execute(f"SELECT COUNT(*) as total_rows FROM {table_name}").fetchone()
            total_rows = result[0]
            
            # Get column info
            columns_result = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
            total_columns = len(columns_result)
            
            # Get column types and null counts
            column_types = {}
            null_counts = {}
            
            for col_info in columns_result:
                col_name = col_info[1]
                col_type = col_info[2]
                column_types[col_name] = col_type
                
                # Get null count for this column
                null_result = conn.execute(f"SELECT COUNT(*) FROM {table_name} WHERE \"{col_name}\" IS NULL").fetchone()
                null_counts[col_name] = null_result[0]
            
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
    
    async def execute_sampling_synchronously(
        self,
        dataset_id: int,
        version_id: int,
        request: SamplingRequest
    ) -> List[Dict[str, Any]]:
        """
        Execute sampling synchronously and return the result as a list of dictionaries.
        """
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

                # Check file size to ensure we can handle it
                if hasattr(file_info, 'file_size') and file_info.file_size:
                    max_file_size_gb = 50  # Maximum file size in GB
                    file_size_gb = file_info.file_size / (1024 * 1024 * 1024)
                    if file_size_gb > max_file_size_gb:
                        raise ValueError(f"File size ({file_size_gb:.2f} GB) exceeds maximum allowed size ({max_file_size_gb} GB)")
                
                # Create DuckDB connection
                conn = duckdb.connect(':memory:')
                
                try:
                    # Create a view instead of table to avoid loading entire file into memory
                    conn.execute(f"CREATE VIEW main_data AS SELECT * FROM read_parquet('{file_info.file_path}')")

                    # Apply filtering and sampling
                    sampled_query = await self._apply_sampling_with_duckdb(conn, request)
                    
                    # For synchronous sampling, limit the result size to prevent memory issues
                    max_sync_rows = 100000  # Maximum rows for synchronous response
                    
                    # First check the count
                    count_query = f"SELECT COUNT(*) FROM ({sampled_query}) t"
                    row_count = conn.execute(count_query).fetchone()[0]
                    
                    if row_count > max_sync_rows:
                        logger.warning(f"Sampled data has {row_count} rows, limiting to {max_sync_rows} for sync response")
                        # Add LIMIT to the query
                        limited_query = f"SELECT * FROM ({sampled_query}) t LIMIT {max_sync_rows}"
                    else:
                        limited_query = sampled_query
                    
                    # Fetch data directly without creating temporary table
                    result = conn.execute(limited_query).fetchall()
                    columns = [desc[0] for desc in conn.description]
                    
                    # Return as list of dictionaries
                    return [dict(zip(columns, row)) for row in result]
                    
                finally:
                    conn.close()

        except Exception as e:
            logger.error(f"Error executing sampling synchronously: {str(e)}", exc_info=True)
            # Re-raise as ValueError to be handled by the controller
            raise ValueError(f"Error executing sampling synchronously: {str(e)}")

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
            
            # Get column types and null counts
            column_types = {}
            null_counts = {}
            
            for col_info in columns_result:
                col_name = col_info[1]
                col_type = col_info[2]
                column_types[col_name] = col_type
                
                # Get null count for this column
                null_result = conn.execute(f'SELECT COUNT(*) FROM sample_data WHERE "{col_name}" IS NULL').fetchone()
                null_counts[col_name] = null_result[0]
            
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
        if not filters or not filters.conditions:
            return ""

        condition_strings = []
        for condition in filters.conditions:
            col = f'"{condition.column}"'  # Quote column names

            if condition.operator in ['IS NULL', 'IS NOT NULL']:
                condition_strings.append(f"{col} {condition.operator}")
            elif condition.operator in ['IN', 'NOT IN']:
                if isinstance(condition.value, list):
                    if not condition.value:  # Empty list
                        if condition.operator == 'IN':
                            condition_strings.append("0=1")  # Always false for IN empty list
                        else:  # NOT IN
                            condition_strings.append("1=1")  # Always true for NOT IN empty list
                    else:  # Non-empty list
                        # Escape each value
                        escaped_values = [f"'{self._escape_sql_string(v)}'" for v in condition.value]
                        condition_strings.append(f"{col} {condition.operator} ({', '.join(escaped_values)})")
                else:  # Single value, treat as = or !=
                    actual_operator = '=' if condition.operator == 'IN' else '!='
                    escaped_value = self._escape_sql_string(condition.value)
                    condition_strings.append(f"{col} {actual_operator} '{escaped_value}'")
            elif condition.operator in ['=', '!=', '>', '<', '>=', '<=']:
                # For numeric comparisons, don't quote the value if it's a number
                if isinstance(condition.value, (int, float)):
                    condition_strings.append(f"{col} {condition.operator} {condition.value}")
                else:
                    escaped_value = self._escape_sql_string(condition.value)
                    condition_strings.append(f"{col} {condition.operator} '{escaped_value}'")
            elif condition.operator in ['LIKE', 'ILIKE']:
                escaped_value = self._escape_sql_string(condition.value)
                condition_strings.append(f"{col} {condition.operator} '{escaped_value}'")
            else:
                # Default case - treat as string
                escaped_value = self._escape_sql_string(condition.value)
                condition_strings.append(f"{col} {condition.operator} '{escaped_value}'")

        if not condition_strings:
            return ""

        return f"WHERE {f' {filters.logic} '.join(condition_strings)}"
    
    def _build_filter_query(self, filters: Optional[DataFilters]) -> Tuple[str, List[Any]]:
        """Build SQL WHERE clause from filter conditions, returning clause and parameters."""
        if not filters or not filters.conditions:
            return "", []

        condition_strings = []
        params: List[Any] = []
        for condition in filters.conditions:
            col = f'"{condition.column}"'  # Quote column names

            if condition.operator in ['IS NULL', 'IS NOT NULL']:
                condition_strings.append(f"{col} {condition.operator}")
            elif condition.operator in ['IN', 'NOT IN']:
                if isinstance(condition.value, list):
                    if not condition.value:  # Empty list
                        if condition.operator == 'IN':
                            condition_strings.append("0=1")  # Always false for IN empty list
                        else:  # NOT IN
                            condition_strings.append("1=1")  # Always true for NOT IN empty list
                    else:  # Non-empty list
                        placeholders = ', '.join(['?'] * len(condition.value))
                        condition_strings.append(f"{col} {condition.operator} ({placeholders})")
                        params.extend(condition.value)
                else:  # Single value, treat as = or !=
                    actual_operator = '=' if condition.operator == 'IN' else '!='
                    condition_strings.append(f"{col} {actual_operator} ?")
                    params.append(condition.value)
            else:  # For other operators like =, !=, >, <, LIKE, ILIKE
                condition_strings.append(f"{col} {condition.operator} ?")
                params.append(condition.value)

        if not condition_strings:
            return "", []

        return f"WHERE {f' {filters.logic} '.join(condition_strings)}", params

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

    # Pipeline step implementations
    def _pipeline_filter_step(self, conn: duckdb.DuckDBPyConnection, base_view: str, params: PipelineFilterParams) -> str:
        """Apply filtering step in pipeline"""
        # Build filter conditions
        filter_conditions = DataFilters(conditions=params.conditions, logic=params.logic)
        filter_clause = self._build_filter_query_embedded(filter_conditions)
        
        if filter_clause:
            return f"SELECT * FROM {base_view} {filter_clause}"
        else:
            return f"SELECT * FROM {base_view}"
    
    def _pipeline_random_step(self, conn: duckdb.DuckDBPyConnection, base_view: str, params: PipelineRandomParams) -> str:
        """Apply random sampling step in pipeline"""
        # Get total count from the view
        total_count = conn.execute(f"SELECT COUNT(*) FROM {base_view}").fetchone()[0]
        
        if params.sample_size is None:
            # Default to 10% if not specified
            sample_size = max(1, int(total_count * 0.1))
        elif isinstance(params.sample_size, float):
            # Fraction-based sampling
            sample_size = max(1, int(total_count * params.sample_size))
        else:
            # Absolute number
            sample_size = min(params.sample_size, total_count)
        
        if sample_size >= total_count:
            return f"SELECT * FROM {base_view}"
        
        # Set seed if provided
        if params.seed is not None:
            seed_value = (params.seed % 1000000) / 1000000.0
            conn.execute(f"SELECT setseed({seed_value})")
        
        # Use DuckDB's sampling
        return f"SELECT * FROM {base_view} USING SAMPLE {sample_size}"
    
    def _pipeline_stratified_step(self, conn: duckdb.DuckDBPyConnection, base_view: str, params: StratifiedSamplingParams) -> str:
        """Apply stratified sampling step in pipeline"""
        # Similar to existing stratified sampling but works on a view
        strata_cols = ", ".join([f'"{col}"' for col in params.strata_columns])
        
        if params.sample_size is None and params.min_per_stratum is None:
            # Default to 10% per stratum
            fraction = 0.1
        elif isinstance(params.sample_size, float):
            fraction = params.sample_size
        else:
            # Need to calculate fraction based on total count
            total_count = conn.execute(f"SELECT COUNT(*) FROM {base_view}").fetchone()[0]
            fraction = params.sample_size / max(1, total_count) if params.sample_size else 0.1
        
        if params.seed:
            # Set the random seed for deterministic results
            # DuckDB uses setseed with a value between -1 and 1
            seed_value = (params.seed % 1000000) / 1000000.0
            conn.execute(f"SELECT setseed({seed_value})")
        
        return f"""
        SELECT * FROM (
            SELECT *, 
                   ROW_NUMBER() OVER (PARTITION BY {strata_cols} ORDER BY RANDOM()) as rn,
                   COUNT(*) OVER (PARTITION BY {strata_cols}) as stratum_count
            FROM {base_view}
        ) t
        WHERE rn <= CEIL(stratum_count * {fraction})
        """
    
    def _pipeline_cluster_step(self, conn: duckdb.DuckDBPyConnection, base_view: str, params: ClusterSamplingParams) -> str:
        """Apply cluster sampling step in pipeline"""
        # Get unique cluster count
        cluster_count_result = conn.execute(f'SELECT COUNT(DISTINCT "{params.cluster_column}") FROM {base_view}').fetchone()
        total_clusters = cluster_count_result[0] if cluster_count_result else 0
        
        if params.num_clusters >= total_clusters:
            return f"SELECT * FROM {base_view}"
        
        base_query = f"""
        WITH cluster_sample AS (
            SELECT DISTINCT "{params.cluster_column}",
                   ROW_NUMBER() OVER (ORDER BY RANDOM()) as rn
            FROM {base_view}
        )
        SELECT f.* FROM {base_view} f
        INNER JOIN cluster_sample cs ON f."{params.cluster_column}" = cs."{params.cluster_column}"
        WHERE cs.rn <= {params.num_clusters}
        """
        
        if params.sample_within_clusters:
            # Sample 50% within each cluster
            return f"""
            WITH ranked AS (
                SELECT *, 
                       ROW_NUMBER() OVER (PARTITION BY "{params.cluster_column}" ORDER BY RANDOM()) as rn,
                       COUNT(*) OVER (PARTITION BY "{params.cluster_column}") as cluster_size
                FROM ({base_query}) t
            )
            SELECT * FROM ranked WHERE rn <= cluster_size / 2
            """
        
        return base_query
    
    def _pipeline_consecutive_step(self, conn: duckdb.DuckDBPyConnection, base_view: str, params: ConsecutiveSamplingParams) -> str:
        """Apply consecutive/systematic sampling step in pipeline"""
        if params.interval <= 0:
            raise ValueError("Interval must be greater than 0")
        
        start = params.start if params.start is not None else 0
        
        return f"""
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER () - 1 as rn 
            FROM {base_view}
        ) t 
        WHERE (rn - {start}) % {params.interval} = 0
        """
    
    async def _apply_pipeline_sampling(self, conn: duckdb.DuckDBPyConnection, request: SamplingRequest) -> str:
        """Apply pipeline-based sampling"""
        if not request.pipeline:
            raise ValueError("Pipeline is empty")
        
        # Start with the main data view
        current_view = "main_data"
        view_counter = 0
        
        # Process each pipeline step
        for i, step_config in enumerate(request.pipeline):
            view_counter += 1
            next_view = f"pipeline_step_{view_counter}"
            
            # Get typed parameters for the step
            params = step_config.get_typed_parameters()
            
            # Apply the appropriate step
            if step_config.step == PipelineStep.FILTER:
                query = self._pipeline_filter_step(conn, current_view, params)
            elif step_config.step == PipelineStep.RANDOM_SAMPLE:
                query = self._pipeline_random_step(conn, current_view, params)
            elif step_config.step == PipelineStep.STRATIFIED_SAMPLE:
                query = self._pipeline_stratified_step(conn, current_view, params)
            elif step_config.step == PipelineStep.CLUSTER_SAMPLE:
                query = self._pipeline_cluster_step(conn, current_view, params)
            elif step_config.step == PipelineStep.CONSECUTIVE_SAMPLE:
                query = self._pipeline_consecutive_step(conn, current_view, params)
            else:
                raise ValueError(f"Unknown pipeline step: {step_config.step}")
            
            # Create a view for this step's output
            conn.execute(f"CREATE OR REPLACE TEMPORARY VIEW {next_view} AS {query}")
            
            # Log step completion
            logger.info(f"Pipeline step {i+1}/{len(request.pipeline)} completed: {step_config.step}")
            
            # Update current view for next step
            current_view = next_view
        
        # Apply final selection if specified
        if request.selection:
            self._validate_selection(conn, request.selection)
            select_clause = self._build_select_from_clause(conn, request.selection, current_view)
            order_limit_clause = self._build_order_limit_offset_clause(request.selection)
            
            if order_limit_clause:
                return f"{select_clause} {order_limit_clause}"
            else:
                return select_clause
        else:
            return f"SELECT * FROM {current_view}"
    
    async def _apply_sampling_with_duckdb(self, conn: duckdb.DuckDBPyConnection, request: SamplingRequest) -> str:
        """Apply filtering, selection, and sampling using DuckDB and return the final SQL query."""
        try:
            # Check if we're in pipeline mode
            if request.is_pipeline_mode():
                return await self._apply_pipeline_sampling(conn, request)
            
            # Traditional sampling mode
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
        if not filters.conditions:
            return
        
        # Get available columns
        columns_result = conn.execute("PRAGMA table_info('main_data')").fetchall()
        available_columns = {col[1]: col[2] for col in columns_result}  # name: type
        
        for condition in filters.conditions:
            # Check if column exists
            if condition.column not in available_columns:
                raise ValueError(f"Filter column '{condition.column}' does not exist")
            
            # Basic type validation for certain operators
            col_type = available_columns[condition.column].lower()
            if condition.operator in ['>', '<', '>=', '<='] and 'text' in col_type:
                logger.warning(f"Using numeric comparison operator on text column '{condition.column}'")
    
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
    
    def get_export_formats(self) -> List[str]:
        """Get supported export formats"""
        # Return hardcoded list for now since config is not available
        return ["csv", "parquet", "json"]
    
    async def export_sample(self, job_id: str, format: str) -> bytes:
        """Export sample data in the specified format"""
        # This is a placeholder for export functionality
        # In a real implementation, you would:
        # 1. Get the job and its sampled data
        # 2. Convert to the requested format
        # 3. Return the bytes
        
        job = await self.sampling_repository.get_job(job_id)
        if not job or job.status != JobStatus.COMPLETED:
            raise ValueError("Job not found or not completed")
        
        if format not in self.get_export_formats():
            raise ValueError(f"Unsupported export format: {format}")
        
        # For now, just return a placeholder
        return b"Export functionality not yet implemented"
