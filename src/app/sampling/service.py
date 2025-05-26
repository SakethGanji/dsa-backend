import duckdb
import pandas as pd
import logging
import asyncio
from io import BytesIO
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
import numpy as np
from app.sampling.models import (
    SamplingMethod, JobStatus, SamplingRequest, 
    SamplingJob, RandomSamplingParams, StratifiedSamplingParams,
    SystematicSamplingParams, ClusterSamplingParams, CustomSamplingParams,
    DataFilters, DataSelection, FilterCondition, DataSummary
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SamplingService:
    def __init__(self, datasets_repository, sampling_repository):
        self.datasets_repository = datasets_repository
        self.sampling_repository = sampling_repository
        
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
                if not file_info or not file_info.file_data:
                    raise ValueError("File data not found")
                
                # Create DuckDB connection and load data
                conn = duckdb.connect(':memory:')
                self._load_data_to_duckdb(conn, file_info)
                
                # Get column information
                data_summary = self._get_data_summary(conn, 'main_data')
                
                # Get sample of unique values for each column (helpful for filters)
                sample_values = {}
                for col_name in data_summary.column_types.keys():
                    try:
                        # Get first 10 unique values for this column
                        result = conn.execute(f'''
                            SELECT DISTINCT "{col_name}" 
                            FROM main_data 
                            WHERE "{col_name}" IS NOT NULL 
                            ORDER BY "{col_name}" 
                            LIMIT 10
                        ''').fetchall()
                        sample_values[col_name] = [row[0] for row in result]
                    except Exception:
                        # Skip columns that can't be sampled (e.g., complex types)
                        sample_values[col_name] = []
                
                return {
                    "columns": list(data_summary.column_types.keys()),
                    "column_types": data_summary.column_types,
                    "total_rows": data_summary.total_rows,
                    "null_counts": data_summary.null_counts,
                    "sample_values": sample_values
                }
                
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
                if not file_info or not file_info.file_data:
                    raise ValueError("File data not found")
                
                # Create DuckDB connection
                conn = duckdb.connect(':memory:')
                
                # Load file into DuckDB
                self._load_data_to_duckdb(conn, file_info, job.request.sheet)
                
                # Generate data summary
                data_summary = self._get_data_summary(conn, 'main_data')
                
                # Apply filtering and sampling
                sampled_df = await self._apply_sampling_with_duckdb(conn, job.request)
                
                # Generate sample summary
                sample_summary = self._get_dataframe_summary(sampled_df)
                
                # Update preview and job
                job.output_preview = sampled_df.head(10).to_dict(orient="records")
                job.data_summary = data_summary
                job.sample_summary = sample_summary
                
                # Use a local file path for the mock URI
                job.output_uri = f"file://outputs/samples/{job.dataset_id}/{job.version_id}/{job_id}.parquet"

                # Update job status
                job.status = JobStatus.COMPLETED
                job.completed_at = datetime.now()
                await self.sampling_repository.update_job(job)
            
        except Exception as e:
            # Handle job failure
            logger.error(f"Error processing job {job_id}: {str(e)}", exc_info=True)
            job.status = JobStatus.FAILED
            job.error_message = str(e)
            if not job.started_at:
                job.started_at = datetime.now()
            job.completed_at = datetime.now()
            await self.sampling_repository.update_job(job)
    
    def _load_dataframe(self, file_info: Any, sheet_name: Optional[str] = None) -> pd.DataFrame:
        """Load file data into a pandas DataFrame"""
        file_data = file_info.file_data
        file_type = file_info.file_type.lower()

        # Create BytesIO object from file data
        buffer = BytesIO(file_data)
        
        try:
            if file_type == "csv":
                return pd.read_csv(buffer)
            elif file_type in ["xls", "xlsx", "xlsm"]:
                if sheet_name:
                    return pd.read_excel(buffer, sheet_name=sheet_name)
                else:
                    # If no sheet name provided, use the first sheet
                    return pd.read_excel(buffer)
            else:
                # Just try csv as a fallback
                return pd.read_csv(buffer)
        except Exception as e:
            logger.error(f"Error loading file: {str(e)}")
            raise ValueError(f"Error loading file: {str(e)}")
    
    async def _apply_sampling(self, df: pd.DataFrame, request: SamplingRequest) -> pd.DataFrame:
        """
        Apply the requested sampling method to the DataFrame
        
        Args:
            df: Input DataFrame to sample
            request: Sampling request with method and parameters
            
        Returns:
            A sampled DataFrame
        """
        try:
            # Get typed parameters
            params = request.get_typed_parameters()
            
            # Apply sampling method
            if request.method == SamplingMethod.RANDOM:
                return self._random_sampling(df, params)
            elif request.method == SamplingMethod.STRATIFIED:
                return self._stratified_sampling(df, params)
            elif request.method == SamplingMethod.SYSTEMATIC:
                return self._systematic_sampling(df, params)
            elif request.method == SamplingMethod.CLUSTER:
                return self._cluster_sampling(df, params)
            elif request.method == SamplingMethod.CUSTOM:
                return self._custom_sampling(df, params)
            else:
                raise ValueError(f"Unknown sampling method: {request.method}")
        except Exception as e:
            logger.error(f"Error applying sampling: {str(e)}", exc_info=True)
            raise ValueError(f"Error applying sampling: {str(e)}")
    
    def _random_sampling(self, df: pd.DataFrame, params: RandomSamplingParams) -> pd.DataFrame:
        """Apply random sampling"""
        if params.sample_size >= len(df):
            return df
        
        # Set seed if provided
        if params.seed is not None:
            np.random.seed(params.seed)
        
        # Sample randomly
        return df.sample(n=params.sample_size)
    
    def _stratified_sampling(self, df: pd.DataFrame, params: StratifiedSamplingParams) -> pd.DataFrame:
        """Apply stratified sampling"""
        # Check if strata columns exist
        for col in params.strata_columns:
            if col not in df.columns:
                raise ValueError(f"Strata column '{col}' not found in dataset")
        
        # Create a combined strata column for sampling
        df['_strata'] = df[params.strata_columns].apply(lambda x: '_'.join(x.astype(str)), axis=1)
        
        # Set seed if provided
        if params.seed is not None:
            np.random.seed(params.seed)
        
        # Determine sampling strategy
        if params.sample_size is None and params.min_per_stratum is None:
            # Default to 10% per stratum
            frac = 0.1
            strata_samples = None
        elif isinstance(params.sample_size, float):
            # Sample by fraction
            frac = params.sample_size
            strata_samples = None
        else:
            # Calculate samples per stratum
            strata_counts = df['_strata'].value_counts()
            total_samples = params.sample_size if params.sample_size else int(len(df) * 0.1)
            
            # Allocate samples proportionally
            strata_samples = {}
            for stratum, count in strata_counts.items():
                allocated = max(
                    int(total_samples * (count / len(df))),
                    params.min_per_stratum or 0
                )
                # Cap at the stratum size
                strata_samples[stratum] = min(allocated, count)
            
            frac = None
        
        # Sample from each stratum
        if strata_samples:
            # Sample specific counts from each stratum
            samples = []
            for stratum, count in strata_samples.items():
                stratum_df = df[df['_strata'] == stratum]
                if len(stratum_df) > 0:
                    samples.append(stratum_df.sample(n=min(count, len(stratum_df))))
            
            result = pd.concat(samples) if samples else pd.DataFrame(columns=df.columns)
        else:
            # Sample by fraction
            result = df.groupby('_strata', group_keys=False).apply(
                lambda x: x.sample(frac=frac)
            )
        
        # Remove the temporary strata column
        if '_strata' in result.columns:
            result = result.drop('_strata', axis=1)
        
        return result
    
    def _systematic_sampling(self, df: pd.DataFrame, params: SystematicSamplingParams) -> pd.DataFrame:
        """Apply systematic sampling"""
        if params.interval <= 0:
            raise ValueError("Interval must be greater than 0")
        
        # Get the indices to sample
        start = params.start if params.start is not None else 0
        indices = range(start, len(df), params.interval)
        
        # Sample the DataFrame
        return df.iloc[indices].reset_index(drop=True)
    
    def _cluster_sampling(self, df: pd.DataFrame, params: ClusterSamplingParams) -> pd.DataFrame:
        """Apply cluster sampling"""
        # Check if cluster column exists
        if params.cluster_column not in df.columns:
            raise ValueError(f"Cluster column '{params.cluster_column}' not found in dataset")
        
        # Get unique clusters
        clusters = df[params.cluster_column].unique()
        
        if params.num_clusters >= len(clusters):
            # If we want more clusters than exist, return all
            return df
        
        # Sample clusters
        sampled_clusters = np.random.choice(
            clusters, 
            size=params.num_clusters,
            replace=False
        )
        
        # Get data for sampled clusters
        result = df[df[params.cluster_column].isin(sampled_clusters)]
        
        # Optionally sample within clusters
        if params.sample_within_clusters:
            # Simple 50% sample within each cluster
            result = result.groupby(params.cluster_column, group_keys=False).apply(
                lambda x: x.sample(frac=0.5)
            )
        
        return result
    
    def _custom_sampling(self, df: pd.DataFrame, params: CustomSamplingParams) -> pd.DataFrame:
        """Apply custom sampling with a query"""
        try:
            # Apply the query
            return df.query(params.query)
        except Exception as e:
            raise ValueError(f"Error in custom query: {str(e)}")
    
    def _load_data_to_duckdb(self, conn: duckdb.DuckDBPyConnection, file_info: Any, sheet_name: Optional[str] = None) -> None:
        """Load file data into DuckDB table"""
        file_data = file_info.file_data
        file_type = file_info.file_type.lower()
        
        # Create BytesIO object from file data
        buffer = BytesIO(file_data)
        
        try:
            if file_type == "csv":
                df = pd.read_csv(buffer)
            elif file_type in ["xls", "xlsx", "xlsm"]:
                if sheet_name:
                    df = pd.read_excel(buffer, sheet_name=sheet_name)
                else:
                    df = pd.read_excel(buffer)
            else:
                df = pd.read_csv(buffer)
            
            # Register the DataFrame as a table in DuckDB
            conn.register('main_data', df)
            
        except Exception as e:
            logger.error(f"Error loading file to DuckDB: {str(e)}")
            raise ValueError(f"Error loading file: {str(e)}")
    
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
    ) -> pd.DataFrame:
        """
        Execute sampling synchronously and return the result as a DataFrame.
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
                if not file_info or not file_info.file_data:
                    raise ValueError("File data not found")

                # Create DuckDB connection
                conn = duckdb.connect(':memory:')

                # Load file into DuckDB
                self._load_data_to_duckdb(conn, file_info, request.sheet)

                # Apply filtering and sampling
                sampled_df = await self._apply_sampling_with_duckdb(conn, request)

                return sampled_df

        except Exception as e:
            logger.error(f"Error executing sampling synchronously: {str(e)}", exc_info=True)
            # Re-raise as ValueError to be handled by the controller
            raise ValueError(f"Error executing sampling synchronously: {str(e)}")

    def _get_dataframe_summary(self, df: pd.DataFrame) -> DataSummary:
        """Generate summary statistics for a pandas DataFrame"""
        column_types = {col: str(df[col].dtype) for col in df.columns}
        null_counts = df.isnull().sum().to_dict()
        memory_usage_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
        
        return DataSummary(
            total_rows=len(df),
            total_columns=len(df.columns),
            column_types=column_types,
            memory_usage_mb=round(memory_usage_mb, 2),
            null_counts=null_counts
        )
    
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

    async def _apply_sampling_with_duckdb(self, conn: duckdb.DuckDBPyConnection, request: SamplingRequest) -> pd.DataFrame:
        """Apply filtering, selection, and sampling using DuckDB for initial processing."""
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

            final_query = " ".join(query_parts).strip()

            logger.debug(f"Executing DuckDB query: {final_query} with params: {filter_params}")

            # Execute query to get filtered/selected data
            query_result = conn.execute(final_query, filter_params if filter_params else None)
            filtered_df = query_result.fetchdf()

            # Validate that we still have data after filtering
            if len(filtered_df) == 0:
                # This can be a valid result of filtering, so just log a warning.
                logger.warning("No data remaining after applying filters/selection, or the source table was empty.")

            # Now apply sampling method to the filtered data (which might be empty)
            return await self._apply_sampling(filtered_df, request)
            
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
        from app.sampling.config import get_sampling_settings
        settings = get_sampling_settings()
        return settings.supported_export_formats
    
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
