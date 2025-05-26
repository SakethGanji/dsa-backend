import pandas as pd
import logging
import asyncio
from io import BytesIO
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
import uuid
import json
import numpy as np
from app.sampling.models import (
    SamplingMethod, JobStatus, SamplingRequest, 
    SamplingJob, RandomSamplingParams, StratifiedSamplingParams,
    SystematicSamplingParams, ClusterSamplingParams, CustomSamplingParams
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
                
                # Load file into pandas DataFrame
                df = self._load_dataframe(file_info, job.request.sheet)
                
                # Apply sampling method
                sampled_df = await self._apply_sampling(df, job.request)
                
                # Update preview and job
                job.output_preview = sampled_df.head(10).to_dict(orient="records")
                
                # In a real implementation, save the full sample to storage
                # For now, we'll just generate a mock URI
                job.output_uri = f"s3://sample-bucket/samples/{job.dataset_id}/{job.version_id}/{job_id}.parquet"
                
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
    
    # This method is no longer used - validation is done in _process_job
    # But we're keeping it for reference
    async def _validate_and_get_data(self, dataset_id: int, version_id: int) -> Tuple[Any, Any]:
        """Validate dataset and version IDs and get file data"""
        # Get version info
        logger.info(f"Sampling dataset {dataset_id}, version {version_id}")
        version = await self.datasets_repository.get_dataset_version(version_id)
        if not version:
            raise ValueError(f"Dataset version with ID {version_id} not found")
            
        # Verify dataset ID matches
        if version.dataset_id != dataset_id:
            raise ValueError(f"Version {version_id} does not belong to dataset {dataset_id}")
            
        # Get file data
        file_info = await self.datasets_repository.get_file(version.file_id)
        if not file_info or not file_info.file_data:
            raise ValueError("File data not found")
            
        return version, file_info
    
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