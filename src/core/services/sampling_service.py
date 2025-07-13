"""PostgreSQL-based implementation of ISamplingService."""

import logging
from typing import List, Dict, Any, AsyncGenerator, Optional
from uuid import uuid4

from ..abstractions.services import ISamplingService, SamplingMethod, SampleConfig, SampleResult
from ..abstractions.repositories import ITableReader
from ...infrastructure.postgres.database import DatabasePool
from ...workers.sampling_executor import SamplingJobExecutor

logger = logging.getLogger(__name__)


class PostgresSamplingService(ISamplingService):
    """PostgreSQL-based streaming implementation of ISamplingService."""
    
    def __init__(self, db_pool: DatabasePool):
        self._db_pool = db_pool
        self._executor = SamplingJobExecutor()
        self._available_methods = [
            SamplingMethod.RANDOM,
            SamplingMethod.STRATIFIED,
            SamplingMethod.SYSTEMATIC,
            SamplingMethod.CLUSTER,
            SamplingMethod.MULTI_ROUND
        ]
    
    async def sample(
        self,
        table_reader: ITableReader,
        commit_id: str,
        table_key: str,
        config: SampleConfig
    ) -> SampleResult:
        """Perform sampling on table data."""
        logger.info(f"Starting {config.method.value} sampling for table {table_key}")
        
        # Handle multi-round sampling
        if config.method == SamplingMethod.MULTI_ROUND:
            return await self._multi_round_sample(table_reader, commit_id, table_key, config)
        
        # Single round sampling
        sampled_data = []
        sample_params = self._config_to_params(config)
        
        # Get total row count for some methods
        if config.method == SamplingMethod.RANDOM and not config.random_seed:
            total_rows = await table_reader.count_table_rows(commit_id, table_key)
            sample_params['total_rows'] = total_rows
        
        # Stream results from table reader
        async for row in table_reader.get_table_sample_stream(
            commit_id, table_key, config.method.value, sample_params
        ):
            sampled_data.append(row)
            
            # Stop if we've reached the desired sample size
            if len(sampled_data) >= config.sample_size:
                break
        
        # Build result metadata
        metadata = {
            'commit_id': commit_id,
            'table_key': table_key,
            'sampling_params': sample_params,
            'actual_sample_size': len(sampled_data)
        }
        
        # Add method-specific metadata
        result = SampleResult(
            sampled_data=sampled_data,
            sample_size=len(sampled_data),
            method_used=config.method,
            metadata=metadata
        )
        
        # Add stratification counts if applicable
        if config.method == SamplingMethod.STRATIFIED and config.stratify_columns:
            result.strata_counts = self._calculate_strata_counts(
                sampled_data, config.stratify_columns
            )
        
        # Add cluster info if applicable
        if config.method == SamplingMethod.CLUSTER and config.cluster_column:
            result.selected_clusters = list(set(
                row.get(config.cluster_column) for row in sampled_data
            ))
        
        logger.info(f"Sampling completed: {len(sampled_data)} rows sampled")
        return result
    
    async def _multi_round_sample(
        self,
        table_reader: ITableReader,
        commit_id: str,
        table_key: str,
        config: SampleConfig
    ) -> SampleResult:
        """Handle multi-round sampling."""
        if not config.round_configs:
            raise ValueError("Multi-round sampling requires round_configs")
        
        all_sampled_data = []
        round_results = []
        sampled_ids = set()
        
        for round_idx, round_config in enumerate(config.round_configs):
            logger.info(f"Executing round {round_idx + 1} of {len(config.round_configs)}")
            
            # Sample for this round
            round_result = await self.sample(
                table_reader, commit_id, table_key, round_config
            )
            
            # Filter out already sampled rows
            new_rows = []
            for row in round_result.sampled_data:
                row_id = row.get('_logical_row_id')
                if row_id and row_id not in sampled_ids:
                    new_rows.append(row)
                    sampled_ids.add(row_id)
            
            all_sampled_data.extend(new_rows)
            round_results.append(round_result)
            
            logger.info(f"Round {round_idx + 1}: {len(new_rows)} new rows sampled")
        
        # Combine results
        return SampleResult(
            sampled_data=all_sampled_data,
            sample_size=len(all_sampled_data),
            method_used=SamplingMethod.MULTI_ROUND,
            metadata={
                'commit_id': commit_id,
                'table_key': table_key,
                'num_rounds': len(config.round_configs),
                'total_unique_samples': len(all_sampled_data)
            },
            round_results=round_results
        )
    
    def create_strategy(self, method: SamplingMethod) -> Any:
        """Create a sampling strategy for the given method."""
        # In this SQL-based implementation, strategies are handled by SQL queries
        # This method is kept for interface compatibility
        return f"SQL-based {method.value} sampling strategy"
    
    def list_available_methods(self) -> List[SamplingMethod]:
        """List all available sampling methods."""
        return self._available_methods
    
    def _config_to_params(self, config: SampleConfig) -> Dict[str, Any]:
        """Convert SampleConfig to executor parameters."""
        params = {
            'sample_size': config.sample_size,
            'seed': config.random_seed
        }
        
        if config.method == SamplingMethod.STRATIFIED:
            params['strata_columns'] = config.stratify_columns or []
            params['min_per_stratum'] = 1 if config.proportional else config.sample_size
            
        elif config.method == SamplingMethod.CLUSTER:
            params['cluster_column'] = config.cluster_column
            params['num_clusters'] = config.num_clusters or 10
            # Determine samples per cluster
            if isinstance(config.sample_size, float) and config.sample_size <= 1.0:
                params['sample_percentage'] = config.sample_size * 100
            else:
                params['samples_per_cluster'] = int(config.sample_size / (config.num_clusters or 10))
        
        elif config.method == SamplingMethod.SYSTEMATIC:
            # Calculate interval based on sample size
            # This is a simplified calculation - in practice, we'd need total row count
            params['interval'] = max(1, int(100 / config.sample_size)) if config.sample_size < 100 else 2
            params['start'] = 1
            
        return params
    
    def _calculate_strata_counts(
        self, 
        sampled_data: List[Dict[str, Any]], 
        strata_columns: List[str]
    ) -> Dict[str, int]:
        """Calculate counts for each stratum."""
        strata_counts = {}
        
        for row in sampled_data:
            # Build stratum key
            stratum_values = []
            for col in strata_columns:
                stratum_values.append(str(row.get(col, 'NULL')))
            stratum_key = '|'.join(stratum_values)
            
            # Count
            strata_counts[stratum_key] = strata_counts.get(stratum_key, 0) + 1
        
        return strata_counts


class SamplingJobService:
    """Service for creating and managing sampling jobs."""
    
    def __init__(self, db_pool: DatabasePool):
        self._db_pool = db_pool
    
    async def create_sampling_job(
        self,
        dataset_id: int,
        source_commit_id: str,
        user_id: int,
        sampling_config: Dict[str, Any]
    ) -> str:
        """Create a sampling job in the database."""
        async with self._db_pool.acquire() as conn:
            job_id = await conn.fetchval("""
                INSERT INTO dsa_jobs.analysis_runs (
                    run_type, dataset_id, source_commit_id, user_id, run_parameters
                )
                VALUES ('sampling', $1, $2, $3, $4)
                RETURNING id
            """, dataset_id, source_commit_id, user_id, json.dumps(sampling_config))
            
            logger.info(f"Created sampling job {job_id} for dataset {dataset_id}")
            return str(job_id)
    
    async def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get the status of a sampling job."""
        async with self._db_pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT status, output_summary, error_message, created_at, completed_at
                FROM dsa_jobs.analysis_runs
                WHERE id = $1
            """, job_id)
            
            if not row:
                raise EntityNotFoundException("Job", job_id)
            
            return {
                'job_id': job_id,
                'status': row['status'],
                'output_summary': json.loads(row['output_summary']) if row['output_summary'] else None,
                'error_message': row['error_message'],
                'created_at': row['created_at'].isoformat() if row['created_at'] else None,
                'completed_at': row['completed_at'].isoformat() if row['completed_at'] else None
            }


# Required import
import json
from src.core.domain_exceptions import EntityNotFoundException