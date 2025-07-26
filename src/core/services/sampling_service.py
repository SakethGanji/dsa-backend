"""Refactored sampling service that follows clean architecture."""

import logging
from typing import List, Dict, Any, Optional
import json
from uuid import uuid4

from ..abstractions.service_interfaces import ISamplingService, SamplingMethod, SampleConfig, SampleResult
from ..abstractions.repositories import ITableReader, IJobRepository
from ..abstractions.uow import IUnitOfWork
from ..domain_exceptions import EntityNotFoundException

logger = logging.getLogger(__name__)


class SamplingService(ISamplingService):
    """Clean implementation of ISamplingService using abstractions."""
    
    def __init__(self, uow: IUnitOfWork):
        self._uow = uow
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
        
        # Perform the sampling through table reader
        sampled_data = await self._perform_sampling(
            table_reader, commit_id, table_key, config, sample_params
        )
        
        logger.info(f"Sampled {len(sampled_data)} rows using {config.method.value}")
        
        return SampleResult(
            sampled_data=sampled_data,
            sample_size=len(sampled_data),
            method_used=config.method,
            metadata={
                'commit_id': commit_id,
                'table_key': table_key,
                **sample_params
            }
        )
    
    async def _perform_sampling(
        self,
        table_reader: ITableReader,
        commit_id: str,
        table_key: str,
        config: SampleConfig,
        params: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Perform the actual sampling based on method."""
        if config.method == SamplingMethod.RANDOM:
            # For random sampling, we can use table reader with limit/offset
            # In a real implementation, this would use proper random sampling
            offset = 0
            if config.random_seed:
                # Use seed to calculate offset
                offset = config.random_seed % (params.get('total_rows', 1000) - config.sample_size)
            
            return await table_reader.get_table_data(
                commit_id=commit_id,
                table_key=table_key,
                offset=offset,
                limit=config.sample_size
            )
        
        elif config.method == SamplingMethod.STRATIFIED:
            # Stratified sampling would group by stratify_column
            # This is a simplified implementation
            return await table_reader.get_table_data(
                commit_id=commit_id,
                table_key=table_key,
                offset=0,
                limit=config.sample_size
            )
        
        elif config.method == SamplingMethod.SYSTEMATIC:
            # Systematic sampling takes every nth row
            # This would need custom implementation in table reader
            return await table_reader.get_table_data(
                commit_id=commit_id,
                table_key=table_key,
                offset=0,
                limit=config.sample_size
            )
        
        else:
            # Default fallback
            return await table_reader.get_table_data(
                commit_id=commit_id,
                table_key=table_key,
                offset=0,
                limit=config.sample_size
            )
    
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
        # In this implementation, strategies are handled internally
        return f"{method.value} sampling strategy"
    
    def list_available_methods(self) -> List[SamplingMethod]:
        """List all available sampling methods."""
        return self._available_methods.copy()
    
    def _config_to_params(self, config: SampleConfig) -> Dict[str, Any]:
        """Convert SampleConfig to a parameter dictionary."""
        params = {
            'method': config.method.value,
            'sample_size': config.sample_size
        }
        
        if config.stratify_columns:
            params['stratify_columns'] = config.stratify_columns
        if config.cluster_column:
            params['cluster_column'] = config.cluster_column
        if config.random_seed is not None:
            params['random_seed'] = config.random_seed
        
        return params


class SamplingJobManager:
    """Manages sampling jobs using unit of work pattern."""
    
    def __init__(self, uow: IUnitOfWork):
        self._uow = uow
    
    async def create_sampling_job(
        self,
        dataset_id: int,
        source_commit_id: str,
        user_id: int,
        sampling_config: Dict[str, Any]
    ) -> str:
        """Create a sampling job through the job repository."""
        async with self._uow:
            job_id = await self._uow.jobs.create_job(
                run_type='sampling',
                dataset_id=dataset_id,
                source_commit_id=source_commit_id,
                user_id=user_id,
                run_parameters=sampling_config
            )
            await self._uow.commit()
            
            logger.info(f"Created sampling job {job_id} for dataset {dataset_id}")
            return str(job_id)
    
    async def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get the status of a sampling job."""
        async with self._uow:
            job = await self._uow.jobs.get_job_by_id(job_id)
            
            if not job:
                raise EntityNotFoundException("Job", job_id)
            
            return {
                'job_id': job_id,
                'status': job['status'],
                'output_summary': job.get('output_summary'),
                'error_message': job.get('error_message'),
                'created_at': job.get('created_at'),
                'completed_at': job.get('completed_at')
            }