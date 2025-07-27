"""Handler for direct data sampling."""

from typing import Dict, Any, List, Optional, Union
from dataclasses import dataclass
from src.infrastructure.postgres.uow import PostgresUnitOfWork
from enum import Enum

# Import SamplingMethod from models
from ..models.sampling import SamplingMethod

@dataclass
class SampleConfig:
    """Configuration for sampling operations."""
    method: SamplingMethod
    sample_size: Union[int, float]
    random_seed: Optional[int] = None
    stratify_columns: Optional[List[str]] = None
    proportional: bool = True
    cluster_column: Optional[str] = None
    num_clusters: Optional[int] = None
    llm_prompt: Optional[str] = None
    relevance_threshold: Optional[float] = None
    num_rounds: Optional[int] = None
    round_configs: Optional[List['SampleConfig']] = None
from src.infrastructure.services.sampling_service import SamplingService
from ...base_handler import BaseHandler
from src.core.permissions import PermissionService
from src.core.domain_exceptions import EntityNotFoundException
from ..models import DirectSamplingCommand


@dataclass 
class SamplingResultResponse:
    method: str
    sample_size: int
    data: List[Dict[str, Any]]
    metadata: Dict[str, Any]
    strata_counts: Optional[Dict[str, int]] = None
    selected_clusters: Optional[List[Any]] = None


class SampleDataDirectHandler(BaseHandler):
    """Handler for performing direct sampling."""
    
    def __init__(self, uow: PostgresUnitOfWork, permissions: PermissionService):
        super().__init__(uow)
        self._permissions = permissions
    
    async def handle(self, command: DirectSamplingCommand) -> SamplingResultResponse:
        """
        Perform direct sampling and return results immediately.
        
        Returns:
            SamplingResultResponse with sampled data
        """
        # Check permissions - read permission needed
        await self._permissions.require("dataset", command.dataset_id, command.user_id, "read")
        
        # Get current commit for ref
        ref = await self._uow.commits.get_ref(command.dataset_id, command.ref_name)
        if not ref:
            raise EntityNotFoundException("Ref", command.ref_name)
        
        commit_id = ref['commit_id']
        
        # Convert string method to enum
        try:
            method_enum = SamplingMethod[command.method.upper()] if isinstance(command.method, str) else command.method
        except KeyError:
            raise ValueError(f"Invalid sampling method: {command.method}")
        
        # Create sampling config
        config = SampleConfig(
            method=method_enum,
            sample_size=command.sample_size,
            random_seed=command.random_seed,
            stratify_columns=command.stratify_columns,
            proportional=command.proportional,
            cluster_column=command.cluster_column,
            num_clusters=command.num_clusters
        )
        
        # Perform sampling
        # Get table reader from UOW
        table_reader = self._uow.table_reader
        sampling_service = SamplingService(table_reader)
        
        # Call the appropriate sampling method based on the method type
        try:
            if method_enum.value == "random":
                result = await sampling_service.sample_random(
                    command.dataset_id,
                    commit_id,
                    command.table_key or 'data',
                    command.sample_size,
                    seed=command.random_seed
                )
            elif method_enum.value == "stratified":
                result = await sampling_service.sample_stratified(
                    command.dataset_id,
                    commit_id,
                    command.table_key or 'data',
                    command.sample_size,
                    stratify_columns=command.stratify_columns or [],
                    proportional=command.proportional
                )
            elif method_enum.value == "systematic":
                result = await sampling_service.sample_systematic(
                    command.dataset_id,
                    commit_id,
                    command.table_key or 'data',
                    command.sample_size,
                    interval=None
                )
            elif method_enum.value == "cluster":
                result = await sampling_service.sample_cluster(
                    command.dataset_id,
                    commit_id,
                    command.table_key or 'data',
                    command.sample_size,
                    cluster_column=command.cluster_column,
                    num_clusters=command.num_clusters
                )
            else:
                raise ValueError(f"Unsupported sampling method: {method_enum}")
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Error during sampling: {e}", exc_info=True)
            raise
        
        # Apply pagination to results
        paginated_data = result.data[command.offset:command.offset + command.limit]
        
        return SamplingResultResponse(
            method=result.sampling_method,
            sample_size=result.sample_size,
            data=paginated_data,
            metadata={
                **result.metadata,
                'total_sampled': result.sample_size,
                'total_size': result.total_size,
                'offset': command.offset,
                'limit': command.limit,
                'returned': len(paginated_data)
            },
            strata_counts=getattr(result, 'strata_counts', None),
            selected_clusters=getattr(result, 'selected_clusters', None)
        )