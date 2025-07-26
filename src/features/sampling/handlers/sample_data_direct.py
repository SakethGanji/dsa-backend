"""Handler for direct data sampling."""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from src.core.abstractions import IUnitOfWork
from src.core.abstractions.service_interfaces import SamplingMethod, SampleConfig
from src.core.services.sampling_service import SamplingService
from ...base_handler import BaseHandler
from src.core.decorators import requires_permission
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
    
    def __init__(self, uow: IUnitOfWork):
        super().__init__(uow)
    
    @requires_permission("datasets", "read")
    async def handle(self, command: DirectSamplingCommand) -> SamplingResultResponse:
        """
        Perform direct sampling and return results immediately.
        
        Returns:
            SamplingResultResponse with sampled data
        """
        # Get current commit for ref
        ref = await self._uow.commits.get_ref(command.dataset_id, command.ref_name)
        if not ref:
            raise EntityNotFoundException("Ref", command.ref_name)
        
        commit_id = ref['commit_id']
        
        # Convert string method to enum
        method_enum = SamplingMethod[command.method.upper()] if isinstance(command.method, str) else command.method
        
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
        sampling_service = SamplingService(self._uow)
        result = await sampling_service.sample(
            self._uow.table_reader, commit_id, command.table_key, config
        )
        
        # Apply pagination to results
        paginated_data = result.sampled_data[command.offset:command.offset + command.limit]
        
        return SamplingResultResponse(
            method=result.method_used.value,
            sample_size=result.sample_size,
            data=paginated_data,
            metadata={
                **result.metadata,
                'total_sampled': result.sample_size,
                'offset': command.offset,
                'limit': command.limit,
                'returned': len(paginated_data)
            },
            strata_counts=result.strata_counts,
            selected_clusters=result.selected_clusters
        )