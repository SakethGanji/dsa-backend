"""Handler for getting column value samples."""

from typing import Dict, Any, List
from dataclasses import dataclass
from src.core.abstractions import IUnitOfWork
from ...base_handler import BaseHandler
from src.core.decorators import requires_permission
from src.core.domain_exceptions import EntityNotFoundException


@dataclass
class GetColumnSamplesCommand:
    user_id: int  # Must be first for decorator
    dataset_id: int
    ref_name: str
    table_key: str
    columns: List[str]
    samples_per_column: int


@dataclass
class ColumnSamplesResponse:
    samples: Dict[str, List[Any]]
    metadata: Dict[str, Any]


class GetColumnSamplesHandler(BaseHandler):
    """Handler for retrieving unique value samples for columns."""
    
    def __init__(self, uow: IUnitOfWork):
        super().__init__(uow)
    
    @requires_permission("datasets", "read")
    async def handle(self, command: GetColumnSamplesCommand) -> ColumnSamplesResponse:
        """
        Get unique value samples for specified columns.
        
        Returns:
            ColumnSamplesResponse with samples
        """
        async with self._uow:
            # Get current commit for ref
            ref = await self._uow.commits.get_ref(command.dataset_id, command.ref_name)
            if not ref:
                raise EntityNotFoundException("Ref", command.ref_name)
            
            commit_id = ref['commit_id']
            
            # Get column samples using table reader abstraction
            samples = await self._uow.table_reader.get_column_samples(
                commit_id, command.table_key, command.columns, command.samples_per_column
            )
            
            return ColumnSamplesResponse(
                samples=samples,
                metadata={
                    'dataset_id': command.dataset_id,
                    'ref_name': command.ref_name,
                    'table_key': command.table_key,
                    'commit_id': commit_id,
                    'columns_requested': len(command.columns),
                    'samples_per_column': command.samples_per_column
                }
            )