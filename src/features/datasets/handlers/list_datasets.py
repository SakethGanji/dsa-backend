"""Handler for listing datasets."""

from typing import List, Tuple, Optional
from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.infrastructure.postgres.dataset_repo import PostgresDatasetRepository
from src.api.models import DatasetSummary
from ...base_handler import BaseHandler
from ..models import ListDatasetsCommand


class ListDatasetsHandler(BaseHandler):
    """Handler for listing datasets accessible to a user."""
    
    def __init__(
        self,
        uow: PostgresUnitOfWork,
        dataset_repo: PostgresDatasetRepository
    ):
        super().__init__(uow)
        self._dataset_repo = dataset_repo
    
    async def handle(
        self, 
        command: ListDatasetsCommand
    ) -> Tuple[List[DatasetSummary], int]:
        """
        List datasets that the user has access to.
        
        Returns:
            Tuple of (datasets, total_count)
        """
        # Validate pagination
        offset = max(0, command.offset)
        limit = min(max(1, command.limit), 1000)  # Cap at 1000
        
        # Get all datasets for the user
        all_datasets = await self._dataset_repo.list_user_datasets(command.user_id)
        
        # Apply pagination
        total = len(all_datasets)
        datasets = all_datasets[offset:offset + limit]
        
        # Convert to response models
        dataset_items = []
        for dataset in datasets:
            # Get tags for each dataset
            tags = await self._dataset_repo.get_dataset_tags(dataset['dataset_id'])
            
            dataset_items.append(DatasetSummary(
                dataset_id=dataset['dataset_id'],
                name=dataset['name'],
                description=dataset['description'],
                tags=tags,
                created_at=dataset['created_at'],
                updated_at=dataset['updated_at'],
                created_by=dataset['created_by'],
                permission_type=dataset['permission_type'],
                import_status=None,
                import_job_id=None
            ))
        
        return dataset_items, total