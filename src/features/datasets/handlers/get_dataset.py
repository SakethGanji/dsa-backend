"""Handler for getting dataset details."""

from typing import Dict, Any, Optional
from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.infrastructure.postgres.dataset_repo import PostgresDatasetRepository
from src.api.models import DatasetDetailResponse
from ...base_handler import BaseHandler
from src.core.permissions import PermissionService
from src.core.domain_exceptions import EntityNotFoundException
from ..models import GetDatasetCommand


class GetDatasetHandler(BaseHandler):
    """Handler for retrieving detailed dataset information."""
    
    def __init__(
        self,
        uow: PostgresUnitOfWork,
        dataset_repo: PostgresDatasetRepository,
        permissions: PermissionService
    ):
        super().__init__(uow)
        self._dataset_repo = dataset_repo
        self._permissions = permissions
    
    async def handle(self, command: GetDatasetCommand) -> DatasetDetailResponse:
        """
        Get detailed information about a dataset.
        
        Returns:
            DatasetDetailResponse with full dataset details
        """
        # Check read permission
        await self._permissions.require("dataset", command.dataset_id, command.user_id, "read")
        
        # Get dataset details
        dataset = await self._dataset_repo.get_dataset_by_id(command.dataset_id)
        if not dataset:
            raise EntityNotFoundException("Dataset", command.dataset_id)
        
        # Get tags
        tags = await self._dataset_repo.get_dataset_tags(command.dataset_id)
        
        # Get user's permission type for this dataset
        user_datasets = await self._dataset_repo.list_user_datasets(command.user_id)
        permission_type = None
        for ds in user_datasets:
            if ds['dataset_id'] == command.dataset_id:
                permission_type = ds['permission_type']
                break
        
        return DatasetDetailResponse(
            id=dataset['id'],
            name=dataset['name'],
            description=dataset['description'],
            created_by=dataset['created_by'],
            created_at=dataset['created_at'],
            updated_at=dataset['updated_at'],
            tags=tags,
            permission_type=permission_type,
            # Import status will be added by the endpoint
            import_status=None,
            import_job_id=None
        )