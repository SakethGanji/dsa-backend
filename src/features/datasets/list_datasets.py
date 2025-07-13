"""Handler for listing datasets."""

from typing import List, Tuple, Optional
from dataclasses import dataclass
from src.core.abstractions import IUnitOfWork, IDatasetRepository
from src.models.pydantic_models import DatasetListItem
from src.features.base_handler import BaseHandler
from src.core.common.pagination import PaginationMixin


@dataclass
class ListDatasetsCommand:
    user_id: int
    offset: int = 0
    limit: int = 100
    search: Optional[str] = None
    tags: Optional[List[str]] = None
    sort_by: Optional[str] = "created_at"
    sort_order: Optional[str] = "desc"


class ListDatasetsHandler(BaseHandler, PaginationMixin):
    """Handler for listing datasets accessible to a user."""
    
    def __init__(
        self,
        uow: IUnitOfWork,
        dataset_repo: IDatasetRepository
    ):
        super().__init__(uow)
        self._dataset_repo = dataset_repo
    
    async def handle(
        self, 
        command: ListDatasetsCommand
    ) -> Tuple[List[DatasetListItem], int]:
        """
        List datasets that the user has access to.
        
        Returns:
            Tuple of (datasets, total_count)
        """
        # Validate pagination
        offset, limit = self.validate_pagination(command.offset, command.limit)
        
        # Get user info to check if admin
        user = None
        if hasattr(self._uow, 'users'):
            user = await self._uow.users.get_by_id(command.user_id)
        
        is_admin = user and user.get('role_name') == 'admin'
        
        # Get datasets based on user permissions
        if is_admin:
            # Admins can see all datasets
            datasets, total = await self._dataset_repo.list_all_datasets(
                offset=offset,
                limit=limit,
                search=command.search,
                tags=command.tags,
                sort_by=command.sort_by,
                sort_order=command.sort_order
            )
        else:
            # Regular users see only datasets they have permission for
            datasets, total = await self._dataset_repo.list_datasets_for_user(
                user_id=command.user_id,
                offset=offset,
                limit=limit,
                search=command.search,
                tags=command.tags,
                sort_by=command.sort_by,
                sort_order=command.sort_order
            )
        
        # Convert to response models
        dataset_items = []
        for dataset in datasets:
            # Get tags for each dataset
            tags = await self._dataset_repo.get_dataset_tags(dataset['id'])
            
            # Get user's permission level
            permission_level = None
            if not is_admin:
                permission = await self._dataset_repo.get_user_permission(
                    dataset_id=dataset['id'],
                    user_id=command.user_id
                )
                permission_level = permission.get('permission_type') if permission else None
            else:
                permission_level = 'admin'
            
            dataset_items.append(DatasetListItem(
                id=dataset['id'],
                name=dataset['name'],
                description=dataset['description'],
                tags=tags,
                created_at=dataset['created_at'],
                updated_at=dataset['updated_at'],
                created_by=dataset['created_by'],
                permission_level=permission_level,
                metadata=dataset.get('metadata', {})
            ))
        
        return dataset_items, total