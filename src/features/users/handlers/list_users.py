"""Handler for listing users."""

from typing import List, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime
from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.infrastructure.postgres.user_repo import PostgresUserRepository
from ...base_handler import BaseHandler
from src.core.permissions import PermissionService
from src.core.common.pagination import PaginationMixin
from ..models.commands import ListUsersCommand


@dataclass
class UserListItem:
    id: int
    soeid: str
    role_id: int
    role_name: Optional[str]
    created_at: datetime
    last_login: Optional[datetime]
    dataset_count: Optional[int] = 0


class ListUsersHandler(BaseHandler, PaginationMixin):
    """Handler for listing users (admin only)."""
    
    def __init__(self, uow: PostgresUnitOfWork, user_repo: PostgresUserRepository, permissions: PermissionService):
        super().__init__(uow)
        self._user_repo = user_repo
        self._permissions = permissions
    
    async def handle(self, command: ListUsersCommand) -> Tuple[List[UserListItem], int]:
        """
        List all users in the system.
        
        Returns:
            Tuple of (users, total_count)
        """
        # Check permissions - only admins can list all users
        await self._permissions.require_role(command.user_id, "admin")
        
        # Validate pagination
        offset, limit = self.validate_pagination(command.offset, command.limit)
        
        # Get users
        users, total = await self._user_repo.list_users(
            offset=offset,
            limit=limit,
            search=command.search,
            role_id=command.role_id,
            sort_by=command.sort_by,
            sort_order=command.sort_order
        )
        
        # Convert to response models
        user_items = []
        for user in users:
            # Get dataset count for user if available
            dataset_count = 0
            if hasattr(self._uow, 'datasets'):
                dataset_count = await self._uow.datasets.count_datasets_for_user(user['id'])
            
            user_items.append(UserListItem(
                id=user['id'],
                soeid=user['soeid'],
                role_id=user['role_id'],
                role_name=user.get('role_name'),
                created_at=user['created_at'],
                last_login=user.get('last_login'),
                dataset_count=dataset_count
            ))
        
        return user_items, total