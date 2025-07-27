"""Permission service for centralized permission checking."""

from typing import List, Optional
from dataclasses import dataclass
from ..infrastructure.postgres.uow import PostgresUnitOfWork
from .domain_exceptions import PermissionDeniedError


@dataclass
class PermissionCheck:
    """Data class for permission check parameters."""
    resource: str
    resource_id: int
    user_id: int
    permission: str


class PermissionType:
    """Permission type constants."""
    READ = "read"
    WRITE = "write"
    ADMIN = "admin"


class PermissionService:
    """Centralized permission checking service with request-scoped caching."""
    
    def __init__(self, uow: PostgresUnitOfWork):
        """Initialize with unit of work."""
        self._uow = uow
        self._cache = {}  # Simple request-scoped cache
    
    async def require(self, resource: str, resource_id: int, user_id: int, permission: str):
        """
        Check permission and throw if not granted.
        
        Args:
            resource: Type of resource (e.g., "dataset", "job")
            resource_id: ID of the resource
            user_id: ID of the user
            permission: Required permission level (e.g., "read", "write", "admin")
            
        Raises:
            PermissionDeniedError: If permission is not granted
        """
        cache_key = f"{resource}:{resource_id}:{user_id}:{permission}"
        if cache_key not in self._cache:
            self._cache[cache_key] = await self._check_permission(resource, resource_id, user_id, permission)
        
        if not self._cache[cache_key]:
            raise PermissionDeniedError(f"{resource}:{resource_id}", permission, user_id)
    
    async def has_permission(self, resource: str, resource_id: int, user_id: int, permission: str) -> bool:
        """
        Check permission and return boolean.
        
        Args:
            resource: Type of resource (e.g., "dataset", "job")
            resource_id: ID of the resource
            user_id: ID of the user
            permission: Required permission level (e.g., "read", "write", "admin")
            
        Returns:
            bool: True if permission is granted, False otherwise
        """
        cache_key = f"{resource}:{resource_id}:{user_id}:{permission}"
        if cache_key not in self._cache:
            self._cache[cache_key] = await self._check_permission(resource, resource_id, user_id, permission)
        return self._cache[cache_key]
    
    async def require_any(self, resource: str, resource_id: int, user_id: int, permissions: List[str]):
        """
        Require at least one of the permissions.
        
        Args:
            resource: Type of resource
            resource_id: ID of the resource
            user_id: ID of the user
            permissions: List of permission levels (any one is sufficient)
            
        Raises:
            PermissionDeniedError: If none of the permissions are granted
        """
        for perm in permissions:
            if await self.has_permission(resource, resource_id, user_id, perm):
                return
        raise PermissionDeniedError(f"{resource}:{resource_id}", f"any of {permissions}", user_id)
    
    async def require_all(self, checks: List[PermissionCheck]):
        """
        Require all permissions in the list.
        
        Args:
            checks: List of permission checks to perform
            
        Raises:
            PermissionDeniedError: If any permission is not granted
        """
        for check in checks:
            await self.require(check.resource, check.resource_id, check.user_id, check.permission)
    
    async def require_role(self, user_id: int, role_name: str):
        """
        Require user to have a specific role.
        
        Args:
            user_id: ID of the user
            role_name: Required role name (e.g., "admin", "manager")
            
        Raises:
            PermissionDeniedError: If user doesn't have the role
        """
        user = await self._uow.users.get_by_id(user_id)
        if not user:
            raise PermissionDeniedError("system", role_name, user_id)
        
        user_role = user.get('role_name')
        
        if role_name == "admin" and user_role != "admin":
            raise PermissionDeniedError("system", "admin", user_id)
        elif role_name == "manager" and user_role not in ["admin", "manager"]:
            raise PermissionDeniedError("system", "manager", user_id)
    
    async def _check_permission(self, resource: str, resource_id: int, user_id: int, permission: str) -> bool:
        """
        Internal method to check permission from repository.
        
        Args:
            resource: Type of resource
            resource_id: ID of the resource
            user_id: ID of the user
            permission: Required permission level
            
        Returns:
            bool: True if permission is granted, False otherwise
        """
        if resource == "dataset":
            return await self._uow.datasets.check_user_permission(resource_id, user_id, permission)
        elif resource == "job":
            # For jobs, check if user owns the job
            job = await self._uow.jobs.get_by_id(resource_id)
            if job and job.get('user_id') == user_id:
                return True
            # Otherwise check dataset permission if job is related to a dataset
            if job and job.get('dataset_id'):
                return await self._uow.datasets.check_user_permission(
                    job['dataset_id'], user_id, permission
                )
            return False
        # Add other resource types as needed
        return False