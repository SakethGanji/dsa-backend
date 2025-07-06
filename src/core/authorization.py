"""Authorization middleware and permission checking."""

from typing import Dict, Any, Optional
from fastapi import Depends, HTTPException, status
from .auth import get_current_user
from .abstractions import IDatasetRepository
from ..models.pydantic_models import CurrentUser, PermissionType
from .database import DatabasePool


# Dependency injection helper
def get_db_pool() -> DatabasePool:
    """Get database pool - will be overridden."""
    raise NotImplementedError("Database pool not configured")


async def get_current_user_info(
    token_data: Dict[str, Any] = Depends(get_current_user),
    db_pool: DatabasePool = Depends(get_db_pool)
) -> CurrentUser:
    """Convert token data to CurrentUser model with full user info."""
    if not token_data.get("role_id"):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token missing role information",
        )
    
    # Get user_id from database based on soeid
    from .infrastructure.postgres import PostgresUserRepository
    async with db_pool.acquire() as conn:
        user_repo = PostgresUserRepository(conn)
        user = await user_repo.get_by_soeid(token_data["soeid"])
        
        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User not found",
            )
    
    return CurrentUser(
        soeid=token_data["soeid"],
        user_id=user["id"],
        role_id=token_data["role_id"],
        role_name=token_data.get("role_name")
    )


class PermissionChecker:
    """Dependency for checking dataset permissions."""
    
    def __init__(self, required_permission: PermissionType):
        self.required_permission = required_permission
    
    async def __call__(
        self,
        dataset_id: int,
        current_user: CurrentUser = Depends(get_current_user_info),
        db_pool: DatabasePool = Depends(get_db_pool)
    ) -> CurrentUser:
        """Check if user has required permission on dataset."""
        # POC MODE: Allow all users to access all datasets
        # TODO: Remove this bypass when moving to production
        return current_user
        
        # Original permission checking code (commented out for POC)
        # Admin users have all permissions
        # if current_user.is_admin():
        #     return current_user
        # 
        # # Check specific dataset permission
        # from .infrastructure.postgres import PostgresDatasetRepository
        # async with db_pool.acquire() as conn:
        #     dataset_repo = PostgresDatasetRepository(conn)
        #     has_permission = await dataset_repo.check_user_permission(
        #         dataset_id=dataset_id,
        #         user_id=current_user.user_id,
        #         required_permission=self.required_permission.value
        #     )
        # 
        # if not has_permission:
        #     raise HTTPException(
        #         status_code=status.HTTP_403_FORBIDDEN,
        #         detail=f"User does not have {self.required_permission.value} permission on this dataset"
        #     )
        # 
        # return current_user


# Pre-configured permission checkers
require_dataset_read = PermissionChecker(PermissionType.READ)
require_dataset_write = PermissionChecker(PermissionType.WRITE)
require_dataset_admin = PermissionChecker(PermissionType.ADMIN)


def require_admin_role(
    current_user: CurrentUser = Depends(get_current_user_info)
) -> CurrentUser:
    """Require user to have admin role."""
    if not current_user.is_admin():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin role required"
        )
    return current_user


def require_manager_role(
    current_user: CurrentUser = Depends(get_current_user_info)
) -> CurrentUser:
    """Require user to have manager role or higher."""
    if not (current_user.is_admin() or current_user.is_manager()):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Manager role or higher required"
        )
    return current_user