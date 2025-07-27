"""User management API endpoints."""

from fastapi import APIRouter, Depends, status, Query
from fastapi.security import OAuth2PasswordRequestForm
from ..infrastructure.postgres.database import DatabasePool
from ..infrastructure.postgres.uow import PostgresUnitOfWork
from ..infrastructure.postgres import PostgresUserRepository
from ..features.users.services import UserService
from ..api.models import (
    CreateUserRequest, CreateUserResponse,
    LoginRequest, LoginResponse,
    CurrentUser
)
from ..core.authorization import require_admin_role, get_current_user_info
from .dependencies import get_db_pool, get_permission_service, get_uow
from ..core.domain_exceptions import ConflictException
from typing import Annotated, List, Optional
from pydantic import BaseModel


router = APIRouter(prefix="/users", tags=["users"])


# Request/Response models
class UpdateUserRequest(BaseModel):
    """Request for updating user."""
    soeid: Optional[str] = None
    password: Optional[str] = None
    role_id: Optional[int] = None


class UserListResponse(BaseModel):
    """Response for user list."""
    users: List[dict]
    total: int
    offset: int
    limit: int


# Local dependency helpers
async def get_user_repo(
    pool: DatabasePool = Depends(get_db_pool)
) -> PostgresUserRepository:
    """Get user repository."""
    async with pool.acquire() as conn:
        yield PostgresUserRepository(conn)


@router.post("/register", response_model=CreateUserResponse)
async def create_user(
    request: CreateUserRequest,
    uow: PostgresUnitOfWork = Depends(get_uow),
    user_repo: PostgresUserRepository = Depends(get_user_repo),
    permission_service = Depends(get_permission_service),
    current_user: CurrentUser = Depends(require_admin_role)  # Only admins can create users
) -> CreateUserResponse:
    """Create a new user (admin only)."""
    from ..features.users.models import CreateUserCommand
    
    # Create command with current user as creator
    command = CreateUserCommand(
        soeid=request.soeid,
        password=request.password,
        role_id=request.role_id,
        created_by=current_user.user_id
    )
    
    # Create service and execute
    service = UserService(uow, user_repo, permission_service)
    result = await service.create_user(command)
    
    # Convert service response to API response
    return CreateUserResponse(
        user_id=result.id,
        soeid=result.soeid,
        role_id=result.role_id,
        role_name=result.role_name,
        is_active=result.is_active,
        created_at=result.created_at
    )


@router.post("/login", response_model=LoginResponse)
async def login(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
    uow: PostgresUnitOfWork = Depends(get_uow),
    user_repo: PostgresUserRepository = Depends(get_user_repo),
    permission_service = Depends(get_permission_service)
) -> LoginResponse:
    """Login with username (SOEID) and password."""
    # Create service and execute
    service = UserService(uow, user_repo, permission_service)
    return await service.login(
        soeid=form_data.username,
        password=form_data.password
    )


@router.post("/token", response_model=LoginResponse)
async def token(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
    uow: PostgresUnitOfWork = Depends(get_uow),
    user_repo: PostgresUserRepository = Depends(get_user_repo),
    permission_service = Depends(get_permission_service)
) -> LoginResponse:
    """OAuth2 compatible token endpoint."""
    return await login(form_data, uow, user_repo, permission_service)


@router.post("/register-public", response_model=CreateUserResponse)
async def create_user_public(
    request: CreateUserRequest,
    uow: PostgresUnitOfWork = Depends(get_uow),
    user_repo: PostgresUserRepository = Depends(get_user_repo),
    permission_service = Depends(get_permission_service)
) -> CreateUserResponse:
    """Create a new user (PUBLIC - for testing only, remove in production)."""
    # Create service and execute
    service = UserService(uow, user_repo, permission_service)
    result = await service.create_user_public(
        soeid=request.soeid,
        password=request.password,
        role_id=request.role_id
    )
    
    # Convert service response to API response
    return CreateUserResponse(
        user_id=result.id,
        soeid=result.soeid,
        role_id=result.role_id,
        role_name=result.role_name,
        is_active=result.is_active,
        created_at=result.created_at
    )


@router.get("", response_model=UserListResponse)
async def list_users(
    offset: int = Query(0, ge=0, description="Pagination offset"),
    limit: int = Query(20, ge=1, le=100, description="Items per page"),
    search: Optional[str] = Query(None, description="Search by SOEID"),
    role_id: Optional[int] = Query(None, description="Filter by role ID"),
    sort_by: Optional[str] = Query("created_at", description="Sort field"),
    sort_order: Optional[str] = Query("desc", description="Sort order (asc/desc)"),
    current_user: CurrentUser = Depends(get_current_user_info),
    uow: PostgresUnitOfWork = Depends(get_uow),
    user_repo: PostgresUserRepository = Depends(get_user_repo),
    permission_service = Depends(get_permission_service)
) -> UserListResponse:
    """List all users (admin only)."""
    from ..features.users.models import ListUsersCommand
    
    # Create command
    command = ListUsersCommand(
        user_id=current_user.user_id,
        offset=offset,
        limit=limit,
        search=search,
        role_id=role_id,
        sort_by=sort_by,
        sort_order=sort_order
    )
    
    # Create service and execute
    service = UserService(uow, user_repo, permission_service)
    users, total = await service.list_users(command)
    
    # Convert to API response
    return UserListResponse(
        users=[{
            "id": user.id,
            "soeid": user.soeid,
            "role_id": user.role_id,
            "role_name": user.role_name,
            "created_at": user.created_at.isoformat(),
            "last_login": user.last_login.isoformat() if user.last_login else None,
            "dataset_count": user.dataset_count
        } for user in users],
        total=total,
        offset=offset,
        limit=limit
    )


@router.put("/{user_id}")
async def update_user(
    user_id: int,
    request: UpdateUserRequest,
    current_user: CurrentUser = Depends(require_admin_role),
    uow: PostgresUnitOfWork = Depends(get_uow),
    user_repo: PostgresUserRepository = Depends(get_user_repo),
    permission_service = Depends(get_permission_service)
):
    """Update user information (admin only)."""
    from ..features.users.models import UpdateUserCommand
    
    # Create command
    command = UpdateUserCommand(
        user_id=current_user.user_id,
        target_user_id=user_id,
        soeid=request.soeid,
        password=request.password,
        role_id=request.role_id
    )
    
    # Create service and execute
    service = UserService(uow, user_repo, permission_service)
    result = await service.update_user(command)
    
    # Return updated user
    return {
        "id": result.id,
        "soeid": result.soeid,
        "role_id": result.role_id,
        "role_name": result.role_name,
        "updated_at": result.updated_at.isoformat() if result.updated_at else None
    }


@router.delete("/{user_id}")
async def delete_user(
    user_id: int,
    current_user: CurrentUser = Depends(require_admin_role),
    uow: PostgresUnitOfWork = Depends(get_uow),
    user_repo: PostgresUserRepository = Depends(get_user_repo),
    permission_service = Depends(get_permission_service)
):
    """Delete a user (admin only)."""
    from ..features.users.models import DeleteUserCommand
    
    # Create command
    command = DeleteUserCommand(
        user_id=current_user.user_id,
        target_user_id=user_id
    )
    
    # Create service and execute
    service = UserService(uow, user_repo, permission_service)
    result = await service.delete_user(command)
    
    return {
        "message": result.message,
        "entity_type": result.entity_type,
        "entity_id": result.entity_id
    }