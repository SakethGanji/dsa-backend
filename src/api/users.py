"""User management API endpoints."""

from fastapi import APIRouter, Depends, status
from fastapi.security import OAuth2PasswordRequestForm
from ..infrastructure.postgres.database import DatabasePool, UnitOfWorkFactory
from ..infrastructure.postgres import PostgresUserRepository
from ..features.users.handlers.create_user import CreateUserHandler
from ..features.users.handlers.login_user import LoginUserHandler
from ..api.models import (
    CreateUserRequest, CreateUserResponse,
    LoginRequest, LoginResponse
)
from ..core.authorization import require_admin_role
from .dependencies import get_db_pool
from ..core.domain_exceptions import ConflictException
from typing import Annotated


router = APIRouter(prefix="/users", tags=["users"])


# Local dependency helpers
async def get_uow_factory(
    pool: DatabasePool = Depends(get_db_pool)
) -> UnitOfWorkFactory:
    """Get unit of work factory."""
    return UnitOfWorkFactory(pool)


async def get_user_repo(
    pool: DatabasePool = Depends(get_db_pool)
) -> PostgresUserRepository:
    """Get user repository."""
    async with pool.acquire() as conn:
        yield PostgresUserRepository(conn)


@router.post("/register", response_model=CreateUserResponse)
async def create_user(
    request: CreateUserRequest,
    uow_factory: UnitOfWorkFactory = Depends(get_uow_factory),
    user_repo: PostgresUserRepository = Depends(get_user_repo),
    _: None = Depends(require_admin_role)  # Only admins can create users
) -> CreateUserResponse:
    """Create a new user (admin only)."""
    uow = uow_factory.create()
    handler = CreateUserHandler(uow, user_repo)
    
    return await handler.handle(request)


@router.post("/login", response_model=LoginResponse)
async def login(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
    user_repo: PostgresUserRepository = Depends(get_user_repo)
) -> LoginResponse:
    """Login with username (SOEID) and password."""
    # Convert OAuth2 form to our login request
    request = LoginRequest(
        soeid=form_data.username,
        password=form_data.password
    )
    
    handler = LoginUserHandler(user_repo)
    
    return await handler.handle(request)


@router.post("/token", response_model=LoginResponse)
async def token(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
    user_repo: PostgresUserRepository = Depends(get_user_repo)
) -> LoginResponse:
    """OAuth2 compatible token endpoint."""
    return await login(form_data, user_repo)


@router.post("/register-public", response_model=CreateUserResponse)
async def create_user_public(
    request: CreateUserRequest,
    pool: DatabasePool = Depends(get_db_pool)
) -> CreateUserResponse:
    """Create a new user (PUBLIC - for testing only, remove in production)."""
    from ..features.users.handlers.create_user_public import (
        CreateUserPublicHandler, 
        CreateUserPublicCommand
    )
    
    # Create command
    command = CreateUserPublicCommand(
        soeid=request.soeid,
        password=request.password,
        role_id=request.role_id
    )
    
    # Create handler and execute
    handler = CreateUserPublicHandler(pool)
    result = await handler.handle(command)
    
    # Convert handler response to API response
    return CreateUserResponse(
        user_id=result.user_id,
        soeid=result.soeid,
        role_id=result.role_id,
        role_name=result.role_name,
        is_active=result.is_active,
        created_at=result.created_at
    )