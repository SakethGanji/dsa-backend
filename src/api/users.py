"""User management API endpoints."""

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from ..core.database import DatabasePool, UnitOfWorkFactory
from ..core.services.postgres import PostgresUserRepository
from ..features.users.create_user import CreateUserHandler
from ..features.users.login_user import LoginUserHandler
from ..models.pydantic_models import (
    CreateUserRequest, CreateUserResponse,
    LoginRequest, LoginResponse
)
from ..core.authorization import require_admin_role
from typing import Annotated


router = APIRouter(prefix="/users", tags=["users"])


# Dependency injection helpers
def get_db_pool() -> DatabasePool:
    """Get database pool - will be overridden in main.py"""
    raise NotImplementedError("Database pool not configured")


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
    
    try:
        return await handler.handle(request)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


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
    
    try:
        return await handler.handle(request)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Bearer"}
        )


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
    async with pool.acquire() as conn:
        user_repo = PostgresUserRepository(conn)
        
        # Check if user already exists
        existing_user = await user_repo.get_by_soeid(request.soeid)
        if existing_user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"User with SOEID {request.soeid} already exists"
            )
        
        # Hash the password
        from passlib.context import CryptContext
        pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
        password_hash = pwd_context.hash(request.password)
        
        # Ensure role exists (default to admin role for testing)
        role_id = request.role_id
        if not role_id:
            # Get or create admin role
            role = await conn.fetchrow("""
                INSERT INTO dsa_auth.roles (role_name, description) 
                VALUES ('admin', 'Administrator role')
                ON CONFLICT (role_name) DO UPDATE SET role_name = EXCLUDED.role_name
                RETURNING id
            """)
            role_id = role['id']
        
        # Create user
        user_id = await user_repo.create_user(
            soeid=request.soeid,
            password_hash=password_hash,
            role_id=role_id
        )
        
        # Get the created user details
        user = await user_repo.get_by_id(user_id)
        
        return CreateUserResponse(
            id=user['id'],
            soeid=user['soeid'],
            role_id=user['role_id'],
            role_name=user.get('role_name'),
            created_at=user['created_at']
        )