from fastapi import APIRouter, Depends, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.ext.asyncio import AsyncSession
from app.users.controller import list_users_ctrl, create_user_ctrl, register_user_ctrl
from app.users.models import UserOut, UserCreate
from app.users.service import UserService
from app.users.auth import get_current_user, Token
from app.db.connection import get_session

router = APIRouter(prefix="/api/users", tags=["Users"])

# OAuth2 token endpoint
@router.post(
    "/token", response_model=Token, summary="OAuth2 compatible token login"
)
async def login_for_access_token(
    form_data: OAuth2PasswordRequestForm = Depends(),
    session: AsyncSession = Depends(get_session)
):
    return await UserService(session).authenticate_user(
        form_data.username, form_data.password
    )

# Public: Register a new user
@router.post(
    "/register",
    response_model=UserOut,
    status_code=status.HTTP_201_CREATED,
    summary="Register a new user"
)
async def register_user(
    payload: UserCreate,
    session: AsyncSession = Depends(get_session)
):
    return await register_user_ctrl(payload, session)

# Protected: List users
@router.get(
    "/", response_model=list[UserOut], summary="List all users"
)
async def get_users(
    current_user = Depends(get_current_user),
    session: AsyncSession = Depends(get_session)
):
    return await list_users_ctrl(session)

# Protected: Create user
@router.post(
    "/", response_model=UserOut,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new user (requires authentication, for admin use)"
)
async def create_user(
    payload: UserCreate,
    current_user = Depends(get_current_user),
    session: AsyncSession = Depends(get_session)
):
    return await create_user_ctrl(payload, session)
