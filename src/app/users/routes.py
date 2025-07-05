"""Routes for users API v2"""
from fastapi import APIRouter, Depends, status, HTTPException
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.ext.asyncio import AsyncSession

from app.users.controller import list_users_ctrl, create_user_ctrl, register_user_ctrl
from app.users.models import UserOut, UserCreate
from app.users.service import UserService
# Update auth imports to include new helper functions
from app.users.auth import (
    get_current_user, get_current_soeid, get_current_role_id, get_current_user_info, CurrentUser,
    Token, TokenData, create_access_token, verify_token, create_refresh_token
)
from app.db.connection import get_session
from app.core.logging_config import logger

# Remove /api prefix for v2
router = APIRouter(prefix="/users", tags=["Users"])

# OAuth2 token endpoint
@router.post(
    "/token", response_model=Token, summary="OAuth2 compatible token login"
)
async def login_for_access_token(
    form_data: OAuth2PasswordRequestForm = Depends(),
    session: AsyncSession = Depends(get_session)
):
    logger.info(f"Login attempt for user: {form_data.username}")
    # authenticate_user now returns the user DB object (dict-like) or None
    user_data = await UserService(session).authenticate_user(
        form_data.username, form_data.password
    )
    if not user_data:
        logger.warning(f"Failed login attempt for user: {form_data.username}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    # user_data is a UserOut Pydantic model
    user_soeid = user_data.soeid # Changed from user_data.get('soeid')
    if not user_soeid:
        logger.error(f"SOEID not found for authenticated user: {form_data.username}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error: SOEID not found after authentication.",
        )

    # Get role_id and role_name from user_data
    user_role_id = user_data.role_id
    user_role_name = user_data.role_name
    
    # Pass subject, role_id, and role_name as required by the function signature
    access_token = create_access_token(subject=user_soeid, role_id=user_role_id, role_name=user_role_name)
    # Create refresh token using the new function
    refresh_token = create_refresh_token(subject=user_soeid)
    logger.info(f"Token created for user: {user_soeid}")
    return {"access_token": access_token, "refresh_token": refresh_token, "token_type": "bearer"}

@router.post("/token/refresh", response_model=Token, summary="Refresh access token")
async def refresh_access_token(refresh_token: str, session: AsyncSession = Depends(get_session)):
    logger.info("Token refresh attempt")
    try:
        token_data = verify_token(refresh_token, token_type="refresh")
    except HTTPException as e:
        logger.warning(f"Token refresh failed: {e.detail}")
        raise e

    # Fetch user details to get role_id for the new access token
    user_service = UserService(session)
    user_data = await user_service.get_user_by_soeid(token_data.soeid) # This returns a UserOut model

    if not user_data: 
        logger.warning(f"Token refresh failed: User not found for soeid {token_data.soeid}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid refresh token - user not found",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Since user_data is a UserOut model, access role_id and role_name as attributes
    user_role_id = user_data.role_id
    user_role_name = user_data.role_name
    if user_role_id is None:
        logger.error(f"User data missing role_id for user: {token_data.soeid} during token refresh")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="User data incomplete for token refresh.",
        )

    new_access_token = create_access_token(subject=token_data.soeid, role_id=user_role_id, role_name=user_role_name)
    logger.info(f"Access token refreshed for user: {token_data.soeid}")
    return {"access_token": new_access_token, "refresh_token": refresh_token, "token_type": "bearer"}

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
    "", response_model=list[UserOut], summary="List all users"
)
async def get_users(
    current_user: CurrentUser = Depends(get_current_user_info),
    session: AsyncSession = Depends(get_session)
):
    logger.info(f"List users requested by {current_user.soeid} with role {current_user.role_id}")
    return await list_users_ctrl(session)

# Protected: Create user
@router.post(
    "", response_model=UserOut,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new user (requires authentication, for admin use)"
)
async def create_user(
    payload: UserCreate,
    current_user: CurrentUser = Depends(get_current_user_info),
    session: AsyncSession = Depends(get_session)
):
    logger.info(f"Create user requested by {current_user.soeid} with role {current_user.role_id}")
    #  role-based access control:
    # if current_user.role_id != ADMIN_ROLE_ID:
    #     raise HTTPException(status_code=403, detail="Not authorized")
    return await create_user_ctrl(payload, session)