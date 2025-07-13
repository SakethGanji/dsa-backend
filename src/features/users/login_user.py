"""Handler for user authentication."""

from typing import Dict, Any
from datetime import timedelta
from passlib.context import CryptContext
from ...core.services.interfaces import IUserRepository
from ...models.pydantic_models import LoginRequest, LoginResponse
from ...core.auth import create_access_token, create_refresh_token


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


class LoginUserHandler:
    """Handler for user login and token generation."""
    
    def __init__(self, user_repo: IUserRepository):
        self._user_repo = user_repo
    
    async def handle(self, request: LoginRequest) -> LoginResponse:
        """Authenticate user and generate tokens."""
        # Get user with password
        user = await self._user_repo.get_user_with_password(request.soeid)
        if not user:
            raise ValidationException("Invalid \1")
        
        # Verify password
        if not pwd_context.verify(request.password, user['password_hash']):
            raise ValidationException("Invalid \1")
        
        # Generate tokens
        access_token = create_access_token(
            subject=user['soeid'],
            role_id=user['role_id'],
            role_name=user.get('role_name')
        )
        
        refresh_token = create_refresh_token(
            subject=user['soeid']
        )
        
        return LoginResponse(
            access_token=access_token,
            refresh_token=refresh_token,
            token_type="bearer",
            user_id=user['id'],
            soeid=user['soeid'],
            role_id=user['role_id'],
            role_name=user.get('role_name')
        )