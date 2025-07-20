"""Handler for user authentication."""

from typing import Dict, Any
from datetime import timedelta
from ....core.abstractions.repositories import IUserRepository
from ....core.abstractions.external import IPasswordManager
from ....infrastructure.external.password_hasher import PasswordHasher
from ....api.models.requests import LoginRequest
from ....api.models.responses import LoginResponse
from ....core.auth import create_access_token, create_refresh_token
from ....core.domain_exceptions import ValidationException


class LoginUserHandler:
    """Handler for user login and token generation."""
    
    def __init__(self, user_repo: IUserRepository, password_manager: IPasswordManager = None):
        self._user_repo = user_repo
        self._password_manager = password_manager or PasswordHasher()
    
    async def handle(self, request: LoginRequest) -> LoginResponse:
        """Authenticate user and generate tokens."""
        # Get user with password
        user = await self._user_repo.get_user_with_password(request.soeid)
        if not user:
            raise ValidationException("Invalid credentials")
        
        # Verify password
        if not self._password_manager.verify_password(request.password, user['password_hash']):
            raise ValidationException("Invalid credentials")
        
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