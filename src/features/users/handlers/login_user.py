"""Handler for user authentication."""

from typing import Dict, Any
from datetime import timedelta
from ....infrastructure.postgres.user_repo import PostgresUserRepository
from ....infrastructure.external.password_hasher import PasswordHasher
from ....api.models.requests import LoginRequest
from ....api.models.responses import LoginResponse
from ....core.auth import create_access_token, create_refresh_token
from ....core.domain_exceptions import ValidationException, BusinessRuleViolation
from ..models import User


class LoginUserHandler:
    """Handler for user login and token generation."""
    
    def __init__(self, user_repo: PostgresUserRepository, password_manager: PasswordHasher = None):
        self._user_repo = user_repo
        self._password_manager = password_manager or PasswordHasher()
    
    async def handle(self, request: LoginRequest) -> LoginResponse:
        """Authenticate user and generate tokens."""
        # Get user with password
        user_data = await self._user_repo.get_user_with_password(request.soeid)
        if not user_data:
            raise ValidationException("Invalid credentials")
        
        # Create domain model from repository data
        user = User.from_repository_data(user_data)
        
        # Validate user can login (checks status, etc.)
        try:
            user.validate_for_login()
        except BusinessRuleViolation:
            raise ValidationException("Invalid credentials")
        
        # Verify credentials using domain method
        if not user.verify_credentials(request.password, self._password_manager):
            raise ValidationException("Invalid credentials")
        
        # Record successful login
        user.record_login()
        # Note: In a complete implementation, we would persist this through the repository
        
        # Generate tokens
        access_token = create_access_token(
            subject=user.soeid,
            role_id=user.role.to_id(),
            role_name=user.role.value
        )
        
        refresh_token = create_refresh_token(
            subject=user.soeid
        )
        
        return LoginResponse(
            access_token=access_token,
            refresh_token=refresh_token,
            token_type="bearer",
            user_id=user.id,
            soeid=user.soeid,
            role_id=user.role.to_id(),
            role_name=user.role.value
        )