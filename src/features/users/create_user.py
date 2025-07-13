"""Handler for creating new users."""

from typing import Dict, Any
from passlib.context import CryptContext
from ...core.abstractions import IUnitOfWork, IUserRepository
from ...models.pydantic_models import CreateUserRequest, CreateUserResponse
from ...features.base_handler import BaseHandler, with_transaction
from ...core.decorators import requires_role
from dataclasses import dataclass


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


@dataclass
class CreateUserCommand:
    user_id: int  # Must be first for decorator - this is the creating user (admin)
    soeid: str
    password: str
    role_id: int


class CreateUserHandler(BaseHandler):
    """Handler for creating new users with proper password hashing."""
    
    def __init__(self, uow: IUnitOfWork, user_repo: IUserRepository):
        super().__init__(uow)
        self._user_repo = user_repo
    
    @with_transaction
    @requires_role("admin")  # Only admins can create users
    async def handle(self, command: CreateUserCommand) -> CreateUserResponse:
        """Create a new user with hashed password."""
        # Transaction and role check handled by decorators
        
        # Check if user already exists
        existing_user = await self._user_repo.get_by_soeid(command.soeid)
        if existing_user:
            raise ValueError(f"User with SOEID {command.soeid} already exists")
        
        # Hash the password
        password_hash = pwd_context.hash(command.password)
        
        # Create user
        user_id = await self._user_repo.create_user(
            soeid=command.soeid,
            password_hash=password_hash,
            role_id=command.role_id
        )
        
        # Get the created user details
        user = await self._user_repo.get_by_id(user_id)
        
        return CreateUserResponse(
            id=user['id'],
            soeid=user['soeid'],
            role_id=user['role_id'],
            role_name=user.get('role_name'),
            created_at=user['created_at']
        )