"""Handler for updating user information."""

from typing import Optional
from dataclasses import dataclass
from datetime import datetime
from passlib.context import CryptContext
from src.core.abstractions import IUnitOfWork, IUserRepository
from src.features.base_handler import BaseHandler, with_transaction
from src.core.decorators import requires_role


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


@dataclass
class UpdateUserCommand:
    user_id: int  # Must be first for decorator - this is the requesting user (admin)
    target_user_id: int
    soeid: Optional[str] = None
    password: Optional[str] = None
    role_id: Optional[int] = None


@dataclass
class UpdateUserResponse:
    id: int
    soeid: str
    role_id: int
    role_name: Optional[str]
    updated_at: datetime


class UpdateUserHandler(BaseHandler):
    """Handler for updating user information."""
    
    def __init__(self, uow: IUnitOfWork, user_repo: IUserRepository):
        super().__init__(uow)
        self._user_repo = user_repo
    
    @with_transaction
    @requires_role("admin")  # Only admins can update users
    async def handle(self, command: UpdateUserCommand) -> UpdateUserResponse:
        """Update user information."""
        # Check if target user exists
        user = await self._user_repo.get_by_id(command.target_user_id)
        if not user:
            raise ValueError(f"User {command.target_user_id} not found")
        
        # Prepare update data
        update_data = {}
        
        if command.soeid is not None:
            # Check if new soeid is already taken
            existing = await self._user_repo.get_by_soeid(command.soeid)
            if existing and existing['id'] != command.target_user_id:
                raise ValueError(f"SOEID {command.soeid} is already taken")
            update_data['soeid'] = command.soeid
        
        if command.password is not None:
            # Hash the new password
            update_data['password_hash'] = pwd_context.hash(command.password)
        
        if command.role_id is not None:
            update_data['role_id'] = command.role_id
        
        # Update user if there are changes
        if update_data:
            await self._user_repo.update_user(
                user_id=command.target_user_id,
                **update_data
            )
        
        # Get updated user
        updated_user = await self._user_repo.get_by_id(command.target_user_id)
        
        return UpdateUserResponse(
            id=updated_user['id'],
            soeid=updated_user['soeid'],
            role_id=updated_user['role_id'],
            role_name=updated_user.get('role_name'),
            updated_at=updated_user['updated_at']
        )