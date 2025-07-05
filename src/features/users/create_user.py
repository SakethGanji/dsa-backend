"""Handler for creating new users."""

from typing import Dict, Any
from passlib.context import CryptContext
from ...core.services.interfaces import IUnitOfWork, IUserRepository
from ...models.pydantic_models import CreateUserRequest, CreateUserResponse


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


class CreateUserHandler:
    """Handler for creating new users with proper password hashing."""
    
    def __init__(self, uow: IUnitOfWork, user_repo: IUserRepository):
        self._uow = uow
        self._user_repo = user_repo
    
    async def handle(self, request: CreateUserRequest) -> CreateUserResponse:
        """Create a new user with hashed password."""
        # Check if user already exists
        existing_user = await self._user_repo.get_by_soeid(request.soeid)
        if existing_user:
            raise ValueError(f"User with SOEID {request.soeid} already exists")
        
        # Hash the password
        password_hash = pwd_context.hash(request.password)
        
        # Create user in transaction
        await self._uow.begin()
        try:
            user_id = await self._user_repo.create_user(
                soeid=request.soeid,
                password_hash=password_hash,
                role_id=request.role_id
            )
            await self._uow.commit()
            
            # Get the created user details
            user = await self._user_repo.get_by_id(user_id)
            
            return CreateUserResponse(
                id=user['id'],
                soeid=user['soeid'],
                role_id=user['role_id'],
                role_name=user.get('role_name'),
                created_at=user['created_at']
            )
        except Exception as e:
            await self._uow.rollback()
            raise