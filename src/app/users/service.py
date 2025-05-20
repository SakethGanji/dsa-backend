import bcrypt
from fastapi import HTTPException, status
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from app.users.repository import list_users as repo_list_users, create_user as repo_create_user, get_user_by_soeid
from app.users.models import UserCreate, UserOut
from app.users.auth import create_access_token
from typing import List, Dict, Any

class UserService:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def authenticate_user(self, username: str, password: str) -> UserOut | None:
        # TODO: Implement actual authentication logic
        # This is a placeholder implementation
        user = await get_user_by_soeid(self.session, username)
        if not user:
            return None
            
        # Check if password matches
        if bcrypt.checkpw(password.encode('utf-8'), user['password_hash'].encode('utf-8')):
            # Ensure the user dict has all required fields for UserOut model
            if 'id' not in user or 'soeid' not in user or 'role_id' not in user or 'created_at' not in user or 'updated_at' not in user:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="User data incomplete in database",
                )
            return UserOut.model_validate(user)
        return None

    async def create_user(self, user_create: UserCreate) -> UserOut:
        # Hash the password before storing it
        password_hash = bcrypt.hashpw(user_create.password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

        try:
            user = await repo_create_user(
                self.session,
                soeid=user_create.soeid,
                password_hash=password_hash,
                role_id=user_create.role_id
            )
            if not user:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Failed to create user",
                )
            return UserOut.model_validate(user)
        except IntegrityError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="User with this SOEID already exists",
            )

    async def list_users(self) -> List[UserOut]:
        # TODO: Implement actual user listing logic
        # This is a placeholder implementation
        users = await repo_list_users(self.session)
        return [UserOut.model_validate(user) for user in users]

    async def get_user_by_soeid(self, soeid: str) -> UserOut | None:
        # TODO: Implement actual logic to get user by SOEID
        # This is a placeholder implementation
        user = await get_user_by_soeid(self.session, soeid)
        if user:
            return UserOut.model_validate(user)
        return None
