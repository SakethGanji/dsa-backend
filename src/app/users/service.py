from fastapi import HTTPException
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from app.users.repository import list_users, create_user as repo_create_user
from app.users.models import UserCreate, UserOut
from typing import List

class UserService:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def list_users(self) -> List[UserOut]:
        raw = await list_users(self.session)
        return [UserOut(**u) for u in raw]

    async def create_user(self, payload: UserCreate) -> UserOut:
        try:
            raw = await repo_create_user(
                self.session,
                payload.soeid,
                payload.password_hash,
                payload.role_id
            )
        except IntegrityError:
            raise HTTPException(status_code=409, detail="A user with this SOEID already exists.")
        if not raw:
            raise HTTPException(status_code=500, detail="User creation failed")
        return UserOut(**raw)
