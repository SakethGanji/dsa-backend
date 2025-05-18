from sqlalchemy.ext.asyncio import AsyncSession
from app.users.service import UserService
from app.users.models import UserCreate, UserOut

async def list_users_ctrl(session: AsyncSession) -> list[UserOut]:
    return await UserService(session).list_users()

async def create_user_ctrl(payload: UserCreate, session: AsyncSession) -> UserOut:
    return await UserService(session).create_user(payload)

async def register_user_ctrl(payload: UserCreate, session: AsyncSession) -> UserOut:
    return await UserService(session).register_user(payload)

