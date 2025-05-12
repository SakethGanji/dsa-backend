from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession
from app.users.controller import list_users_ctrl, create_user_ctrl
from app.users.models import UserCreate, UserOut
from app.db.connection import get_session

router = APIRouter(prefix="/api/users", tags=["Users"])

@router.get("/", response_model=list[UserOut], summary="List all users")
async def get_users(session: AsyncSession = Depends(get_session)):
    return await list_users_ctrl(session)

@router.post("/", response_model=UserOut, status_code=status.HTTP_201_CREATED, summary="Create a new user")
async def create_user(payload: UserCreate, session: AsyncSession = Depends(get_session)):
    return await create_user_ctrl(payload, session)
