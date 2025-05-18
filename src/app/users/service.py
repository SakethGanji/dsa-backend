import bcrypt
from fastapi import HTTPException, status
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from app.users.repository import list_users as repo_list_users, create_user as repo_create_user, get_user_by_soeid
from app.users.models import UserCreate, UserOut
from app.users.auth import create_access_token, Token
from typing import List

class UserService:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def list_users(self) -> List[UserOut]:
        raw = await repo_list_users(self.session)
        return [UserOut(**u) for u in raw]

    async def create_user(self, payload: UserCreate) -> UserOut:
        try:
            # This method expects payload.password to be a pre-hashed password
            # if called directly (e.g., by an admin create user route).
            # For public registration, hashing is handled in register_user.
            # Consider adding hashing here too if admin should provide plain text.
            raw = await repo_create_user(
                self.session,
                payload.soeid,
                payload.password, # Expects pre-hashed if called directly by admin
                payload.role_id
            )
        except IntegrityError as e:
            # Basic check, can be improved with more specific DB error parsing
            if "unique" in str(e).lower() or "duplicate key" in str(e).lower():
                 raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"A user with SOEID '{payload.soeid}' already exists or another unique constraint was violated.")
            elif "foreign key" in str(e).lower():
                 raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid role_id '{payload.role_id}' or other foreign key violation.")
            else:
                 print(f"DEBUG: create_user IntegrityError: {e}") # Log other integrity errors
                 raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database integrity error during user creation.")
        except Exception as e:
            if "unique" in str(e).lower():
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="A user with this SOEID already exists.")
            print(f"DEBUG: create_user Unexpected error: {type(e).__name__} - {str(e)}") # Log other unexpected errors
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="User creation failed due to an unexpected error.")
        if not raw:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="User creation failed to return data.")
        return UserOut(**raw)

    async def register_user(self, payload: UserCreate) -> UserOut:
        hashed_password = bcrypt.hashpw(payload.password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        try:
            raw = await repo_create_user(
                self.session,
                payload.soeid,
                hashed_password,
                payload.role_id
            )
        except IntegrityError as e:
            db_error_message = str(e).lower()
            print(f"DEBUG: IntegrityError during registration for SOEID {payload.soeid}, role_id {payload.role_id}. DB Error: {e}")

            # Database-specific checks for unique SOEID violation
            # PostgreSQL: "violates unique constraint" ... "users_soeid_key" or similar constraint name
            # SQLite: "unique constraint failed: users.soeid"
            # MySQL: "duplicate entry '...' for key 'users.soeid'" (or your actual key name)
            is_soeid_unique_violation = (
                ("violates unique constraint" in db_error_message and "soeid" in db_error_message) or
                ("unique constraint failed: users.soeid" in db_error_message) or
                ("duplicate entry" in db_error_message and "soeid" in db_error_message) # General, might need refinement
            )

            # Database-specific checks for foreign key violation on role_id
            is_role_id_fk_violation = (
                ("foreign key constraint" in db_error_message and ("role_id" in db_error_message or "roles" in db_error_message) )
            )

            if is_soeid_unique_violation:
                 raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"A user with SOEID '{payload.soeid}' already exists.")
            elif is_role_id_fk_violation:
                 raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid role_id '{payload.role_id}'. It may not exist or is not permissible.")
            else:
                 # Generic IntegrityError that isn't a recognized SOEID unique or role_id FK violation
                 raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Database integrity error during registration. Please check server logs for: {e.orig if hasattr(e, 'orig') else e}")

        except Exception as e:
            print(f"DEBUG: Unexpected error during user registration for SOEID {payload.soeid}: {type(e).__name__} - {str(e)}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="User registration failed due to an unexpected error.")

        if not raw:
            print(f"DEBUG: repo_create_user returned None for SOEID {payload.soeid} without raising an exception.")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="User registration failed to return data (repository returned no data).")

        return UserOut(**raw)

    async def authenticate_user(self, soeid: str, password: str) -> Token:
        user = await get_user_by_soeid(self.session, soeid)

        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid credentials",
                headers={"WWW-Authenticate": "Bearer"}
            )

        stored_hashed_password = user.get("password_hash")
        if not stored_hashed_password or not bcrypt.checkpw(password.encode('utf-8'), stored_hashed_password.encode('utf-8')):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid credentials",
                headers={"WWW-Authenticate": "Bearer"}
            )

        access_token = create_access_token(data={"sub": soeid})
        return Token(access_token=access_token, token_type="bearer")

