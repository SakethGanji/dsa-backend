from datetime import datetime, timedelta
from typing import Optional
import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel

# Import configuration from core module
from app.core.config import SECRET_KEY, ALGORITHM, ACCESS_TOKEN_EXPIRE_MINUTES, REFRESH_TOKEN_EXPIRE_DAYS

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/users/token")

class Token(BaseModel):
    access_token: str
    refresh_token: str # Added refresh_token
    token_type: str

class TokenData(BaseModel):
    soeid: Optional[str] = None
    role_id: Optional[int] = None # Added role_id

# Create a JWT access token
def create_access_token(subject: str, role_id: int, expires_delta: Optional[timedelta] = None) -> str: # Added role_id parameter
    to_encode = {"sub": subject, "role_id": role_id} # Added role_id to payload
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

# Create a JWT refresh token
def create_refresh_token(subject: str, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = {"sub": subject}
    expire = datetime.utcnow() + (expires_delta or timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS))
    # Add a claim to distinguish refresh tokens, e.g., "type": "refresh"
    # This is useful if the same verify_token function is used, or for auditing.
    to_encode.update({"exp": expire, "token_type": "refresh"})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

# Verify and decode a JWT token
def verify_token(token: str, token_type: str = "access") -> TokenData: # Added token_type parameter
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        soeid: str = payload.get("sub")
        role_id: int = payload.get("role_id") # Extract role_id

        # If verifying a refresh token, check its type
        if token_type == "refresh":
            payload_token_type = payload.get("token_type")
            if payload_token_type != "refresh":
                raise credentials_exception # Not a refresh token
        elif payload.get("token_type") == "refresh": # an access token should not have token_type refresh
            raise credentials_exception

        if soeid is None:
            raise credentials_exception
        return TokenData(soeid=soeid, role_id=role_id) # Include role_id in TokenData
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except jwt.PyJWTError: # Corrected this line
        raise credentials_exception

# Dependency to get current user from token
def get_current_user(token: str = Depends(oauth2_scheme)) -> TokenData:
    return verify_token(token, token_type="access") # Explicitly specify token_type

# User info model for authenticated current user
class CurrentUser(BaseModel):
    soeid: str
    role_id: int
    
    # You can add helper methods here for role-based checks
    def is_admin(self) -> bool:
        # Replace 1 with your actual admin role ID
        return self.role_id == 1
        
    def is_manager(self) -> bool:
        # Replace 2 with your actual manager role ID
        return self.role_id == 2

# Helper dependency to get combined user info (both soeid and role_id)
def get_current_user_info(token_data: TokenData = Depends(get_current_user)) -> CurrentUser:
    if token_data.role_id is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token missing role information",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return CurrentUser(soeid=token_data.soeid, role_id=token_data.role_id)

# Helper dependency to get just the soeid
def get_current_soeid(token_data: TokenData = Depends(get_current_user)) -> str:
    return token_data.soeid

# Helper dependency to get just the role_id
def get_current_role_id(token_data: TokenData = Depends(get_current_user)) -> int:
    if token_data.role_id is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token missing role information",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return token_data.role_id

