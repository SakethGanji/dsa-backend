"""JWT authentication utilities."""

from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from .config import get_settings


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/users/login")
settings = get_settings()


def create_access_token(
    subject: str, 
    role_id: int, 
    role_name: Optional[str] = None, 
    expires_delta: Optional[timedelta] = None
) -> str:
    """Create a JWT access token."""
    to_encode = {"sub": subject, "role_id": role_id}
    if role_name:
        to_encode["role_name"] = role_name
    
    expire = datetime.utcnow() + (
        expires_delta or timedelta(minutes=settings.access_token_expire_minutes)
    )
    to_encode.update({"exp": expire})
    
    return jwt.encode(to_encode, settings.secret_key, algorithm=settings.algorithm)


def create_refresh_token(
    subject: str, 
    expires_delta: Optional[timedelta] = None
) -> str:
    """Create a JWT refresh token."""
    to_encode = {"sub": subject}
    expire = datetime.utcnow() + (
        expires_delta or timedelta(days=settings.refresh_token_expire_days)
    )
    to_encode.update({"exp": expire, "token_type": "refresh"})
    
    return jwt.encode(to_encode, settings.secret_key, algorithm=settings.algorithm)


def verify_token(token: str, token_type: str = "access") -> Dict[str, Any]:
    """Verify and decode a JWT token."""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    try:
        payload = jwt.decode(
            token, 
            settings.secret_key, 
            algorithms=[settings.algorithm]
        )
        
        soeid: str = payload.get("sub")
        if soeid is None:
            raise credentials_exception
        
        # Check token type
        if token_type == "refresh":
            if payload.get("token_type") != "refresh":
                raise credentials_exception
        elif payload.get("token_type") == "refresh":
            raise credentials_exception
        
        return {
            "soeid": soeid,
            "role_id": payload.get("role_id"),
            "role_name": payload.get("role_name")
        }
        
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except jwt.PyJWTError:
        raise credentials_exception


async def get_current_user(token: str = Depends(oauth2_scheme)) -> Dict[str, Any]:
    """Get current user from JWT token."""
    return verify_token(token, token_type="access")