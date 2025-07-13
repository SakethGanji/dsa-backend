"""JWT authentication service implementation."""

from typing import Dict, Any, Optional
from datetime import datetime, timedelta
import jwt
from src.core.abstractions.external import IAuthenticationService


class AuthenticationError(Exception):
    """Authentication related errors."""
    pass


class JWTAuthenticationService(IAuthenticationService):
    """JWT implementation of IAuthenticationService."""
    
    def __init__(
        self,
        secret_key: str,
        algorithm: str = "HS256",
        access_token_expire_minutes: int = 30,
        refresh_token_expire_days: int = 7
    ):
        self._secret_key = secret_key
        self._algorithm = algorithm
        self._access_token_expire = timedelta(minutes=access_token_expire_minutes)
        self._refresh_token_expire = timedelta(days=refresh_token_expire_days)
        self._revoked_tokens: set[str] = set()  # In production, use Redis or similar
    
    async def verify_token(self, token: str) -> Dict[str, Any]:
        """Verify an authentication token and return token data."""
        if token in self._revoked_tokens:
            raise AuthenticationError("Token has been revoked")
        
        try:
            payload = jwt.decode(
                token,
                self._secret_key,
                algorithms=[self._algorithm]
            )
            
            # Check expiration
            exp = payload.get("exp")
            if exp and datetime.fromtimestamp(exp) < datetime.utcnow():
                raise AuthenticationError("Token has expired")
            
            return payload
        except jwt.ExpiredSignatureError:
            raise AuthenticationError("Token has expired")
        except jwt.InvalidTokenError as e:
            raise AuthenticationError(f"Invalid token: {str(e)}")
    
    async def create_token(self, user_id: int, additional_claims: Optional[Dict[str, Any]] = None) -> str:
        """Create an authentication token for a user."""
        payload = {
            "user_id": user_id,
            "iat": datetime.utcnow(),
            "exp": datetime.utcnow() + self._access_token_expire,
            "type": "access"
        }
        
        if additional_claims:
            payload.update(additional_claims)
        
        return jwt.encode(payload, self._secret_key, algorithm=self._algorithm)
    
    async def refresh_token(self, refresh_token: str) -> str:
        """Refresh an authentication token."""
        try:
            payload = await self.verify_token(refresh_token)
            
            # Verify it's a refresh token
            if payload.get("type") != "refresh":
                raise AuthenticationError("Not a refresh token")
            
            # Create new access token
            user_id = payload.get("user_id")
            if not user_id:
                raise AuthenticationError("Invalid refresh token")
            
            return await self.create_token(user_id)
        except Exception as e:
            raise AuthenticationError(f"Failed to refresh token: {str(e)}")
    
    async def revoke_token(self, token: str) -> None:
        """Revoke an authentication token."""
        self._revoked_tokens.add(token)
    
    async def create_refresh_token(self, user_id: int) -> str:
        """Create a refresh token for a user."""
        payload = {
            "user_id": user_id,
            "iat": datetime.utcnow(),
            "exp": datetime.utcnow() + self._refresh_token_expire,
            "type": "refresh"
        }
        
        return jwt.encode(payload, self._secret_key, algorithm=self._algorithm)