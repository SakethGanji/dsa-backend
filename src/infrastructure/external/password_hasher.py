"""Password hasher implementation using passlib."""

from passlib.context import CryptContext
from src.core.abstractions.external import IPasswordManager


class PasswordHasher(IPasswordManager):
    """Implementation of password hashing and verification using passlib."""
    
    def __init__(self, schemes: list[str] = None):
        """
        Initialize password hasher with hashing schemes.
        
        Args:
            schemes: List of hashing schemes to use. Defaults to ["bcrypt"].
        """
        if schemes is None:
            schemes = ["bcrypt"]
        self._pwd_context = CryptContext(schemes=schemes, deprecated="auto")
    
    def hash_password(self, password: str) -> str:
        """
        Hash a plain text password.
        
        Args:
            password: Plain text password to hash
            
        Returns:
            Hashed password string
        """
        return self._pwd_context.hash(password)
    
    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        """
        Verify a plain text password against a hashed password.
        
        Args:
            plain_password: Plain text password to verify
            hashed_password: Previously hashed password
            
        Returns:
            True if password matches, False otherwise
        """
        return self._pwd_context.verify(plain_password, hashed_password)