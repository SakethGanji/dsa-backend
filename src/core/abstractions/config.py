"""Configuration abstractions for the application."""

from abc import ABC, abstractmethod
from typing import List


class ISettings(ABC):
    """Abstract interface for application settings."""
    
    @property
    @abstractmethod
    def app_name(self) -> str:
        """Application name."""
        pass
    
    @property
    @abstractmethod
    def version(self) -> str:
        """Application version."""
        pass
    
    @property
    @abstractmethod
    def debug(self) -> bool:
        """Debug mode flag."""
        pass
    
    @property
    @abstractmethod
    def database_url(self) -> str:
        """Database connection URL."""
        pass
    
    @property
    @abstractmethod
    def db_pool_min_size(self) -> int:
        """Minimum database pool size."""
        pass
    
    @property
    @abstractmethod
    def db_pool_max_size(self) -> int:
        """Maximum database pool size."""
        pass
    
    @property
    @abstractmethod
    def secret_key(self) -> str:
        """Secret key for JWT tokens."""
        pass
    
    @property
    @abstractmethod
    def algorithm(self) -> str:
        """JWT algorithm."""
        pass
    
    @property
    @abstractmethod
    def access_token_expire_minutes(self) -> int:
        """Access token expiration time in minutes."""
        pass
    
    @property
    @abstractmethod
    def cors_origins(self) -> List[str]:
        """Allowed CORS origins."""
        pass
    
    @property
    @abstractmethod
    def upload_dir(self) -> str:
        """Directory for file uploads."""
        pass
    
    @property
    @abstractmethod
    def max_upload_size(self) -> int:
        """Maximum upload file size in bytes."""
        pass
    
    @property
    @abstractmethod
    def import_batch_size(self) -> int:
        """Batch size for import operations."""
        pass