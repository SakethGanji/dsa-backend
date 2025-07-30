"""Application configuration implementation using Pydantic settings."""

from functools import lru_cache
from typing import Optional, List
from pydantic_settings import BaseSettings
# Remove interface import


class Settings(BaseSettings):
    """Concrete implementation of application settings."""
    
    # App settings
    app_name: str = "DSA Platform"
    version: str = "2.0.0"
    debug: bool = True
    
    # Database pool settings
    db_pool_min_size: int = 10
    db_pool_max_size: int = 20
    
    # PostgreSQL settings (required)
    POSTGRESQL_HOST: str
    POSTGRESQL_PORT: int = 5432
    POSTGRESQL_USER: str
    POSTGRESQL_PASSWORD: str
    POSTGRESQL_DATABASE: str
    
    # PostgreSQL secret management (optional)
    POSTGRESQL_PASSWORD_SECRET_NAME: Optional[str] = None
    
    # Secret management
    VAULT_DATA_PATH: str = "/var/vault-data"
    NGC_CSIID: Optional[str] = "179492"
    
    # Security
    secret_key: str = "your-secret-key"
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 1440  # 24 hours
    refresh_token_expire_days: int = 7
    
    # CORS
    cors_origins: List[str] = ["*"]
    
    # Frontend URL
    frontend_url: str = "http://localhost:3000"
    
    # File storage
    upload_dir: str = "/tmp/dsa_uploads"
    max_upload_size: int = 5 * 1024 * 1024 * 1024  # 5GB
    
    # Import settings
    import_batch_size: int = 10000
    import_chunk_size: int = 1048576  # 1MB
    import_parallel_workers: int = 4
    import_progress_update_interval: int = 10
    
    class Config:
        env_file = ".env"
        case_sensitive = False
        extra = "allow"


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()