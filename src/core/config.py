"""Application configuration using Pydantic settings."""

from functools import lru_cache
from typing import Optional
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings."""
    
    # App settings
    app_name: str = "DSA Platform"
    version: str = "2.0.0"
    debug: bool = True
    
    # Database
    database_url: str = "postgresql://postgres:postgres@localhost:5432/postgres"
    db_pool_min_size: int = 10
    db_pool_max_size: int = 20
    
    # Security
    secret_key: str = "your-secret-key-here-change-in-production"
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 30
    refresh_token_expire_days: int = 7
    
    # CORS
    cors_origins: list[str] = ["http://localhost:3000"]
    
    # File storage
    upload_dir: str = "/tmp/dsa_uploads"
    max_upload_size: int = 5 * 1024 * 1024 * 1024  # 5GB
    
    class Config:
        env_file = ".env"
        case_sensitive = False
        extra = "allow"


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()