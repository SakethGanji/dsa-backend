"""
Configuration settings for the sampling module
"""
import os
from typing import Dict, Any, Optional
from pydantic import BaseSettings, Field
from enum import Enum

class Environment(str, Enum):
    """Environment types"""
    DEVELOPMENT = "development"
    TESTING = "testing"
    STAGING = "staging"
    PRODUCTION = "production"

class SamplingSettings(BaseSettings):
    """Configuration settings for sampling operations"""
    
    # Environment
    environment: Environment = Field(
        default=Environment.DEVELOPMENT,
        env="SAMPLING_ENV",
        description="Application environment"
    )
    
    # Performance settings
    max_rows_per_job: int = Field(
        default=1000000,
        env="SAMPLING_MAX_ROWS_PER_JOB",
        description="Maximum number of rows to process in a single job"
    )
    
    max_memory_usage_mb: float = Field(
        default=512.0,
        env="SAMPLING_MAX_MEMORY_MB",
        description="Maximum memory usage per job in MB"
    )
    
    max_execution_time_seconds: int = Field(
        default=3600,
        env="SAMPLING_MAX_EXECUTION_TIME",
        description="Maximum execution time per job in seconds"
    )
    
    # Preview settings
    default_preview_rows: int = Field(
        default=10,
        env="SAMPLING_DEFAULT_PREVIEW_ROWS",
        description="Default number of rows to show in preview"
    )
    
    max_preview_rows: int = Field(
        default=100,
        env="SAMPLING_MAX_PREVIEW_ROWS",
        description="Maximum number of rows allowed in preview"
    )
    
    # Rate limiting
    rate_limit_requests_per_minute: int = Field(
        default=60,
        env="SAMPLING_RATE_LIMIT_RPM",
        description="Rate limit: requests per minute per user"
    )
    
    rate_limit_jobs_per_hour: int = Field(
        default=10,
        env="SAMPLING_RATE_LIMIT_JPH",
        description="Rate limit: new jobs per hour per user"
    )
    
    # Export settings
    max_export_rows: int = Field(
        default=100000,
        env="SAMPLING_MAX_EXPORT_ROWS",
        description="Maximum number of rows to export"
    )
    
    supported_export_formats: list = Field(
        default=["csv", "json", "xlsx", "parquet"],
        description="Supported export formats"
    )
    
    # Storage settings
    enable_persistent_storage: bool = Field(
        default=False,
        env="SAMPLING_ENABLE_PERSISTENT_STORAGE",
        description="Enable persistent storage for job results"
    )
    
    storage_backend: str = Field(
        default="local",
        env="SAMPLING_STORAGE_BACKEND",
        description="Storage backend (local, s3, gcs, azure)"
    )
    
    storage_bucket: Optional[str] = Field(
        default=None,
        env="SAMPLING_STORAGE_BUCKET",
        description="Storage bucket name for cloud storage"
    )
    
    storage_prefix: str = Field(
        default="sampling-results",
        env="SAMPLING_STORAGE_PREFIX",
        description="Prefix for stored files"
    )
    
    # Monitoring and logging
    enable_detailed_logging: bool = Field(
        default=True,
        env="SAMPLING_ENABLE_DETAILED_LOGGING",
        description="Enable detailed logging"
    )
    
    enable_performance_monitoring: bool = Field(
        default=True,
        env="SAMPLING_ENABLE_PERFORMANCE_MONITORING",
        description="Enable performance monitoring"
    )
    
    metrics_collection_interval: int = Field(
        default=60,
        env="SAMPLING_METRICS_INTERVAL",
        description="Metrics collection interval in seconds"
    )
    
    # Security settings
    enable_input_validation: bool = Field(
        default=True,
        env="SAMPLING_ENABLE_INPUT_VALIDATION",
        description="Enable strict input validation"
    )
    
    max_filter_complexity: int = Field(
        default=10,
        env="SAMPLING_MAX_FILTER_COMPLEXITY",
        description="Maximum number of filters per request"
    )
    
    allowed_regex_patterns: bool = Field(
        default=False,
        env="SAMPLING_ALLOW_REGEX_PATTERNS",
        description="Allow regex patterns in filters (security risk)"
    )
    
    # Job queue settings
    max_concurrent_jobs: int = Field(
        default=5,
        env="SAMPLING_MAX_CONCURRENT_JOBS",
        description="Maximum number of concurrent jobs per user"
    )
    
    job_timeout_seconds: int = Field(
        default=1800,
        env="SAMPLING_JOB_TIMEOUT",
        description="Job timeout in seconds"
    )
    
    enable_job_retry: bool = Field(
        default=True,
        env="SAMPLING_ENABLE_JOB_RETRY",
        description="Enable automatic job retry on failure"
    )
    
    max_retry_attempts: int = Field(
        default=3,
        env="SAMPLING_MAX_RETRY_ATTEMPTS",
        description="Maximum retry attempts for failed jobs"
    )
    
    # Cache settings
    enable_result_caching: bool = Field(
        default=True,
        env="SAMPLING_ENABLE_CACHING",
        description="Enable result caching"
    )
    
    cache_ttl_seconds: int = Field(
        default=3600,
        env="SAMPLING_CACHE_TTL",
        description="Cache TTL in seconds"
    )
    
    cache_max_size_mb: int = Field(
        default=256,
        env="SAMPLING_CACHE_MAX_SIZE_MB",
        description="Maximum cache size in MB"
    )
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
    
    @property
    def is_production(self) -> bool:
        """Check if running in production environment"""
        return self.environment == Environment.PRODUCTION
    
    @property
    def is_development(self) -> bool:
        """Check if running in development environment"""
        return self.environment == Environment.DEVELOPMENT
    
    def get_performance_limits(self) -> Dict[str, Any]:
        """Get performance-related limits"""
        return {
            "max_rows_per_job": self.max_rows_per_job,
            "max_memory_usage_mb": self.max_memory_usage_mb,
            "max_execution_time_seconds": self.max_execution_time_seconds,
            "max_concurrent_jobs": self.max_concurrent_jobs,
            "job_timeout_seconds": self.job_timeout_seconds
        }
    
    def get_rate_limits(self) -> Dict[str, int]:
        """Get rate limiting configuration"""
        return {
            "requests_per_minute": self.rate_limit_requests_per_minute,
            "jobs_per_hour": self.rate_limit_jobs_per_hour
        }
    
    def get_security_settings(self) -> Dict[str, Any]:
        """Get security-related settings"""
        return {
            "enable_input_validation": self.enable_input_validation,
            "max_filter_complexity": self.max_filter_complexity,
            "allowed_regex_patterns": self.allowed_regex_patterns
        }

# Global settings instance
sampling_settings = SamplingSettings()

def get_sampling_settings() -> SamplingSettings:
    """Get the global sampling settings instance"""
    return sampling_settings

def override_settings(**kwargs) -> None:
    """Override settings (useful for testing)"""
    global sampling_settings
    for key, value in kwargs.items():
        if hasattr(sampling_settings, key):
            setattr(sampling_settings, key, value)
        else:
            raise ValueError(f"Unknown setting: {key}")

def reset_settings() -> None:
    """Reset settings to defaults (useful for testing)"""
    global sampling_settings
    sampling_settings = SamplingSettings()