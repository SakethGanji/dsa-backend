"""Environment-based configuration provider."""

import os
from typing import Any, Optional, Dict
from src.core.abstractions.external import IConfigurationProvider


class EnvConfigurationProvider(IConfigurationProvider):
    """Environment variables implementation of IConfigurationProvider."""
    
    def __init__(self, prefix: str = "DSA_"):
        self._prefix = prefix
        self._cache: Dict[str, Any] = {}
        self.reload()
    
    def _get_env_key(self, key: str) -> str:
        """Convert key to environment variable name."""
        # Convert to uppercase and prepend prefix
        env_key = f"{self._prefix}{key.upper()}"
        # Replace dots with underscores
        return env_key.replace('.', '_')
    
    def get_setting(self, key: str, default: Any = None) -> Any:
        """Get a configuration setting."""
        env_key = self._get_env_key(key)
        
        # Check cache first
        if env_key in self._cache:
            return self._cache[env_key]
        
        # Get from environment
        value = os.environ.get(env_key)
        if value is None:
            return default
        
        # Cache and return
        self._cache[env_key] = value
        return value
    
    def get_int(self, key: str, default: int = 0) -> int:
        """Get an integer configuration setting."""
        value = self.get_setting(key)
        if value is None:
            return default
        
        try:
            return int(value)
        except ValueError:
            return default
    
    def get_str(self, key: str, default: str = "") -> str:
        """Get a string configuration setting."""
        value = self.get_setting(key)
        return str(value) if value is not None else default
    
    def get_bool(self, key: str, default: bool = False) -> bool:
        """Get a boolean configuration setting."""
        value = self.get_setting(key)
        if value is None:
            return default
        
        # Handle common boolean representations
        if isinstance(value, bool):
            return value
        
        str_value = str(value).lower()
        return str_value in ('true', '1', 'yes', 'on', 'enabled')
    
    def get_list(self, key: str, default: list = None) -> list:
        """Get a list configuration setting."""
        if default is None:
            default = []
        
        value = self.get_setting(key)
        if value is None:
            return default
        
        # Assume comma-separated values
        str_value = str(value)
        if not str_value:
            return default
        
        return [item.strip() for item in str_value.split(',')]
    
    def reload(self) -> None:
        """Reload configuration from environment."""
        self._cache.clear()
        
        # Pre-cache all environment variables with our prefix
        for key, value in os.environ.items():
            if key.startswith(self._prefix):
                self._cache[key] = value


class FileConfigurationProvider(IConfigurationProvider):
    """
    File-based configuration provider.
    
    This is a placeholder for file-based configuration (JSON, YAML, etc.).
    """
    
    def __init__(self, config_file: str):
        self._config_file = config_file
        self._config: Dict[str, Any] = {}
        self.reload()
    
    def get_setting(self, key: str, default: Any = None) -> Any:
        """Get a configuration setting using dot notation."""
        keys = key.split('.')
        value = self._config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def get_int(self, key: str, default: int = 0) -> int:
        value = self.get_setting(key)
        if value is None:
            return default
        try:
            return int(value)
        except (ValueError, TypeError):
            return default
    
    def get_str(self, key: str, default: str = "") -> str:
        value = self.get_setting(key)
        return str(value) if value is not None else default
    
    def get_bool(self, key: str, default: bool = False) -> bool:
        value = self.get_setting(key)
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        return str(value).lower() in ('true', '1', 'yes', 'on')
    
    def get_list(self, key: str, default: list = None) -> list:
        if default is None:
            default = []
        value = self.get_setting(key)
        if value is None:
            return default
        if isinstance(value, list):
            return value
        return default
    
    def reload(self) -> None:
        """Reload configuration from file."""
        # Placeholder - would load from JSON/YAML file
        pass