"""Abstractions for JSONB operations."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union


class IJSONBOperations(ABC):
    """Interface for JSONB operations in databases."""
    
    @abstractmethod
    def jsonb_build_object(self, *args) -> str:
        """
        Build a JSONB object from key-value pairs.
        
        Example:
            jsonb_build_object('key1', 'value1', 'key2', 'value2')
            
        Args:
            *args: Alternating keys and values
            
        Returns:
            SQL expression for building JSONB object
        """
        pass
    
    @abstractmethod
    def jsonb_extract_path(self, column: str, *path: str) -> str:
        """
        Extract value at specified path from JSONB column.
        
        Example:
            jsonb_extract_path('metadata', 'user', 'name')
            
        Args:
            column: JSONB column name
            *path: Path components
            
        Returns:
            SQL expression for extracting value
        """
        pass
    
    @abstractmethod
    def jsonb_contains(self, column: str, value: Dict[str, Any]) -> str:
        """
        Check if JSONB column contains specified value.
        
        Example:
            jsonb_contains('tags', {'category': 'tech'})
            
        Args:
            column: JSONB column name
            value: Value to check for containment
            
        Returns:
            SQL expression for containment check
        """
        pass
    
    @abstractmethod
    def jsonb_exists(self, column: str, key: str) -> str:
        """
        Check if JSONB column contains specified key.
        
        Example:
            jsonb_exists('metadata', 'user_id')
            
        Args:
            column: JSONB column name
            key: Key to check existence
            
        Returns:
            SQL expression for existence check
        """
        pass
    
    @abstractmethod
    def jsonb_array_contains(self, column: str, value: Any) -> str:
        """
        Check if JSONB array contains specified value.
        
        Example:
            jsonb_array_contains('tags', 'python')
            
        Args:
            column: JSONB column name
            value: Value to search in array
            
        Returns:
            SQL expression for array containment
        """
        pass
    
    @abstractmethod
    def jsonb_set(self, column: str, path: List[str], value: Any) -> str:
        """
        Set value at specified path in JSONB column.
        
        Example:
            jsonb_set('metadata', ['user', 'name'], 'John')
            
        Args:
            column: JSONB column name
            path: Path where to set value
            value: Value to set
            
        Returns:
            SQL expression for setting value
        """
        pass
    
    @abstractmethod
    def jsonb_remove(self, column: str, path: Union[str, List[str]]) -> str:
        """
        Remove value at specified path from JSONB column.
        
        Example:
            jsonb_remove('metadata', 'temp_data')
            jsonb_remove('metadata', ['user', 'temp'])
            
        Args:
            column: JSONB column name
            path: Path to remove (string for top-level, list for nested)
            
        Returns:
            SQL expression for removing value
        """
        pass


class PostgreSQLJSONBOperations(IJSONBOperations):
    """PostgreSQL implementation of JSONB operations."""
    
    def jsonb_build_object(self, *args) -> str:
        """Build a JSONB object from key-value pairs."""
        if len(args) % 2 != 0:
            raise ValueError("jsonb_build_object requires even number of arguments")
        
        formatted_args = []
        for i in range(0, len(args), 2):
            key = f"'{args[i]}'" if isinstance(args[i], str) else str(args[i])
            value = f"${(i//2) + 1}" if isinstance(args[i+1], int) else f"'{args[i+1]}'"
            formatted_args.extend([key, value])
        
        return f"jsonb_build_object({', '.join(formatted_args)})"
    
    def jsonb_extract_path(self, column: str, *path: str) -> str:
        """Extract value at specified path from JSONB column."""
        path_str = ", ".join(f"'{p}'" for p in path)
        return f"jsonb_extract_path({column}, {path_str})"
    
    def jsonb_contains(self, column: str, value: Dict[str, Any]) -> str:
        """Check if JSONB column contains specified value."""
        # In real usage, value would be parameterized
        return f"{column} @> $1::jsonb"
    
    def jsonb_exists(self, column: str, key: str) -> str:
        """Check if JSONB column contains specified key."""
        return f"{column} ? '{key}'"
    
    def jsonb_array_contains(self, column: str, value: Any) -> str:
        """Check if JSONB array contains specified value."""
        # In real usage, value would be parameterized
        return f"{column} @> $1::jsonb"
    
    def jsonb_set(self, column: str, path: List[str], value: Any) -> str:
        """Set value at specified path in JSONB column."""
        path_str = "{" + ",".join(f'"{p}"' for p in path) + "}"
        return f"jsonb_set({column}, '{path_str}', $1::jsonb)"
    
    def jsonb_remove(self, column: str, path: Union[str, List[str]]) -> str:
        """Remove value at specified path from JSONB column."""
        if isinstance(path, str):
            return f"{column} - '{path}'"
        else:
            path_str = "{" + ",".join(f'"{p}"' for p in path) + "}"
            return f"{column} #- '{path_str}'"