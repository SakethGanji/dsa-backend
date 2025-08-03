"""Simple in-memory cache for preview results in IDE-like environment."""

import hashlib
import time
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass

@dataclass
class CachedResult:
    """Cached query result with metadata."""
    data: list[Dict[str, Any]]
    columns: list[Dict[str, str]]
    total_row_count: Optional[int]
    execution_time_ms: int
    cached_at: float
    query_hash: str


class PreviewCache:
    """
    Simple LRU cache for preview query results.
    Perfect for IDE-like environments where users run the same query multiple times.
    """
    
    def __init__(self, max_entries: int = 100, ttl_seconds: int = 300):
        self.cache: Dict[str, CachedResult] = {}
        self.max_entries = max_entries
        self.ttl_seconds = ttl_seconds
        self.access_order: list[str] = []
    
    def _compute_cache_key(self, sql: str, sources: list[Dict], limit: int, offset: int) -> str:
        """Generate cache key from query parameters."""
        # Include sources to handle same query on different datasets/refs
        key_data = {
            'sql': sql,
            'sources': sorted(sources, key=lambda x: x['alias']),
            'limit': limit,
            'offset': offset
        }
        key_str = str(key_data)
        return hashlib.md5(key_str.encode()).hexdigest()
    
    def get(self, sql: str, sources: list[Dict], limit: int, offset: int) -> Optional[CachedResult]:
        """Get cached result if available and not expired."""
        cache_key = self._compute_cache_key(sql, sources, limit, offset)
        
        if cache_key in self.cache:
            result = self.cache[cache_key]
            
            # Check if expired
            if time.time() - result.cached_at > self.ttl_seconds:
                del self.cache[cache_key]
                self.access_order.remove(cache_key)
                return None
            
            # Update access order (LRU)
            self.access_order.remove(cache_key)
            self.access_order.append(cache_key)
            
            return result
        
        return None
    
    def put(self, sql: str, sources: list[Dict], limit: int, offset: int,
            data: list[Dict[str, Any]], columns: list[Dict[str, str]], 
            execution_time_ms: int, total_row_count: Optional[int] = None):
        """Cache query result."""
        cache_key = self._compute_cache_key(sql, sources, limit, offset)
        
        # Evict oldest if at capacity
        if len(self.cache) >= self.max_entries and cache_key not in self.cache:
            oldest_key = self.access_order.pop(0)
            del self.cache[oldest_key]
        
        # Add to cache
        self.cache[cache_key] = CachedResult(
            data=data,
            columns=columns,
            total_row_count=total_row_count,
            execution_time_ms=execution_time_ms,
            cached_at=time.time(),
            query_hash=cache_key
        )
        
        if cache_key in self.access_order:
            self.access_order.remove(cache_key)
        self.access_order.append(cache_key)
    
    def invalidate_dataset(self, dataset_id: int):
        """Invalidate all cached queries for a dataset."""
        # In production, would track which queries use which datasets
        # For now, clear all on any change
        self.cache.clear()
        self.access_order.clear()


# Global cache instance
_preview_cache = PreviewCache()