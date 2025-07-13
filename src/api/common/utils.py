"""Common utilities for API endpoints."""

from typing import List, Optional, Dict, Any
from datetime import datetime
import re


def parse_tags(tags_str: Optional[str]) -> List[str]:
    """
    Parse comma-separated tags string into a list.
    
    Args:
        tags_str: Comma-separated tags string
        
    Returns:
        List of cleaned tag strings
    """
    if not tags_str:
        return []
    
    # Split by comma and clean each tag
    tags = [tag.strip() for tag in tags_str.split(',')]
    # Remove empty tags
    return [tag for tag in tags if tag]


def clean_search_query(query: str) -> str:
    """
    Clean and normalize search query.
    
    Args:
        query: Raw search query
        
    Returns:
        Cleaned query string
    """
    # Remove extra whitespace
    query = ' '.join(query.split())
    # Remove special characters that might break search
    query = re.sub(r'[<>\"\'\\]', '', query)
    return query.strip()


def format_datetime(dt: Optional[datetime]) -> Optional[str]:
    """
    Format datetime to ISO 8601 string.
    
    Args:
        dt: Datetime object
        
    Returns:
        ISO formatted string or None
    """
    if dt:
        return dt.isoformat()
    return None


def parse_sort_params(
    sort_by: Optional[str],
    sort_order: Optional[str] = "asc"
) -> Optional[Dict[str, str]]:
    """
    Parse and validate sort parameters.
    
    Args:
        sort_by: Field to sort by
        sort_order: Sort order (asc/desc)
        
    Returns:
        Dict with sort field and order, or None
    """
    if not sort_by:
        return None
    
    # Normalize sort order
    sort_order = sort_order.lower()
    if sort_order not in ["asc", "desc"]:
        sort_order = "asc"
    
    return {
        "field": sort_by,
        "order": sort_order
    }


def build_filter_dict(
    **kwargs
) -> Dict[str, Any]:
    """
    Build a filter dictionary from keyword arguments,
    excluding None values.
    
    Returns:
        Dict with non-None values only
    """
    return {k: v for k, v in kwargs.items() if v is not None}


def validate_enum(
    value: Optional[str],
    allowed_values: List[str],
    default: Optional[str] = None
) -> Optional[str]:
    """
    Validate that a value is in the allowed list.
    
    Args:
        value: Value to validate
        allowed_values: List of allowed values
        default: Default value if validation fails
        
    Returns:
        Validated value or default
    """
    if not value:
        return default
    
    if value in allowed_values:
        return value
    
    return default


def sanitize_filename(filename: str) -> str:
    """
    Sanitize a filename for safe storage.
    
    Args:
        filename: Original filename
        
    Returns:
        Sanitized filename
    """
    # Remove path components
    filename = filename.split('/')[-1].split('\\')[-1]
    # Replace unsafe characters
    filename = re.sub(r'[^\w\s.-]', '_', filename)
    # Remove multiple underscores
    filename = re.sub(r'_+', '_', filename)
    return filename.strip('_')


def get_file_extension(filename: str) -> str:
    """
    Get file extension from filename.
    
    Args:
        filename: Filename
        
    Returns:
        Extension without dot, lowercase
    """
    parts = filename.split('.')
    if len(parts) > 1:
        return parts[-1].lower()
    return ''