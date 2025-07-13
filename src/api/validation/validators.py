"""Security validators for API input validation."""

import re
from pathlib import Path
from typing import Any
from src.core.domain_exceptions import ValidationException


def validate_no_sql_injection(v: Any) -> Any:
    """Validate that input doesn't contain potential SQL injection patterns."""
    if v is None:
        return v
    
    str_value = str(v).lower()
    
    # Common SQL injection patterns
    sql_patterns = [
        r"(\bunion\b.*\bselect\b|\bselect\b.*\bfrom\b|\binsert\b.*\binto\b)",
        r"(\bdelete\b.*\bfrom\b|\bdrop\b.*\btable\b|\bupdate\b.*\bset\b)",
        r"(--|\#|\/\*|\*\/|xp_|sp_|exec\s*\()",
        r"(\bor\b\s*\d+\s*=\s*\d+|\band\b\s*\d+\s*=\s*\d+)"
    ]
    
    for pattern in sql_patterns:
        if re.search(pattern, str_value):
            raise ValidationException(f"Invalid input: potential SQL injection detected")
    
    return v


def validate_no_script_tags(v: str) -> str:
    """Validate that input doesn't contain script tags."""
    if v and re.search(r'<\s*script[^>]*>.*?<\s*/\s*script\s*>', v, re.IGNORECASE | re.DOTALL):
        raise ValidationException("Script tags are not allowed")
    return v


def validate_safe_filename(v: str) -> str:
    """Validate filename for safety."""
    if not v:
        raise ValidationException("Filename cannot be empty")
    
    # Check for path traversal attempts
    if '..' in v or '/' in v or '\\' in v:
        raise ValidationException("Invalid filename: path traversal not allowed")
    
    # Check extension
    allowed_extensions = {'.csv', '.xlsx', '.xls', '.json', '.parquet', '.tsv'}
    if not any(v.lower().endswith(ext) for ext in allowed_extensions):
        raise ValidationException(f"Invalid file type. Allowed: {', '.join(allowed_extensions)}")
    
    return v