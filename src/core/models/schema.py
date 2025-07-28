"""Shared schema models for tables across the system."""
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional

try:
    from src.core.domain_exceptions import ValidationException
except ImportError:
    # Fallback if domain_exceptions is not available
    ValidationException = ValueError


@dataclass
class TableSchema:
    """Unified schema definition for tables across the system."""
    columns: List[Dict[str, Any]]
    primary_key: Optional[List[str]] = None
    indexes: List[Dict[str, Any]] = field(default_factory=list)
    row_count: int = 0
    size_bytes: Optional[int] = None
    
    def __post_init__(self):
        """Validate schema (from versioning/models/commit.py)."""
        if not self.columns:
            raise ValidationException("Schema must have at least one column", field="columns")
        
        # Validate column definitions
        column_names = set()
        for col in self.columns:
            if 'name' not in col:
                raise ValidationException("Column must have a name", field="columns")
            if 'type' not in col:
                raise ValidationException(f"Column '{col.get('name')}' must have a type", field="columns")
            
            # Check for duplicate column names
            col_name = col['name']
            if col_name in column_names:
                raise ValidationException(f"Duplicate column name: {col_name}", field="columns")
            column_names.add(col_name)
        
        # Validate primary key columns exist
        if self.primary_key:
            column_names_list = {col['name'] for col in self.columns}
            for pk_col in self.primary_key:
                if pk_col not in column_names_list:
                    raise ValidationException(
                        f"Primary key column '{pk_col}' not found in schema",
                        field="primary_key"
                    )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert schema to dictionary representation."""
        return {
            'columns': self.columns,
            'primary_key': self.primary_key,
            'indexes': self.indexes,
            'row_count': self.row_count,
            'size_bytes': self.size_bytes
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TableSchema':
        """Create TableSchema from dictionary."""
        return cls(**data)
    
    def get_column(self, name: str) -> Optional[Dict[str, Any]]:
        """Get column definition by name."""
        for col in self.columns:
            if col['name'] == name:
                return col
        return None
    
    def get_column_type(self, name: str) -> Optional[str]:
        """Get column type by name."""
        col = self.get_column(name)
        return col.get('type') if col else None
    
    def get_column_names(self) -> List[str]:
        """Get list of all column names."""
        return [col['name'] for col in self.columns]