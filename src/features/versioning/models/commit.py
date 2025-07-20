"""Versioning commit domain models."""

from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from datetime import datetime
import hashlib
import json

from src.core.domain_exceptions import ValidationException, BusinessRuleViolation


@dataclass
class TableManifestEntry:
    """Value object for a single row in a table manifest."""
    logical_row_id: str
    row_hash: str
    
    def __post_init__(self):
        """Validate manifest entry."""
        if not self.logical_row_id:
            raise ValidationException("Logical row ID is required", field="logical_row_id")
        if not self.row_hash:
            raise ValidationException("Row hash is required", field="row_hash")


@dataclass
class TableSchema:
    """Value object for table schema definition."""
    columns: List[Dict[str, Any]]
    primary_key: Optional[List[str]] = None
    
    def __post_init__(self):
        """Validate schema."""
        if not self.columns:
            raise ValidationException("Schema must have at least one column", field="columns")
        
        # Validate column definitions
        column_names = set()
        for col in self.columns:
            if 'name' not in col:
                raise ValidationException("Column must have a name", field="columns")
            if 'type' not in col:
                raise ValidationException(f"Column '{col.get('name')}' must have a type", field="columns")
            
            name = col['name']
            if name in column_names:
                raise ValidationException(f"Duplicate column name: {name}", field="columns")
            column_names.add(name)
        
        # Validate primary key columns exist
        if self.primary_key:
            for pk_col in self.primary_key:
                if pk_col not in column_names:
                    raise ValidationException(
                        f"Primary key column '{pk_col}' not found in schema",
                        field="primary_key"
                    )


@dataclass
class CommitManifest:
    """Value object for commit manifest containing table data."""
    tables: Dict[str, List[TableManifestEntry]] = field(default_factory=dict)
    
    def add_table_entry(self, table_key: str, logical_id: str, row_hash: str) -> None:
        """Add an entry to a table manifest."""
        if table_key not in self.tables:
            self.tables[table_key] = []
        
        entry = TableManifestEntry(logical_id, row_hash)
        self.tables[table_key].append(entry)
    
    def get_table_row_count(self, table_key: str) -> int:
        """Get row count for a specific table."""
        return len(self.tables.get(table_key, []))
    
    def get_total_row_count(self) -> int:
        """Get total row count across all tables."""
        return sum(len(entries) for entries in self.tables.values())
    
    def get_table_keys(self) -> List[str]:
        """Get list of table keys in manifest."""
        return list(self.tables.keys())


@dataclass
class CommitStatistics:
    """Value object for commit statistics."""
    total_rows: int = 0
    tables_count: int = 0
    data_size_bytes: Optional[int] = None
    processing_time_ms: Optional[int] = None
    
    def __post_init__(self):
        """Validate statistics."""
        if self.total_rows < 0:
            raise ValidationException("Total rows cannot be negative", field="total_rows")
        if self.tables_count < 0:
            raise ValidationException("Tables count cannot be negative", field="tables_count")


@dataclass
class Commit:
    """Commit entity representing a version of dataset."""
    id: str  # Commit hash
    dataset_id: int
    parent_commit_id: Optional[str]
    message: str
    author_id: int
    created_at: datetime
    manifest: CommitManifest
    schemas: Dict[str, TableSchema] = field(default_factory=dict)
    statistics: Optional[CommitStatistics] = None
    
    def __post_init__(self):
        """Validate commit."""
        if not self.message or len(self.message.strip()) == 0:
            raise ValidationException("Commit message is required", field="message")
        if len(self.message) > 1000:
            raise ValidationException("Commit message cannot exceed 1000 characters", field="message")
    
    @staticmethod
    def generate_commit_id(
        dataset_id: int,
        parent_commit_id: Optional[str],
        manifest: CommitManifest,
        message: str,
        author_id: int,
        timestamp: datetime
    ) -> str:
        """Generate deterministic commit ID based on content."""
        # Create a deterministic hash of commit content
        content = {
            "dataset_id": dataset_id,
            "parent_commit_id": parent_commit_id or "",
            "message": message,
            "author_id": author_id,
            "timestamp": timestamp.isoformat(),
            "manifest": _serialize_manifest(manifest)
        }
        
        content_str = json.dumps(content, sort_keys=True)
        return hashlib.sha256(content_str.encode()).hexdigest()
    
    def add_table_schema(self, table_key: str, schema: TableSchema) -> None:
        """Add schema for a table."""
        if table_key not in self.manifest.tables:
            raise BusinessRuleViolation(
                f"Cannot add schema for non-existent table: {table_key}",
                rule="table_must_exist_in_manifest"
            )
        self.schemas[table_key] = schema
    
    def get_table_schema(self, table_key: str) -> Optional[TableSchema]:
        """Get schema for a specific table."""
        return self.schemas.get(table_key)
    
    def has_parent(self) -> bool:
        """Check if this commit has a parent."""
        return self.parent_commit_id is not None
    
    def is_initial_commit(self) -> bool:
        """Check if this is the initial commit."""
        return self.parent_commit_id is None
    
    def get_table_keys(self) -> List[str]:
        """Get list of tables in this commit."""
        return self.manifest.get_table_keys()
    
    def get_row_count(self, table_key: Optional[str] = None) -> int:
        """Get row count for a table or total."""
        if table_key:
            return self.manifest.get_table_row_count(table_key)
        return self.manifest.get_total_row_count()


def _serialize_manifest(manifest: CommitManifest) -> Dict[str, Any]:
    """Serialize manifest for hashing."""
    result = {}
    for table_key in sorted(manifest.tables.keys()):
        entries = manifest.tables[table_key]
        # Sort entries by logical_row_id for deterministic hashing
        sorted_entries = sorted(entries, key=lambda e: e.logical_row_id)
        result[table_key] = [
            {"id": e.logical_row_id, "hash": e.row_hash}
            for e in sorted_entries
        ]
    return result