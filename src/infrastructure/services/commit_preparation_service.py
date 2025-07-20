"""Service for preparing commit data including canonicalization and hashing."""
import hashlib
import json
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
import pandas as pd

from src.core.abstractions.services import ICommitPreparationService
from src.core.abstractions.repositories import ITableReader


@dataclass
class CommitData:
    """Prepared data for creating a commit."""
    dataset_id: str
    parent_commit_id: str
    manifest: Dict[str, Any]
    row_hashes: Dict[str, List[str]]
    schemas: Dict[str, Dict[str, Any]]
    message: str
    author: str


@dataclass
class TableSchema:
    """Schema information for a table."""
    columns: List[Dict[str, Any]]
    primary_key: Optional[List[str]]
    indexes: List[Dict[str, Any]]


class CommitPreparationService(ICommitPreparationService):
    """Service for preparing data for commits."""
    
    def __init__(self, table_reader: ITableReader):
        self._table_reader = table_reader
    
    async def prepare_commit_data(
        self,
        dataset_id: str,
        parent_commit_id: str,
        changes: Dict[str, Any],
        message: str,
        author: str
    ) -> CommitData:
        """Prepare all data needed for creating a commit."""
        manifest = {}
        row_hashes = {}
        schemas = {}
        
        # Process each table in changes
        for table_name, table_changes in changes.items():
            # Extract schema
            schema = await self.extract_schema(dataset_id, parent_commit_id, table_name)
            schemas[table_name] = self._schema_to_dict(schema)
            
            # Canonicalize and hash data
            if 'data' in table_changes:
                canonical_data = await self.canonicalize_data(
                    table_changes['data'],
                    schema
                )
                hashes = await self.calculate_row_hashes(canonical_data, schema)
                row_hashes[table_name] = hashes
                
                # Create manifest entry
                manifest[table_name] = {
                    'row_count': len(canonical_data),
                    'hash_count': len(hashes),
                    'schema_version': table_changes.get('schema_version', '1.0'),
                    'last_modified': datetime.utcnow().isoformat()
                }
            else:
                # Table deleted or schema-only change
                manifest[table_name] = table_changes
        
        return CommitData(
            dataset_id=dataset_id,
            parent_commit_id=parent_commit_id,
            manifest=manifest,
            row_hashes=row_hashes,
            schemas=schemas,
            message=message,
            author=author
        )
    
    async def calculate_row_hashes(
        self,
        data: List[Dict[str, Any]],
        schema: TableSchema
    ) -> List[str]:
        """Calculate hashes for each row of data."""
        hashes = []
        
        # Get column order from schema
        column_order = [col['name'] for col in schema.columns]
        
        for row in data:
            # Create canonical representation
            canonical_values = []
            for col_name in column_order:
                value = row.get(col_name)
                canonical_values.append(self._canonicalize_value(value))
            
            # Calculate hash
            row_str = json.dumps(canonical_values, sort_keys=True, separators=(',', ':'))
            row_hash = hashlib.sha256(row_str.encode()).hexdigest()
            hashes.append(row_hash)
        
        return hashes
    
    async def extract_schema(
        self,
        dataset_id: str,
        commit_id: str,
        table_name: str
    ) -> TableSchema:
        """Extract schema information for a table."""
        # Get schema from table reader
        raw_schema = await self._table_reader.get_table_schema(
            dataset_id=dataset_id,
            commit_id=commit_id,
            table_name=table_name
        )
        
        # Convert to our schema format
        columns = []
        for col in raw_schema.columns:
            columns.append({
                'name': col.name,
                'type': col.data_type,
                'nullable': col.is_nullable,
                'default': col.default_value,
                'constraints': col.constraints or []
            })
        
        return TableSchema(
            columns=columns,
            primary_key=raw_schema.primary_key,
            indexes=raw_schema.indexes or []
        )
    
    async def canonicalize_data(
        self,
        data: List[Dict[str, Any]],
        schema: TableSchema
    ) -> List[Dict[str, Any]]:
        """Canonicalize data according to schema."""
        canonical_data = []
        
        for row in data:
            canonical_row = {}
            
            for col_info in schema.columns:
                col_name = col_info['name']
                col_type = col_info['type']
                value = row.get(col_name)
                
                # Apply type-specific canonicalization
                canonical_value = self._canonicalize_by_type(value, col_type)
                canonical_row[col_name] = canonical_value
            
            canonical_data.append(canonical_row)
        
        return canonical_data
    
    def _canonicalize_value(self, value: Any) -> Any:
        """Canonicalize a single value for hashing."""
        if value is None:
            return None
        elif isinstance(value, (int, float)):
            return value
        elif isinstance(value, bool):
            return value
        elif isinstance(value, datetime):
            return value.isoformat()
        elif isinstance(value, (list, dict)):
            return json.dumps(value, sort_keys=True, separators=(',', ':'))
        else:
            return str(value)
    
    def _canonicalize_by_type(self, value: Any, data_type: str) -> Any:
        """Canonicalize value based on its data type."""
        if value is None:
            return None
            
        data_type_lower = data_type.lower()
        
        if 'int' in data_type_lower:
            return int(value) if value is not None else None
        elif 'float' in data_type_lower or 'double' in data_type_lower:
            return float(value) if value is not None else None
        elif 'bool' in data_type_lower:
            return bool(value) if value is not None else None
        elif 'date' in data_type_lower or 'time' in data_type_lower:
            if isinstance(value, str):
                return value  # Already in string format
            return value.isoformat() if hasattr(value, 'isoformat') else str(value)
        elif 'json' in data_type_lower:
            if isinstance(value, str):
                return json.loads(value)
            return value
        else:
            return str(value) if value is not None else None
    
    def _schema_to_dict(self, schema: TableSchema) -> Dict[str, Any]:
        """Convert schema object to dictionary."""
        return {
            'columns': schema.columns,
            'primary_key': schema.primary_key,
            'indexes': schema.indexes
        }