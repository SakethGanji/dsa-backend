"""Base PostgreSQL repository implementation."""

from typing import Optional, Dict, Any, List, TypeVar, Generic, Union
import json
from datetime import datetime
from uuid import UUID

from src.core.abstractions.base_repository import IBaseRepository
from src.core.abstractions.database import IDatabaseConnection
from .adapters import AsyncpgConnectionAdapter

# Type for entities (PostgreSQL returns Dict[str, Any])
TEntity = Dict[str, Any]
TId = TypeVar('TId', int, str, UUID)


class BasePostgresRepository(IBaseRepository[TEntity, int], Generic[TId]):
    """Base PostgreSQL repository with common implementations."""
    
    def __init__(
        self, 
        connection: Union[IDatabaseConnection, 'asyncpg.Connection'], 
        table_name: str, 
        id_column: str = "id",
        id_type: type = int
    ):
        """
        Initialize base repository.
        
        Args:
            connection: Database connection (generic or asyncpg)
            table_name: Name of the database table
            id_column: Name of the ID column (default: "id")
            id_type: Type of the ID (int, str, or UUID)
        """
        # Handle both generic interface and asyncpg connection
        if hasattr(connection, 'fetchrow') and hasattr(connection, 'execute'):
            # It's already a proper connection (either IDatabaseConnection or asyncpg)
            self._conn = connection
        else:
            # Wrap asyncpg connection in adapter
            from asyncpg import Connection as AsyncpgConnection
            if isinstance(connection, AsyncpgConnection):
                self._conn = AsyncpgConnectionAdapter(connection)
            else:
                self._conn = connection
        
        self._table_name = table_name
        self._id_column = id_column
        self._id_type = id_type
    
    async def get_by_id(self, entity_id: TId) -> Optional[TEntity]:
        """Generic get by ID implementation."""
        query = f"""
            SELECT * FROM {self._table_name}
            WHERE {self._id_column} = $1
        """
        row = await self._conn.fetchrow(query, entity_id)
        # Handle both dict (from generic interface) and asyncpg.Record
        if row is None:
            return None
        elif isinstance(row, dict):
            return row
        else:
            # asyncpg.Record
            return dict(row)
    
    async def exists(self, entity_id: TId) -> bool:
        """Efficient existence check."""
        query = f"""
            SELECT EXISTS(
                SELECT 1 FROM {self._table_name}
                WHERE {self._id_column} = $1
            )
        """
        return await self._conn.fetchval(query, entity_id)
    
    async def delete(self, entity_id: TId) -> bool:
        """Generic delete implementation."""
        query = f"""
            DELETE FROM {self._table_name}
            WHERE {self._id_column} = $1
            RETURNING {self._id_column}
        """
        result = await self._conn.fetchval(query, entity_id)
        return result is not None
    
    async def count(self, **filters) -> int:
        """Generic count implementation with filters."""
        query = f"SELECT COUNT(*) FROM {self._table_name}"
        
        if filters:
            conditions = []
            values = []
            for i, (key, value) in enumerate(filters.items(), 1):
                if value is None:
                    conditions.append(f"{key} IS NULL")
                else:
                    conditions.append(f"{key} = ${i}")
                    values.append(value)
            
            query += " WHERE " + " AND ".join(conditions)
            result = await self._conn.fetchval(query, *values)
        else:
            result = await self._conn.fetchval(query)
        
        return result
    
    async def list(
        self, 
        offset: int = 0, 
        limit: int = 100,
        order_by: Optional[str] = None,
        order_desc: bool = False,
        **filters
    ) -> List[TEntity]:
        """Generic list implementation with pagination and filters."""
        query = f"SELECT * FROM {self._table_name}"
        values = []
        
        # Add filters
        if filters:
            conditions = []
            for i, (key, value) in enumerate(filters.items(), 1):
                if value is None:
                    conditions.append(f"{key} IS NULL")
                else:
                    conditions.append(f"{key} = ${i}")
                    values.append(value)
            query += " WHERE " + " AND ".join(conditions)
        
        # Add ordering
        if order_by:
            query += f" ORDER BY {order_by}"
            if order_desc:
                query += " DESC"
        else:
            query += f" ORDER BY {self._id_column}"
        
        # Add pagination
        param_offset = len(values) + 1
        param_limit = param_offset + 1
        query += f" OFFSET ${param_offset} LIMIT ${param_limit}"
        values.extend([offset, limit])
        
        rows = await self._conn.fetch(query, *values)
        # Handle both list of dicts (from generic interface) and asyncpg.Records
        if not rows:
            return []
        elif isinstance(rows[0], dict):
            return rows
        else:
            # asyncpg.Records
            return [dict(row) for row in rows]
    
    async def find_one(self, **filters) -> Optional[TEntity]:
        """Find single entity matching filters."""
        results = await self.list(limit=1, **filters)
        return results[0] if results else None
    
    async def create(self, **kwargs) -> TId:
        """
        Generic create implementation.
        
        Subclasses should override this method as it needs specific
        knowledge of required fields and their types.
        """
        # Filter out None values
        data = {k: v for k, v in kwargs.items() if v is not None}
        
        # Build INSERT query
        columns = list(data.keys())
        placeholders = [f"${i+1}" for i in range(len(columns))]
        values = list(data.values())
        
        query = f"""
            INSERT INTO {self._table_name} ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
            RETURNING {self._id_column}
        """
        
        result = await self._conn.fetchval(query, *values)
        return self._id_type(result)
    
    async def update(self, entity_id: TId, **kwargs) -> bool:
        """Generic update implementation."""
        # Filter out None values unless explicitly updating to NULL
        data = {k: v for k, v in kwargs.items()}
        
        if not data:
            return True  # Nothing to update
        
        # Build UPDATE query
        set_clauses = []
        values = []
        for i, (key, value) in enumerate(data.items(), 1):
            set_clauses.append(f"{key} = ${i}")
            values.append(value)
        
        # Add entity_id as last parameter
        id_param = len(values) + 1
        
        query = f"""
            UPDATE {self._table_name}
            SET {', '.join(set_clauses)}
            WHERE {self._id_column} = ${id_param}
            RETURNING {self._id_column}
        """
        
        values.append(entity_id)
        result = await self._conn.fetchval(query, *values)
        return result is not None
    
    async def bulk_create(self, entities: List[Dict[str, Any]]) -> List[TId]:
        """Efficient bulk insert using PostgreSQL COPY."""
        if not entities:
            return []
        
        # Get columns from first entity
        columns = list(entities[0].keys())
        
        # Prepare data for COPY
        records = []
        for entity in entities:
            values = []
            for col in columns:
                value = entity.get(col)
                # Handle special types
                if isinstance(value, (dict, list)):
                    value = json.dumps(value)
                elif isinstance(value, datetime):
                    value = value.isoformat()
                values.append(value)
            records.append(tuple(values))
        
        # Use COPY for bulk insert (only available on asyncpg connections)
        if hasattr(self._conn, 'copy_records_to_table'):
            result = await self._conn.copy_records_to_table(
                self._table_name,
                records=records,
                columns=columns
            )
        elif hasattr(self._conn, 'raw_connection'):
            # Using adapter, get raw connection
            result = await self._conn.raw_connection.copy_records_to_table(
                self._table_name,
                records=records,
                columns=columns
            )
        else:
            # Fallback to regular inserts
            for record in records:
                values_dict = dict(zip(columns, record))
                await self.create(**values_dict)
        
        # Get the IDs of inserted records
        # This is a simplified approach - in production you might want to use RETURNING
        query = f"""
            SELECT {self._id_column} FROM {self._table_name}
            ORDER BY {self._id_column} DESC
            LIMIT $1
        """
        rows = await self._conn.fetch(query, len(entities))
        # Handle both dict and asyncpg.Record
        if not rows:
            return []
        elif isinstance(rows[0], dict):
            return [self._id_type(row[self._id_column]) for row in reversed(rows)]
        else:
            # asyncpg.Records
            return [self._id_type(row[self._id_column]) for row in reversed(rows)]
    
    async def bulk_delete(self, entity_ids: List[TId]) -> int:
        """Efficient bulk delete."""
        if not entity_ids:
            return 0
        
        query = f"""
            DELETE FROM {self._table_name}
            WHERE {self._id_column} = ANY($1::{'bigint' if self._id_type == int else 'text'}[])
        """
        
        result = await self._conn.execute(query, entity_ids)
        # Extract number of deleted rows from result like "DELETE 5"
        return int(result.split()[-1]) if result else 0
    
    def _build_where_clause(self, filters: Dict[str, Any]) -> tuple[str, List[Any]]:
        """Helper to build WHERE clause from filters."""
        if not filters:
            return "", []
        
        conditions = []
        values = []
        for i, (key, value) in enumerate(filters.items(), 1):
            if value is None:
                conditions.append(f"{key} IS NULL")
            elif isinstance(value, list):
                # Handle IN clause
                conditions.append(f"{key} = ANY(${i})")
                values.append(value)
            elif isinstance(value, dict) and 'min' in value:
                # Handle range queries
                if 'min' in value:
                    conditions.append(f"{key} >= ${i}")
                    values.append(value['min'])
                    i += 1
                if 'max' in value:
                    conditions.append(f"{key} <= ${i}")
                    values.append(value['max'])
            else:
                conditions.append(f"{key} = ${i}")
                values.append(value)
        
        where_clause = " WHERE " + " AND ".join(conditions)
        return where_clause, values