"""PostgreSQL implementation of event store."""

import json
from typing import Dict, Any, List, Optional
from datetime import datetime
import asyncpg

from src.core.abstractions.events import (
    IEventStore, DomainEvent, EventType
)
from src.infrastructure.postgres.database import DatabasePool


class PostgresEventStore(IEventStore):
    """PostgreSQL-based event store implementation."""
    
    def __init__(self, db_pool: DatabasePool):
        self._db_pool = db_pool
        self._initialized = False
    
    async def _ensure_tables(self):
        """Ensure event store tables exist."""
        if self._initialized:
            return
            
        async with self._db_pool.acquire() as conn:
            # Create schema if not exists
            await conn.execute("""
                CREATE SCHEMA IF NOT EXISTS dsa_events
            """)
            
            # Create events table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS dsa_events.domain_events (
                    event_id UUID PRIMARY KEY,
                    event_type VARCHAR(100) NOT NULL,
                    aggregate_id VARCHAR(255) NOT NULL,
                    aggregate_type VARCHAR(100) NOT NULL,
                    payload JSONB NOT NULL,
                    metadata JSONB NOT NULL DEFAULT '{}',
                    occurred_at TIMESTAMP NOT NULL,
                    user_id INTEGER,
                    correlation_id UUID,
                    version INTEGER NOT NULL DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes separately
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_aggregate 
                ON dsa_events.domain_events (aggregate_id, aggregate_type)
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_event_type 
                ON dsa_events.domain_events (event_type)
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_occurred_at 
                ON dsa_events.domain_events (occurred_at)
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_correlation 
                ON dsa_events.domain_events (correlation_id)
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_user 
                ON dsa_events.domain_events (user_id)
            """)
            
            # Create snapshots table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS dsa_events.aggregate_snapshots (
                    aggregate_id VARCHAR(255) NOT NULL,
                    aggregate_type VARCHAR(100) NOT NULL,
                    version INTEGER NOT NULL,
                    data JSONB NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    
                    PRIMARY KEY (aggregate_id, aggregate_type, version)
                )
            """)
            
            # Create index for latest snapshots
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_latest 
                ON dsa_events.aggregate_snapshots (aggregate_id, aggregate_type, version DESC)
            """)
            
        self._initialized = True
    
    async def append(self, event: DomainEvent) -> None:
        """Append an event to the event store."""
        await self._ensure_tables()
        
        async with self._db_pool.acquire() as conn:
            # Get next version for this aggregate
            version = await self._get_next_version(
                conn, event.aggregate_id, event.aggregate_type
            )
            
            await conn.execute("""
                INSERT INTO dsa_events.domain_events (
                    event_id, event_type, aggregate_id, aggregate_type,
                    payload, metadata, occurred_at, user_id, 
                    correlation_id, version
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            """,
                event.event_id,
                event.event_type.value,
                event.aggregate_id,
                event.aggregate_type,
                json.dumps(event.payload),
                json.dumps(event.metadata),
                event.occurred_at,
                event.user_id,
                event.correlation_id,
                version
            )
    
    async def append_batch(self, events: List[DomainEvent]) -> None:
        """Append multiple events to the store."""
        if not events:
            return
            
        await self._ensure_tables()
        
        async with self._db_pool.acquire() as conn:
            async with conn.transaction():
                # Group events by aggregate for version tracking
                aggregate_versions = {}
                
                for event in events:
                    key = (event.aggregate_id, event.aggregate_type)
                    if key not in aggregate_versions:
                        aggregate_versions[key] = await self._get_next_version(
                            conn, event.aggregate_id, event.aggregate_type
                        )
                    
                    version = aggregate_versions[key]
                    aggregate_versions[key] += 1
                    
                    await conn.execute("""
                        INSERT INTO dsa_events.domain_events (
                            event_id, event_type, aggregate_id, aggregate_type,
                            payload, metadata, occurred_at, user_id, 
                            correlation_id, version
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    """,
                        event.event_id,
                        event.event_type.value,
                        event.aggregate_id,
                        event.aggregate_type,
                        json.dumps(event.payload),
                        json.dumps(event.metadata),
                        event.occurred_at,
                        event.user_id,
                        event.correlation_id,
                        version
                    )
    
    async def get_events(
        self,
        aggregate_id: str,
        aggregate_type: Optional[str] = None,
        from_version: Optional[int] = None,
        to_version: Optional[int] = None
    ) -> List[DomainEvent]:
        """Get events for a specific aggregate."""
        await self._ensure_tables()
        
        query = """
            SELECT * FROM dsa_events.domain_events
            WHERE aggregate_id = $1
        """
        params = [aggregate_id]
        
        if aggregate_type:
            query += " AND aggregate_type = $2"
            params.append(aggregate_type)
        
        if from_version is not None:
            query += f" AND version >= ${len(params) + 1}"
            params.append(from_version)
        
        if to_version is not None:
            query += f" AND version <= ${len(params) + 1}"
            params.append(to_version)
        
        query += " ORDER BY version ASC"
        
        async with self._db_pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            return [self._row_to_event(row) for row in rows]
    
    async def get_events_by_type(
        self,
        event_type: EventType,
        from_timestamp: Optional[datetime] = None,
        to_timestamp: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[DomainEvent]:
        """Get events of a specific type within a time range."""
        await self._ensure_tables()
        
        query = """
            SELECT * FROM dsa_events.domain_events
            WHERE event_type = $1
        """
        params = [event_type.value]
        
        if from_timestamp:
            query += f" AND occurred_at >= ${len(params) + 1}"
            params.append(from_timestamp)
        
        if to_timestamp:
            query += f" AND occurred_at <= ${len(params) + 1}"
            params.append(to_timestamp)
        
        query += " ORDER BY occurred_at DESC"
        
        if limit:
            query += f" LIMIT ${len(params) + 1}"
            params.append(limit)
        
        async with self._db_pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            return [self._row_to_event(row) for row in rows]
    
    async def get_events_by_correlation_id(
        self,
        correlation_id: str
    ) -> List[DomainEvent]:
        """Get all events with a specific correlation ID."""
        await self._ensure_tables()
        
        query = """
            SELECT * FROM dsa_events.domain_events
            WHERE correlation_id = $1
            ORDER BY occurred_at ASC
        """
        
        async with self._db_pool.acquire() as conn:
            rows = await conn.fetch(query, correlation_id)
            return [self._row_to_event(row) for row in rows]
    
    async def get_latest_snapshot(
        self,
        aggregate_id: str,
        aggregate_type: str
    ) -> Optional[Dict[str, Any]]:
        """Get the latest snapshot for an aggregate."""
        await self._ensure_tables()
        
        query = """
            SELECT data FROM dsa_events.aggregate_snapshots
            WHERE aggregate_id = $1 AND aggregate_type = $2
            ORDER BY version DESC
            LIMIT 1
        """
        
        async with self._db_pool.acquire() as conn:
            row = await conn.fetchrow(query, aggregate_id, aggregate_type)
            if row:
                return json.loads(row['data'])
            return None
    
    async def save_snapshot(
        self,
        aggregate_id: str,
        aggregate_type: str,
        version: int,
        data: Dict[str, Any]
    ) -> None:
        """Save a snapshot of an aggregate state."""
        await self._ensure_tables()
        
        async with self._db_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO dsa_events.aggregate_snapshots (
                    aggregate_id, aggregate_type, version, data
                ) VALUES ($1, $2, $3, $4)
                ON CONFLICT (aggregate_id, aggregate_type, version) 
                DO UPDATE SET data = EXCLUDED.data
            """,
                aggregate_id,
                aggregate_type,
                version,
                json.dumps(data)
            )
    
    async def _get_next_version(
        self,
        conn: asyncpg.Connection,
        aggregate_id: str,
        aggregate_type: str
    ) -> int:
        """Get the next version number for an aggregate."""
        result = await conn.fetchval("""
            SELECT COALESCE(MAX(version), 0) + 1
            FROM dsa_events.domain_events
            WHERE aggregate_id = $1 AND aggregate_type = $2
        """, aggregate_id, aggregate_type)
        return result or 1
    
    def _row_to_event(self, row: asyncpg.Record) -> DomainEvent:
        """Convert a database row to a DomainEvent."""
        return DomainEvent(
            event_id=str(row['event_id']),
            event_type=EventType(row['event_type']),
            aggregate_id=row['aggregate_id'],
            aggregate_type=row['aggregate_type'],
            payload=json.loads(row['payload']),
            metadata=json.loads(row['metadata']),
            occurred_at=row['occurred_at'],
            user_id=row['user_id'],
            correlation_id=str(row['correlation_id']) if row['correlation_id'] else None
        )