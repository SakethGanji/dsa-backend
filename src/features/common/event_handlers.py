"""Common event handlers for cross-cutting concerns."""

import logging
from typing import List, Dict, Any
from datetime import datetime
import json

from src.core.events.publisher import DomainEvent
from src.core.events.publisher import EventType
from src.infrastructure.postgres.database import DatabasePool


logger = logging.getLogger(__name__)


class CacheInvalidationHandler:
    """Handler for invalidating caches based on domain events."""
    
    def __init__(self, cache_client=None):
        self._cache_client = cache_client
        self._invalidation_patterns = {
            EventType.DATASET_UPDATED: ['dataset:{aggregate_id}', 'datasets:*'],
            EventType.DATASET_DELETED: ['dataset:{aggregate_id}', 'datasets:*'],
            EventType.COMMIT_CREATED: ['dataset:{dataset_id}:commits', 'commit:{aggregate_id}'],
            EventType.USER_UPDATED: ['user:{aggregate_id}', 'users:*'],
            EventType.USER_DELETED: ['user:{aggregate_id}', 'users:*']
        }
    
    def handles(self) -> List[EventType]:
        """Return list of event types this handler processes."""
        return list(self._invalidation_patterns.keys())
    
    async def handle(self, event: DomainEvent) -> None:
        """Handle events by invalidating relevant cache entries."""
        if not self._cache_client:
            logger.debug("No cache client configured, skipping invalidation")
            return
        
        patterns = self._invalidation_patterns.get(event.event_type, [])
        
        for pattern in patterns:
            # Replace placeholders with actual values
            cache_key = pattern.format(
                aggregate_id=event.aggregate_id,
                **event.payload
            )
            
            try:
                if '*' in cache_key:
                    # Pattern-based invalidation
                    await self._invalidate_pattern(cache_key)
                else:
                    # Single key invalidation
                    await self._invalidate_key(cache_key)
                    
                logger.info(f"Invalidated cache for pattern: {cache_key}")
            except Exception as e:
                logger.error(f"Failed to invalidate cache for {cache_key}: {e}")
    
    async def _invalidate_key(self, key: str) -> None:
        """Invalidate a single cache key."""
        if hasattr(self._cache_client, 'delete'):
            await self._cache_client.delete(key)
    
    async def _invalidate_pattern(self, pattern: str) -> None:
        """Invalidate all keys matching a pattern."""
        if hasattr(self._cache_client, 'delete_pattern'):
            await self._cache_client.delete_pattern(pattern)
        elif hasattr(self._cache_client, 'keys'):
            # Fallback: get keys and delete individually
            keys = await self._cache_client.keys(pattern)
            for key in keys:
                await self._cache_client.delete(key)
    
    @property
    def handler_name(self) -> str:
        """Return the name of this handler for logging."""
        return "CacheInvalidationHandler"


class AuditLogHandler:
    """Handler for creating audit logs from domain events."""
    
    def __init__(self, db_pool: DatabasePool):
        self._db_pool = db_pool
    
    def handles(self) -> List[EventType]:
        """Return list of event types this handler processes."""
        # Audit all events
        return list(EventType)
    
    async def handle(self, event: DomainEvent) -> None:
        """Handle events by creating audit log entries."""
        try:
            await self._create_audit_log(event)
            logger.debug(f"Created audit log for {event.event_type.value}")
        except Exception as e:
            logger.error(f"Failed to create audit log: {e}", exc_info=True)
    
    async def _create_audit_log(self, event: DomainEvent) -> None:
        """Create an audit log entry in the database."""
        async with self._db_pool.acquire() as conn:
            # Ensure audit schema exists
            await conn.execute("""
                CREATE SCHEMA IF NOT EXISTS dsa_audit
            """)
            
            # Create audit log table if not exists
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS dsa_audit.audit_logs (
                    id BIGSERIAL PRIMARY KEY,
                    event_id UUID NOT NULL,
                    event_type VARCHAR(100) NOT NULL,
                    aggregate_type VARCHAR(100) NOT NULL,
                    aggregate_id VARCHAR(255) NOT NULL,
                    user_id INTEGER,
                    action VARCHAR(100) NOT NULL,
                    payload JSONB NOT NULL,
                    metadata JSONB NOT NULL DEFAULT '{}',
                    occurred_at TIMESTAMP NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes separately
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_event_id 
                ON dsa_audit.audit_logs (event_id)
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_aggregate 
                ON dsa_audit.audit_logs (aggregate_type, aggregate_id)
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_user 
                ON dsa_audit.audit_logs (user_id)
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_occurred 
                ON dsa_audit.audit_logs (occurred_at)
            """)
            
            # Extract action from event type
            action = event.event_type.value.split('.')[-1]
            
            # Insert audit log
            await conn.execute("""
                INSERT INTO dsa_audit.audit_logs (
                    event_id, event_type, aggregate_type, aggregate_id,
                    user_id, action, payload, metadata, occurred_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """,
                event.event_id,
                event.event_type.value,
                event.aggregate_type,
                event.aggregate_id,
                event.user_id,
                action,
                json.dumps(event.payload),
                json.dumps(event.metadata),
                event.occurred_at
            )
    
    @property
    def handler_name(self) -> str:
        """Return the name of this handler for logging."""
        return "AuditLogHandler"


class NotificationHandler:
    """Handler for sending notifications based on domain events."""
    
    def __init__(self, notification_service=None):
        self._notification_service = notification_service
        self._notification_config = {
            EventType.DATASET_CREATED: {
                'template': 'dataset_created',
                'channels': ['email', 'webhook']
            },
            EventType.JOB_FAILED: {
                'template': 'job_failed',
                'channels': ['email', 'slack'],
                'priority': 'high'
            },
            EventType.IMPORT_COMPLETED: {
                'template': 'import_completed',
                'channels': ['email']
            },
            EventType.USER_DELETED: {
                'template': 'user_deleted',
                'channels': ['email'],
                'notify_admins': True
            }
        }
    
    def handles(self) -> List[EventType]:
        """Return list of event types this handler processes."""
        return list(self._notification_config.keys())
    
    async def handle(self, event: DomainEvent) -> None:
        """Handle events by sending appropriate notifications."""
        if not self._notification_service:
            logger.debug("No notification service configured")
            return
        
        config = self._notification_config.get(event.event_type)
        if not config:
            return
        
        try:
            await self._send_notification(event, config)
        except Exception as e:
            logger.error(
                f"Failed to send notification for {event.event_type.value}: {e}",
                exc_info=True
            )
    
    async def _send_notification(
        self,
        event: DomainEvent,
        config: Dict[str, Any]
    ) -> None:
        """Send notification based on event and configuration."""
        notification_data = {
            'event_type': event.event_type.value,
            'aggregate_id': event.aggregate_id,
            'aggregate_type': event.aggregate_type,
            'user_id': event.user_id,
            'occurred_at': event.occurred_at.isoformat(),
            'payload': event.payload,
            'template': config['template'],
            'channels': config['channels'],
            'priority': config.get('priority', 'normal')
        }
        
        # Determine recipients
        recipients = []
        if event.user_id:
            recipients.append({'type': 'user', 'id': event.user_id})
        
        if config.get('notify_admins'):
            recipients.append({'type': 'role', 'role': 'admin'})
        
        # Add dataset collaborators for dataset events
        if event.aggregate_type == 'Dataset':
            recipients.append({
                'type': 'dataset_collaborators',
                'dataset_id': int(event.aggregate_id)
            })
        
        notification_data['recipients'] = recipients
        
        # Send via notification service
        if hasattr(self._notification_service, 'send'):
            await self._notification_service.send(notification_data)
            logger.info(
                f"Sent {config['template']} notification for {event.event_type.value}"
            )
    
    @property
    def handler_name(self) -> str:
        """Return the name of this handler for logging."""
        return "NotificationHandler"