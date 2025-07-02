"""Event-driven architecture module.

Re-exports the main event types and interfaces for convenience.
"""

from app.core.events.types import Event, EventType, IEventBus
from app.core.events.registration import register_handler, setup_event_handlers

__all__ = [
    "Event",
    "EventType", 
    "IEventBus",
    "register_handler",
    "setup_event_handlers",
]