"""Search feature module."""

from .services import SearchService
from .event_handlers import SearchIndexEventHandler
from .models import (
    SearchRequest,
    SuggestRequest,
    SearchResponse,
    SuggestResponse,
    SearchResult,
    SearchFacets,
    Suggestion
)

__all__ = [
    # Services
    'SearchService',
    
    # Event Handlers
    'SearchIndexEventHandler',
    
    # Models
    'SearchRequest',
    'SuggestRequest',
    'SearchResponse',
    'SuggestResponse',
    'SearchResult',
    'SearchFacets',
    'Suggestion'
]