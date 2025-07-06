"""Search feature module."""

from .handlers import (
    SearchDatasetsHandler,
    SuggestHandler,
    RefreshSearchIndexHandler
)
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
    # Handlers
    'SearchDatasetsHandler',
    'SuggestHandler', 
    'RefreshSearchIndexHandler',
    
    # Models
    'SearchRequest',
    'SuggestRequest',
    'SearchResponse',
    'SuggestResponse',
    'SearchResult',
    'SearchFacets',
    'Suggestion'
]