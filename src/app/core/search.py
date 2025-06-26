"""Core search interfaces and models.

This module provides generic search functionality that can be reused
across different vertical slices for implementing search features.
"""
from abc import ABC, abstractmethod
from typing import List, Optional, Generic, TypeVar, Dict, Any
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum


T = TypeVar('T')  # Generic type for search results


class SortOrder(str, Enum):
    """Sort order options for search results."""
    ASC = "asc"
    DESC = "desc"


@dataclass
class BaseSearchFilters:
    """Base class for search filters."""
    created_after: Optional[datetime] = None
    created_before: Optional[datetime] = None
    updated_after: Optional[datetime] = None
    updated_before: Optional[datetime] = None
    offset: int = 0
    limit: int = 20
    sort_by: Optional[str] = None
    sort_order: SortOrder = SortOrder.DESC


@dataclass
class SearchResult(Generic[T]):
    """Individual search result with relevance scoring."""
    item: T
    relevance_score: float
    matched_fields: List[str] = field(default_factory=list)
    highlights: Dict[str, str] = field(default_factory=dict)


@dataclass
class SearchResults(Generic[T]):
    """Container for paginated search results."""
    results: List[SearchResult[T]]
    total_count: int
    offset: int
    limit: int
    query_time_ms: float
    facets: Optional[Dict[str, Dict[str, int]]] = None
    
    @property
    def has_more(self) -> bool:
        """Check if there are more results available."""
        return self.offset + self.limit < self.total_count
    
    @property
    def page_count(self) -> int:
        """Calculate total number of pages."""
        return (self.total_count + self.limit - 1) // self.limit


class ISearchService(ABC, Generic[T]):
    """Generic interface for search services.
    
    This interface can be implemented by any vertical slice to provide
    search functionality for their domain entities.
    """
    
    @abstractmethod
    async def search(
        self,
        query: str,
        filters: Optional[BaseSearchFilters] = None
    ) -> SearchResults[T]:
        """Perform a search with the given query and filters.
        
        Args:
            query: The search query string
            filters: Optional filters to apply
            
        Returns:
            SearchResults containing the matching items
        """
        pass
    
    @abstractmethod
    async def suggest(
        self,
        prefix: str,
        max_suggestions: int = 10
    ) -> List[str]:
        """Get autocomplete suggestions for a given prefix.
        
        Args:
            prefix: The prefix to search for
            max_suggestions: Maximum number of suggestions to return
            
        Returns:
            List of suggested completions
        """
        pass
    
    @abstractmethod
    async def get_facets(
        self,
        query: Optional[str] = None,
        facet_fields: Optional[List[str]] = None
    ) -> Dict[str, Dict[str, int]]:
        """Get faceted search results.
        
        Args:
            query: Optional query to filter facets
            facet_fields: Fields to calculate facets for
            
        Returns:
            Dictionary mapping facet fields to their value counts
        """
        pass


class IFullTextSearchEngine(ABC):
    """Interface for full-text search engine implementations.
    
    This interface abstracts the underlying search engine (e.g., PostgreSQL FTS,
    Elasticsearch, etc.) to allow for different implementations.
    """
    
    @abstractmethod
    async def index_document(
        self,
        doc_id: str,
        content: Dict[str, Any],
        doc_type: str
    ) -> None:
        """Index a document for searching.
        
        Args:
            doc_id: Unique identifier for the document
            content: Document content to index
            doc_type: Type/category of the document
        """
        pass
    
    @abstractmethod
    async def remove_document(self, doc_id: str, doc_type: str) -> None:
        """Remove a document from the search index.
        
        Args:
            doc_id: Unique identifier for the document
            doc_type: Type/category of the document
        """
        pass
    
    @abstractmethod
    async def search_documents(
        self,
        query: str,
        doc_type: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
        fields: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Search for documents matching the query.
        
        Args:
            query: Search query
            doc_type: Optional document type filter
            limit: Maximum results to return
            offset: Number of results to skip
            fields: Optional list of fields to search in
            
        Returns:
            List of matching documents with relevance scores
        """
        pass
    
    @abstractmethod
    async def bulk_index(
        self,
        documents: List[Dict[str, Any]],
        doc_type: str
    ) -> int:
        """Bulk index multiple documents.
        
        Args:
            documents: List of documents to index (must include 'id' field)
            doc_type: Type/category of the documents
            
        Returns:
            Number of documents successfully indexed
        """
        pass