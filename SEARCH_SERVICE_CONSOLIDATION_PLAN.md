# Search Service Consolidation Plan

## Current State Analysis

Currently, the search functionality is split across 3 handlers and 1 event handler:
1. `SearchDatasetsHandler` - Executes dataset searches with filters, sorting, and faceting
2. `SuggestHandler` - Provides autocomplete suggestions for dataset names and tags
3. `RefreshSearchIndexHandler` - Manually refreshes the search materialized view
4. `SearchIndexEventHandler` - Event-driven handler that updates search indexes on dataset changes

### Key Observations:
- Search uses a materialized view (`search_datasets`) for performance
- Complex search capabilities: full-text, fuzzy matching, filters, facets
- Event-driven index updates via domain events
- Context-based user filtering (security)
- Relatively simple handlers with most logic in the repository
- Event handler is separate and handles background index updates

## Proposed Solution: Consolidated SearchService

### Benefits:
1. **Unified search interface** - Single service for all search operations
2. **Better separation of concerns** - Clear distinction between query and index management
3. **Easier testing** - Mock one service instead of multiple handlers
4. **Consistent patterns** - Matches other consolidated services
5. **Event handler integration** - Can coordinate with event handling

### Proposed Structure:

```python
# src/features/search/services/search_service.py

from typing import Dict, Any, List, Optional
from datetime import datetime
from dataclasses import dataclass

from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.core.domain_exceptions import ValidationException
from ...base_handler import with_error_handling
from ..models import (
    SearchRequest,
    SuggestRequest,
    SearchResponse,
    SuggestResponse
)


class SearchService:
    """Consolidated service for all search operations."""
    
    def __init__(
        self,
        uow: PostgresUnitOfWork
    ):
        self._uow = uow
    
    # ========== Query Methods ==========
    
    @with_error_handling
    async def search_datasets(
        self,
        user_id: int,
        query: Optional[str] = None,
        fuzzy: bool = True,
        tags: Optional[List[str]] = None,
        created_by: Optional[List[int]] = None,
        created_after: Optional[datetime] = None,
        created_before: Optional[datetime] = None,
        updated_after: Optional[datetime] = None,
        updated_before: Optional[datetime] = None,
        limit: int = 20,
        offset: int = 0,
        sort_by: str = 'relevance',
        sort_order: str = 'desc',
        include_facets: bool = True,
        facet_fields: Optional[List[str]] = None
    ) -> SearchResponse:
        """
        Execute a dataset search with advanced filtering and faceting.
        
        Supports:
        - Full-text search with fuzzy matching
        - Filtering by tags, creator, and date ranges
        - Sorting by relevance, name, or timestamps
        - Faceted search for discovering filter options
        """
        # Validate sort parameters
        valid_sort_by = ['relevance', 'name', 'created_at', 'updated_at']
        if sort_by not in valid_sort_by:
            raise ValidationException(f"Invalid sort_by. Must be one of: {', '.join(valid_sort_by)}")
        
        if sort_order not in ['asc', 'desc']:
            raise ValidationException("Invalid sort_order. Must be 'asc' or 'desc'")
        
        # Default facet fields
        if facet_fields is None:
            facet_fields = ['tags', 'created_by']
        
        # Validate facet fields
        valid_facet_fields = ['tags', 'created_by']
        for field in facet_fields:
            if field not in valid_facet_fields:
                raise ValidationException(f"Invalid facet field: {field}")
        
        async with self._uow as uow:
            # Execute search through repository
            result = await uow.search_repository.search(
                user_id=user_id,
                query=query,
                fuzzy=fuzzy,
                tags=tags,
                created_by=created_by,
                created_after=created_after,
                created_before=created_before,
                updated_after=updated_after,
                updated_before=updated_before,
                limit=limit,
                offset=offset,
                sort_by=sort_by,
                sort_order=sort_order,
                include_facets=include_facets,
                facet_fields=facet_fields
            )
            
            return SearchResponse(**result)
    
    @with_error_handling
    async def suggest(
        self,
        user_id: int,
        query: str,
        limit: int = 10
    ) -> SuggestResponse:
        """
        Get autocomplete suggestions for dataset names and tags.
        
        Returns suggestions based on:
        - Dataset names that match the partial query
        - Tag names that match the partial query
        """
        if not query or len(query) < 1:
            raise ValidationException("Query must be at least 1 character")
        
        if limit < 1 or limit > 50:
            raise ValidationException("Limit must be between 1 and 50")
        
        async with self._uow as uow:
            # Get suggestions through repository
            result = await uow.search_repository.suggest(
                user_id=user_id,
                query=query,
                limit=limit
            )
            
            return SuggestResponse(**result)
    
    # ========== Index Management Methods ==========
    
    @with_error_handling
    async def refresh_search_index(self) -> Dict[str, Any]:
        """
        Manually refresh the search materialized view.
        
        This is typically called:
        - After bulk imports
        - During maintenance windows
        - When immediate index updates are needed
        """
        async with self._uow as uow:
            success = await uow.search_repository.refresh_search_index()
            
            return {
                "success": success,
                "message": "Search index refreshed successfully" if success else "Failed to refresh search index"
            }
    
    # ========== Event Handler Methods ==========
    
    async def handle_dataset_created(self, dataset_id: int) -> None:
        """Handle dataset creation by updating search index."""
        # Note: In production, consider batching these updates
        await self.refresh_search_index()
    
    async def handle_dataset_updated(self, dataset_id: int, changes: Dict[str, Any]) -> None:
        """Handle dataset update by refreshing search index if searchable fields changed."""
        # Only refresh if searchable fields changed
        searchable_fields = {'name', 'description', 'tags'}
        if any(field in changes for field in searchable_fields):
            await self.refresh_search_index()
    
    async def handle_dataset_deleted(self, dataset_id: int) -> None:
        """Handle dataset deletion by updating search index."""
        await self.refresh_search_index()
```

## Implementation Steps

### 1. Create Service Structure
```bash
mkdir -p src/features/search/services
touch src/features/search/services/__init__.py
touch src/features/search/services/search_service.py
```

### 2. Migrate Handler Logic
- Copy search logic from handlers into service methods
- Consolidate validation logic
- Keep repository pattern for actual search execution

### 3. Update API Endpoints
Transform endpoints to use the service:
```python
# From:
handler = SearchDatasetsHandler(uow)
return await handler.handle(request)

# To:
service = SearchService(uow)
return await service.search_datasets(
    user_id=current_user.user_id,
    query=query,
    # ... other params
)
```

### 4. Event Handler Integration
The `SearchIndexEventHandler` can remain separate but use the service:
```python
class SearchIndexEventHandler:
    def __init__(self, db_pool: DatabasePool):
        self._db_pool = db_pool
        self._search_service = None  # Created on demand
    
    async def _get_service(self):
        if not self._search_service:
            uow = PostgresUnitOfWork(self._db_pool)
            self._search_service = SearchService(uow)
        return self._search_service
```

### 5. Clean Up
- Remove old handler files (except event handler)
- Update imports throughout codebase
- Update module exports

## Special Considerations

### 1. Materialized View Management
- Search uses PostgreSQL materialized view for performance
- Refresh operations are DDL and auto-commit
- Consider refresh frequency vs performance trade-offs

### 2. Context vs Parameters
- Current handlers use context dict for user_id
- Service methods should accept user_id as explicit parameter
- Cleaner interface and easier testing

### 3. Event Handler Separation
- Keep `SearchIndexEventHandler` separate for event system
- It can use the SearchService for index operations
- Maintains clean separation between events and core logic

### 4. Search Query Parsing
- Special syntax support: 'tag:value', 'user:value'
- Consider extracting query parser in future
- Keep in repository for now

## Migration Checklist

- [ ] Create services directory structure
- [ ] Create SearchService class
- [ ] Migrate search_datasets logic
- [ ] Migrate suggest logic
- [ ] Migrate refresh_search_index logic
- [ ] Add event handler helper methods
- [ ] Update API endpoints in `src/api/search.py`
- [ ] Update SearchIndexEventHandler to use service
- [ ] Remove old handler files (keep event handler)
- [ ] Update handler exports
- [ ] Test search functionality
- [ ] Test autocomplete suggestions
- [ ] Verify event-driven index updates

## Common Pitfalls to Avoid

1. **User Context**: Extract user_id from context in API, pass explicitly to service
2. **Validation**: Move all validation into service methods
3. **Auto-commit DDL**: Remember REFRESH MATERIALIZED VIEW auto-commits
4. **Event Handler**: Keep it separate but can use service methods

## Expected Outcome

After consolidation:
- Single `SearchService` class with clear method organization
- Separation of query operations from index management
- Event handler remains separate but uses service
- Better testability and maintainability
- Consistent with other service patterns

## Testing Commands

After implementation:
```bash
# Check Python syntax
python3 -m py_compile src/api/search.py src/features/search/services/search_service.py

# Run server
python3 -m uvicorn src.main:app --reload

# Test search endpoint
curl "http://localhost:8000/datasets/search/?query=test&fuzzy=true"

# Test suggest endpoint
curl "http://localhost:8000/datasets/search/suggest?query=dat"
```

## Future Enhancements

1. **Query Parser**: Extract special syntax parsing into separate component
2. **Batch Updates**: Batch index updates for better performance
3. **Search Analytics**: Track popular searches and click-through rates
4. **Advanced Features**: Synonyms, stemming, language detection
5. **Caching**: Cache frequent searches and suggestions
6. **Elasticsearch Integration**: For more advanced search capabilities