# Search Engine Implementation Plan for DSA Platform

## Overview
This document outlines the implementation plan for integrating the advanced search engine blueprint into the existing vertical slice architecture.

## Phase 1: Core Infrastructure Setup

### 1.1 Database Schema Extension
```sql
-- Add search schema to existing database
CREATE SCHEMA IF NOT EXISTS dsa_search;

-- Enable required extensions (to be added to migrations)
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS btree_gin;
```

### 1.2 Create Search Feature Slice
```
/src/features/search/
├── __init__.py
├── handlers/
│   ├── __init__.py
│   ├── search_datasets_handler.py
│   ├── suggest_handler.py
│   └── refresh_search_index_handler.py
├── models/
│   ├── __init__.py
│   ├── search_request.py
│   ├── search_response.py
│   └── search_filters.py
└── services/
    ├── __init__.py
    └── search_index_service.py
```

## Phase 2: Repository Pattern Integration

### 2.1 Extend Core Abstractions
```python
# /src/core/abstractions/search_repository.py
from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
from datetime import datetime

class ISearchRepository(ABC):
    @abstractmethod
    async def search(
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
    ) -> Dict[str, Any]:
        """Execute a search query with filters and return results with metadata."""
        pass

    @abstractmethod
    async def suggest(
        self,
        user_id: int,
        query: str,
        limit: int = 10
    ) -> Dict[str, Any]:
        """Get autocomplete suggestions for a partial query."""
        pass

    @abstractmethod
    async def refresh_search_index(self) -> bool:
        """Refresh the materialized view for search."""
        pass
```

### 2.2 PostgreSQL Implementation
```python
# /src/core/infrastructure/postgres/search_repository.py
from typing import List, Optional, Dict, Any
from datetime import datetime
import asyncpg
from src.core.abstractions.search_repository import ISearchRepository

class PostgresSearchRepository(ISearchRepository):
    def __init__(self, connection: asyncpg.Connection):
        self._connection = connection

    async def search(self, user_id: int, **kwargs) -> Dict[str, Any]:
        # Call the PostgreSQL function from blueprint
        result = await self._connection.fetchval(
            "SELECT dsa_search.search($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)",
            user_id,
            kwargs.get('query'),
            kwargs.get('fuzzy', True),
            kwargs.get('tags'),
            kwargs.get('created_by'),
            kwargs.get('created_after'),
            kwargs.get('created_before'),
            kwargs.get('updated_after'),
            kwargs.get('updated_before'),
            kwargs.get('limit', 20),
            kwargs.get('offset', 0),
            kwargs.get('sort_by', 'relevance'),
            kwargs.get('sort_order', 'desc'),
            kwargs.get('include_facets', True),
            kwargs.get('facet_fields', ['tags', 'created_by'])
        )
        return result

    async def suggest(self, user_id: int, query: str, limit: int = 10) -> Dict[str, Any]:
        result = await self._connection.fetchval(
            "SELECT dsa_search.suggest($1, $2, $3)",
            user_id, query, limit
        )
        return result

    async def refresh_search_index(self) -> bool:
        await self._connection.execute(
            "REFRESH MATERIALIZED VIEW CONCURRENTLY dsa_search.datasets_summary"
        )
        return True
```

## Phase 3: Handler Implementation

### 3.1 Search Handler
```python
# /src/features/search/handlers/search_datasets_handler.py
from typing import Optional, List, Dict, Any
from datetime import datetime
from src.features.base_handler import BaseHandler
from src.core.abstractions.unit_of_work import IUnitOfWork
from src.features.search.models.search_request import SearchRequest
from src.features.search.models.search_response import SearchResponse

class SearchDatasetsHandler(BaseHandler[SearchRequest, SearchResponse]):
    def __init__(self, unit_of_work: IUnitOfWork):
        super().__init__(unit_of_work)

    async def handle(self, request: SearchRequest) -> SearchResponse:
        async with self._unit_of_work as uow:
            # Get current user ID from context
            user_id = request.context.user_id
            
            # Execute search through repository
            result = await uow.search_repository.search(
                user_id=user_id,
                query=request.query,
                fuzzy=request.fuzzy,
                tags=request.tags,
                created_by=request.created_by,
                created_after=request.created_after,
                created_before=request.created_before,
                updated_after=request.updated_after,
                updated_before=request.updated_before,
                limit=request.limit,
                offset=request.offset,
                sort_by=request.sort_by,
                sort_order=request.sort_order,
                include_facets=request.include_facets,
                facet_fields=request.facet_fields
            )
            
            return SearchResponse(**result)
```

### 3.2 Suggest Handler
```python
# /src/features/search/handlers/suggest_handler.py
from src.features.base_handler import BaseHandler
from src.core.abstractions.unit_of_work import IUnitOfWork
from src.features.search.models.search_request import SuggestRequest
from src.features.search.models.search_response import SuggestResponse

class SuggestHandler(BaseHandler[SuggestRequest, SuggestResponse]):
    def __init__(self, unit_of_work: IUnitOfWork):
        super().__init__(unit_of_work)

    async def handle(self, request: SuggestRequest) -> SuggestResponse:
        async with self._unit_of_work as uow:
            user_id = request.context.user_id
            
            result = await uow.search_repository.suggest(
                user_id=user_id,
                query=request.query,
                limit=request.limit
            )
            
            return SuggestResponse(**result)
```

## Phase 4: API Integration

### 4.1 Search Router
```python
# /src/api/search.py
from fastapi import APIRouter, Depends, Query
from typing import Optional, List
from datetime import datetime
from src.api.dependencies import get_current_user, get_unit_of_work
from src.features.search.handlers import SearchDatasetsHandler, SuggestHandler
from src.features.search.models import SearchRequest, SuggestRequest, SearchResponse, SuggestResponse

router = APIRouter(prefix="/datasets/search", tags=["search"])

@router.get("/", response_model=SearchResponse)
async def search_datasets(
    query: Optional[str] = Query(None),
    fuzzy: bool = Query(True),
    tags: Optional[List[str]] = Query(None),
    created_by: Optional[List[int]] = Query(None),
    created_after: Optional[datetime] = Query(None),
    created_before: Optional[datetime] = Query(None),
    updated_after: Optional[datetime] = Query(None),
    updated_before: Optional[datetime] = Query(None),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    sort_by: str = Query('relevance'),
    sort_order: str = Query('desc'),
    include_facets: bool = Query(True),
    facet_fields: Optional[List[str]] = Query(['tags', 'created_by']),
    current_user = Depends(get_current_user),
    uow = Depends(get_unit_of_work)
):
    handler = SearchDatasetsHandler(uow)
    request = SearchRequest(
        context={'user_id': current_user.id},
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
    return await handler.handle(request)

@router.get("/suggest", response_model=SuggestResponse)
async def suggest(
    query: str = Query(..., min_length=1),
    limit: int = Query(10, ge=1, le=50),
    current_user = Depends(get_current_user),
    uow = Depends(get_unit_of_work)
):
    handler = SuggestHandler(uow)
    request = SuggestRequest(
        context={'user_id': current_user.id},
        query=query,
        limit=limit
    )
    return await handler.handle(request)
```

## Phase 5: Models & DTOs

### 5.1 Request Models
```python
# /src/features/search/models/search_request.py
from pydantic import BaseModel, Field
from typing import Optional, List, Literal
from datetime import datetime

class SearchRequest(BaseModel):
    context: dict  # Contains user_id
    query: Optional[str] = None
    fuzzy: bool = True
    tags: Optional[List[str]] = None
    created_by: Optional[List[int]] = None
    created_after: Optional[datetime] = None
    created_before: Optional[datetime] = None
    updated_after: Optional[datetime] = None
    updated_before: Optional[datetime] = None
    limit: int = Field(default=20, ge=1, le=100)
    offset: int = Field(default=0, ge=0)
    sort_by: Literal['relevance', 'name', 'created_at', 'updated_at'] = 'relevance'
    sort_order: Literal['asc', 'desc'] = 'desc'
    include_facets: bool = True
    facet_fields: Optional[List[Literal['tags', 'created_by']]] = ['tags', 'created_by']

class SuggestRequest(BaseModel):
    context: dict  # Contains user_id
    query: str = Field(..., min_length=1)
    limit: int = Field(default=10, ge=1, le=50)
```

### 5.2 Response Models
```python
# /src/features/search/models/search_response.py
from pydantic import BaseModel
from typing import List, Optional, Dict, Literal
from datetime import datetime

class SearchResult(BaseModel):
    id: int
    name: str
    description: Optional[str]
    created_by: int
    created_by_name: Optional[str]
    created_at: datetime
    updated_at: datetime
    tags: List[str]
    score: Optional[float]
    user_permission: Literal['read', 'write', 'admin']

class SearchFacets(BaseModel):
    tags: Optional[Dict[str, int]] = None
    created_by: Optional[Dict[str, int]] = None

class SearchResponse(BaseModel):
    results: List[SearchResult]
    total: int
    limit: int
    offset: int
    has_more: bool
    query: Optional[str]
    execution_time_ms: int
    facets: Optional[SearchFacets]

class Suggestion(BaseModel):
    text: str
    type: Literal['dataset_name', 'tag']
    score: float

class SuggestResponse(BaseModel):
    suggestions: List[Suggestion]
    query: str
    execution_time_ms: int
```

## Phase 6: Integration Steps

### 6.1 Update Unit of Work
```python
# Add to /src/core/abstractions/unit_of_work.py
from src.core.abstractions.search_repository import ISearchRepository

class IUnitOfWork(ABC):
    # ... existing properties ...
    search_repository: ISearchRepository  # Add this
```

### 6.2 Update PostgreSQL Unit of Work
```python
# Update /src/core/infrastructure/postgres/unit_of_work.py
from src.core.infrastructure.postgres.search_repository import PostgresSearchRepository

class PostgresUnitOfWork(IUnitOfWork):
    def __init__(self, connection: asyncpg.Connection):
        # ... existing initialization ...
        self._search_repository = PostgresSearchRepository(connection)
    
    @property
    def search_repository(self) -> ISearchRepository:
        return self._search_repository
```

### 6.3 Add Search Router to Main App
```python
# Update /src/main.py
from src.api import search

app.include_router(search.router)
```

## Phase 7: Database Migrations

### 7.1 Create Migration for Search Schema
```sql
-- migrations/XXX_add_search_functionality.sql

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS btree_gin;

-- Create search schema
CREATE SCHEMA IF NOT EXISTS dsa_search;

-- Create materialized view (from blueprint)
CREATE MATERIALIZED VIEW dsa_search.datasets_summary AS
WITH dataset_tags_agg AS (
    SELECT dt.dataset_id, array_agg(t.tag_name ORDER BY t.tag_name) AS tags
    FROM dsa_core.dataset_tags dt 
    JOIN dsa_core.tags t ON dt.tag_id = t.id
    GROUP BY dt.dataset_id
)
SELECT
    d.id AS dataset_id, 
    d.name, 
    d.description, 
    d.created_by AS created_by_id,
    u.soeid AS created_by_name, 
    d.created_at, 
    d.updated_at,
    COALESCE(dta.tags, '{}'::text[]) AS tags,
    d.name || ' ' || COALESCE(d.description, '') || ' ' || 
    array_to_string(COALESCE(dta.tags, '{}'), ' ') AS search_text,
    to_tsvector('english', d.name) ||
    to_tsvector('english', COALESCE(d.description, '')) ||
    to_tsvector('english', array_to_string(COALESCE(dta.tags, '{}'), ' ')) AS search_tsv
FROM dsa_core.datasets d
LEFT JOIN dsa_auth.users u ON d.created_by = u.id
LEFT JOIN dataset_tags_agg dta ON d.id = dta.dataset_id;

-- Create indexes (from blueprint)
CREATE UNIQUE INDEX idx_datasets_summary_id ON dsa_search.datasets_summary(dataset_id);
CREATE INDEX idx_datasets_summary_search_text_trgm ON dsa_search.datasets_summary USING gin (search_text gin_trgm_ops);
CREATE INDEX idx_datasets_summary_search_tsv ON dsa_search.datasets_summary USING gin (search_tsv);
CREATE INDEX idx_datasets_summary_tags ON dsa_search.datasets_summary USING gin (tags);
CREATE INDEX idx_datasets_summary_name ON dsa_search.datasets_summary(name);
CREATE INDEX idx_datasets_summary_created_at ON dsa_search.datasets_summary(created_at DESC);
CREATE INDEX idx_datasets_summary_updated_at ON dsa_search.datasets_summary(updated_at DESC);
CREATE INDEX idx_datasets_summary_created_by_id ON dsa_search.datasets_summary(created_by_id);
CREATE INDEX idx_datasets_summary_created_by_name ON dsa_search.datasets_summary(created_by_name);

-- Create search functions (from blueprint)
-- [Include the full search and suggest functions from the blueprint]
```

## Phase 8: Background Job for Index Refresh

### 8.1 Create Refresh Handler
```python
# /src/features/search/handlers/refresh_search_index_handler.py
from src.features.base_handler import BaseHandler
from src.core.abstractions.unit_of_work import IUnitOfWork

class RefreshSearchIndexHandler(BaseHandler[dict, dict]):
    async def handle(self, request: dict) -> dict:
        async with self._unit_of_work as uow:
            success = await uow.search_repository.refresh_search_index()
            await uow.commit()
            return {"success": success, "message": "Search index refreshed"}
```

### 8.2 Create Background Job
```python
# /src/features/jobs/search_index_refresh_job.py
from src.core.abstractions.job import IJob
from src.features.search.handlers import RefreshSearchIndexHandler

class SearchIndexRefreshJob(IJob):
    def __init__(self, handler: RefreshSearchIndexHandler):
        self._handler = handler
    
    async def execute(self):
        await self._handler.handle({})
```

## Phase 9: Testing Strategy

### 9.1 Unit Tests
- Test search repository methods
- Test handlers with mocked repositories
- Test request/response model validation

### 9.2 Integration Tests
- Test full search flow with test data
- Test permission filtering
- Test facet generation
- Test autocomplete suggestions

### 9.3 Performance Tests
- Benchmark search response times
- Test with large datasets
- Validate index effectiveness

## Implementation Timeline

1. **Week 1**: Core infrastructure setup, database migrations
2. **Week 2**: Repository and handler implementation
3. **Week 3**: API integration and testing
4. **Week 4**: Performance optimization and deployment

## Key Benefits of This Approach

1. **Maintains Architectural Integrity**: Follows existing vertical slice patterns
2. **Reusable Components**: Search repository can be used across features
3. **DRY Principle**: Shared models and interfaces prevent duplication
4. **Scalable**: Easy to add search to other entities (commits, jobs, etc.)
5. **Testable**: Clear separation allows for comprehensive testing
6. **Performant**: Leverages PostgreSQL's native capabilities