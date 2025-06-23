# Unified Dataset Search API Integration Documentation

**Version**: 1.0  
**Date**: January 2025  
**Status**: Technical Feasibility Assessment & Integration Guide

## Executive Summary

The Unified Dataset Search API is **highly feasible** for integration into the DSA platform. The existing FastAPI architecture, PostgreSQL database, and established patterns for datasets make this a straightforward implementation that can be completed in phases.

### Key Findings:
- ✅ **Technical Compatibility**: 100% compatible with current stack
- ✅ **Infrastructure Ready**: PostgreSQL supports required features
- ✅ **Pattern Alignment**: Fits existing API and repository patterns
- ⏱️ **Estimated Timeline**: 2-3 weeks for full implementation
- 🎯 **Risk Level**: Low to Medium

## 1. Technical Compatibility Assessment

### 1.1 Current Stack Analysis
Your platform uses:
- **Backend**: FastAPI with Python 3.9+
- **Database**: PostgreSQL with SQLAlchemy ORM
- **Architecture**: Service/Repository pattern with dependency injection
- **Authentication**: JWT-based with user permissions
- **API Pattern**: RESTful with Pydantic models

### 1.2 Search API Requirements vs Current Capabilities

| Requirement | Current Status | Action Needed |
|------------|----------------|---------------|
| PostgreSQL pg_trgm extension | Not installed | Simple database migration |
| Full-text search | Basic support via LIKE | Upgrade to GIN indexes |
| Fuzzy matching | Not available | Enable via pg_trgm |
| Faceted search | Not implemented | Add aggregation queries |
| Permission filtering | Basic structure exists | Extend for search context |
| API pagination | Already implemented | Reuse existing patterns |

## 2. Infrastructure Changes Required

### 2.1 Database Extensions
```sql
-- Required PostgreSQL extensions
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS btree_gin;  -- Optional but recommended

-- Performance configuration
ALTER SYSTEM SET shared_preload_libraries = 'pg_trgm';
-- Requires PostgreSQL restart
```

### 2.2 Database Schema Updates
```sql
-- Add search-optimized indexes
CREATE INDEX idx_datasets_name_trgm ON datasets USING gin (name gin_trgm_ops);
CREATE INDEX idx_datasets_description_trgm ON datasets USING gin (description gin_trgm_ops);
CREATE INDEX idx_datasets_tags ON datasets USING gin (tags);

-- Add search history table (optional)
CREATE TABLE dataset_search_history (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    search_query TEXT,
    results_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## 3. Step-by-Step Integration Guide

### Phase 1: Database Preparation (2-3 days)

#### Step 1.1: Create Alembic Migration
```python
# alembic/versions/xxx_add_search_capabilities.py
def upgrade():
    # Enable extensions
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    
    # Add search indexes
    op.create_index(
        'idx_datasets_name_trgm',
        'datasets',
        ['name'],
        postgresql_using='gin',
        postgresql_ops={'name': 'gin_trgm_ops'}
    )
    
    # Add search configuration
    op.execute("ALTER DATABASE dsa SET pg_trgm.similarity_threshold = 0.3")
```

#### Step 1.2: Update Dataset Model
```python
# src/app/datasets/models.py
class Dataset(Base):
    # Existing fields...
    
    # Add search-related computed fields
    search_vector = Column(TSVector)  # For full-text search
    
    @hybrid_property
    def search_text(self):
        return f"{self.name} {self.description} {' '.join(self.tags or [])}"
```

### Phase 2: Search API Implementation (5-7 days)

#### Step 2.1: Create Search Models
```python
# src/app/datasets/search/models.py
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime

class DatasetSearchRequest(BaseModel):
    q: Optional[str] = ""
    tags: Optional[List[str]] = None
    owner_ids: Optional[List[int]] = None
    limit: int = 25
    offset: int = 0
    sort_by: str = "relevance"
    sort_order: str = "desc"

class DatasetSearchResponse(BaseModel):
    query: DatasetSearchRequest
    pagination: PaginationInfo
    data: List[DatasetInfo]
    facets: SearchFacets
```

#### Step 2.2: Implement Search Repository
```python
# src/app/datasets/search/repository.py
class DatasetSearchRepository:
    def __init__(self, db: AsyncSession):
        self.db = db
    
    async def search(self, params: DatasetSearchRequest) -> DatasetSearchResponse:
        # Build base query with permission checks
        query = select(Dataset).join(DatasetPermission).filter(
            DatasetPermission.user_id == current_user.id,
            DatasetPermission.permission_level >= PermissionLevel.READ
        )
        
        # Apply fuzzy search
        if params.q:
            similarity_threshold = 0.3
            query = query.filter(
                or_(
                    func.similarity(Dataset.name, params.q) > similarity_threshold,
                    func.similarity(Dataset.description, params.q) > similarity_threshold
                )
            )
            
        # Calculate relevance score
        relevance = (
            0.7 * func.similarity(Dataset.name, params.q) +
            0.3 * func.similarity(Dataset.description, params.q)
        ).label('relevance')
        
        # Add to query
        query = query.add_column(relevance).order_by(relevance.desc())
        
        # Apply filters and pagination...
        return results
```

#### Step 2.3: Create Search Service
```python
# src/app/datasets/search/service.py
class DatasetSearchService:
    def __init__(self, repository: DatasetSearchRepository):
        self.repository = repository
    
    async def search_datasets(self, params: DatasetSearchRequest) -> DatasetSearchResponse:
        # Validate parameters
        if params.limit > 100:
            params.limit = 100
            
        # Perform search
        results = await self.repository.search(params)
        
        # Log search for analytics
        await self._log_search(params, results.pagination.total_hits)
        
        return results
    
    async def suggest_datasets(self, prefix: str, limit: int = 10) -> List[str]:
        return await self.repository.get_name_suggestions(prefix, limit)
```

#### Step 2.4: Implement API Endpoints
```python
# src/app/datasets/search/routes.py
router = APIRouter(prefix="/api/v1/search", tags=["search"])

@router.get("/datasets")
async def search_datasets(
    q: str = Query("", description="Search query"),
    tags: Optional[str] = Query(None, description="Comma-separated tags"),
    owner_ids: Optional[str] = Query(None, description="Comma-separated owner IDs"),
    limit: int = Query(25, le=100),
    offset: int = Query(0, ge=0),
    service: DatasetSearchService = Depends(get_search_service),
    current_user: User = Depends(get_current_user)
):
    params = DatasetSearchRequest(
        q=q,
        tags=tags.split(",") if tags else None,
        owner_ids=[int(id) for id in owner_ids.split(",")] if owner_ids else None,
        limit=limit,
        offset=offset
    )
    return await service.search_datasets(params)

@router.get("/suggest")
async def suggest_datasets(
    prefix: str = Query(..., min_length=1),
    limit: int = Query(10, le=50),
    service: DatasetSearchService = Depends(get_search_service)
):
    return await service.suggest_datasets(prefix, limit)
```

### Phase 3: Performance Optimization (2-3 days)

#### Step 3.1: Add Caching
```python
# src/app/core/cache.py
from functools import lru_cache
import hashlib

class SearchCache:
    def __init__(self, redis_client):
        self.redis = redis_client
        self.ttl = 300  # 5 minutes
    
    async def get_or_set(self, key: str, func, *args, **kwargs):
        cached = await self.redis.get(key)
        if cached:
            return json.loads(cached)
        
        result = await func(*args, **kwargs)
        await self.redis.setex(key, self.ttl, json.dumps(result))
        return result
```

#### Step 3.2: Implement Materialized Views for Facets
```sql
CREATE MATERIALIZED VIEW dataset_facets AS
SELECT 
    'tags' as facet_type,
    tag_name as facet_value,
    COUNT(DISTINCT dataset_id) as count
FROM dataset_tags dt
JOIN tags t ON dt.tag_id = t.id
GROUP BY tag_name

UNION ALL

SELECT 
    'owners' as facet_type,
    u.soeid as facet_value,
    COUNT(DISTINCT d.id) as count
FROM datasets d
JOIN users u ON d.created_by = u.id
GROUP BY u.soeid;

-- Refresh periodically
CREATE INDEX idx_dataset_facets ON dataset_facets(facet_type, count DESC);
```

### Phase 4: Testing & Deployment (3-4 days)

#### Step 4.1: Unit Tests
```python
# tests/test_search.py
async def test_fuzzy_search():
    # Test typo tolerance
    response = await client.get("/api/v1/search/datasets?q=finacial")
    assert response.status_code == 200
    assert any("financial" in d["name"].lower() for d in response.json()["data"])

async def test_permission_filtering():
    # Test that users only see permitted datasets
    response = await client.get(
        "/api/v1/search/datasets",
        headers={"Authorization": f"Bearer {user_token}"}
    )
    dataset_ids = [d["id"] for d in response.json()["data"]]
    
    # Verify all returned datasets have read permission
    for dataset_id in dataset_ids:
        assert await has_permission(user_id, dataset_id, "read")
```

#### Step 4.2: Load Testing
```python
# tests/load/test_search_performance.py
import asyncio
import aiohttp
import time

async def load_test_search(concurrent_users=100):
    queries = ["report", "financial", "quarterly", "data", "analysis"]
    
    async def search_request(session, query):
        start = time.time()
        async with session.get(f"{BASE_URL}/api/v1/search/datasets?q={query}") as resp:
            await resp.json()
        return time.time() - start
    
    # Run concurrent searches
    async with aiohttp.ClientSession() as session:
        tasks = []
        for _ in range(concurrent_users):
            query = random.choice(queries)
            tasks.append(search_request(session, query))
        
        response_times = await asyncio.gather(*tasks)
        
    # Assert performance requirements
    assert statistics.mean(response_times) < 0.5  # Average < 500ms
    assert statistics.quantiles(response_times, n=100)[94] < 1.0  # p95 < 1s
```

## 4. Performance Considerations

### 4.1 Query Performance Targets
- Search endpoint: < 200ms average, < 500ms p95
- Autocomplete: < 50ms average, < 100ms p95
- Concurrent users: Support 100+ concurrent searches

### 4.2 Optimization Strategies
1. **Database Level**
   - GIN indexes for text search
   - Partial indexes for common filters
   - Connection pooling (min=10, max=50)
   - Read replicas for search queries

2. **Application Level**
   - Redis caching for frequent queries
   - Debouncing on frontend (300ms)
   - Pagination limits (max 100)
   - Query complexity limits

3. **Infrastructure Level**
   - Dedicated search database replica
   - CDN for static facet data
   - Rate limiting (100 req/min per user)

## 5. Security Implications

### 5.1 Permission Model Integration
```python
# Current permission check
async def check_dataset_permission(user_id: int, dataset_id: int, level: str):
    permission = await db.query(DatasetPermission).filter(
        DatasetPermission.user_id == user_id,
        DatasetPermission.dataset_id == dataset_id,
        DatasetPermission.level >= level
    ).first()
    return permission is not None

# Search-integrated permission check (bulk)
async def filter_by_permissions(user_id: int, dataset_ids: List[int]):
    permitted = await db.query(DatasetPermission.dataset_id).filter(
        DatasetPermission.user_id == user_id,
        DatasetPermission.dataset_id.in_(dataset_ids),
        DatasetPermission.level >= 'read'
    ).all()
    return [p.dataset_id for p in permitted]
```

### 5.2 Security Considerations
- **SQL Injection**: Use parameterized queries only
- **Search Bombing**: Implement rate limiting
- **Data Leakage**: Never expose dataset names in autocomplete without permission check
- **Query Logging**: Hash sensitive search terms before logging

## 6. Timeline Estimation

### Development Timeline (2-3 weeks)
| Phase | Duration | Dependencies |
|-------|----------|--------------|
| Database Setup | 2-3 days | DBA approval for extensions |
| Core Search API | 5-7 days | Database setup complete |
| Performance Optimization | 2-3 days | Core API complete |
| Testing & Documentation | 3-4 days | All features complete |
| Deployment & Monitoring | 1-2 days | Testing complete |

### Resource Requirements
- **Backend Developer**: 1 FTE for 3 weeks
- **Database Administrator**: 0.2 FTE for setup and optimization
- **QA Engineer**: 0.5 FTE for last week
- **DevOps**: 0.2 FTE for deployment

## 7. Risk Assessment & Mitigation

### 7.1 Technical Risks

| Risk | Probability | Impact | Mitigation |
|------|------------|--------|------------|
| pg_trgm performance issues | Low | High | Test with production data volume; have Elasticsearch backup plan |
| Complex permission queries slow | Medium | Medium | Implement caching layer; consider permission materialized view |
| Search result inconsistency | Low | Medium | Implement search result versioning |
| Database migration failure | Low | High | Test in staging; have rollback plan |

### 7.2 Operational Risks

| Risk | Probability | Impact | Mitigation |
|------|------------|--------|------------|
| Search abuse/DOS | Medium | High | Implement rate limiting and monitoring |
| Index bloat | Medium | Low | Schedule regular VACUUM and REINDEX |
| Cache invalidation issues | Medium | Low | Implement cache warming and TTL strategy |

## 8. Alternative Approaches

### 8.1 Elasticsearch Integration (Future Enhancement)
```yaml
# docker-compose.yml addition
elasticsearch:
  image: elasticsearch:8.11.0
  environment:
    - discovery.type=single-node
    - "ES_JAVA_OPTS=-Xms512m -Xmx512m"
  volumes:
    - es_data:/usr/share/elasticsearch/data
```

**Pros**: Better search features, dedicated search infrastructure  
**Cons**: Additional complexity, operational overhead

### 8.2 PostgreSQL Full-Text Search Only
```sql
-- Use tsvector instead of trigrams
ALTER TABLE datasets ADD COLUMN search_vector tsvector;
CREATE INDEX idx_datasets_search ON datasets USING gin(search_vector);

-- Update trigger
CREATE TRIGGER update_search_vector
BEFORE INSERT OR UPDATE ON datasets
FOR EACH ROW EXECUTE FUNCTION
tsvector_update_trigger(search_vector, 'pg_catalog.english', name, description);
```

**Pros**: Simpler, uses native PostgreSQL features  
**Cons**: Less fuzzy matching capability, English-only

## 9. Frontend Integration Guidelines

### 9.1 Search Component Implementation
```typescript
// components/DatasetSearch.tsx
const DatasetSearch: React.FC = () => {
  const [query, setQuery] = useState('');
  const [suggestions, setSuggestions] = useState<string[]>([]);
  const debouncedQuery = useDebounce(query, 300);
  
  useEffect(() => {
    if (debouncedQuery.length > 0) {
      fetchSuggestions(debouncedQuery).then(setSuggestions);
    }
  }, [debouncedQuery]);
  
  return (
    <SearchInput
      value={query}
      onChange={setQuery}
      suggestions={suggestions}
      onSearch={() => navigate(`/search?q=${query}`)}
    />
  );
};
```

### 9.2 Search Results Page
```typescript
// pages/SearchResults.tsx
const SearchResults: React.FC = () => {
  const { data, loading, error } = useSearch({
    query: searchParams.get('q'),
    filters: {
      tags: searchParams.getAll('tags'),
      owners: searchParams.getAll('owners')
    },
    pagination: {
      offset: parseInt(searchParams.get('offset') || '0'),
      limit: parseInt(searchParams.get('limit') || '25')
    }
  });
  
  return (
    <div>
      <SearchFilters facets={data?.facets} />
      <SearchResultsList results={data?.data} />
      <Pagination total={data?.pagination.total_hits} />
    </div>
  );
};
```

## 10. Monitoring & Success Metrics

### 10.1 Key Performance Indicators
- **Search Usage**: Queries per day, unique users
- **Search Quality**: Click-through rate, refinement rate
- **Performance**: Response time p50/p95/p99
- **Errors**: Error rate, timeout rate

### 10.2 Monitoring Setup
```python
# src/app/core/monitoring.py
from prometheus_client import Counter, Histogram

search_requests = Counter('search_requests_total', 'Total search requests')
search_duration = Histogram('search_duration_seconds', 'Search request duration')
search_results = Histogram('search_results_count', 'Number of search results')

@search_duration.time()
async def monitored_search(params):
    search_requests.inc()
    results = await search_service.search(params)
    search_results.observe(len(results.data))
    return results
```

## 11. Conclusion & Recommendations

### 11.1 Recommended Approach
1. **Start with PostgreSQL pg_trgm** - It's sufficient for most use cases and integrates seamlessly
2. **Implement in phases** - Start with basic search, add features incrementally
3. **Monitor performance** - Set up monitoring from day one
4. **Plan for scale** - Design with future Elasticsearch migration in mind

### 11.2 Success Criteria
- ✅ All datasets discoverable via search
- ✅ Sub-second response times for 95% of queries
- ✅ Zero unauthorized dataset exposure
- ✅ 80%+ search success rate (users find what they're looking for)

### 11.3 Next Steps
1. Review and approve this technical specification
2. Set up development environment with PostgreSQL extensions
3. Create detailed JIRA tickets for each implementation phase
4. Schedule architecture review meeting
5. Begin Phase 1 implementation

---

## Appendix A: Sample Implementation Files

The complete implementation would include:
- `/src/app/datasets/search/` - Search module
- `/src/alembic/versions/xxx_search_capability.py` - Migration
- `/tests/test_search_api.py` - Comprehensive tests
- `/docs/api/search.md` - API documentation
- `/scripts/setup_search.sh` - Setup automation

## Appendix B: References

- [PostgreSQL pg_trgm Documentation](https://www.postgresql.org/docs/current/pgtrgm.html)
- [FastAPI Best Practices](https://fastapi.tiangolo.com/tutorial/best-practices/)
- [Search UX Best Practices](https://baymard.com/blog/search-suggestions)
- [PostgreSQL Full-Text Search](https://www.postgresql.org/docs/current/textsearch.html)