# Lean MVP Plan: Core Linear Versioning

## Overview

This document outlines the minimum viable plan to implement core, linear versioning. A user will be able to manage a single line of history (main branch) for a dataset, view its evolution, and check out any past version.

**Core Features:**
1. Save new versions (commits) on a single timeline
2. View the commit history
3. View data from any specific version

**Excluded from MVP:**
- Branching
- Tagging
- Diffing/Comparing
- Merging
- Multiple timelines

## Architecture Principles

1. **Reuse Existing Infrastructure**: Leverage current repository patterns and database schema
2. **Minimal New Code**: Only add what's absolutely necessary
3. **Direct Handler-Repository Communication**: Skip service layer for simple operations
4. **Single Timeline Focus**: All commits happen on 'main' ref only

## 1. Repository Interface Extensions

### 1.1 Minimal ICommitRepository Extensions

```python
# In src/core/abstractions/repositories.py, add to ICommitRepository:

@abstractmethod
async def get_commit_history(self, dataset_id: int, offset: int = 0, limit: int = 50) -> List[Dict[str, Any]]:
    """Get the commit history for dataset's main timeline with pagination."""
    pass

@abstractmethod
async def get_commit_by_id(self, commit_id: str) -> Optional[Dict[str, Any]]:
    """Get commit details including author info."""
    pass
```

**Note**: The `get_commit_history` method already exists in PostgresCommitRepository but needs to be added to the interface and modified to not require ref_name (always use 'main').

## 2. API Handlers

### 2.1 Get Commit History Handler

```python
# src/features/versioning/get_commit_history.py

from typing import List
from ...core.abstractions.uow import IUnitOfWork
from ...models.pydantic_models import GetCommitHistoryResponse, CommitInfo

class GetCommitHistoryHandler:
    def __init__(self, uow: IUnitOfWork):
        self.uow = uow
    
    async def handle(self, dataset_id: int, offset: int = 0, limit: int = 50) -> GetCommitHistoryResponse:
        """Get paginated commit history for the main timeline."""
        async with self.uow:
            # Check read permission
            commits = await self.uow.commits.get_commit_history(
                dataset_id=dataset_id,
                offset=offset,
                limit=limit
            )
            
            # Enrich with author names
            enriched_commits = []
            for commit in commits:
                user = await self.uow.users.get_by_id(commit['author_id'])
                enriched_commits.append(CommitInfo(
                    commit_id=commit['commit_id'],
                    parent_commit_id=commit['parent_commit_id'],
                    message=commit['message'],
                    author_id=commit['author_id'],
                    author_name=user['soeid'] if user else 'Unknown',
                    created_at=commit['created_at'],
                    row_count=commit.get('row_count', 0)
                ))
            
            # Get total count
            total = await self.uow.commits.count_commits_for_dataset(dataset_id)
            
            return GetCommitHistoryResponse(
                commits=enriched_commits,
                total=total,
                offset=offset,
                limit=limit
            )
```

### 2.2 Checkout Commit Handler

```python
# src/features/versioning/checkout_commit.py

from typing import Optional
from ...core.abstractions.uow import IUnitOfWork
from ...models.pydantic_models import CheckoutResponse

class CheckoutCommitHandler:
    def __init__(self, uow: IUnitOfWork):
        self.uow = uow
        
    async def handle(self, dataset_id: int, commit_id: str, 
                    table_key: Optional[str] = None,
                    offset: int = 0, limit: int = 100) -> CheckoutResponse:
        """Get data as it existed at a specific commit."""
        async with self.uow:
            # Verify commit belongs to dataset
            commit = await self.uow.commits.get_commit_by_id(commit_id)
            if not commit or commit['dataset_id'] != dataset_id:
                raise ValueError("Commit not found for this dataset")
            
            # Get data using existing method
            data = await self.uow.commits.get_commit_data(
                commit_id=commit_id,
                sheet_name=table_key,
                offset=offset,
                limit=limit
            )
            
            # Get total count
            total_rows = await self.uow.commits.count_commit_rows(commit_id, table_key)
            
            return CheckoutResponse(
                commit_id=commit_id,
                data=data,
                total_rows=total_rows,
                offset=offset,
                limit=limit
            )
```

### 2.3 Create Commit Handler (Enhance Existing)

The existing `CreateCommitHandler` in `src/features/versioning/create_commit.py` already handles creating new commits. We just need to ensure it:
- Always uses 'main' ref
- Can be called repeatedly to create subsequent versions

## 3. API Endpoints

### 3.1 Add to src/api/versioning.py

```python
@router.get("/datasets/{dataset_id}/history")
async def get_commit_history(
    dataset_id: int = Path(..., description="Dataset ID"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    limit: int = Query(50, ge=1, le=100, description="Number of commits to return"),
    current_user: CurrentUser = Depends(get_current_user_info),
    uow_factory: UnitOfWorkFactory = Depends(get_uow_factory)
) -> GetCommitHistoryResponse:
    """Get the chronological commit history for a dataset."""
    # Check read permission
    async with uow_factory.create() as uow:
        has_permission = await uow.datasets.check_user_permission(
            dataset_id=dataset_id,
            user_id=current_user.user_id,
            required_permission=PermissionType.READ.value
        )
        if not has_permission:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You don't have permission to view this dataset"
            )
    
    handler = GetCommitHistoryHandler(uow_factory.create())
    return await handler.handle(dataset_id, offset, limit)


@router.get("/datasets/{dataset_id}/commits/{commit_id}/data")
async def checkout_commit(
    dataset_id: int = Path(..., description="Dataset ID"),
    commit_id: str = Path(..., description="Commit ID to checkout"),
    table_key: Optional[str] = Query(None, description="Specific table/sheet to retrieve"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    limit: int = Query(100, ge=1, le=1000, description="Number of rows to return"),
    current_user: CurrentUser = Depends(get_current_user_info),
    uow_factory: UnitOfWorkFactory = Depends(get_uow_factory)
) -> CheckoutResponse:
    """Get the data as it existed at a specific commit."""
    # Check read permission
    async with uow_factory.create() as uow:
        has_permission = await uow.datasets.check_user_permission(
            dataset_id=dataset_id,
            user_id=current_user.user_id,
            required_permission=PermissionType.READ.value
        )
        if not has_permission:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You don't have permission to view this dataset"
            )
    
    handler = CheckoutCommitHandler(uow_factory.create())
    try:
        return await handler.handle(dataset_id, commit_id, table_key, offset, limit)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
```

### 3.2 Simplify Existing Create Commit Endpoint

The existing `POST /datasets/{dataset_id}/refs/{ref_name}/commits` endpoint should be simplified:
- Document that only 'main' ref is supported in MVP
- Consider adding a simpler endpoint alias: `POST /datasets/{dataset_id}/commits`

## 4. PostgreSQL Repository Implementation

### 4.1 Update PostgresCommitRepository

```python
# In src/core/infrastructure/postgres/versioning_repo.py

async def get_commit_history(self, dataset_id: int, offset: int = 0, limit: int = 50) -> List[Dict[str, Any]]:
    """Get commit history for main branch only."""
    # Modify existing method to always use 'main' ref
    query = """
    WITH RECURSIVE commit_tree AS (
        -- Start with the commit pointed to by 'main' ref
        SELECT c.* 
        FROM dsa_core.commits c
        JOIN dsa_core.refs r ON r.commit_id = c.commit_id
        WHERE r.dataset_id = $1 AND r.ref_name = 'main'
        
        UNION ALL
        
        -- Recursively get parent commits
        SELECT c.*
        FROM dsa_core.commits c
        JOIN commit_tree ct ON c.commit_id = ct.parent_commit_id
    )
    SELECT 
        commit_id,
        parent_commit_id,
        dataset_id,
        message,
        author_id,
        created_at,
        (SELECT COUNT(*) FROM dsa_core.commit_rows WHERE commit_id = commit_tree.commit_id) as row_count
    FROM commit_tree
    ORDER BY created_at DESC
    LIMIT $2 OFFSET $3
    """
    
    rows = await self.conn.fetch(query, dataset_id, limit, offset)
    return [dict(row) for row in rows]

async def count_commits_for_dataset(self, dataset_id: int) -> int:
    """Count total commits for a dataset."""
    query = """
    WITH RECURSIVE commit_tree AS (
        SELECT c.commit_id 
        FROM dsa_core.commits c
        JOIN dsa_core.refs r ON r.commit_id = c.commit_id
        WHERE r.dataset_id = $1 AND r.ref_name = 'main'
        
        UNION ALL
        
        SELECT c.commit_id
        FROM dsa_core.commits c
        JOIN commit_tree ct ON c.commit_id = ct.parent_commit_id
    )
    SELECT COUNT(*) FROM commit_tree
    """
    
    result = await self.conn.fetchval(query, dataset_id)
    return result or 0

async def get_commit_by_id(self, commit_id: str) -> Optional[Dict[str, Any]]:
    """Get commit details."""
    query = """
    SELECT * FROM dsa_core.commits WHERE commit_id = $1
    """
    row = await self.conn.fetchrow(query, commit_id)
    return dict(row) if row else None

async def count_commit_rows(self, commit_id: str, table_key: Optional[str] = None) -> int:
    """Count rows in a commit, optionally filtered by table."""
    if table_key:
        query = """
        SELECT COUNT(*) FROM dsa_core.commit_rows 
        WHERE commit_id = $1 AND logical_row_id LIKE $2
        """
        result = await self.conn.fetchval(query, commit_id, f"{table_key}:%")
    else:
        query = """
        SELECT COUNT(*) FROM dsa_core.commit_rows WHERE commit_id = $1
        """
        result = await self.conn.fetchval(query, commit_id)
    
    return result or 0
```

## 5. Pydantic Models

### 5.1 Add to src/models/pydantic_models.py

```python
# Commit History Models
class CommitInfo(BaseModel):
    commit_id: str
    parent_commit_id: Optional[str]
    message: str
    author_id: int
    author_name: str  # Enriched field
    created_at: datetime
    row_count: int    # Number of rows in this commit

class GetCommitHistoryResponse(BaseModel):
    commits: List[CommitInfo]
    total: int
    offset: int
    limit: int

# Checkout Models
class CheckoutResponse(BaseModel):
    commit_id: str
    data: List[Dict[str, Any]]
    total_rows: int
    offset: int
    limit: int
```

## 6. Implementation Steps

### Phase 1: Read Operations (3-4 days)
1. **Day 1**: Add repository interface methods and PostgreSQL implementations
2. **Day 2**: Implement GetCommitHistoryHandler and API endpoint
3. **Day 3**: Implement CheckoutCommitHandler and API endpoint
4. **Day 4**: Test read operations end-to-end

### Phase 2: Enhance Write Operations (2-3 days)
1. **Day 5**: Ensure create commit works for subsequent versions
2. **Day 6**: Add validation for linear history (no branching)
3. **Day 7**: Test full create-read cycle

### Phase 3: Polish & Documentation (2 days)
1. **Day 8**: Add error handling and edge cases
2. **Day 9**: Write API documentation and examples

## 7. Testing Checklist

### Unit Tests
- [ ] GetCommitHistoryHandler with mocked repository
- [ ] CheckoutCommitHandler with mocked repository
- [ ] Repository methods with test database

### Integration Tests
- [ ] Create initial dataset with upload
- [ ] Create subsequent commits
- [ ] View commit history
- [ ] Checkout old versions
- [ ] Permission checks

### End-to-End Scenarios
- [ ] Upload CSV → Create new version → View history → Checkout old version
- [ ] Multi-user scenarios with permissions
- [ ] Large dataset handling

## 8. Future Extensibility

This lean implementation provides hooks for future features:
- **Branching**: Change hardcoded 'main' to accept ref parameter
- **Tagging**: Add tag table and endpoints
- **Diff**: Add comparison logic between commits
- **Merge**: Add merge commit support with multiple parents

The foundation remains solid while keeping the initial implementation minimal and focused on core value.