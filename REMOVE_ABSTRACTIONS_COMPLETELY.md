# Plan: Complete Removal of Abstractions

## Why We're Doing This

1. **One implementation per interface** = abstractions serve no purpose
2. **PostgreSQL-specific logic** = switching databases impossible anyway
3. **Python has duck typing** = mocking works without interfaces
4. **YAGNI principle** = maintaining 1000+ lines for "maybe someday"

## Architecture Change

```
Before: Handler → Interface → PostgreSQL → Database
After:  Handler → PostgreSQL → Database
```

## Feature-by-Feature Removal Order

### 1. Downloads Feature (Start Here - Simplest)
**Files:** 2 handlers
**Why first:** Minimal dependencies, easy to test approach
**Testing Required:** Test ALL download endpoints with curl before proceeding

### 2. Refs Feature  
**Files:** 3 handlers
**Why second:** Simple CRUD operations
**Testing Required:** Test ALL ref endpoints with curl before proceeding

### 3. Jobs Feature
**Files:** 5 handlers
**Why third:** Self-contained with clear boundaries
**Testing Required:** Test ALL job endpoints with curl before proceeding

### 4. Exploration Feature
**Files:** 3 handlers
**Testing Required:** Test ALL exploration endpoints with curl before proceeding

### 5. Sampling Feature
**Files:** 5 handlers
**Testing Required:** Test ALL sampling endpoints with curl before proceeding

### 6. SQL Workbench
**Files:** 2 handlers
**Testing Required:** Test ALL SQL endpoints with curl before proceeding

### 7. Search Feature
**Files:** 3 handlers + event handlers
**Testing Required:** Test ALL search endpoints with curl before proceeding

### 8. Users Feature
**Files:** 6 handlers
**Testing Required:** Test ALL user endpoints with curl before proceeding

### 9. Datasets Feature
**Files:** 8 handlers
**Testing Required:** Test ALL dataset endpoints with curl before proceeding

### 10. Versioning Feature (Last - Most Complex)
**Files:** 9 handlers
**Testing Required:** Test ALL version endpoints with curl before proceeding

## Step-by-Step Process (Per Feature)

**IMPORTANT: After updating each feature, test ALL its endpoints with curl before moving to the next feature. Only proceed when all curls return correct responses.**

### Step 1: Update Handler Imports

**BEFORE (create_dataset.py):**
```python
from src.core.abstractions import IUnitOfWork, IUserRepository, IDatasetRepository
from src.core.abstractions.external import IFileStorage
from src.core.abstractions.events import IEventBus

class CreateDatasetHandler(BaseHandler):
    def __init__(self, uow: IUnitOfWork, user_repo: IUserRepository, 
                 dataset_repo: IDatasetRepository, file_storage: IFileStorage = None):
```

**AFTER:**
```python
from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.infrastructure.postgres.user_repo import PostgresUserRepository
from src.infrastructure.postgres.dataset_repo import PostgresDatasetRepository
from src.infrastructure.external.local_file_storage import LocalFileStorage
from src.core.events.publisher import EventPublisher

class CreateDatasetHandler(BaseHandler):
    def __init__(self, uow: PostgresUnitOfWork, user_repo: PostgresUserRepository, 
                 dataset_repo: PostgresDatasetRepository, file_storage: LocalFileStorage = None):
```

### Step 2: Update Infrastructure Classes

**BEFORE (postgres/user_repo.py):**
```python
from src.core.abstractions import IUserRepository

class PostgresUserRepository(BasePostgresRepository[int], IUserRepository):
```

**AFTER:**
```python
# Remove all interface imports and inheritance
class PostgresUserRepository(BasePostgresRepository[int]):
```

### Step 3: Update Event System

**BEFORE:**
```python
from src.core.abstractions.events import IEventBus, DomainEvent

class EventPublisher(IEventPublisher):
```

**AFTER:**
```python
# Move DomainEvent to core/events/events.py as simple dataclass
from dataclasses import dataclass
from datetime import datetime

@dataclass
class DomainEvent:
    event_id: str
    occurred_at: datetime
    event_type: str
    aggregate_id: str
    event_data: dict

class EventPublisher:  # No inheritance
```

### Step 4: Update Dependency Injection (CRITICAL!)

**This is the most critical step that was missing from the original plan!**

**BEFORE (src/api/dependencies.py):**
```python
from src.core.abstractions import IUnitOfWork, IEventBus

async def get_uow() -> AsyncGenerator[IUnitOfWork, None]:
    pool = await get_db_pool()
    uow = PostgresUnitOfWork(pool)
    async with uow:
        yield uow

async def get_event_bus() -> Optional[IEventBus]:
    return _event_bus

async def get_current_user(
    token: str = Depends(oauth2_scheme),
    uow: IUnitOfWork = Depends(get_uow)
) -> dict:
```

**AFTER:**
```python
from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.core.events.publisher import EventPublisher

async def get_uow() -> AsyncGenerator[PostgresUnitOfWork, None]:
    pool = await get_db_pool()
    uow = PostgresUnitOfWork(pool)
    async with uow:
        yield uow

async def get_event_bus() -> Optional[EventPublisher]:
    return _event_bus

async def get_current_user(
    token: str = Depends(oauth2_scheme),
    uow: PostgresUnitOfWork = Depends(get_uow)
) -> dict:
```

### Step 5: Fix Tests

**BEFORE:**
```python
from src.core.abstractions import IUserRepository

mock_repo = Mock(spec=IUserRepository)
```

**AFTER:**
```python
from src.infrastructure.postgres.user_repo import PostgresUserRepository

# Option 1: Direct mock
mock_repo = Mock(spec=PostgresUserRepository)

# Option 2: Patch decorator
@patch('src.infrastructure.postgres.user_repo.PostgresUserRepository')
def test_something(mock_repo):
    mock_repo.get_by_id.return_value = None
```

### Step 6: Test Each Feature with Curl

**CRITICAL: DO NOT proceed to the next feature until ALL endpoints return correct responses!**

```bash
# After updating each feature:

# 1. Check no abstraction imports remain
grep -r "core.abstractions" src/features/[feature_name]/

# 2. Test ALL endpoints with curl (user: bg54677, password: password)
# Each feature must pass ALL tests before moving to next feature

# Downloads Feature Testing:
curl -u bg54677:password http://localhost:8000/api/downloads  # List downloads
curl -u bg54677:password http://localhost:8000/api/downloads/1  # Get specific download
# Add more download endpoints as discovered

# Refs Feature Testing:
curl -u bg54677:password http://localhost:8000/api/refs  # List refs
curl -u bg54677:password -X POST http://localhost:8000/api/refs -H "Content-Type: application/json" -d '{"name": "test"}'  # Create ref
# Add more ref endpoints as discovered

# Jobs Feature Testing:
curl -u bg54677:password http://localhost:8000/api/jobs  # List jobs
curl -u bg54677:password http://localhost:8000/api/jobs/1  # Get job status
# Add more job endpoints as discovered

# Exploration Feature Testing:
curl -u bg54677:password http://localhost:8000/api/exploration  # List explorations
# Add more exploration endpoints as discovered

# Sampling Feature Testing:
curl -u bg54677:password http://localhost:8000/api/sampling  # List samples
# Add more sampling endpoints as discovered

# SQL Workbench Testing:
curl -u bg54677:password http://localhost:8000/api/sql  # Execute query
# Add more SQL endpoints as discovered

# Search Feature Testing:
curl -u bg54677:password http://localhost:8000/api/search?q=test  # Search
# Add more search endpoints as discovered

# Users Feature Testing:
curl -u bg54677:password http://localhost:8000/api/users  # List users
curl -u bg54677:password http://localhost:8000/api/users/me  # Current user
# Add more user endpoints as discovered

# Datasets Feature Testing:
curl -u bg54677:password http://localhost:8000/api/datasets  # List datasets
curl -u bg54677:password http://localhost:8000/api/datasets/1  # Get dataset
# Add more dataset endpoints as discovered

# Versioning Feature Testing:
curl -u bg54677:password http://localhost:8000/api/versions  # List versions
# Add more version endpoints as discovered
```

### Step 7: Commit Only After Successful Testing

```bash
# Only after ALL curls return correct responses for a feature:
git add src/features/[feature_name]/ src/infrastructure/ src/api/dependencies.py
git commit -m "refactor: Remove abstractions from [Feature] feature - all endpoints tested"
```

## Infrastructure Changes

### UnitOfWork Simplification

**BEFORE:**
```python
from src.core.abstractions import IUnitOfWork, IUserRepository, IDatasetRepository

class PostgresUnitOfWork(IUnitOfWork):
    def __init__(self, connection):
        self._conn = connection
        self._users: IUserRepository = PostgresUserRepository(connection)
        self._datasets: IDatasetRepository = PostgresDatasetRepository(connection)
```

**AFTER:**
```python
# No interface imports needed
class PostgresUnitOfWork:
    def __init__(self, connection):
        self._conn = connection
        self._users = PostgresUserRepository(connection)
        self._datasets = PostgresDatasetRepository(connection)
```

## Final Cleanup (After ALL Features Updated)

1. **Delete abstractions directory:**
   ```bash
   rm -rf src/core/abstractions/
   ```

2. **Move remaining shared types:**
   - `DomainEvent` → `src/core/events/events.py`
   - Common models → `src/core/models/`

3. **Global validation:**
   ```bash
   # Should return nothing:
   grep -r "from src.core.abstractions" src/
   grep -r "from core.abstractions" src/
   grep -r "ABC)" src/
   grep -r "@abstractmethod" src/
   ```

4. **Final validation with curl:**
   ```bash
   # Test all endpoints one more time
   curl -u bg54677:password http://localhost:8000/api/downloads
   curl -u bg54677:password http://localhost:8000/api/refs
   # ... etc for all endpoints
   ```

## Git Strategy

**IMPORTANT: Only commit after successful curl testing!**

Each feature is a separate commit AFTER testing passes:
```bash
# 1. Make changes to feature
# 2. Test ALL endpoints with curl
# 3. Only if ALL tests pass:
git add src/features/downloads/ src/infrastructure/
git commit -m "refactor: Remove abstractions from Downloads feature - all endpoints tested"

# Repeat for each feature
git add src/features/refs/ 
git commit -m "refactor: Remove abstractions from Refs feature - all endpoints tested"
# ... etc
```

Final commit (only after ALL features work):
```bash
git rm -rf src/core/abstractions/
git commit -m "refactor: Delete abstractions directory - complete removal"
```

## Testing Strategy

Since we're removing interfaces, update test approach:

### Simple Mock Example
```python
# tests/unit/features/users/test_create_user.py
from unittest.mock import Mock, patch
from src.infrastructure.postgres.user_repo import PostgresUserRepository

@patch('src.features.users.handlers.create_user.PostgresUserRepository')
def test_create_user_success(mock_repo_class):
    # Setup
    mock_repo = Mock()
    mock_repo.get_by_soeid.return_value = None
    mock_repo.create_user.return_value = 123
    mock_repo_class.return_value = mock_repo
    
    # Test
    handler = CreateUserHandler(mock_uow)
    result = await handler.handle(command)
    
    # Assert
    assert result.user_id == 123
```

## Common Patterns to Update

### Pattern 1: Type Hints
```python
# Before
def __init__(self, repo: IUserRepository):

# After  
def __init__(self, repo: PostgresUserRepository):
```

### Pattern 2: Imports
```python
# Before
from src.core.abstractions import IUnitOfWork, IUserRepository

# After
from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.infrastructure.postgres.user_repo import PostgresUserRepository
```

### Pattern 3: Event Publishing
```python
# Before
if self._event_bus:  # IEventBus
    await self._event_bus.publish(event)

# After
if self._event_bus:  # EventPublisher
    await self._event_bus.publish(event)
```

## Success Metrics

✅ All tests pass  
✅ No abstraction imports anywhere  
✅ `src/core/abstractions/` deleted  
✅ Application runs normally  
✅ ~1000 lines of code removed  
✅ Direct navigation in IDE works  

## Timeline

- **Per simple feature**: 30-45 minutes + testing time
- **Per complex feature**: 1-2 hours + testing time
- **Testing per feature**: 15-30 minutes (must pass ALL curls)
- **Total estimate**: 2-3 days of focused work + testing

## Why This Will Work

1. **Python's flexibility** - Mocking/patching works without interfaces
2. **Your code is PostgreSQL-specific** - Abstractions were lying about flexibility
3. **Simpler = better** - Less code to maintain, easier to understand
4. **YAGNI wins** - You'll never need these abstractions

## Additional Considerations (from Gemini's Review)

### External Service Abstractions
Before deleting `src/core/abstractions/external.py`, verify if these have multiple implementations:
- `IFileStorage` - Check if only `LocalFileStorage` exists
- `ICache` - Check if only `MemoryCache` exists
- `IAuthService`, `IPasswordHasher`, etc.

**Action:** Keep abstractions that have multiple implementations or environment-specific variations.

### Testing Brittleness
Be aware that patching concrete classes couples tests to:
- Constructor signatures
- Full public API
- Implementation details

**Mitigation:** Use dependency injection in tests where possible instead of patching.