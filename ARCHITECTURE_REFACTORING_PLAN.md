# Final Architecture Refactoring Plan

## Executive Summary

This document consolidates all architectural improvements into a single, actionable plan that addresses:
- Handler inconsistencies and business logic placement
- Removal of misused CQRS patterns
- Service layer standardization
- Dependency injection for API and worker consistency

## Context: Why This Refactoring Is Needed

### Current Problems

#### 1. Misused CQRS Pattern
The codebase attempts to use CQRS (Command Query Responsibility Segregation) but implements it incorrectly:
- **Commands that are actually queries**: `GetDatasetCommand`, `ListDatasetsCommand`
- **No real separation**: Same database, same models, no event sourcing
- **Added complexity without benefits**: Extra abstraction layers that just mirror API requests
- **Confusing terminology**: Developers waste time understanding unnecessary patterns

#### 2. Handler Inconsistencies
Analysis of handlers across features reveals:
- **No consistent base class usage**: Some use `BaseHandler`, others have no inheritance
- **Business logic in wrong places**: Complex logic in handlers instead of services
- **Transaction management chaos**: Mix of decorators, manual management, and no management
- **Permission checking inconsistency**: Some handlers check internally, others assume external
- **Event publishing gaps**: State changes without audit trails

#### 3. Specific Examples of Problems

**Downloads Feature** - No base class, manual error handling:
```python
# Current problematic code
class DownloadDatasetHandler:
    async def handle(self, request: DownloadDatasetRequest):
        async with self._uow as uow:  # Manual transaction
            dataset = await uow.datasets.get_by_id(request.dataset_id)
            if not dataset:
                raise NotFoundError("Dataset not found")  # Inconsistent error
```

**Sampling Feature** - Dangerous async/sync mixing:
```python
# Current problematic code  
async def handle(self, command: Command):
    data = self._generate_sample_sync(command.config)  # Blocks event loop!
    result = await self._save_async(data)
```

**Jobs Feature** - Mixed patterns within same feature:
```python
# Some handlers use decorators
@with_transaction
async def handle(self, command): ...

# Others don't
async def handle(self, command): ...  # No transaction management!
```

## Target Architecture

### Key Principles

1. **Services contain ALL business logic** - No logic in handlers, API routes, or workers
2. **Direct service calls from API** - No handler layer needed for 90%+ of cases
3. **Centralized dependency injection** - Single source of truth for service construction
4. **Feature-based organization** - All related code co-located within features
5. **No CQRS abstractions** - Simple, direct method calls instead of commands/queries

### Target Directory Structure

```
./src
├── api/
│   ├── dependencies.py      # Uses container.py to provide services to FastAPI
│   ├── error_handlers.py
│   └── routers/            # One router per feature
│       ├── datasets.py     # Directly calls DatasetService
│       ├── users.py        # Directly calls UserService
│       ├── jobs.py
│       └── ...
├── container.py            # NEW: Central dependency injection factories
├── core/
│   ├── auth/              # Grouped auth logic
│   │   ├── password.py
│   │   └── security.py
│   ├── decorators.py      # @with_transaction, @with_error_handling
│   ├── domain_exceptions.py
│   ├── events/
│   └── permissions.py
├── features/
│   ├── base_service.py    # NEW: Base class for all services
│   ├── datasets/
│   │   ├── models.py      # Request/Response DTOs (Pydantic)
│   │   └── services.py    # ALL dataset business logic
│   ├── users/
│   │   ├── models.py
│   │   └── services.py
│   ├── jobs/
│   │   ├── models.py
│   │   └── services.py
│   └── ... (other features follow same pattern)
├── infrastructure/
│   ├── config/
│   │   └── settings.py
│   ├── postgres/
│   │   ├── repositories/  # All repository implementations
│   │   │   ├── dataset_repo.py
│   │   │   └── user_repo.py
│   │   ├── uow.py        # Enhanced with event collection
│   │   └── database.py
│   └── external/         # Third-party integrations
├── main.py
└── workers/
    ├── dataset_tasks.py  # Thin orchestrators using services
    ├── job_tasks.py
    └── ...
```

### What Gets Removed

- ❌ `/src/services/` directory (logic moves to feature services)
- ❌ `/src/features/*/handlers/` directories
- ❌ `/src/features/*/models/commands.py` files
- ❌ Command/Query objects and CQRS patterns
- ❌ `BaseHandler`, `BaseUpdateHandler` classes

### Phase 3: Update Workers (Week 6)

Transform workers from business logic containers to thin orchestrators.

#### 3.1 Import Worker Example

**Before:**
```python
# workers/import_executor.py
class ImportJobExecutor:
    def execute(self, job_id, parameters, db_pool):
        # Lots of business logic...
        # Direct database access...
        # File parsing logic...
```

**After:**
```python
# workers/dataset_tasks.py
from src.container import get_dataset_service
from src.infrastructure.postgres.database import get_session
from src.infrastructure.postgres.uow import PostgresUnitOfWork

async def import_dataset_task(job_id: str, file_path: str, user_id: str):
    """Thin orchestrator for dataset import."""
    async with get_session() as session:
        uow = PostgresUnitOfWork(session)
        dataset_service = get_dataset_service(uow)
        
        # All business logic is in the service
        await dataset_service.import_from_file(
            job_id=job_id,
            file_path=file_path,
            user_id=user_id
        )

# Celery task wrapper
@app.task
def import_dataset(job_id: str, file_path: str, user_id: str):
    import asyncio
    asyncio.run(import_dataset_task(job_id, file_path, user_id))
```

#### 3.2 Update All Workers
- `sampling_executor.py` → Uses `SamplingService`
- `exploration_executor.py` → Uses `ExplorationService`
- `sql_transform_executor.py` → Uses `SqlTransformService`

### Phase 4: Final Cleanup (Week 7)

1. **Delete obsolete code:**
   - `/src/services/` directory
   - All handler directories
   - Command/Query objects
   - `BaseHandler` classes

2. **Update configuration:**
   - Remove handler imports from `__init__.py` files
   - Update any remaining import statements

3. **Documentation:**
   - Update README with new architecture
   - Create developer guide for service pattern
   - Document dependency injection approach

## Success Metrics

### Code Quality Metrics
- ✓ 100% of handlers removed
- ✓ 0 Command/Query objects remain
- ✓ All business logic in services
- ✓ Services shared between API and workers
- ✓ Consistent error handling via decorators

### Architecture Metrics
- ✓ Single source of truth for service construction (container.py)
- ✓ Clear separation of concerns
- ✓ No business logic in API layer or workers
- ✓ Transaction boundaries properly managed
- ✓ Events collected and published after commit

### Developer Experience
- ✓ Simpler mental model (no CQRS confusion)
- ✓ Easy to find where logic lives (always in services)
- ✓ Consistent patterns across all features
- ✓ Easier testing (mock at service boundary)
- ✓ Clear dependency graph

## Appendix: Feature-Specific Refactoring Notes

### Downloads Feature
**Current Issues:**
- No base class inheritance
- Manual error handling
- No decorators used

**Refactoring Priority:** High - This feature has the most inconsistencies

### Exploration Feature  
**Current Issues:**
- Response models defined in handler files
- No base class inheritance
- Missing permission checks in some handlers

**Special Consideration:** Complex async operations need careful migration

### Sampling Feature
**Current Issues:**
- Dangerous async/sync mixing causing event loop blocking
- No base class inheritance
- Performance bottlenecks

**Critical Fix:** Must resolve sync operations in async context immediately

### SQL Workbench Feature
**Current Issues:**
- SQL injection risks from raw SQL construction
- Missing query timeouts
- Inconsistent decorator usage

**Security Priority:** Parameterize all queries during refactoring

### Search Feature
**Current Issues:**
- Unnecessary transactions for read operations (10-15% overhead)
- Manual transaction management despite using BaseHandler

**Performance Note:** Remove transactions from read-only operations

### Versioning Feature
**Current Issues:**
- Complex inter-dependencies
- Mixed handler patterns
- Many handlers to refactor

**Approach:** Start with simpler handlers (get operations) before complex ones (create commit)