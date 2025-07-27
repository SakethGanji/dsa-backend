# Exploration Service Consolidation Plan

## Current State Analysis

Currently, the exploration functionality is split across 3 separate handler files:
1. `CreateExplorationJobHandler` - Creates exploration jobs for asynchronous processing
2. `GetExplorationHistoryHandler` - Retrieves exploration history for datasets/users
3. `GetExplorationResultHandler` - Retrieves results from completed exploration jobs

### Key Observations:
- All handlers follow the same pattern as dataset and sampling handlers
- Similar dependency injection patterns
- All use commands for input (except GetExplorationResultHandler which uses direct params)
- Permission checking is consistent
- Less complex than sampling (only 3 handlers vs 4-5)

## Proposed Solution: Consolidated ExplorationService

### Benefits:
1. **Reduced duplication** - Single initialization point
2. **Better cohesion** - All exploration operations together
3. **Easier testing** - One service to test
4. **Simplified API** - Single service dependency
5. **Consistent patterns** - Matches DatasetService and SamplingService approach

### Proposed Structure:

```python
# src/features/exploration/services/exploration_service.py

from typing import Dict, Any, List, Optional
from uuid import UUID
from datetime import datetime
from dataclasses import dataclass
from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.core.permissions import PermissionService
from src.core.events.publisher import EventBus
from src.core.domain_exceptions import EntityNotFoundException, ValidationException
from ...base_handler import with_transaction, with_error_handling
from fastapi import HTTPException
from ..models import *


@dataclass
class ExplorationJobResponse:
    job_id: str
    status: str
    message: str


class ExplorationService:
    """Consolidated service for all exploration operations."""
    
    def __init__(
        self,
        uow: PostgresUnitOfWork,
        permissions: PermissionService,
        event_bus: Optional[EventBus] = None
    ):
        self._uow = uow
        self._permissions = permissions
        self._event_bus = event_bus
    
    @with_transaction
    @with_error_handling
    async def create_exploration_job(
        self,
        command: CreateExplorationJobCommand
    ) -> ExplorationJobResponse:
        """Create an exploration job for asynchronous processing."""
        # Implementation from CreateExplorationJobHandler
        pass
    
    @with_error_handling
    async def get_exploration_history(
        self,
        dataset_id: int,
        user_id: int,
        limit: int = 10,
        offset: int = 0,
        status: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get exploration history for a dataset."""
        # Implementation from GetExplorationHistoryHandler
        pass
    
    @with_error_handling
    async def get_exploration_result(
        self,
        job_id: UUID,
        user_id: int
    ) -> Dict[str, Any]:
        """Get results from a completed exploration job."""
        # Implementation from GetExplorationResultHandler
        pass
```

## Implementation Steps

### 1. Create Service Structure
```bash
mkdir -p src/features/exploration/services
touch src/features/exploration/services/__init__.py
touch src/features/exploration/services/exploration_service.py
```

### 2. Migrate Handler Logic
- Copy each handler's logic into corresponding service methods
- Remove handler class boilerplate
- Adjust imports as needed
- **IMPORTANT**: Remove any references to non-existent core services

### 3. Update API Endpoints
Transform each endpoint from:
```python
handler = CreateExplorationJobHandler(uow, permissions=permission_service)
return await handler.handle(command)
```

To:
```python
service = ExplorationService(uow, permissions=permission_service)
return await service.create_exploration_job(command)
```

### 4. Update Dependencies
The service will need access to:
- `PostgresUnitOfWork` 
- `PermissionService`
- `EventBus` (optional, for future events)

### 5. Clean Up
- Remove old handler files
- Update imports throughout codebase
- Update module exports

## Special Considerations

### 1. Import Fixes (Lessons from Sampling)
- **DO NOT** import non-existent core services like `CoreExplorationService`
- Use local enums and models from the `models` module
- Ensure all imports in `models/__init__.py` match what's actually in the files

### 2. Response Types
- Move response types like `ExplorationJobResponse` to the service file or a dedicated response models file
- Keep them close to where they're used

### 3. Error Handling
- Keep using HTTPException for now (consistent with existing patterns)
- Future improvement: domain exceptions with API layer translation

### 4. Model Imports
- Check that `models/__init__.py` only exports what actually exists in the command/model files
- Remove any references to non-existent commands

## Migration Checklist

- [ ] Create services directory structure
- [ ] Create ExplorationService class
- [ ] Migrate create_exploration_job logic
- [ ] Migrate get_exploration_history logic
- [ ] Migrate get_exploration_result logic
- [ ] Update all API endpoints in `src/api/exploration.py`
- [ ] Fix imports in `models/__init__.py` if needed
- [ ] Remove old handler files
- [ ] Update handler exports in `handlers/__init__.py`
- [ ] Test all endpoints
- [ ] Run uvicorn and fix any import errors

## Common Pitfalls to Avoid

1. **Import Errors**: Don't reference non-existent core services
2. **Model Exports**: Ensure `__all__` in models/__init__.py matches actual exports
3. **Handler Removal**: Remember to clean up handler __init__.py after migration
4. **API Updates**: Update all handler references to service references

## Expected Outcome

After consolidation:
- Single `ExplorationService` class with 3 methods
- Cleaner API endpoints
- Better testability
- Consistent with DatasetService and SamplingService patterns
- No functional changes - API remains the same

## Testing Commands

After implementation:
```bash
# Check Python syntax
python3 -m py_compile src/api/exploration.py src/features/exploration/services/exploration_service.py

# Run server
python3 -m uvicorn src.main:app --reload

# Test health endpoint
curl http://localhost:8000/health
```