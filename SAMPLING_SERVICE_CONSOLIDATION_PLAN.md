# Sampling Service Consolidation Plan

## Current State Analysis

Currently, the sampling functionality is split across 4 separate handler files:
1. `CreateSamplingJobHandler` - Creates sampling jobs for asynchronous processing
2. `GetJobDataHandler` - Retrieves sampled data from completed jobs
3. `GetSamplingHistoryHandler` - Contains two handlers:
   - `GetDatasetSamplingHistoryHandler` - Gets history for a dataset
   - `GetUserSamplingHistoryHandler` - Gets history for a user
4. `GetSamplingMethodsHandler` - Returns available sampling methods

### Key Observations:
- All handlers follow the same pattern as dataset handlers (inherit from BaseHandler)
- Similar dependency injection patterns
- All use commands for input
- Permission checking is consistent
- Some handlers like `GetSamplingHistoryHandler` already contain multiple related operations

## Proposed Solution: Consolidated SamplingService

### Benefits:
1. **Reduced duplication** - Single initialization point
2. **Better cohesion** - All sampling operations together
3. **Easier testing** - One service to test
4. **Simplified API** - Single service dependency
5. **Consistent patterns** - Matches the DatasetService approach

### Proposed Structure:

```python
# src/features/sampling/services/sampling_service.py

from typing import Dict, Any, List, Optional, Tuple
from uuid import UUID
from datetime import datetime
from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.infrastructure.postgres.table_reader import PostgresTableReader
from src.core.permissions import PermissionService
from src.core.events.publisher import EventBus
from ..models import *
from ...base_handler import with_transaction, with_error_handling

class SamplingService:
    """Consolidated service for all sampling operations."""
    
    def __init__(
        self,
        uow: PostgresUnitOfWork,
        permissions: PermissionService,
        table_reader: Optional[PostgresTableReader] = None,
        event_bus: Optional[EventBus] = None
    ):
        self._uow = uow
        self._permissions = permissions
        self._table_reader = table_reader
        self._event_bus = event_bus
    
    @with_transaction
    @with_error_handling
    async def create_sampling_job(
        self,
        command: CreateSamplingJobCommand
    ) -> SamplingJobResponse:
        """Create a sampling job for asynchronous processing."""
        # Implementation from CreateSamplingJobHandler
        pass
    
    @with_error_handling
    async def get_job_data(
        self,
        job_id: str,
        user_id: int,
        table_key: str = "primary",
        offset: int = 0,
        limit: int = 100,
        columns: Optional[List[str]] = None,
        format: str = "json"
    ) -> Dict[str, Any]:
        """Retrieve sampled data from a completed sampling job."""
        # Implementation from GetSamplingJobDataHandler
        pass
    
    @with_error_handling
    async def get_dataset_sampling_history(
        self,
        dataset_id: int,
        user_id: int,
        ref_name: Optional[str] = None,
        status: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        offset: int = 0,
        limit: int = 20
    ) -> Dict[str, Any]:
        """Get sampling job history for a dataset."""
        # Implementation from GetDatasetSamplingHistoryHandler
        pass
    
    @with_error_handling
    async def get_user_sampling_history(
        self,
        target_user_id: int,
        current_user_id: int,
        is_admin: bool = False,
        dataset_id: Optional[int] = None,
        status: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        offset: int = 0,
        limit: int = 20
    ) -> Dict[str, Any]:
        """Get sampling job history for a user."""
        # Implementation from GetUserSamplingHistoryHandler
        pass
    
    @with_error_handling
    async def get_sampling_methods(
        self,
        dataset_id: int,
        user_id: int
    ) -> Dict[str, Any]:
        """Get available sampling methods and their parameters."""
        # Implementation from GetSamplingMethodsHandler
        pass
```

## Implementation Steps

### 1. Create Service Structure
```bash
mkdir -p src/features/sampling/services
touch src/features/sampling/services/__init__.py
touch src/features/sampling/services/sampling_service.py
```

### 2. Migrate Handler Logic
- Copy each handler's logic into corresponding service methods
- Remove handler class boilerplate
- Adjust imports as needed

### 3. Update API Endpoints
Transform each endpoint from:
```python
handler = CreateSamplingJobHandler(uow, permissions=permission_service)
return await handler.handle(command)
```

To:
```python
service = SamplingService(uow, permissions=permission_service, table_reader=table_reader)
return await service.create_sampling_job(command)
```

### 4. Update Dependencies
The service will need access to:
- `PostgresUnitOfWork` 
- `PermissionService`
- `PostgresTableReader` (for get_job_data)
- `EventBus` (optional, for future events)

### 5. Clean Up
- Remove old handler files
- Update imports throughout codebase
- Update module exports

## Special Considerations

### 1. Table Reader Dependency
The `get_job_data` method requires `PostgresTableReader` which other methods don't need. Options:
- Make it optional in constructor
- Inject it only when needed
- Create a separate factory method

### 2. Response Types
Some handlers return custom response types:
- `SamplingJobResponse` - Should be moved to models or API response models
- Consider using consistent response patterns

### 3. Error Handling
The handlers use HTTPException directly. Consider:
- Using domain exceptions instead
- Let API layer handle HTTP-specific errors

## Migration Checklist

- [ ] Create services directory structure
- [ ] Create SamplingService class
- [ ] Migrate create_sampling_job logic
- [ ] Migrate get_job_data logic
- [ ] Migrate get_dataset_sampling_history logic
- [ ] Migrate get_user_sampling_history logic
- [ ] Migrate get_sampling_methods logic
- [ ] Update all API endpoints
- [ ] Remove old handler files
- [ ] Update imports and exports
- [ ] Test all endpoints

## Expected Outcome

After consolidation:
- Single `SamplingService` class with 5 methods
- Cleaner API endpoints
- Better testability
- Consistent with DatasetService pattern
- No functional changes - API remains the same