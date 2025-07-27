# Dataset Service Consolidation Plan

## Current State Analysis

Currently, the dataset functionality is split across 8 separate handler files:
1. `CreateDatasetHandler` - Creates new datasets
2. `CreateDatasetWithFileHandler` - Creates dataset with initial file upload
3. `GetDatasetHandler` - Retrieves dataset details
4. `ListDatasetsHandler` - Lists datasets for a user
5. `UpdateDatasetHandler` - Updates dataset metadata
6. `DeleteDatasetHandler` - Deletes datasets
7. `GrantPermissionHandler` - Manages dataset permissions
8. `CheckDatasetReadyHandler` - Checks dataset import status

### Common Patterns Identified:
- All handlers inherit from `BaseHandler`
- All use dependency injection for repositories and services
- All use commands for input
- All have similar permission checking patterns
- All follow similar transaction management patterns

## Proposed Solution: Consolidated DatasetService

### Benefits of Consolidation:
1. **Reduced code duplication** - Common dependencies injected once
2. **Easier testing** - Single service to mock/test
3. **Better cohesion** - All dataset operations in one place
4. **Simplified API layer** - Single service dependency
5. **Consistent error handling** - Centralized patterns

### Proposed Structure:

```python
# src/features/datasets/services/dataset_service.py

from typing import List, Tuple, Optional, Dict, Any
from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.core.permissions import PermissionService
from src.core.events.publisher import EventBus
from ..models import *
from ...base_handler import with_transaction, with_error_handling

class DatasetService:
    """Consolidated service for all dataset operations."""
    
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
    async def create_dataset(
        self, 
        command: CreateDatasetCommand
    ) -> CreateDatasetResponse:
        """Create a new dataset."""
        # Implementation from CreateDatasetHandler
        pass
    
    @with_transaction
    @with_error_handling
    async def create_dataset_with_file(
        self,
        command: CreateDatasetWithFileCommand
    ) -> CreateDatasetWithFileResponse:
        """Create dataset with initial file."""
        # Implementation from CreateDatasetWithFileHandler
        pass
    
    @with_error_handling
    async def get_dataset(
        self,
        command: GetDatasetCommand
    ) -> DatasetDetailResponse:
        """Get dataset details."""
        # Implementation from GetDatasetHandler
        pass
    
    @with_error_handling
    async def list_datasets(
        self,
        command: ListDatasetsCommand
    ) -> Tuple[List[DatasetSummary], int]:
        """List datasets for user."""
        # Implementation from ListDatasetsHandler
        pass
    
    @with_transaction
    @with_error_handling
    async def update_dataset(
        self,
        command: UpdateDatasetCommand
    ) -> UpdateDatasetResponse:
        """Update dataset metadata."""
        # Implementation from UpdateDatasetHandler
        pass
    
    @with_transaction
    @with_error_handling
    async def delete_dataset(
        self,
        command: DeleteDatasetCommand
    ) -> DeleteDatasetResponse:
        """Delete a dataset."""
        # Implementation from DeleteDatasetHandler
        pass
    
    @with_transaction
    @with_error_handling
    async def grant_permission(
        self,
        command: GrantPermissionCommand
    ) -> GrantPermissionResponse:
        """Grant dataset permission."""
        # Implementation from GrantPermissionHandler
        pass
    
    @with_error_handling
    async def check_dataset_ready(
        self,
        command: CheckDatasetReadyCommand
    ) -> Dict[str, Any]:
        """Check dataset import status."""
        # Implementation from CheckDatasetReadyHandler
        pass
```

## Required Changes

### 1. API Layer Updates

The API endpoints would need to be updated to use the service instead of individual handlers:

```python
# Before
async def create_dataset(...):
    async with uow_factory.create() as uow:
        handler = CreateDatasetHandler(uow, ...)
        return await handler.handle(command)

# After  
async def create_dataset(...):
    async with uow_factory.create() as uow:
        service = DatasetService(uow, permissions, event_bus)
        return await service.create_dataset(command)
```

### 2. Dependency Injection

Create a service factory or provider:

```python
# src/api/dependencies.py
async def get_dataset_service(
    uow: PostgresUnitOfWork = Depends(get_uow),
    permissions: PermissionService = Depends(get_permission_service),
    event_bus: EventBus = Depends(get_event_bus)
) -> DatasetService:
    return DatasetService(uow, permissions, event_bus)
```

### 3. Migration Steps

1. **Create the new service class** with all methods
2. **Copy implementation** from handlers to service methods
3. **Update API endpoints** one by one to use service
4. **Test each endpoint** after migration
5. **Remove old handler files** once all endpoints migrated
6. **Update imports** throughout the codebase

## Considerations

### Pros:
- Cleaner, more maintainable code
- Easier to understand dataset operations
- Reduced boilerplate
- Better testability

### Cons:
- Larger service class (but well-organized)
- Initial migration effort
- Need to update all tests


## Recommendation

I recommend proceeding with the single `DatasetService` approach because:

1. Dataset operations are cohesive - they all work with the same domain
2. The service won't be overly large (~500-600 lines)
3. It simplifies the API layer significantly
4. It follows common patterns in modern web frameworks

The API interface remains exactly the same - only the internal implementation changes.