# SQL Workbench Service Consolidation Plan

## Current State Analysis

Currently, the SQL workbench functionality is split across 2 separate handler files:
1. `PreviewSqlHandler` - Previews SQL query results on sample data
2. `TransformSqlHandler` - Creates SQL transformation jobs for asynchronous processing

### Key Observations:
- Only 2 handlers (simplest consolidation so far)
- Both handlers already use a core `WorkbenchService` from `src.services.workbench_service`
- Handlers follow the same pattern as other features
- Models are well-organized with request/response types
- Services directory already exists but is empty

### Important Discovery:
There's already a `WorkbenchService` in `src/services/workbench_service.py` that appears to handle the core SQL workbench logic. The handlers are thin wrappers around this service.

## Proposed Solution: Consolidated SqlWorkbenchService

### Benefits:
1. **Reduced duplication** - Single initialization point
2. **Better cohesion** - All SQL workbench operations together
3. **Easier testing** - One service to test
4. **Simplified API** - Single service dependency
5. **Consistent patterns** - Matches DatasetService, SamplingService, and ExplorationService

### Proposed Structure:

```python
# src/features/sql_workbench/services/sql_workbench_service.py

from typing import Dict, Any, Optional
from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.core.permissions import PermissionService
from src.services.workbench_service import WorkbenchService
from ...base_handler import with_transaction, with_error_handling
from ..models import (
    SqlPreviewRequest, SqlPreviewResponse,
    SqlTransformRequest, SqlTransformResponse
)


class SqlWorkbenchService:
    """Consolidated service for all SQL workbench operations."""
    
    def __init__(
        self,
        uow: PostgresUnitOfWork,
        permissions: PermissionService,
        workbench_service: Optional[WorkbenchService] = None
    ):
        self._uow = uow
        self._permissions = permissions
        # Create WorkbenchService if not provided
        self._workbench = workbench_service or WorkbenchService()
    
    @with_error_handling
    async def preview_sql(
        self,
        request: SqlPreviewRequest,
        user_id: int
    ) -> SqlPreviewResponse:
        """Preview SQL query results on sample data."""
        # Implementation from PreviewSqlHandler
        pass
    
    @with_transaction
    @with_error_handling
    async def transform_sql(
        self,
        request: SqlTransformRequest,
        user_id: int
    ) -> SqlTransformResponse:
        """Create SQL transformation job for asynchronous processing."""
        # Implementation from TransformSqlHandler
        pass
```

## Implementation Steps

### 1. Create Service File
```bash
touch src/features/sql_workbench/services/sql_workbench_service.py
```

### 2. Migrate Handler Logic
- Copy each handler's logic into corresponding service methods
- Remove handler class boilerplate
- Adjust imports as needed
- Keep the existing WorkbenchService dependency

### 3. Update API Endpoints
Transform each endpoint from:
```python
handler = PreviewSqlHandler(uow, workbench_service, permissions=permission_service)
return await handler.handle(request, current_user.user_id)
```

To:
```python
service = SqlWorkbenchService(uow, permissions=permission_service, workbench_service=workbench_service)
return await service.preview_sql(request, current_user.user_id)
```

### 4. Update Dependencies
The service will need access to:
- `PostgresUnitOfWork` 
- `PermissionService`
- `WorkbenchService` (from core services)

### 5. Clean Up
- Remove old handler files
- Update imports throughout codebase
- Update module exports

## Special Considerations

### 1. Core WorkbenchService Integration
- The handlers already use a core `WorkbenchService` - keep this dependency
- Make it optional in constructor with lazy initialization
- This maintains separation between feature-specific logic and core SQL execution

### 2. Import Fixes (Lessons from Sampling)
- Keep using the existing `WorkbenchService` from `src.services`
- Don't create duplicate services
- Ensure all model imports are correct

### 3. Response Types
- Response types are already in the models module - keep them there
- No need to move them to the service

### 4. Model Structure
- Models are already well-organized - no changes needed
- Request/Response types are properly separated

## Migration Checklist

- [ ] Create sql_workbench_service.py in services directory
- [ ] Create SqlWorkbenchService class
- [ ] Migrate preview_sql logic from handler
- [ ] Migrate transform_sql logic from handler
- [ ] Update both API endpoints in `src/api/workbench.py`
- [ ] Update services/__init__.py with exports
- [ ] Remove old handler files
- [ ] Update handler exports in `handlers/__init__.py`
- [ ] Test both endpoints
- [ ] Run uvicorn and fix any import errors

## Common Pitfalls to Avoid

1. **Core Service**: Don't remove the WorkbenchService dependency - it's legitimate
2. **Import Paths**: Ensure correct imports for the core WorkbenchService
3. **Model Exports**: Models are already correct - don't change them
4. **API Dependencies**: Remember to pass workbench_service to the new service

## Expected Outcome

After consolidation:
- Single `SqlWorkbenchService` class with 2 methods
- Cleaner API endpoints
- Better testability
- Consistent with other service patterns
- Maintains integration with core WorkbenchService
- No functional changes - API remains the same

## Testing Commands

After implementation:
```bash
# Check Python syntax
python3 -m py_compile src/api/workbench.py src/features/sql_workbench/services/sql_workbench_service.py

# Run server
python3 -m uvicorn src.main:app --reload

# Test health endpoint
curl http://localhost:8000/health
```

## Additional Notes

This is the simplest consolidation with only 2 handlers. The existing structure is already quite clean with:
- Well-organized models
- Clear separation between feature handlers and core service
- Existing services directory ready for use

The main benefit is consistency with other features and slightly cleaner API endpoints.