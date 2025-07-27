# Jobs Service Consolidation Plan

## Current State Analysis

Currently, the jobs functionality is split across 5 separate handler files:
1. `CreateJobHandler` - Creates new jobs
2. `GetJobsHandler` - Retrieves list of jobs with filters
3. `GetJobByIdHandler` - Retrieves detailed information about a specific job
4. `GetJobStatusHandler` - Gets status of a job
5. `CancelJobHandler` - Cancels a running job

### Key Observations:
- Most complex feature so far with 5 handlers
- Mix of handler patterns - some extend BaseHandler, some don't
- Job management is central to the platform (import, sampling, exploration, etc.)
- Models include commands that aren't all used (e.g., RetryJobCommand)
- No existing services directory

## Proposed Solution: Consolidated JobService

### Benefits:
1. **Reduced duplication** - Single initialization point
2. **Better cohesion** - All job operations together
3. **Easier testing** - One service to test
4. **Simplified API** - Single service dependency
5. **Consistent patterns** - Matches other service consolidations

### Proposed Structure:

```python
# src/features/jobs/services/job_service.py

from typing import Dict, Any, List, Optional
from uuid import UUID
from datetime import datetime
from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.core.permissions import PermissionService
from src.core.events.publisher import EventBus
from src.core.domain_exceptions import EntityNotFoundException, ValidationException
from ...base_handler import with_transaction, with_error_handling
from fastapi import HTTPException
from ..models import *


class JobService:
    """Consolidated service for all job operations."""
    
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
    async def create_job(
        self,
        command: CreateJobCommand
    ) -> Dict[str, Any]:
        """Create a new job."""
        # Implementation from CreateJobHandler
        pass
    
    @with_error_handling
    async def get_jobs(
        self,
        user_id: Optional[int] = None,
        dataset_id: Optional[int] = None,
        status: Optional[str] = None,
        run_type: Optional[str] = None,
        offset: int = 0,
        limit: int = 100,
        current_user_id: int = None
    ) -> Dict[str, Any]:
        """Get list of jobs with optional filters."""
        # Implementation from GetJobsHandler
        pass
    
    @with_error_handling
    async def get_job_by_id(
        self,
        job_id: UUID,
        current_user_id: int
    ) -> Optional[Dict[str, Any]]:
        """Get detailed information about a specific job."""
        # Implementation from GetJobByIdHandler
        pass
    
    @with_error_handling
    async def get_job_status(
        self,
        job_id: UUID,
        current_user_id: int
    ) -> Dict[str, Any]:
        """Get status of a job."""
        # Implementation from GetJobStatusHandler
        pass
    
    @with_transaction
    @with_error_handling
    async def cancel_job(
        self,
        job_id: UUID,
        current_user_id: int
    ) -> Dict[str, Any]:
        """Cancel a running job."""
        # Implementation from CancelJobHandler
        pass
```

## Implementation Steps

### 1. Create Service Structure
```bash
mkdir -p src/features/jobs/services
touch src/features/jobs/services/__init__.py
touch src/features/jobs/services/job_service.py
```

### 2. Migrate Handler Logic
- Copy each handler's logic into corresponding service methods
- Remove handler class boilerplate
- Standardize error handling and permissions checking
- Ensure consistent return types

### 3. Update API Endpoints
Transform each endpoint from:
```python
handler = GetJobsHandler(uow, permissions=permission_service)
result = await handler.handle(...)
```

To:
```python
service = JobService(uow, permissions=permission_service)
result = await service.get_jobs(...)
```

### 4. Update Dependencies
The service will need access to:
- `PostgresUnitOfWork` 
- `PermissionService`
- `EventBus` (optional, for job events)

### 5. Clean Up
- Remove old handler files
- Update imports throughout codebase
- Update module exports
- Clean up unused commands in models

## Special Considerations

### 1. Handler Pattern Inconsistency
- Some handlers extend BaseHandler, others don't
- Standardize all methods to use decorators consistently
- Ensure proper transaction handling for write operations

### 2. Permission Checks
- Currently mixed between handlers and API layer
- Standardize permission checking within service methods
- Use permission service consistently

### 3. Event Publishing
- Jobs are prime candidates for events (created, started, completed, failed)
- Consider adding event publishing for job state changes
- Keep optional for now to match current functionality

### 4. Model Cleanup
- Check if all commands in models are actually used
- Remove unused commands like RetryJobCommand if not implemented

## Migration Checklist

- [ ] Create services directory structure
- [ ] Create JobService class
- [ ] Migrate create_job logic
- [ ] Migrate get_jobs logic with filters
- [ ] Migrate get_job_by_id logic
- [ ] Migrate get_job_status logic
- [ ] Migrate cancel_job logic
- [ ] Update all API endpoints in `src/api/jobs.py`
- [ ] Check for job handlers usage in other modules
- [ ] Remove old handler files
- [ ] Update handler exports in `handlers/__init__.py`
- [ ] Clean up unused commands in models
- [ ] Test all endpoints
- [ ] Run uvicorn and fix any import errors

## Common Pitfalls to Avoid

1. **Permission Consistency**: Ensure all methods check permissions appropriately
2. **Transaction Boundaries**: Use @with_transaction for write operations
3. **Filter Logic**: Preserve all filtering logic from GetJobsHandler
4. **SOEID Lookup**: Keep the SOEID to user_id conversion in API layer
5. **Response Format**: Maintain the same response structure for compatibility

## Expected Outcome

After consolidation:
- Single `JobService` class with 5 methods
- Cleaner API endpoints
- Consistent permission checking
- Better testability
- Consistent with other service patterns
- No functional changes - API remains the same

## Testing Commands

After implementation:
```bash
# Check Python syntax
python3 -m py_compile src/api/jobs.py src/features/jobs/services/job_service.py

# Run server
python3 -m uvicorn src.main:app --reload

# Test health endpoint
curl http://localhost:8000/health

# Test job listing
curl http://localhost:8000/jobs
```

## Additional Notes

The jobs feature is central to the platform as it manages:
- Import jobs
- Sampling jobs
- Exploration jobs
- SQL transformation jobs

This consolidation will provide a clean, consistent interface for all job-related operations. Consider future enhancements like:
- Job retry functionality
- Job priority queues
- Job dependencies
- Batch job operations

But keep the initial consolidation focused on preserving existing functionality.