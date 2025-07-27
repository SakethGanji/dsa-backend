# Versioning and Refs Consolidation Plan

## Current State Analysis

### Versioning Feature
Currently has 11 handlers across different concerns:
1. **Commit Operations**:
   - `CreateCommitHandler` - Creates new commits
   - `GetCommitHistoryHandler` - Retrieves commit history
   - `GetCommitSchemaHandler` - Gets schema for a commit
   - `CheckoutCommitHandler` - Checks out data at a specific commit
2. **Data Operations**:
   - `GetDataAtRefHandler` - Gets data at a specific ref
   - `GetTableDataHandler` - Gets table data
   - `ListTablesHandler` - Lists tables in a commit
   - `GetTableSchemaHandler` - Gets schema for a table
   - `GetTableAnalysisHandler` - Analyzes table statistics
3. **Overview**:
   - `GetDatasetOverviewHandler` - Gets dataset overview
4. **Import**:
   - `QueueImportJobHandler` - Queues import jobs

### Refs Feature
Currently has 3 handlers:
1. `CreateBranchHandler` - Creates new branches (refs)
2. `DeleteBranchHandler` - Deletes branches
3. `ListRefsHandler` - Lists all refs for a dataset

### Key Observations:
- **Conceptual Overlap**: Refs are fundamentally part of versioning (pointers to commits)
- **API Integration**: The versioning API already imports ref handlers (line 23 in versioning.py)
- **Model Location**: The Ref model is already in `/src/features/versioning/models/ref.py`
- **Repository Pattern**: PostgresCommitRepository handles both commits AND refs
- **Tight Coupling**: GetDataAtRefHandler in versioning depends on refs concept

## Proposed Solution: Unified VersioningService

### Benefits:
1. **Conceptual Integrity** - Refs and commits belong together in version control
2. **Reduced Complexity** - Single service for all versioning operations
3. **Better Cohesion** - Related functionality in one place
4. **Simplified Imports** - No cross-feature dependencies
5. **Consistent Patterns** - Matches other consolidated services

### Proposed Structure:

```python
# src/features/versioning/services/versioning_service.py

from typing import Dict, Any, List, Optional
from uuid import UUID
from datetime import datetime
from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.core.permissions import PermissionService
from src.core.events.publisher import EventBus
from ...base_handler import with_transaction, with_error_handling
from ..models import *


class VersioningService:
    """Consolidated service for all versioning operations including refs."""
    
    def __init__(
        self,
        uow: PostgresUnitOfWork,
        permissions: PermissionService,
        event_bus: Optional[EventBus] = None
    ):
        self._uow = uow
        self._permissions = permissions
        self._event_bus = event_bus
        self._commit_repo = uow.commits
        self._table_reader = uow.table_reader if hasattr(uow, 'table_reader') else None
    
    # Commit Operations
    @with_transaction
    @with_error_handling
    async def create_commit(self, command: CreateCommitCommand) -> CreateCommitResponse:
        """Create a new commit."""
        pass
    
    @with_error_handling
    async def get_commit_history(self, dataset_id: int, user_id: int, ref_name: Optional[str] = None, 
                                offset: int = 0, limit: int = 20) -> Dict[str, Any]:
        """Get commit history for a dataset."""
        pass
    
    @with_error_handling
    async def get_commit_schema(self, dataset_id: int, commit_id: str, user_id: int) -> Dict[str, Any]:
        """Get schema for a specific commit."""
        pass
    
    @with_error_handling
    async def checkout_commit(self, dataset_id: int, commit_id: str, user_id: int,
                            table_key: str = "primary", offset: int = 0, limit: int = 100) -> Dict[str, Any]:
        """Checkout data at a specific commit."""
        pass
    
    # Ref Operations (formerly in refs feature)
    @with_transaction
    @with_error_handling
    async def create_branch(self, dataset_id: int, branch_name: str, from_ref: str, user_id: int) -> Dict[str, Any]:
        """Create a new branch (ref)."""
        pass
    
    @with_transaction
    @with_error_handling
    async def delete_branch(self, dataset_id: int, branch_name: str, user_id: int) -> Dict[str, Any]:
        """Delete a branch."""
        pass
    
    @with_error_handling
    async def list_refs(self, dataset_id: int, user_id: int) -> Dict[str, Any]:
        """List all refs for a dataset."""
        pass
    
    # Data Operations
    @with_error_handling
    async def get_data_at_ref(self, dataset_id: int, ref_name: str, user_id: int,
                            table_key: str = "primary", offset: int = 0, limit: int = 100) -> Dict[str, Any]:
        """Get data at a specific ref."""
        pass
    
    @with_error_handling
    async def get_table_data(self, dataset_id: int, commit_id: str, table_key: str,
                           user_id: int, offset: int = 0, limit: int = 100) -> Dict[str, Any]:
        """Get table data from a commit."""
        pass
    
    @with_error_handling
    async def list_tables(self, dataset_id: int, commit_id: str, user_id: int) -> Dict[str, Any]:
        """List all tables in a commit."""
        pass
    
    @with_error_handling
    async def get_table_schema(self, dataset_id: int, commit_id: str, table_key: str, user_id: int) -> Dict[str, Any]:
        """Get schema for a specific table."""
        pass
    
    @with_error_handling
    async def get_table_analysis(self, dataset_id: int, commit_id: str, table_key: str, user_id: int) -> Dict[str, Any]:
        """Analyze table statistics."""
        pass
    
    # Overview and Import
    @with_error_handling
    async def get_dataset_overview(self, dataset_id: int, user_id: int) -> Dict[str, Any]:
        """Get overview of a dataset."""
        pass
    
    @with_transaction
    @with_error_handling
    async def queue_import_job(self, command: QueueImportJobCommand) -> Dict[str, Any]:
        """Queue an import job."""
        pass
```

## Implementation Steps

### 1. Create Service Structure
```bash
mkdir -p src/features/versioning/services
touch src/features/versioning/services/__init__.py
touch src/features/versioning/services/versioning_service.py
```

### 2. Migrate Handlers
- Start with versioning handlers (11 methods)
- Then migrate ref handlers (3 methods)
- Total: 14 methods in the consolidated service

### 3. Move Ref Models
- Move `/src/features/refs/models/commands.py` content to `/src/features/versioning/models/commands.py`
- Update imports in models/__init__.py

### 4. Update API Endpoints
- Update all endpoints in `src/api/versioning.py`
- Remove the cross-feature import from refs (line 23)
- All endpoints will use the unified VersioningService

### 5. Clean Up
- Remove entire `/src/features/refs` directory
- Update any imports in other modules
- Clean up handlers in versioning feature

## Special Considerations

### 1. Table Analysis Service
- Some handlers use TableAnalysisService from core services
- Pass it as optional dependency where needed
- Keep the separation of concerns

### 2. Complex Data Operations
- GetTableDataHandler has significant logic for data retrieval
- Preserve all the data formatting and pagination logic
- Consider breaking into smaller private methods

### 3. Event Publishing
- Several operations should publish events (commit created, branch created/deleted)
- Keep event publishing optional but available

### 4. Permission Patterns
- Standardize permission checking across all methods
- Most operations require read permission
- Create/delete operations require write permission

## Migration Checklist

- [ ] Create services directory and versioning_service.py
- [ ] Create VersioningService class with all 14 methods
- [ ] Migrate all versioning handler logic (11 handlers)
- [ ] Migrate all ref handler logic (3 handlers)
- [ ] Move ref commands to versioning models
- [ ] Update models/__init__.py exports
- [ ] Update all endpoints in versioning.py API
- [ ] Remove refs feature directory entirely
- [ ] Update any external imports of refs
- [ ] Remove old handler files
- [ ] Test all endpoints
- [ ] Run uvicorn and fix any issues

## Common Pitfalls to Avoid

1. **Model Migration**: Don't forget to move ref-related commands and models
2. **Repository Access**: The commit repository already handles refs - use it
3. **Import Cleanup**: Remove all refs imports from other modules
4. **API Consistency**: Keep the same API interface for compatibility
5. **Permission Checks**: Maintain consistent permission patterns

## Expected Outcome

After consolidation:
- Single `VersioningService` with 14 methods covering all versioning and ref operations
- No more `/src/features/refs` directory
- Cleaner imports in API layer
- Better conceptual alignment (refs are part of versioning)
- Consistent with other service consolidations
- No functional changes - API remains the same

## Testing Commands

```bash
# Check Python syntax
python3 -m py_compile src/api/versioning.py src/features/versioning/services/versioning_service.py

# Run server
python3 -m uvicorn src.main:app --reload

# Test endpoints
curl http://localhost:8000/datasets/1/refs  # List refs
curl http://localhost:8000/datasets/1/commits  # List commits
```

## Benefits Summary

1. **Conceptual Clarity**: Refs and commits belong together
2. **Reduced Complexity**: 14 handlers → 1 service
3. **Better Organization**: No cross-feature dependencies
4. **Easier Maintenance**: Single place for all versioning logic
5. **Consistent Architecture**: Follows the established service pattern