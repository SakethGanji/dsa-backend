# Complete Migration Plan to New Patterns

## Executive Summary
This plan outlines the complete migration of the codebase to use the new patterns, organized by risk level and dependencies. The migration is designed to be done incrementally with validation at each step.

## Phase 1 Status: ✅ COMPLETED
Phase 1 has been successfully completed with:
- ✅ All pagination imports updated (8 files)
- ✅ Domain exceptions implemented (11 files) 
- ✅ Error handler registration in main.py
- ✅ New modules created: domain_exceptions.py, error_handlers.py, common/pagination.py

## Migration Phases Overview

### Phase 1: Zero-Risk Updates (Day 1-2)
- Import updates only
- No logic changes
- Easily reversible

### Phase 2: Low-Risk Updates (Day 3-5)
- Error handling updates
- Response model inheritance
- API error registration

### Phase 3: Medium-Risk Updates (Week 2)
- Simple CRUD handler migrations
- Repository base class adoption
- Delete operation standardization

### Phase 4: High-Risk Updates (Week 3)
- Complex handler refactoring
- Custom repository migrations
- Breaking changes

### Phase 5: Cleanup (Week 4)
- Remove deprecated code
- Update documentation
- Performance optimization

---

## Phase 1: Zero-Risk Import Updates (Day 1-2)

### 1.1 Pagination Import Updates
**Risk Level:** Zero (import change only)

**Files to Update:**
```bash
src/features/versioning/checkout_commit.py
src/features/versioning/get_commit_history.py
src/features/versioning/get_data_at_ref.py
src/features/versioning/get_table_data.py
src/features/sampling/get_job_data.py
src/features/sampling/get_sampling_history.py
```

**Update Pattern:**
```python
# FROM:
from src.features.base_handler import BaseHandler, PaginationMixin
# OR
from src.api.common import PaginationMixin

# TO:
from src.features.base_handler import BaseHandler
from src.core.common.pagination import PaginationMixin
```

### 1.2 Base Handler Import Preparation
**Add imports but don't use yet:**
```python
# Add to files that will use base classes later:
from src.features.base_update_handler import BaseUpdateHandler
from src.core.domain_exceptions import (
    EntityNotFoundException,
    ValidationException,
    ForbiddenException
)
```

---

## Phase 2: Low-Risk Error & Response Updates (Day 3-5) - UPDATED

### 2.1 ✅ Domain Exception Replacements - MOSTLY COMPLETE
**Status:** Many handlers already updated in Phase 1!

**Already Completed:**
- ✅ delete_dataset.py, update_dataset.py, delete_user.py, update_user.py  
- ✅ cancel_job.py, get_job_status.py, process_import_job.py
- ✅ delete_branch.py, get_dataset_overview.py, login_user.py

**Still Need to Search For:**
```python
# REPLACE ALL:
raise ValueError(f"Dataset {id} not found")
# WITH:
raise EntityNotFoundException("Dataset", id)

# REPLACE ALL:
raise ValueError(f"User {id} not found")  
# WITH:
raise EntityNotFoundException("User", id)

# REPLACE ALL:
raise ValueError(f"Job {id} not found")
# WITH:
raise EntityNotFoundException("Job", id)

# REPLACE ALL:
raise ValueError("Permission denied")
# WITH:
raise ForbiddenException()

# REPLACE ALL:
raise ValueError("Invalid input")
# WITH:
raise ValidationException("Invalid input")
```

**Files to Update (Priority Order):**
1. `src/features/datasets/delete_dataset.py`
2. `src/features/users/update_user.py`
3. `src/features/users/delete_user.py`
4. `src/features/versioning/checkout_commit.py`
5. `src/features/refs/create_branch.py`
6. `src/features/refs/delete_branch.py`
7. `src/features/jobs/cancel_job.py`
8. `src/features/datasets/grant_permission.py`

### 2.2 Response Model Updates - DIRECT REPLACEMENT
**Approach:** Replace immediately, no versions

**Direct Update in pydantic_models.py:**
```python
# In src/models/pydantic_models.py:
from src.models.base_models import (
    BaseDatasetModel, BaseUserModel, BaseJobModel,
    BaseDeleteResponse, BasePaginatedResponse
)

# REPLACE old model completely:
class DatasetSummary(BaseDatasetModel):  # Now inherits from base
    created_by: int
    permission_type: str
    import_status: Optional[str] = None
    import_job_id: Optional[str] = None
    # Remove duplicate fields that are in base

# Do same for ALL models - no V2, just replace
```

**Update ALL Endpoints Immediately:**
```python
# Find all usages and update at once
# No gradual rollout - complete replacement
```

---

## Phase 3: Medium-Risk Handler & Repository Updates (Week 2)

### 3.1 Update Simple CRUD Handlers

**Handlers to Migrate to Base Classes:**

#### Update Handlers (use BaseUpdateHandler):
1. **UpdateUserHandler** (`src/features/users/update_user.py`)
   - Very similar to UpdateDatasetHandler
   - Clear update pattern
   - Good test case

2. **UpdateDatasetHandler** (`src/features/datasets/update_dataset.py`)
   - Already have refactored example
   - Just needs implementation

**Migration Steps:**
```python
# Step 1: Backup original
cp update_user.py update_user_backup.py

# Step 2: Refactor to use BaseUpdateHandler
class UpdateUserHandler(BaseUpdateHandler[UpdateUserCommand, UpdateUserResponse, Dict[str, Any]]):
    def get_entity_id(self, command) -> int:
        return command.target_user_id
    
    def get_entity_name(self) -> str:
        return "User"
    
    # ... implement other abstract methods

# Step 3: Test thoroughly
# Step 4: Remove backup after validation
```

#### Delete Handlers (standardize pattern):
1. **DeleteDatasetHandler**
2. **DeleteUserHandler** 
3. **DeleteBranchHandler**

**Standardize to:**
```python
async def handle(self, command: DeleteCommand) -> DeleteResponse:
    # Check exists
    entity = await self._repo.get_by_id(command.id)
    if not entity:
        raise EntityNotFoundException(self.entity_type, command.id)
    
    # Delete
    success = await self._repo.delete(command.id)
    
    # Return standardized response
    return DeleteResponse(
        entity_type=self.entity_type,
        entity_id=command.id
    )
```

### 3.2 Repository Updates

**Repositories to Extend BasePostgresRepository:**

1. **PostgresUserRepository** (`src/core/infrastructure/postgres/user_repo.py`)
   - Remove: `get_by_id`, `exists`, `delete` (inherited)
   - Keep: `get_by_soeid`, `create_user` (custom)

2. **PostgresDatasetRepository** (if it has standard CRUD)
   - Inherit common methods
   - Keep domain-specific queries

**Migration Example:**
```python
# FROM:
class PostgresUserRepository(IUserRepository):
    async def get_by_id(self, user_id: int):
        # 15 lines of code
    
    async def exists(self, user_id: int):
        # 10 lines of code

# TO:
class PostgresUserRepository(BasePostgresRepository[int], IUserRepository):
    def __init__(self, connection):
        super().__init__(connection, "users", "id", int)
    
    # get_by_id and exists are now inherited!
    # Only implement custom methods
```

---

## Phase 4: High-Risk Complex Updates (Week 3)

### 4.1 Complex Handlers (Keep Custom)

**DO NOT MIGRATE These Handlers:**
- `QueueImportJobHandler` - Complex workflow
- `ProcessImportJobHandler` - Multi-step processing
- `CreateDatasetWithFileHandler` - File handling
- `PreviewSqlHandler` - SQL execution
- `TransformSqlHandler` - SQL transformation
- `CreateCommitHandler` - Complex versioning
- `SearchDatasetsHandler` - Full-text search
- `CalculateTableStatisticsHandler` - Pure computation

**Why:** These have unique business logic that doesn't fit CRUD patterns

### 4.2 Complex Repositories (Keep Custom)

**DO NOT MIGRATE These Repositories:**
- `PostgresCommitRepository` - Complex versioning queries
- `PostgresJobRepository` - Queue-specific operations  
- `TableReader` - Streaming and specialized access

---

## Phase 5: Cleanup & Optimization (Week 4)

### 5.1 Remove Deprecated Code
1. Remove old PaginationMixin from base_handler.py
2. Remove old error handlers from main.py
3. Remove V1 response models after migration
4. Clean up unused imports

### 5.2 Performance Optimization
1. Add database indexes for new query patterns
2. Optimize bulk operations in base repository
3. Add caching where beneficial

### 5.3 Documentation Updates
1. Update API documentation
2. Create migration guide for future entities
3. Update README with new patterns

---

## Implementation Scripts

### Script 1: Bulk Pagination Update
```bash
#!/bin/bash
# update_pagination_imports.sh

FILES=$(find ./src -name "*.py" -type f | xargs grep -l "from src.api.common import PaginationMixin\|from src.features.base_handler import.*PaginationMixin")

for file in $FILES; do
    echo "Updating $file"
    # Backup
    cp "$file" "${file}.bak"
    
    # Update imports
    sed -i 's/from src.api.common import PaginationMixin/from src.core.common.pagination import PaginationMixin/g' "$file"
    sed -i 's/from src.features.base_handler import BaseHandler, PaginationMixin/from src.features.base_handler import BaseHandler\nfrom src.core.common.pagination import PaginationMixin/g' "$file"
done
```

### Script 2: Find and Replace Errors
```python
#!/usr/bin/env python3
# update_exceptions.py

import os
import re
from pathlib import Path

REPLACEMENTS = [
    (r'raise ValueError\(f?"Dataset \{(\w+)\} not found"?\)', r'raise EntityNotFoundException("Dataset", \1)'),
    (r'raise ValueError\(f?"User \{(\w+)\} not found"?\)', r'raise EntityNotFoundException("User", \1)'),
    (r'raise ValueError\(f?"Job \{(\w+)\} not found"?\)', r'raise EntityNotFoundException("Job", \1)'),
    (r'raise ValueError\("Permission denied"\)', r'raise ForbiddenException()'),
]

def update_file(filepath):
    with open(filepath, 'r') as f:
        content = f.read()
    
    original = content
    for pattern, replacement in REPLACEMENTS:
        content = re.sub(pattern, replacement, content)
    
    if content != original:
        # Add import if needed
        if 'EntityNotFoundException' in content and 'from src.core.domain_exceptions' not in content:
            import_line = 'from src.core.domain_exceptions import EntityNotFoundException, ForbiddenException\n'
            # Add after last import
            lines = content.split('\n')
            for i in range(len(lines)-1, -1, -1):
                if lines[i].startswith('import ') or lines[i].startswith('from '):
                    lines.insert(i+1, import_line)
                    break
            content = '\n'.join(lines)
        
        with open(filepath, 'w') as f:
            f.write(content)
        print(f"Updated {filepath}")

# Run on all Python files
for filepath in Path('./src').rglob('*.py'):
    if '__pycache__' not in str(filepath):
        update_file(filepath)
```

---

## Testing Strategy

### 1. Unit Test Updates
For each migrated handler:
```python
# test_update_user.py
def test_user_not_found_returns_404():
    # Old: expects ValueError
    # New: expects EntityNotFoundException
    with pytest.raises(EntityNotFoundException):
        await handler.handle(UpdateUserCommand(user_id=1, target_user_id=999))
```

### 2. Integration Testing
- Run full test suite after each phase
- Test error responses match expected format
- Verify pagination limits are consistent

### 3. Manual Testing Checklist
- [ ] All endpoints return correct status codes
- [ ] Error messages are consistent
- [ ] Pagination works with new limits
- [ ] No functionality is broken

---

## Rollback Strategy

### For Each Phase:
1. **Git branch per phase**: `feature/migration-phase-1`
2. **Backup files**: Keep `.bak` files during migration
3. **Feature flags**: For critical changes
4. **Database migrations**: Keep backward compatible

### If Issues Occur:
1. Revert git commit
2. Restore from backups
3. Re-run old test suite
4. Document issue for retry

---

## NEW: Simplified Direct Migration Approach

### Direct Replacement Strategy (No Feature Flags)
Since this is a POC with version control, we will:
- ✅ Replace code directly without feature flags
- ✅ Remove old code immediately after verification
- ✅ Use git for rollback if needed
- ✅ Complete each handler fully before moving to next

**Benefits:**
- Cleaner codebase
- No dual maintenance
- Faster migration
- Simpler testing

### Performance Monitoring Plan
```yaml
## Metrics to Track During Migration:

Response Time Metrics:
  - p50, p95, p99 latencies per endpoint
  - Compare before/after migration
  - Alert if >20% degradation

Database Metrics:
  - Query count per request
  - Connection pool usage
  - Slow query log analysis

Application Metrics:
  - Memory usage (especially with new base classes)
  - CPU utilization
  - Error rates by type

Implementation:
  - Add performance logging decorators
  - Use APM tools (DataDog, New Relic, etc.)
  - Create migration dashboard
```

### Testing Strategy Per Phase

#### Phase 2 Testing:
```python
# Test domain exception handling
def test_entity_not_found_returns_404():
    response = client.get("/api/datasets/999")
    assert response.status_code == 404
    assert response.json()["error"] == "ENTITY_NOT_FOUND"

# Test response model compatibility
def test_response_model_backward_compatible():
    # Old clients should still work with new models
    response = client.get("/api/datasets")
    assert "items" in response.json()  # Pagination still works
```

#### Phase 3 Testing:
```python
# Test base handler functionality
@pytest.mark.parametrize("handler_class", [
    UpdateUserHandler, UpdateDatasetHandler
])
def test_base_update_handler_contract(handler_class):
    # Verify all abstract methods implemented
    # Test permission checking, validation, etc.

# Performance benchmarks
def test_repository_performance():
    # Measure time for common operations
    # Compare base class vs custom implementation
```

### Communication Plan
```markdown
## Team Communication Strategy

### Before Each Phase:
1. **Kickoff Meeting** (30 min)
   - Review changes in detail
   - Discuss potential risks
   - Assign responsibilities

2. **Documentation Updates**
   - Update API docs with changes
   - Create migration guide for team
   - Update runbooks

3. **Announcement Template:**
   ```
   🚀 Migration Phase X Starting [Date]
   
   Changes:
   - [List key changes]
   
   Impact:
   - [Expected impact]
   
   Rollback Plan:
   - [How to rollback if needed]
   
   Questions: Contact [Owner]
   ```

### During Migration:
1. **Daily Standup Updates**
   - Progress report
   - Blockers identified
   - Metrics review

2. **Shared Dashboard**
   - Real-time metrics
   - Migration progress
   - Error tracking

### After Each Phase:
1. **Retrospective** (1 hour)
   - What went well
   - What could improve
   - Lessons learned

2. **Knowledge Transfer**
   - Code walkthrough
   - Pattern examples
   - Q&A session
```

### Monitoring Checklist
```markdown
## Pre-Migration Checklist:
- [ ] Baseline metrics captured
- [ ] Monitoring dashboards created
- [ ] Alerts configured
- [ ] Rollback plan tested
- [ ] Team notified

## During Migration:
- [ ] Error rate normal (±5%)
- [ ] Response times normal (±20%)
- [ ] No new exception types
- [ ] Database connections stable
- [ ] Memory usage stable

## Post-Migration:
- [ ] All tests passing
- [ ] Performance validated
- [ ] Documentation updated
- [ ] Team trained
- [ ] Metrics archived
```

---

## Success Criteria

### Phase 1 Success:
- ✅ All imports updated
- ✅ No runtime errors
- ✅ Tests still pass

### Phase 2 Success:
- ✅ Consistent error messages
- ✅ 404/403/400 status codes correct
- ✅ Response models compatible

### Phase 3 Success:
- ✅ 30%+ code reduction in migrated handlers
- ✅ Repository inheritance working
- ✅ No performance degradation

### Phase 4 Success:
- ✅ Complex handlers still work
- ✅ No forced abstractions
- ✅ Code is more maintainable

### Phase 5 Success:
- ✅ No deprecated code remains
- ✅ Documentation complete
- ✅ Team trained on patterns

---

## Simplified Timeline (Direct Migration)

**Week 1:**
- ✅ Day 1-2: Phase 1 COMPLETED
- Day 3: Phase 2 - Complete all remaining exceptions & response models
- Day 4-5: Phase 3 - Migrate all simple handlers

**Week 2:**
- Day 1-3: Complete repository migrations
- Day 4-5: Remove all deprecated code, clean up

**Total Duration:** 2 weeks (reduced from 4 weeks)

**Why Faster:**
- No feature flags = no dual code paths
- Direct replacement = no compatibility layers  
- POC environment = aggressive changes allowed
- Version control = easy rollback if needed

---

## Next Immediate Actions (Direct Migration - No Flags)

### Phase 2: Complete Immediately

1. **Find and replace ALL remaining ValueError exceptions** (30 minutes)
   ```bash
   grep -r "raise ValueError" src/ --include="*.py" | grep -v ".bak"
   # Replace ALL with appropriate domain exceptions
   ```

2. **Migrate Response Models - Direct Replacement** (2 hours)
   - Replace old models with inheritance-based models
   - Update ALL endpoints using them
   - Delete old model definitions
   - No backward compatibility needed (POC)

### Phase 3: Handler Migration (Direct Replacement)

1. **UpdateUserHandler** → BaseUpdateHandler (Day 1)
   - Copy to backup: `cp update_user.py update_user.py.bak`
   - Rewrite completely using BaseUpdateHandler
   - Test thoroughly
   - Delete backup after verification
   - Remove old code completely

2. **Delete Handlers Standardization** (Day 2)
   - DeleteDatasetHandler - standardize completely
   - DeleteUserHandler - standardize completely  
   - DeleteBranchHandler - standardize completely
   - Remove all old implementations

3. **Repository Migration** (Day 3)
   - PostgresUserRepository → Extend BasePostgresRepository
   - Remove ALL duplicate methods (get_by_id, exists, delete)
   - Keep only custom methods
   - No legacy code remains

### Aggressive Cleanup Strategy:
- No V1/V2 versions - just replace
- No deprecation warnings - direct updates
- No compatibility layers - clean breaks
- Delete .bak files after each successful migration

This plan ensures complete migration while minimizing risk and maintaining functionality throughout the process.