# Codebase Improvement Plan - Incremental Implementation Guide

## Executive Summary

This document provides a step-by-step plan to incrementally improve the codebase by addressing DRY violations, implementing consistent vertical slice architecture, and adding missing abstractions. Each phase is designed to be implemented independently with minimal disruption to existing functionality.

## Architecture Strengths (Current State)

- ✅ Well-defined interface layer in `/core/abstractions/`
- ✅ Good use of Repository and Unit of Work patterns
- ✅ Partial vertical slice architecture implementation
- ✅ Clean separation between infrastructure and core logic

## Implementation Phases

### Phase 1: Standardize Transaction Management (Week 1-2)

**Goal**: Eliminate manual transaction handling and use consistent patterns

#### Step 1.1: Update Non-BaseHandler Classes
**Priority**: High  
**Effort**: 2-3 days  
**Files to modify**:
- `/src/features/datasets/create_dataset.py`
- `/src/features/datasets/grant_permission.py`
- `/src/features/users/create_user.py`
- `/src/features/jobs/process_import_job.py`

**Actions**:
1. Make these handlers inherit from `BaseHandler`
2. Replace manual transaction code with `@with_transaction` decorator
3. Test each handler after modification

#### Step 1.2: Standardize Transaction Patterns
**Priority**: High  
**Effort**: 1-2 days  
**Files to modify**:
- All handlers using manual `begin/commit/rollback`
- All handlers using context manager pattern

**Actions**:
1. Choose `@with_transaction` as the standard pattern
2. Update all handlers to use this pattern
3. Document the standard in a `CODING_STANDARDS.md` file

### Phase 2: Unify Permission Checking (Week 2-3)

**Goal**: Create a single, consistent permission checking mechanism

#### Step 2.1: Create Permission Decorator
**Priority**: High  
**Effort**: 1 day  
**New file**: `/src/core/decorators/permissions.py`

```python
def require_permission(permission_type: str):
    """Decorator for handler-level permission checking"""
    def decorator(func):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            # Implementation here
            pass
        return wrapper
    return decorator
```

#### Step 2.2: Remove Inline Permission Checks
**Priority**: High  
**Effort**: 2-3 days  
**Files to modify**:
- `/src/api/exploration.py`
- `/src/api/sampling.py`
- `/src/api/datasets_enhanced.py`
- All other files with inline permission checks

**Actions**:
1. Replace inline checks with middleware dependencies
2. Remove duplicate permission checking code
3. Consolidate repository methods (`check_user_permission` vs `user_has_permission`)

### Phase 3: Centralize Error Handling (Week 3-4)

**Goal**: Eliminate duplicate error handling code

#### Step 3.1: Create Error Handling Middleware
**Priority**: Medium  
**Effort**: 1-2 days  
**New file**: `/src/core/middleware/error_handler.py`

**Actions**:
1. Create centralized error mapping (ValueError → 400, PermissionError → 403, etc.)
2. Standardize error response format
3. Add logging for all errors

#### Step 3.2: Apply Error Handling Decorator
**Priority**: Medium  
**Effort**: 2-3 days  

**Actions**:
1. Add `@with_error_handling` to all handlers
2. Remove manual try-catch blocks from API endpoints
3. Create custom exception classes for common scenarios

### Phase 4: Eliminate API Layer Duplication (Week 4-5)

**Goal**: Remove 500-700 lines of duplicate code

#### Step 4.1: Centralize Dependency Injection
**Priority**: High  
**Effort**: 1 day  
**File to modify**: `/src/core/dependencies.py`

**Actions**:
1. Move all `get_db_pool()`, `get_uow_factory()` functions to dependencies.py
2. Remove duplicate definitions from all API files
3. Update imports across the codebase

#### Step 4.2: Create Common API Utilities
**Priority**: Medium  
**Effort**: 2-3 days  
**New file**: `/src/api/common/utils.py`

**Components to create**:
- `PaginationParams` class
- `@paginated` decorator
- `ResponseBuilder` class
- Common validation functions

#### Step 4.3: Extract Repeated Patterns
**Priority**: Medium  
**Effort**: 3-4 days  

**Patterns to extract**:
- File upload handling → `FileUploadService`
- CSV export logic → `DataExportService`
- Search index refresh → Middleware or event
- Tag handling → `TagService`

### Phase 5: Complete Vertical Slice Architecture (Week 5-7)

**Goal**: Move all business logic from API layer to feature handlers

#### Step 5.1: Refactor datasets.py
**Priority**: High  
**Effort**: 3-4 days  

**Actions**:
1. Extract `create_dataset_with_file` logic (147 lines) to handler
2. Create missing handlers:
   - `UpdateDatasetHandler`
   - `DeleteDatasetHandler`
   - `ListDatasetsHandler`
3. API endpoints should only handle HTTP concerns

#### Step 5.2: Create Missing Feature Handlers
**Priority**: Medium  
**Effort**: 3-4 days  

**Handlers to create**:
- Job management handlers
- User management handlers (beyond create)
- Dataset metadata handlers

#### Step 5.3: Ensure Feature Independence
**Priority**: Medium  
**Effort**: 2-3 days  

**Actions**:
1. Review cross-feature dependencies
2. Implement domain events for cross-cutting concerns
3. Document feature boundaries

### Phase 6: Add Critical Abstractions (Week 7-9)

**Goal**: Improve testability and flexibility

#### Step 6.1: Abstract External Dependencies
**Priority**: High  
**Effort**: 2-3 days  

**Interfaces to create**:
```python
# /src/core/abstractions/external.py
class IConnectionPool(ABC):
    """Database connection abstraction"""
    
class IAuthenticationService(ABC):
    """JWT/Auth abstraction"""
    
class IConfigurationProvider(ABC):
    """Configuration abstraction"""
```

#### Step 6.2: Create Service Abstractions
**Priority**: Medium  
**Effort**: 3-4 days  

**Services to abstract**:
- `IFileStorageService` - Abstract file system operations
- `ICacheService` - Add caching capability
- `IJobQueue` - Abstract job processing
- `IEventBus` - Enable event-driven architecture

#### Step 6.3: Implement Adapters
**Priority**: Medium  
**Effort**: 4-5 days  

**Actions**:
1. Create concrete implementations for each interface
2. Update dependency injection to use interfaces
3. Add unit tests using mock implementations

### Phase 7: Implement Pagination Consistency (Week 9-10)

**Goal**: Standardize pagination across all endpoints

#### Step 7.1: Use PaginationMixin Everywhere
**Priority**: Low  
**Effort**: 2-3 days  

**Actions**:
1. Apply `PaginationMixin` to all list endpoints
2. Standardize limit constraints (1-1000)
3. Create consistent response format

#### Step 7.2: Add Missing Pagination
**Priority**: Low  
**Effort**: 1-2 days  

**Endpoints needing pagination**:
- `/datasets/{dataset_id}/refs`
- Other list endpoints returning unbounded results

## Implementation Guidelines

### For Each Phase:

1. **Before Starting**:
   - Create a feature branch
   - Review affected code
   - Write/update tests

2. **During Implementation**:
   - Make incremental commits
   - Run tests frequently
   - Update documentation

3. **After Completion**:
   - Code review
   - Integration testing
   - Update team on changes

### Success Metrics

- ✅ Reduced code duplication by 500+ lines
- ✅ Consistent use of decorators and patterns
- ✅ All handlers follow BaseHandler pattern
- ✅ Business logic contained within feature slices
- ✅ External dependencies behind interfaces
- ✅ Improved test coverage and maintainability

## Quick Wins (Can be done immediately)

1. **Use existing decorators** - Start using `@with_transaction` and `@with_error_handling` on new code immediately
2. **Stop adding to datasets.py** - Any new dataset functionality goes in handlers
3. **Centralize new dependencies** - Add any new dependency injection to `core/dependencies.py`
4. **Follow patterns in new code** - Use `BaseHandler` for all new handlers

## Long-term Vision

After completing all phases, the codebase will have:
- Clean vertical slice architecture
- Minimal code duplication
- Consistent patterns throughout
- High testability with proper abstractions
- Clear separation of concerns
- Easy onboarding for new developers

## Notes

- Each phase can be implemented independently
- Phases can be reordered based on team priorities
- Consider implementing phases in parallel with different team members
- Regular refactoring sprints can tackle 1-2 phases each