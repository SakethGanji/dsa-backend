# Permission Checking Patterns Analysis

## Overview
After analyzing the codebase, I've identified several patterns and inconsistencies in how permissions are checked across the application.

## Current Permission Checking Patterns

### 1. **Middleware-Based Permission Checking (Recommended Pattern)**
Located in `/src/core/authorization.py`:
- Uses FastAPI dependency injection with `PermissionChecker` class
- Pre-configured checkers: `require_dataset_read`, `require_dataset_write`, `require_dataset_admin`
- Applied at the API endpoint level using `Depends()`
- **NOTE**: Currently bypassed in POC mode (lines 61-63)

Example usage in `/src/api/datasets.py`:
```python
@router.post("/{dataset_id}/permissions", response_model=GrantPermissionResponse)
async def grant_permission(
    dataset_id: int = Path(...),
    request: GrantPermissionRequest = ...,
    current_user: CurrentUser = Depends(get_current_user_info),
    _: CurrentUser = Depends(require_dataset_admin)  # Permission check
):
```

### 2. **Inline Permission Checking (Inconsistent Pattern)**
Found in multiple API endpoints, particularly in:
- `/src/api/exploration.py`
- `/src/api/sampling.py`

Pattern:
```python
# Check permissions
has_permission = await uow.datasets.user_has_permission(
    dataset_id, current_user.user_id, "read"
)
if not has_permission:
    raise HTTPException(status_code=403, detail="No read permission on dataset")
```

### 3. **Handler-Level Permission Checking (Mixed Pattern)**
Some handlers perform their own permission checks:
- `/src/features/refs/delete_branch.py` - Checks write permission and admin role
- Others rely on middleware checks being done at API level

### 4. **Role-Based Checking**
Two patterns exist:
- Middleware functions: `require_admin_role`, `require_manager_role` 
- Model methods: `CurrentUser.is_admin()`, `CurrentUser.is_manager()`

## Inconsistencies Found

### 1. **Duplicate Permission Checking Logic**
- Some endpoints use both middleware and inline checks
- Different error messages for same permission failures
- Inconsistent HTTP status codes (403 vs 401)

### 2. **Missing Permission Checks**
Several endpoints that handle user data access lack explicit permission checks:
- Some job-related endpoints
- Some data retrieval endpoints rely only on implicit checks

### 3. **Inconsistent Permission Validation**
- Repository layer has `check_user_permission` and `user_has_permission` (duplicates)
- Some handlers check permissions, others don't
- Permission hierarchy implemented in repository but not consistently used

### 4. **POC Mode Bypass**
The main `PermissionChecker` is currently bypassed for POC mode, but inline checks are still active, creating inconsistency.

## Repeated Permission Checking Code

### Pattern 1: Inline Dataset Permission Check
Found in 5+ files:
```python
has_permission = await uow.datasets.user_has_permission(
    dataset_id, current_user.user_id, "read"
)
if not has_permission:
    raise HTTPException(status_code=403, detail="No read permission on dataset")
```

### Pattern 2: Admin Role Check
Found in multiple handlers:
```python
user = await self._uow.users.get_by_id(user_id)
if not user or user.get('role_name') != 'admin':
    raise PermissionError(f"User {user_id} does not have permission...")
```

## Existing Permission Abstractions

### 1. **Authorization Module** (`/src/core/authorization.py`)
- `PermissionChecker` class - Dependency injection for dataset permissions
- Pre-configured checkers for read/write/admin
- Role-based dependencies

### 2. **Permission Types** (`/src/models/pydantic_models.py`)
- `PermissionType` enum (READ, WRITE, ADMIN)
- `CurrentUser` model with role checking methods

### 3. **Repository Layer** (`/src/core/infrastructure/postgres/dataset_repo.py`)
- `check_user_permission` - Implements permission hierarchy
- `user_has_permission` - Wrapper method (duplicate)

## Recommendations

1. **Standardize on Middleware Pattern**
   - Use `Depends(require_dataset_*)` for all dataset operations
   - Remove inline permission checks from API endpoints
   - Move permission logic from handlers to middleware

2. **Create Consistent Error Handling**
   - Standardize error messages and status codes
   - Use 403 for permission denied, 401 for authentication issues

3. **Remove Duplicate Code**
   - Consolidate `check_user_permission` and `user_has_permission`
   - Create reusable permission checking decorators for handlers

4. **Enhance Permission Abstractions**
   - Create a `@require_permission` decorator for handlers
   - Implement permission caching to reduce database queries
   - Add audit logging for permission checks

5. **Address POC Mode**
   - Create a configuration flag for POC mode
   - Ensure consistent behavior across all permission checks