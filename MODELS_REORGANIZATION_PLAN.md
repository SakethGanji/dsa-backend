# Models Reorganization Plan

## Current Structure Issues
- All models are in a single top-level `models/` directory
- Mix of API models, domain models, and utilities in the same location
- No clear separation between request/response models and domain entities

## Proposed New Structure

### 1. Move API Models to `src/api/models/`
- **Request Models**: All `*Request` classes from pydantic_models.py
- **Response Models**: All `*Response` classes from pydantic_models.py  
- **API-specific types**: CurrentUser, PermissionType enum
- **Pagination**: PaginationParams from validation_models.py

### 2. Move Domain Models to `src/core/domain/`
- **Entity Models**: Core business entities without API concerns
  - Dataset entity
  - User entity
  - Job entity
  - Commit entity
  - Branch/Ref entity
- These should be pure domain models without Pydantic dependencies

### 3. Keep Base Models in `src/core/abstractions/models/`
- Move base_models.py content here
- These are abstract building blocks used across the system
- Keep constants/enums here (PermissionLevel, JobStatus, ImportStatus)

### 4. Move Validation Models to `src/api/validation/`
- All enhanced validation models from validation_models.py
- Security validators (SQL injection, XSS, filename validation)
- These are API-layer concerns

### 5. Move Response Factories to `src/api/factories/`
- response_factories.py content
- API-specific transformation utilities

## Implementation Steps

### Phase 1: Create New Directory Structure
```
src/
├── api/
│   ├── models/
│   │   ├── __init__.py
│   │   ├── requests.py      # All request models
│   │   ├── responses.py     # All response models
│   │   └── common.py        # Shared API types (CurrentUser, etc)
│   ├── validation/
│   │   ├── __init__.py
│   │   ├── validators.py    # Security validators
│   │   └── models.py        # Enhanced validation models
│   └── factories/
│       ├── __init__.py
│       └── response.py      # Response factory utilities
├── core/
│   ├── domain/
│   │   ├── __init__.py
│   │   ├── dataset.py       # Dataset domain entity
│   │   ├── user.py          # User domain entity
│   │   ├── job.py           # Job domain entity
│   │   ├── commit.py        # Commit domain entity
│   │   └── ref.py           # Branch/Ref domain entity
│   └── abstractions/
│       └── models/
│           ├── __init__.py
│           ├── base.py      # Base model classes
│           └── constants.py # Enums and constants
```

### Phase 2: Move Models to New Locations
1. Extract and move API request/response models
2. Extract and move domain entities (may need to create them)
3. Move base models to abstractions
4. Move validation models to API layer
5. Move factories to API layer

### Phase 3: Update All Imports
- Use automated script to update imports across the codebase
- Test each change to ensure nothing breaks

### Phase 4: Remove Old Models Directory
- Delete the old `src/models/` directory
- Verify all tests still pass

## Benefits
1. **Clear separation of concerns**: API models vs domain models
2. **Better architecture alignment**: Models live where they're used
3. **Reduced coupling**: Domain doesn't depend on API concerns
4. **Easier to maintain**: Related models are grouped together
5. **Follows Clean Architecture**: Dependencies point inward

## Notes
- This is a large refactoring that will touch many files
- Should be done incrementally with testing at each step
- May reveal opportunities to create pure domain entities
- Will make the codebase more maintainable long-term