# Architecture Refinements Plan

## Overview
This plan addresses the next-level refinements to elevate the codebase from a great structure to an exceptional one, focusing on:
1. Removing infrastructure concerns from core
2. Reorganizing models for clarity
3. Standardizing feature structure

## Phase 1: Remove Infrastructure from Core (Priority: HIGH)

### 1.1 Move core/database.py
**Issue**: Database connection/session management is infrastructure, not core domain
**Action**: 
- Move `core/database.py` → `infrastructure/postgres/database.py`
- Update all imports
- The core should only define abstractions (IUnitOfWork), not concrete database implementations

### 1.2 Move core/dependencies.py  
**Issue**: Dependency injection wiring is framework-specific (FastAPI)
**Action**:
- Move `core/dependencies.py` → `api/dependencies.py`
- Update all imports
- API layer should handle framework-specific DI, not core

### 1.3 Refactor core/config.py
**Issue**: Configuration loading is infrastructure concern
**Action**:
- Create `core/abstractions/config.py` with abstract Settings interface
- Move concrete implementation to `infrastructure/config/settings.py`
- Use existing `infrastructure/external/env_config_provider.py`
- Core defines what config it needs, infrastructure provides it

## Phase 2: Reorganize Models (Priority: MEDIUM)

### Current Issues:
- `src/models/` is a "junk drawer" mixing API DTOs, domain entities, and config
- `pydantic_models.py` is a monolithic file with mixed concerns
- Unclear separation between API models and domain models

### New Structure:
```
src/
├── core/
│   └── domain/
│       ├── entities/          # Core business entities
│       │   ├── dataset.py
│       │   ├── user.py
│       │   └── commit.py
│       └── value_objects/     # Domain value objects
│           ├── permissions.py
│           └── metadata.py
├── features/
│   ├── datasets/
│   │   └── models/           # Feature-specific request/response models
│   │       ├── requests.py   # CreateDatasetRequest, UpdateDatasetRequest
│   │       └── responses.py  # DatasetResponse, etc.
│   └── [other features follow same pattern]
└── api/
    └── common/
        └── models/           # Shared API models
            ├── pagination.py # PaginationParams, PaginatedResponse
            └── errors.py     # ErrorResponse models
```

### Migration Steps:
1. Create domain entities in `core/domain/entities/`
2. Move feature-specific models to respective `features/*/models/`
3. Move shared API models to `api/common/models/`
4. Delete the top-level `models/` directory

## Phase 3: Standardize Features Structure (Priority: MEDIUM)

### Current Inconsistency:
- Some features are flat: `features/datasets/*.py`
- Others are structured: `features/search/handlers/`, `features/search/models/`

### Standardized Structure:
```
features/
├── datasets/
│   ├── handlers/
│   │   ├── create_dataset.py
│   │   ├── update_dataset.py
│   │   ├── delete_dataset.py
│   │   └── list_datasets.py
│   ├── models/
│   │   ├── requests.py
│   │   └── responses.py
│   └── services/          # Feature-specific services if needed
│       └── dataset_validator.py
├── users/
│   ├── handlers/
│   ├── models/
│   └── services/
└── [all features follow this pattern]
```

### Benefits:
- Consistent navigation
- Scalable as features grow
- Clear separation of concerns within features
- Easier to find related code

## Phase 4: Additional Refinements

### 4.1 Clean up authorization/auth split
- `core/auth.py` and `core/authorization.py` - consider consolidating or clarifying the split
- Move to `infrastructure/security/` if they contain implementation details

### 4.2 Review core/events.py
- If it contains infrastructure event bus implementation, move to infrastructure
- Core should only define event interfaces/base classes

### 4.3 Consider core/exceptions.py vs core/domain_exceptions.py
- Consolidate into single `core/domain/exceptions.py`
- Remove duplication

## Implementation Order

1. **First**: Fix core leaks (database, dependencies, config) - High impact on architecture ✅ COMPLETED
2. **Second**: Reorganize models - Medium complexity but high clarity improvement  
3. **Third**: Standardize features - Lower priority but improves consistency
4. **Fourth**: Additional cleanups - Nice to have

## Execution Status

### Phase 1: Remove Infrastructure from Core ✅ COMPLETED
- ✅ Moved `core/database.py` → `infrastructure/postgres/database.py`
- ✅ Moved `core/dependencies.py` → `api/dependencies.py`
- ✅ Created config abstraction in `core/abstractions/config.py`
- ✅ Moved concrete config to `infrastructure/config/settings.py`
- ✅ Updated all imports (35 files total)

## Success Criteria

After these refinements:
- Core contains ONLY domain logic and abstractions
- No framework-specific code in core
- Clear model organization with no ambiguity
- Consistent feature structure throughout
- Easier onboarding for new developers

## Notes
- Each phase can be done independently
- Test after each major move to ensure nothing breaks
- Update import statements carefully
- Document any breaking changes for the team