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

## Phase 2: Reorganize Models (Priority: MEDIUM) ✅ COMPLETED

### Current Issues:
- `src/models/` is a "junk drawer" mixing API DTOs, domain entities, and config
- `pydantic_models.py` is a monolithic file with mixed concerns
- Unclear separation between API models and domain models

### Implemented Structure:
```
src/
├── api/
│   ├── models/                # API request/response models
│   │   ├── requests.py        # All request DTOs
│   │   ├── responses.py       # All response DTOs
│   │   └── common.py          # Shared types (CurrentUser, enums, etc.)
│   ├── validation/            # Enhanced validation
│   │   ├── models.py          # Validation models with business rules
│   │   └── validators.py      # Security validators
│   └── factories/             # Response factories
│       └── response.py        # Factory utilities
└── core/
    └── abstractions/
        └── models/            # Base models and constants
            ├── base.py        # Abstract base models
            └── constants.py   # System-wide constants/enums
```

### What Was Done:
1. ✅ Created new directory structure
2. ✅ Moved API models to `api/models/` (requests, responses, common types)
3. ✅ Moved validation models to `api/validation/`
4. ✅ Moved base models to `core/abstractions/models/`
5. ✅ Moved response factories to `api/factories/`
6. ✅ Updated all imports across 26+ files
7. ✅ Deleted the old `models/` directory
8. ✅ Verified all functionality works

## Phase 3: Standardize Features Structure (Priority: MEDIUM) ✅ COMPLETED

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

## Phase 4: Service Layer Optimization (Priority: HIGH) ✅ COMPLETED

### Issues Found:
1. **Direct Infrastructure Dependencies**
   - `PostgresSamplingService` imports `DatabasePool` from infrastructure
   - `SamplingJobService` uses `DatabasePool` directly
   - `TableAnalyzer` imports `DatabasePool` from infrastructure

2. **Improper Separation of Concerns**
   - Services were doing repository work (direct SQL queries)
   - Services should work through repositories and unit of work

### What Was Done:
1. ✅ Created `sampling_service_refactored.py` using only abstractions
2. ✅ Created `table_analyzer_refactored.py` following clean architecture
3. ✅ Removed all direct infrastructure imports from services
4. ✅ Services now use IUnitOfWork and repositories
5. ✅ Removed duplicate pagination.py from api/common

### Clean Architecture Rules Applied:
- Services only depend on abstractions (interfaces)
- No direct infrastructure imports
- Use Unit of Work pattern for transactions
- Use Repositories for data access

## Phase 5: Additional Refinements

### 5.1 Clean up authorization/auth split
- `core/auth.py` and `core/authorization.py` - consider consolidating or clarifying the split
- Move to `infrastructure/security/` if they contain implementation details

### 5.2 Review core/events.py
- If it contains infrastructure event bus implementation, move to infrastructure
- Core should only define event interfaces/base classes

### 5.3 Consider core/exceptions.py vs core/domain_exceptions.py
- Consolidate into single `core/domain/exceptions.py`
- Remove duplication

## Implementation Order

1. **First**: Fix core leaks (database, dependencies, config) - High impact on architecture ✅ COMPLETED
2. **Second**: Reorganize models - Medium complexity but high clarity improvement ✅ COMPLETED
3. **Third**: Standardize features - Lower priority but improves consistency ✅ COMPLETED
4. **Fourth**: Service layer optimization - Critical for clean architecture ✅ COMPLETED
5. **Fifth**: Additional cleanups - Nice to have ⏳ NEXT

## Execution Status

### Phase 1: Remove Infrastructure from Core ✅ COMPLETED
- ✅ Moved `core/database.py` → `infrastructure/postgres/database.py`
- ✅ Moved `core/dependencies.py` → `api/dependencies.py`
- ✅ Created config abstraction in `core/abstractions/config.py`
- ✅ Moved concrete config to `infrastructure/config/settings.py`
- ✅ Updated all imports (35 files total)

### Phase 2: Reorganize Models ✅ COMPLETED
- ✅ Created new model directory structure
- ✅ Moved API models to `api/models/`
- ✅ Moved validation models to `api/validation/`
- ✅ Moved base models to `core/abstractions/models/`
- ✅ Moved factories to `api/factories/`
- ✅ Updated all imports (26+ files)
- ✅ Removed old `models/` directory
- ✅ Verified API functionality

### Phase 3: Standardize Features Structure ✅ COMPLETED
- ✅ Created handlers/models subdirectories in all features
- ✅ Moved all handlers to handlers/ subdirectory
- ✅ Updated imports in API layer
- ✅ Fixed relative imports for new directory depth
- ✅ All features now follow consistent structure

### Phase 4: Service Layer Optimization ✅ COMPLETED
- ✅ Refactored services to remove infrastructure dependencies
- ✅ Created clean implementations using only abstractions
- ✅ Services now use Unit of Work and Repository patterns
- ✅ Removed duplicate pagination.py
- ✅ Documented clean architecture rules for services

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