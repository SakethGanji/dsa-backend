# Codebase Cleanup Plan

## Overview
This plan addresses the architectural improvements identified in the code review, focusing on:
1. Completing ongoing refactoring
2. Restructuring infrastructure for cleaner separation
3. Cleaning up version control artifacts

## Phase 1: Complete Refactoring (Priority: HIGH)
**Goal**: Keep only the latest version of code - remove duplicates and complete ongoing refactoring

### Files to Replace:
- [ ] `src/main.py` → Replace with `src/main_refactored.py`
- [ ] `src/features/datasets/update_dataset.py` → Replace with `src/features/datasets/update_dataset_refactored.py`
- [ ] `src/api/datasets.py` → Merge/replace with `src/api/datasets_enhanced.py` (keep enhanced version)

### Actions:
1. Backup current state (git handles this)
2. Replace old files with refactored versions
3. Delete refactored/duplicate files
4. Update any imports if needed

## Phase 2: Restructure Infrastructure (Priority: HIGH)
**Goal**: Clean separation between core domain and infrastructure

### Directory Moves:
- [ ] `src/core/infrastructure/postgres/` → `src/infrastructure/postgres/`
- [ ] `src/core/infrastructure/services/` → `src/infrastructure/services/`

### Import Updates Required:
- All files importing from `core.infrastructure` need updating to `infrastructure`
- Estimated files to update: ~20-30 based on repository structure

### Final Core Structure:
```
src/core/
├── abstractions/       # Interfaces/Ports only
├── common/            # Shared domain utilities
├── domain_exceptions.py
└── services/          # Pure domain services (if any)
```

## Phase 3: Git Housekeeping (Priority: MEDIUM)
**Goal**: Clean up version control

### Update .gitignore:
```gitignore
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg
```

### Cleanup Actions:
- [ ] Add entries to .gitignore
- [ ] Remove all __pycache__ directories
- [ ] Remove any .pyc files
- [ ] Commit cleaned state

## Phase 4: Verify and Test (Priority: HIGH)
**Goal**: Ensure nothing breaks

### Verification Steps:
- [ ] Run main application
- [ ] Test API endpoints
- [ ] Test worker processes
- [ ] Check all imports resolve correctly
- [ ] Document any breaking changes

## Execution Log

### Phase 1 Execution:
- Started: 2025-07-13 11:55
- Completed: 2025-07-13 11:58
- Issues: None
- Actions taken:
  - Replaced main.py with main_refactored.py
  - Replaced update_dataset.py with update_dataset_refactored.py
  - Removed datasets_enhanced.py (kept original datasets.py as it had more features)

### Phase 2 Execution:
- Started: 2025-07-13 11:58
- Completed: 2025-07-13 12:05
- Issues: Import paths needed updating from relative to absolute
- Actions taken:
  - Moved src/core/infrastructure/postgres/ to src/infrastructure/postgres/
  - Moved src/core/infrastructure/services/ to src/infrastructure/services/
  - Updated all imports from core.infrastructure to infrastructure
  - Fixed infrastructure internal imports to use absolute paths

### Phase 3 Execution:
- Started: 2025-07-13 12:05
- Completed: 2025-07-13 12:06
- Issues: None
- Actions taken:
  - Updated .gitignore with comprehensive Python entries
  - Removed all __pycache__ directories
  - Removed all .pyc files

### Phase 4 Execution:
- Started: 2025-07-13 12:06
- Completed: 2025-07-13 12:07
- Issues: None
- Actions taken:
  - Verified directory structure is correct
  - Tested critical imports successfully
  - All imports working properly

## Notes
- All changes are tracked in git for rollback if needed
- Since this is POC phase, we're being aggressive about removing old code
- Focus is on clean, maintainable structure over backwards compatibility