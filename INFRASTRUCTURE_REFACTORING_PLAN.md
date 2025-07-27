# Infrastructure Refactoring Plan

## Overview
This document outlines the plan to reorganize the infrastructure folder for better modularity, clarity, and extensibility.

## Target Structure
```
infrastructure/
├── adapters/              # External service implementations
│   ├── auth/
│   │   ├── jwt_service.py
│   │   └── password_manager.py
│   ├── storage/
│   │   ├── local_file.py
│   │   ├── ngc_client.py
│   │   └── postgres_pool.py
│   ├── cache/
│   │   └── memory.py
│   └── config/
│       └── env_provider.py
├── persistence/           # Data layer
│   ├── postgres/
│   │   ├── connection.py  # (formerly database.py)
│   │   ├── repositories/
│   │   │   ├── base.py
│   │   │   ├── dataset.py
│   │   │   ├── job.py
│   │   │   ├── user.py
│   │   │   ├── versioning.py
│   │   │   ├── exploration.py
│   │   │   └── search.py
│   │   ├── readers/
│   │   │   └── table.py
│   │   └── patterns/
│   │       ├── uow.py
│   │       ├── event_store.py
│   │       └── adapters.py
├── services/              # Business logic services
│   ├── data/
│   │   ├── export.py
│   │   ├── sampling.py
│   │   └── statistics/
│   ├── processing/
│   │   ├── file/
│   │   ├── sql_execution.py
│   │   └── table_analysis.py
│   └── workflows/          # Technical orchestration services
│       ├── commit_preparation.py
│       ├── exploration.py
│       └── workbench.py
└── config/                # (Already exists - no need to create)
    ├── __init__.py
    └── settings.py
```

## Migration Steps

### Phase 1: Create New Directory Structure
```bash
# Create adapters directories
mkdir -p src/infrastructure/adapters/auth
mkdir -p src/infrastructure/adapters/storage
mkdir -p src/infrastructure/adapters/cache
mkdir -p src/infrastructure/adapters/config

# Create persistence directories
mkdir -p src/infrastructure/persistence/postgres/repositories
mkdir -p src/infrastructure/persistence/postgres/readers
mkdir -p src/infrastructure/persistence/postgres/patterns

# Create services directories
mkdir -p src/infrastructure/services/data/statistics
mkdir -p src/infrastructure/services/processing/file
mkdir -p src/infrastructure/services/workflows

# Note: src/infrastructure/config/ already exists - no need to create
```

### Phase 2: Move Files

#### Adapters Migration
```bash
# Auth adapters
git mv src/infrastructure/external/jwt_auth_service.py src/infrastructure/adapters/auth/jwt_service.py
git mv src/infrastructure/external/password_manager.py src/infrastructure/adapters/auth/password_manager.py
git mv src/infrastructure/external/password_hasher.py src/infrastructure/adapters/auth/password_hasher.py

# Storage adapters
git mv src/infrastructure/external/local_file_storage.py src/infrastructure/adapters/storage/local_file.py
git mv src/infrastructure/external/ngc_client.py src/infrastructure/adapters/storage/ngc_client.py
git mv src/infrastructure/external/postgres_pool.py src/infrastructure/adapters/storage/postgres_pool.py

# Cache adapters
git mv src/infrastructure/external/memory_cache.py src/infrastructure/adapters/cache/memory.py

# Config adapters
git mv src/infrastructure/external/env_config_provider.py src/infrastructure/adapters/config/env_provider.py

# Move __init__.py files
git mv src/infrastructure/external/__init__.py src/infrastructure/adapters/__init__.py
```

#### Persistence Migration
```bash
# Connection
git mv src/infrastructure/postgres/database.py src/infrastructure/persistence/postgres/connection.py

# Repositories
git mv src/infrastructure/postgres/base_repository.py src/infrastructure/persistence/postgres/repositories/base.py
git mv src/infrastructure/postgres/dataset_repo.py src/infrastructure/persistence/postgres/repositories/dataset.py
git mv src/infrastructure/postgres/job_repo.py src/infrastructure/persistence/postgres/repositories/job.py
git mv src/infrastructure/postgres/user_repo.py src/infrastructure/persistence/postgres/repositories/user.py
git mv src/infrastructure/postgres/versioning_repo.py src/infrastructure/persistence/postgres/repositories/versioning.py
git mv src/infrastructure/postgres/exploration_repo.py src/infrastructure/persistence/postgres/repositories/exploration.py
git mv src/infrastructure/postgres/search_repository.py src/infrastructure/persistence/postgres/repositories/search.py

# Readers
git mv src/infrastructure/postgres/table_reader.py src/infrastructure/persistence/postgres/readers/table.py

# Patterns
git mv src/infrastructure/postgres/uow.py src/infrastructure/persistence/postgres/patterns/uow.py
git mv src/infrastructure/postgres/event_store.py src/infrastructure/persistence/postgres/patterns/event_store.py
git mv src/infrastructure/postgres/adapters.py src/infrastructure/persistence/postgres/patterns/adapters.py

# Move __init__.py file
git mv src/infrastructure/postgres/__init__.py src/infrastructure/persistence/postgres/__init__.py
```

#### Services Migration
```bash
# Data services
git mv src/infrastructure/services/data_export_service.py src/infrastructure/services/data/export.py
git mv src/infrastructure/services/sampling_service.py src/infrastructure/services/data/sampling.py
git mv src/infrastructure/services/statistics/* src/infrastructure/services/data/statistics/

# Processing services
git mv src/infrastructure/services/file_processing/* src/infrastructure/services/processing/file/
git mv src/infrastructure/services/sql_execution.py src/infrastructure/services/processing/sql_execution.py
git mv src/infrastructure/services/table_analysis.py src/infrastructure/services/processing/table_analysis.py

# Workflow services
git mv src/infrastructure/services/commit_preparation_service.py src/infrastructure/services/workflows/commit_preparation.py
git mv src/infrastructure/services/exploration_service.py src/infrastructure/services/workflows/exploration.py
git mv src/infrastructure/services/workbench_service.py src/infrastructure/services/workflows/workbench.py

# Move __init__.py files
git mv src/infrastructure/services/__init__.py src/infrastructure/services/__init__.py  # Keep in same location
git mv src/infrastructure/services/file_processing/__init__.py src/infrastructure/services/processing/file/__init__.py
git mv src/infrastructure/services/statistics/__init__.py src/infrastructure/services/data/statistics/__init__.py
```

### Phase 3: Update Import Statements

#### Import Mapping
| Old Import | New Import |
|------------|------------|
| `from src.infrastructure.external.jwt_auth_service` | `from src.infrastructure.adapters.auth.jwt_service` |
| `from src.infrastructure.external.password_manager` | `from src.infrastructure.adapters.auth.password_manager` |
| `from src.infrastructure.external.password_hasher` | `from src.infrastructure.adapters.auth.password_hasher` |
| `from src.infrastructure.external.local_file_storage` | `from src.infrastructure.adapters.storage.local_file` |
| `from src.infrastructure.external.ngc_client` | `from src.infrastructure.adapters.storage.ngc_client` |
| `from src.infrastructure.external.postgres_pool` | `from src.infrastructure.adapters.storage.postgres_pool` |
| `from src.infrastructure.external.memory_cache` | `from src.infrastructure.adapters.cache.memory` |
| `from src.infrastructure.external.env_config_provider` | `from src.infrastructure.adapters.config.env_provider` |
| `from src.infrastructure.postgres.database` | `from src.infrastructure.persistence.postgres.connection` |
| `from src.infrastructure.postgres.base_repository` | `from src.infrastructure.persistence.postgres.repositories.base` |
| `from src.infrastructure.postgres.dataset_repo` | `from src.infrastructure.persistence.postgres.repositories.dataset` |
| `from src.infrastructure.postgres.job_repo` | `from src.infrastructure.persistence.postgres.repositories.job` |
| `from src.infrastructure.postgres.user_repo` | `from src.infrastructure.persistence.postgres.repositories.user` |
| `from src.infrastructure.postgres.versioning_repo` | `from src.infrastructure.persistence.postgres.repositories.versioning` |
| `from src.infrastructure.postgres.exploration_repo` | `from src.infrastructure.persistence.postgres.repositories.exploration` |
| `from src.infrastructure.postgres.search_repository` | `from src.infrastructure.persistence.postgres.repositories.search` |
| `from src.infrastructure.postgres.table_reader` | `from src.infrastructure.persistence.postgres.readers.table` |
| `from src.infrastructure.postgres.uow` | `from src.infrastructure.persistence.postgres.patterns.uow` |
| `from src.infrastructure.postgres.event_store` | `from src.infrastructure.persistence.postgres.patterns.event_store` |
| `from src.infrastructure.postgres.adapters` | `from src.infrastructure.persistence.postgres.patterns.adapters` |
| `from src.infrastructure.services.data_export_service` | `from src.infrastructure.services.data.export` |
| `from src.infrastructure.services.sampling_service` | `from src.infrastructure.services.data.sampling` |
| `from src.infrastructure.services.statistics` | `from src.infrastructure.services.data.statistics` |
| `from src.infrastructure.services.file_processing` | `from src.infrastructure.services.processing.file` |
| `from src.infrastructure.services.sql_execution` | `from src.infrastructure.services.processing.sql_execution` |
| `from src.infrastructure.services.table_analysis` | `from src.infrastructure.services.processing.table_analysis` |
| `from src.infrastructure.services.commit_preparation_service` | `from src.infrastructure.services.workflows.commit_preparation` |
| `from src.infrastructure.services.exploration_service` | `from src.infrastructure.services.workflows.exploration` |
| `from src.infrastructure.services.workbench_service` | `from src.infrastructure.services.workflows.workbench` |

### Phase 4: Update Import Statements

#### Automated Import Update Script
Create `scripts/update_infrastructure_imports.py`:
```python
#!/usr/bin/env python3
import os
import re
from pathlib import Path

# Import mapping from Phase 3
IMPORT_MAPPING = {
    "from src.infrastructure.external.jwt_auth_service": "from src.infrastructure.adapters.auth.jwt_service",
    "from src.infrastructure.external.password_manager": "from src.infrastructure.adapters.auth.password_manager",
    "from src.infrastructure.external.password_hasher": "from src.infrastructure.adapters.auth.password_hasher",
    "from src.infrastructure.external.local_file_storage": "from src.infrastructure.adapters.storage.local_file",
    "from src.infrastructure.external.ngc_client": "from src.infrastructure.adapters.storage.ngc_client",
    "from src.infrastructure.external.postgres_pool": "from src.infrastructure.adapters.storage.postgres_pool",
    "from src.infrastructure.external.memory_cache": "from src.infrastructure.adapters.cache.memory",
    "from src.infrastructure.external.env_config_provider": "from src.infrastructure.adapters.config.env_provider",
    "from src.infrastructure.postgres.database": "from src.infrastructure.persistence.postgres.connection",
    "from src.infrastructure.postgres.base_repository": "from src.infrastructure.persistence.postgres.repositories.base",
    "from src.infrastructure.postgres.dataset_repo": "from src.infrastructure.persistence.postgres.repositories.dataset",
    "from src.infrastructure.postgres.job_repo": "from src.infrastructure.persistence.postgres.repositories.job",
    "from src.infrastructure.postgres.user_repo": "from src.infrastructure.persistence.postgres.repositories.user",
    "from src.infrastructure.postgres.versioning_repo": "from src.infrastructure.persistence.postgres.repositories.versioning",
    "from src.infrastructure.postgres.exploration_repo": "from src.infrastructure.persistence.postgres.repositories.exploration",
    "from src.infrastructure.postgres.search_repository": "from src.infrastructure.persistence.postgres.repositories.search",
    "from src.infrastructure.postgres.table_reader": "from src.infrastructure.persistence.postgres.readers.table",
    "from src.infrastructure.postgres.uow": "from src.infrastructure.persistence.postgres.patterns.uow",
    "from src.infrastructure.postgres.event_store": "from src.infrastructure.persistence.postgres.patterns.event_store",
    "from src.infrastructure.postgres.adapters": "from src.infrastructure.persistence.postgres.patterns.adapters",
    "from src.infrastructure.services.data_export_service": "from src.infrastructure.services.data.export",
    "from src.infrastructure.services.sampling_service": "from src.infrastructure.services.data.sampling",
    "from src.infrastructure.services.statistics": "from src.infrastructure.services.data.statistics",
    "from src.infrastructure.services.file_processing": "from src.infrastructure.services.processing.file",
    "from src.infrastructure.services.sql_execution": "from src.infrastructure.services.processing.sql_execution",
    "from src.infrastructure.services.table_analysis": "from src.infrastructure.services.processing.table_analysis",
    "from src.infrastructure.services.commit_preparation_service": "from src.infrastructure.services.workflows.commit_preparation",
    "from src.infrastructure.services.exploration_service": "from src.infrastructure.services.workflows.exploration",
    "from src.infrastructure.services.workbench_service": "from src.infrastructure.services.workflows.workbench",
}

def update_imports_in_file(filepath):
    """Update imports in a single file."""
    with open(filepath, 'r') as f:
        content = f.read()
    
    original_content = content
    for old_import, new_import in IMPORT_MAPPING.items():
        content = re.sub(
            rf'^{re.escape(old_import)}\b',
            new_import,
            content,
            flags=re.MULTILINE
        )
    
    if content != original_content:
        with open(filepath, 'w') as f:
            f.write(content)
        print(f"Updated imports in {filepath}")
        return True
    return False

def main():
    """Update all Python files in the src directory."""
    src_path = Path("src")
    updated_count = 0
    
    for py_file in src_path.rglob("*.py"):
        if update_imports_in_file(py_file):
            updated_count += 1
    
    print(f"\nTotal files updated: {updated_count}")

if __name__ == "__main__":
    main()
```

Run the script:
```bash
python scripts/update_infrastructure_imports.py
```

### Phase 5: Update __init__.py Files
Create appropriate `__init__.py` files in each new directory to maintain proper Python package structure and exports.

### Phase 6: Cleanup
```bash
# Verify old directories are empty before removing
ls -la src/infrastructure/external/
ls -la src/infrastructure/postgres/

# Only remove if empty (will fail if not empty, which is safe)
rmdir src/infrastructure/external
rmdir src/infrastructure/postgres
```

### Phase 7: Testing and Validation

#### Pre-Migration Validation
```bash
# Check for circular imports before migration
python -m pip install pydeps
pydeps src/infrastructure --cluster --max-bacon=2 -o infrastructure_deps.svg

# Save current test results as baseline
pytest > test_results_before.txt
```

#### Post-Migration Testing
1. Run all unit tests to ensure imports are working
2. Run integration tests to verify functionality
3. Check for any circular imports using the dependency graph
4. Verify all services start correctly
5. Update `src/api/dependencies.py` if needed for new paths

#### Rollback Plan
If critical issues arise:
```bash
# Revert all changes
git checkout -- src/
git clean -fd src/infrastructure/

# Or create a backup branch before starting
git checkout -b infrastructure-refactor-backup
```

## Benefits of New Structure

1. **Clear Separation of Concerns**
   - Adapters: External service integrations
   - Persistence: Data access layer
   - Services: Business logic

2. **Extensibility**
   - Easy to add new databases (MongoDB, Redis)
   - Clear pattern for adding new external services
   - Modular service organization

3. **Maintainability**
   - Related files grouped together
   - Consistent naming conventions
   - Easier to locate specific functionality

4. **Future-Ready**
   - Structure supports adding interfaces for abstraction
   - Ready for dependency injection patterns
   - Supports multiple database backends

## Execution Checklist

### Pre-Migration
- [ ] Create infrastructure-refactor-backup branch
- [ ] Run dependency analysis for circular imports
- [ ] Save baseline test results
- [ ] Review api/dependencies.py for required updates

### Migration
- [ ] Create new directory structure
- [ ] Move auth adapter files
- [ ] Move storage adapter files (including postgres_pool.py)
- [ ] Move cache adapter files
- [ ] Move config adapter files
- [ ] Move postgres connection file
- [ ] Move repository files
- [ ] Move reader files
- [ ] Move pattern files
- [ ] Move data service files
- [ ] Move processing service files
- [ ] Move workflow service files
- [ ] Run automated import update script
- [ ] Manually verify critical imports
- [ ] Create __init__.py files with proper exports
- [ ] Update api/dependencies.py if needed

### Post-Migration
- [ ] Run unit tests
- [ ] Run integration tests
- [ ] Check for circular imports
- [ ] Verify all services start correctly
- [ ] Compare test results with baseline
- [ ] Remove old directories
- [ ] Commit changes

### Notes
- Total files to update: ~53 files with 118 import statements
- Consider creating compatibility shims if gradual migration needed
- Monitor for any performance impacts after restructuring