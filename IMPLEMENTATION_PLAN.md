# Dataset Management System - Incremental Implementation Plan

## Overview
This document outlines an incremental refactoring plan to support new schema features while modifying the existing codebase in place. Each phase adds specific functionality that can be tested independently.

## Core Principles
1. **Modify in place**: Update existing modules without creating new structure initially
2. **Use Alembic**: Incremental schema changes with proper migrations
3. **Test as we go**: Each phase must be fully testable before moving on
4. **No new flows**: Adapt existing flows to support new schema
5. **No legacy adapters**: Direct updates to existing services and routes

## Key Features to Add
- Content-addressable file storage with SHA256 deduplication
- Enhanced versioning with parent references (DAG support)
- Automatic schema capture per version
- File reference counting for garbage collection
- Multi-file support per version
- Branch and tag pointers
- Basic permission system

## Phase 1: Add Content-Addressable Storage Fields (Day 1)

### Goal
Add deduplication support to existing files table without changing core functionality.

### Alembic Migration
```sql
-- Add new columns to files table
ALTER TABLE files ADD COLUMN content_hash CHAR(64);
ALTER TABLE files ADD COLUMN reference_count BIGINT NOT NULL DEFAULT 0;
ALTER TABLE files ADD COLUMN compression_type VARCHAR(50);
ALTER TABLE files ADD COLUMN metadata JSONB;

-- Create unique index for deduplication
CREATE UNIQUE INDEX idx_files_content_hash ON files(content_hash);
```

### Code Changes

1. **Update File Model** (`src/app/datasets/models.py`):
   - Add content_hash field
   - Add reference_count field 
   - Add compression_type and metadata fields

2. **Update Storage Backend** (`src/app/storage/local_backend.py`):
   - Calculate SHA256 hash before storing file
   - Check if hash already exists in database
   - If exists: increment reference_count, return existing file_id
   - If new: store file and set reference_count = 1

3. **Update File Service** (`src/app/storage/file_service.py`):
   - Add hash calculation utility method
   - Update file deletion to decrement reference_count
   - Only delete physical file when reference_count = 0

4. **Update Routes if needed** (`src/app/datasets/routes.py`):
   - File upload endpoint should return content_hash in response
   - Add file info endpoint to show reference_count

### Testing
- Upload same file twice → verify single storage with reference_count = 2
- Delete one reference → verify file still exists with reference_count = 1
- Delete last reference → verify physical file is removed

## Phase 2: Update Dataset Versioning (Day 2-3)

### Goal
Add parent version support to enable branching and DAG structure.

### Alembic Migration
```sql
-- Add parent reference and message to dataset_versions
ALTER TABLE dataset_versions ADD COLUMN parent_version_id INT REFERENCES dataset_versions(id);
ALTER TABLE dataset_versions ADD COLUMN message TEXT;
ALTER TABLE dataset_versions ADD COLUMN overlay_file_id INT REFERENCES files(id);

-- Add index for parent lookups
CREATE INDEX idx_dataset_versions_parent ON dataset_versions(parent_version_id);
```

### Code Changes

1. **Update Version Model** (`src/app/datasets/models.py`):
   - Add parent_version_id field (Optional[int])
   - Add message field for version descriptions
   - Add overlay_file_id for incremental updates

2. **Update Dataset Service** (`src/app/datasets/service.py`):
   - Modify create_version method:
     ```python
     def create_version(dataset_id, file_id, message, parent_version_id=None):
         if parent_version_id:
             # Get parent version
             # version_number = parent.version_number + 1
         else:
             # version_number = get_max_version(dataset_id) + 1
     ```
   - Add branch creation logic
   - Update version listing to show tree structure

3. **Update Dataset Repository** (`src/app/datasets/repository.py`):
   - Update insert queries to include new fields
   - Add query to get version tree/DAG
   - Add query to find children of a version

4. **Update Routes** (`src/app/datasets/routes.py`):
   - Add optional parent_version_id parameter to create version endpoint
   - Add message parameter for version creation
   - Update version list endpoint to show parent relationships

### Testing
- Create linear versions → verify sequential numbering
- Create branch from v2 → verify new branch starts correctly
- List versions → verify parent-child relationships shown

## Phase 3: Add Schema Capture (Day 4)

### Goal
Automatically capture and store schema information when files are uploaded.

### Alembic Migration
```sql
-- Create table for schema snapshots
CREATE TABLE dataset_schema_versions (
    id SERIAL PRIMARY KEY,
    dataset_version_id INT NOT NULL REFERENCES dataset_versions(id),
    schema_json JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Index for version lookups
CREATE INDEX idx_schema_versions_dataset ON dataset_schema_versions(dataset_version_id);
```

### Code Changes

1. **Add Schema Model** (`src/app/datasets/models.py`):
   - Create SchemaVersion model
   - schema_json field to store column info
   - Link to dataset_version_id

2. **Update DuckDB Service** (`src/app/datasets/duckdb_service.py`):
   - Add extract_schema method:
     ```python
     def extract_schema(file_path: str, file_type: str) -> dict:
         # Use DuckDB to read file
         # Extract column names, types, nullable flags
         # Return as JSON schema format
     ```
   - Support CSV, Parquet, Excel formats

3. **Update Dataset Service** (`src/app/datasets/service.py`):
   - Call schema extraction after file upload
   - Store schema snapshot with version
   - Add schema comparison method

4. **Update Routes** (`src/app/datasets/routes.py`):
   - Include schema_json in version response
   - Add GET /datasets/{id}/schema/{version_id} endpoint
   - Add schema comparison endpoint (optional)

### Testing
- Upload CSV → verify schema captured correctly
- Upload Parquet → verify nested types handled
- Compare schemas between versions

## Phase 4: Multi-File Support per Version (Day 5)

### Goal
Support multiple files attached to a single dataset version.

### Alembic Migration
```sql
-- Create junction table for version-file relationships
CREATE TABLE dataset_version_files (
    version_id INT NOT NULL REFERENCES dataset_versions(id),
    file_id INT NOT NULL REFERENCES files(id),
    component_type VARCHAR(50) NOT NULL,
    component_name TEXT,
    component_index INT,
    metadata JSONB,
    PRIMARY KEY (version_id, file_id)
);

-- Index for version lookups
CREATE INDEX idx_version_files_version ON dataset_version_files(version_id);
```

### Code Changes

1. **Add VersionFile Model** (`src/app/datasets/models.py`):
   - Create VersionFile model for junction table
   - component_type (e.g., 'data', 'metadata', 'schema')
   - component_name for identification

2. **Update Dataset Service** (`src/app/datasets/service.py`):
   - Add attach_file_to_version method
   - Update create_version to handle file list
   - Manage reference counts for all files

3. **Update Storage Integration**:
   - Modify file upload to support batch operations
   - Update reference counting for multi-file
   - Add component type tagging

4. **Update Routes** (`src/app/datasets/routes.py`):
   - Add multi-file upload endpoint
   - Update version details to include file list
   - Add component_type parameter to file uploads

### Testing
- Attach multiple CSVs to one version
- Verify reference counting for each file
- Delete version → verify all file references updated

## Phase 5: Add Branch/Tag Support (Day 6)

### Goal
Add named pointers (branches/tags) to specific versions.

### Alembic Migration
```sql
-- Create pointers table for branches and tags
CREATE TABLE dataset_pointers (
    id SERIAL PRIMARY KEY,
    dataset_id INT NOT NULL REFERENCES datasets(id),
    pointer_name VARCHAR(255) NOT NULL,
    dataset_version_id INT NOT NULL REFERENCES dataset_versions(id),
    is_tag BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (dataset_id, pointer_name)
);

-- Index for dataset lookups
CREATE INDEX idx_pointers_dataset ON dataset_pointers(dataset_id);
```

### Code Changes

1. **Add Pointer Model** (`src/app/datasets/models.py`):
   - Create DatasetPointer model
   - pointer_name (e.g., 'main', 'develop', 'v1.0')
   - is_tag flag (tags are immutable)

2. **Update Dataset Service** (`src/app/datasets/service.py`):
   - Add create_branch method:
     ```python
     def create_branch(dataset_id, branch_name, from_version_id):
         # Create or update pointer
         # Set is_tag = False
     ```
   - Add create_tag method (immutable)
   - Add update_branch method (move pointer)

3. **Add New Routes** (`src/app/datasets/routes.py`):
   - Add POST /datasets/{id}/branches
   - Add POST /datasets/{id}/tags  
   - Add GET /datasets/{id}/pointers
   - Update version creation to respect branch context

### Testing
- Create 'main' branch → verify pointer created
- Update branch → verify pointer moved
- Create tag → verify immutable

## Phase 6: Basic Permissions (Day 7)

### Goal
Add permission checks to existing dataset operations.

### Alembic Migration
```sql
-- Create permissions table
CREATE TABLE permissions (
    id SERIAL PRIMARY KEY,
    resource_type VARCHAR(50) NOT NULL, -- 'dataset' or 'file'
    resource_id INT NOT NULL,
    user_id INT NOT NULL REFERENCES users(id),
    permission_type VARCHAR(20) NOT NULL, -- 'read', 'write', 'admin'
    granted_at TIMESTAMP NOT NULL DEFAULT NOW(),
    granted_by INT NOT NULL REFERENCES users(id),
    UNIQUE (resource_type, resource_id, user_id, permission_type)
);

-- Index for permission lookups
CREATE INDEX idx_permissions_lookup ON permissions(resource_type, resource_id, user_id);
```

### Code Changes

1. **Add Permission Model** (`src/app/users/models.py`):
   - Create Permission model
   - Add permission types enum

2. **Update User Service** (`src/app/users/service.py`):
   - Add grant_permission method
   - Add check_permission method
   - Auto-grant admin permission on dataset creation

3. **Update Dataset Service** (`src/app/datasets/service.py`):
   - Add permission checks to all methods:
     ```python
     def update_dataset(dataset_id, user_id, ...):
         # Check write permission first
         if not check_permission('dataset', dataset_id, user_id, 'write'):
             raise PermissionDeniedError()
     ```

4. **Update Routes** (`src/app/datasets/routes.py`):
   - Add permission endpoints (grant, revoke, list)
   - Ensure all routes pass user_id to service methods
   - Return 403 status for permission errors

### Testing
- Create dataset → verify creator has admin permission
- Try to update without permission → verify denied
- Grant permission → verify operation succeeds

## Phase 7: Final Integration Testing

### Goal
Comprehensive testing of all features working together.

### Testing Checklist
- Create dataset with multiple versions in a tree structure
- Upload duplicate files and verify deduplication
- Create branches and tags
- Test permission restrictions
- Verify schema capture and comparison
- Test multi-file uploads
- Ensure all existing functionality still works

### Performance Validation
- Measure file upload times with deduplication
- Check query performance with new indexes
- Verify reference counting doesn't slow operations

## Implementation Summary

### Incremental Approach Benefits
1. **Minimal disruption**: Each phase builds on existing code
2. **Testable steps**: Can verify each feature works before proceeding
3. **No big rewrites**: Modify existing modules in place
4. **Gradual migration**: Users can test features as they're added

### Key Files Modified Throughout
- `src/app/datasets/models.py` - Add new model fields
- `src/app/datasets/service.py` - Core business logic updates
- `src/app/datasets/repository.py` - Database query updates
- `src/app/storage/local_backend.py` - Add deduplication logic
- `src/app/datasets/routes.py` - API endpoint updates
- `src/app/users/service.py` - Permission management

### Migration Strategy
1. Run alembic migrations one at a time
2. Update code for each phase
3. Test through existing UI
4. No need for adapter layers
5. Existing data remains intact

## Next Steps After All Phases

### Optional Future Enhancements
1. **Advanced Features**:
   - Merge versions from different branches
   - Conflict resolution for schema changes
   - Advanced permission inheritance
   - File compression optimization

2. **Performance Optimizations**:
   - Implement caching for deduplicated files
   - Add database query optimization
   - Implement async file operations

3. **Module Extraction** (if needed later):
   - Once all features work, can refactor into modules
   - Extract permission system to separate module
   - Create dedicated versioning module
   - But only after everything is working!

### Success Criteria
- All existing functionality preserved
- New schema features fully implemented
- No breaking changes to API
- Performance maintained or improved
- All tests passing

