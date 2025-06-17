# Dataset Management System - Incremental Implementation Plan

## Overview
This document outlines a step-by-step implementation plan for migrating from the current vertical slice architecture to a modular monolith with the new schema. Each phase is designed to be independently testable and deployable.

## Architecture Decisions

### Core Principles
1. **Incremental Migration**: Keep existing API working while building new system
2. **Test at Each Step**: Each phase must be fully testable before moving on
3. **No Feature Loss**: Maintain all current functionality during migration
4. **Modular Boundaries**: Clear separation of concerns between modules
5. **Direct Route Updates**: Update API routes in each phase to use new services directly, enabling UI testing without legacy adapters

### Module Structure
```
src/modules/
├── core/           # Shared domain primitives, base classes
├── files/          # Content-addressable file storage
├── datasets/       # Dataset lifecycle management
├── versioning/     # Version DAG and branching logic
├── permissions/    # Authorization and access control
└── api/           # HTTP layer and DTOs
```

### Cross-Module Communication
- Use dependency injection with interfaces
- No direct module-to-module imports (only through interfaces)
- Events for eventual consistency where needed

## Phase 1: Core Infrastructure (Day 1-2)

### Goals
- Set up modular structure
- Create base domain classes
- Establish testing patterns

### Implementation Steps

1. **Create Base Domain Classes**
```python
# src/modules/core/domain/base.py
- Entity base class with id, created_at, updated_at
- ValueObject base class
- Repository interface (generic)
- AggregateRoot with event sourcing support
- DomainEvent base class
```

2. **Create Shared Types**
```python
# src/modules/core/domain/types.py
- UserId (value object)
- ContentHash (value object) 
- FilePath (value object)
- Permission enum
- Status enums
```

3. **Database Connection Management**
```python
# src/modules/core/infrastructure/database.py
- Connection pool management
- Transaction context manager
- Query builder helpers
```

4. **Testing Infrastructure**
```python
# tests/modules/core/
- Base test classes
- Database fixtures
- Factory patterns for test data
```

### Validation
- [ ] Can create and persist a simple entity
- [ ] Transaction rollback works correctly
- [ ] Base repository pattern works

## Phase 2: Content-Addressable File Store (Day 3-4)

### Goals
- Implement file storage with deduplication
- Content hashing (SHA256)
- Reference counting for garbage collection

### Schema Changes
```sql
-- Already exists in new schema
CREATE TABLE files (
    id SERIAL PRIMARY KEY,
    storage_type VARCHAR(50) NOT NULL,
    file_type VARCHAR(50) NOT NULL,
    mime_type VARCHAR(100),
    file_path TEXT,
    file_size BIGINT,
    content_hash CHAR(64) UNIQUE,
    reference_count BIGINT NOT NULL DEFAULT 0,
    compression_type VARCHAR(50),
    metadata JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
```

### Implementation Steps

1. **File Domain Model**
```python
# src/modules/files/domain/file.py
@dataclass
class File(Entity):
    content_hash: ContentHash
    file_path: FilePath
    file_size: int
    reference_count: int
    storage_type: str
    file_type: str
    
    def increment_references(self) -> None:
        self.reference_count += 1
        
    def decrement_references(self) -> None:
        if self.reference_count > 0:
            self.reference_count -= 1
```

2. **File Service**
```python
# src/modules/files/service/file_service.py
class FileService:
    async def store_file(
        self, 
        content: bytes, 
        file_type: str
    ) -> File:
        # 1. Calculate SHA256 hash
        # 2. Check if file exists by hash
        # 3. If exists, increment reference count
        # 4. If not, store file and create record
        # 5. Return File entity
        
    async def get_file(self, file_id: int) -> Optional[bytes]:
        # Retrieve file content by ID
        
    async def delete_reference(self, file_id: int) -> None:
        # Decrement reference count
        # If count = 0, mark for garbage collection
```

3. **Storage Backend**
```python
# src/modules/files/infrastructure/storage_backend.py
class StorageBackend(ABC):
    @abstractmethod
    async def store(self, path: str, content: bytes) -> None:
        pass
        
    @abstractmethod
    async def retrieve(self, path: str) -> bytes:
        pass
        
    @abstractmethod
    async def delete(self, path: str) -> None:
        pass

# Local implementation for testing
class LocalStorageBackend(StorageBackend):
    # Implementation using filesystem
```

### Validation
- [ ] Can store a file and get same hash for duplicate content
- [ ] Reference counting works correctly
- [ ] Can retrieve file by ID
- [ ] Deduplication works (same content = same record)

## Phase 3: Basic Dataset Creation (Day 5-6)

### Goals
- Create datasets without versioning
- Basic metadata management
- User association

### Schema Changes
```sql
-- Already exists in new schema
CREATE TABLE datasets (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    created_by INT NOT NULL REFERENCES users(id),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(name, created_by)
);
```

### Implementation Steps

1. **Dataset Domain Model**
```python
# src/modules/datasets/domain/dataset.py
@dataclass
class Dataset(AggregateRoot):
    name: str
    description: Optional[str]
    created_by: UserId
    
    def update_metadata(self, name: str, description: str) -> None:
        self.name = name
        self.description = description
        self.add_event(DatasetUpdatedEvent(self.id))
```

2. **Dataset Service**
```python
# src/modules/datasets/service/dataset_service.py
class DatasetService:
    def __init__(self, dataset_repo: DatasetRepository):
        self.dataset_repo = dataset_repo
        
    async def create_dataset(
        self,
        name: str,
        description: str,
        user_id: int
    ) -> Dataset:
        # Check for duplicate name per user
        # Create dataset
        # Save to repository
        
    async def update_dataset(
        self,
        dataset_id: int,
        name: str,
        description: str
    ) -> Dataset:
        # Load dataset
        # Update metadata
        # Save changes
```

3. **API Endpoints**
```python
# src/modules/api/datasets/routes.py
@router.post("/datasets")
async def create_dataset(request: CreateDatasetRequest) -> DatasetResponse:
    # Call dataset service
    # Return response DTO
    
@router.get("/datasets/{dataset_id}")
async def get_dataset(dataset_id: int) -> DatasetResponse:
    # Retrieve and return dataset
```

**Note**: Update these routes to work with the UI immediately after implementing the service layer. This allows testing through the UI without waiting for legacy adapters.

### Validation
- [ ] Can create a dataset with metadata
- [ ] Duplicate names per user are prevented
- [ ] Can update dataset metadata
- [ ] API endpoints work correctly

## Phase 4: Simple Linear Versioning (Day 7-8)

### Goals
- Add version tracking to datasets
- Linear versioning only (no branching yet)
- Version numbers auto-increment

### Schema Changes
```sql
-- Simplified version without DAG support initially
CREATE TABLE dataset_versions (
    id SERIAL PRIMARY KEY,
    dataset_id INT NOT NULL REFERENCES datasets(id),
    version_number INT NOT NULL,
    message TEXT,
    created_by INT NOT NULL REFERENCES users(id),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (dataset_id, version_number)
);
```

### Implementation Steps

1. **Version Domain Model**
```python
# src/modules/versioning/domain/version.py
@dataclass
class DatasetVersion(Entity):
    dataset_id: int
    version_number: int
    message: str
    created_by: UserId
    
    @staticmethod
    def create_initial(dataset_id: int, user_id: int) -> 'DatasetVersion':
        return DatasetVersion(
            dataset_id=dataset_id,
            version_number=1,
            message="Initial version",
            created_by=UserId(user_id)
        )
```

2. **Versioning Service**
```python
# src/modules/versioning/service/version_service.py
class VersionService:
    async def create_version(
        self,
        dataset_id: int,
        message: str,
        user_id: int
    ) -> DatasetVersion:
        # Get latest version number
        # Increment by 1
        # Create new version
        # Save to repository
        
    async def list_versions(
        self,
        dataset_id: int
    ) -> List[DatasetVersion]:
        # Return all versions for dataset
```

### Validation
- [ ] First version is automatically v1
- [ ] Subsequent versions increment correctly
- [ ] Can list all versions for a dataset
- [ ] Version messages are stored

## Phase 5: File Attachment to Versions (Day 9-10)

### Goals
- Attach files to dataset versions
- Implement reference counting
- Support multiple files per version

### Schema Changes
```sql
-- Update dataset_versions to include file references
ALTER TABLE dataset_versions 
ADD COLUMN overlay_file_id INT REFERENCES files(id);

-- Multi-file support
CREATE TABLE dataset_version_files (
    version_id INT NOT NULL REFERENCES dataset_versions(id),
    file_id INT NOT NULL REFERENCES files(id),
    component_type VARCHAR(50) NOT NULL,
    component_name TEXT,
    component_index INT,
    metadata JSONB,
    PRIMARY KEY (version_id, file_id)
);
```

### Implementation Steps

1. **Updated Version Model**
```python
# src/modules/versioning/domain/version.py
@dataclass
class DatasetVersion(Entity):
    dataset_id: int
    version_number: int
    message: str
    created_by: UserId
    overlay_file_id: Optional[int]
    attached_files: List[VersionFile] = field(default_factory=list)
    
    def attach_file(self, file_id: int, component_type: str) -> None:
        self.attached_files.append(
            VersionFile(file_id, component_type)
        )
```

2. **Integrated Service**
```python
# src/modules/datasets/service/dataset_version_service.py
class DatasetVersionService:
    def __init__(
        self,
        version_service: VersionService,
        file_service: FileService
    ):
        self.version_service = version_service
        self.file_service = file_service
        
    async def create_version_with_file(
        self,
        dataset_id: int,
        file_content: bytes,
        file_type: str,
        message: str,
        user_id: int
    ) -> DatasetVersion:
        # Store file using file service
        # Create version
        # Attach file to version
        # Increment file reference count
```

### Validation
- [ ] Can create version with attached file
- [ ] File reference counting works
- [ ] Can attach multiple files to a version
- [ ] Deleting version decrements file references

## Phase 6: Schema Capture (Day 11-12)

### Goals
- Automatically capture schema from uploaded files
- Store schema snapshots per version
- Support schema evolution tracking

### Schema Changes
```sql
-- Already in new schema
CREATE TABLE dataset_schema_versions (
    id SERIAL PRIMARY KEY,
    dataset_version_id INT NOT NULL REFERENCES dataset_versions(id),
    schema_json JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
```

### Implementation Steps

1. **Schema Extraction**
```python
# src/modules/datasets/service/schema_service.py
class SchemaService:
    async def extract_schema(self, file_path: str, file_type: str) -> dict:
        # Use DuckDB to analyze file
        # Extract column names, types, nullable
        # Return as JSON schema
        
    async def capture_version_schema(
        self,
        version_id: int,
        file_id: int
    ) -> None:
        # Get file from file service
        # Extract schema
        # Store schema snapshot
```

### Validation
- [ ] Schema extracted correctly from CSV/Parquet/Excel
- [ ] Schema stored with each version
- [ ] Can compare schemas between versions

## Phase 7: Permission System (Day 13-14)

### Goals
- Implement role-based permissions
- Dataset and file level permissions
- Permission checks in services

### Implementation Steps

1. **Permission Domain**
```python
# src/modules/permissions/domain/permission.py
@dataclass
class Permission(ValueObject):
    resource_type: str  # 'dataset' or 'file'
    resource_id: int
    user_id: UserId
    permission_type: PermissionType  # read/write/admin
```

2. **Permission Service**
```python
# src/modules/permissions/service/permission_service.py
class PermissionService:
    async def grant_permission(
        self,
        resource_type: str,
        resource_id: int,
        user_id: int,
        permission_type: str
    ) -> None:
        # Add permission record
        
    async def check_permission(
        self,
        resource_type: str,
        resource_id: int,
        user_id: int,
        required_permission: str
    ) -> bool:
        # Check if user has permission
```

3. **Permission Decorators**
```python
# src/modules/permissions/decorators.py
def requires_permission(resource_type: str, permission: str):
    def decorator(func):
        async def wrapper(self, resource_id: int, user_id: int, *args, **kwargs):
            if not await self.permission_service.check_permission(
                resource_type, resource_id, user_id, permission
            ):
                raise PermissionDeniedError()
            return await func(self, resource_id, user_id, *args, **kwargs)
        return wrapper
    return decorator
```

### Validation
- [ ] Can grant/revoke permissions
- [ ] Permission checks work in services
- [ ] API returns 403 for unauthorized access

## Phase 8: DAG Support (Day 15-16)

### Goals
- Enable branching in version history
- Support parent version references
- Implement branch pointers

### Schema Changes
```sql
-- Add parent reference to versions
ALTER TABLE dataset_versions
ADD COLUMN parent_version_id INT REFERENCES dataset_versions(id);

-- Add branch/tag pointers
CREATE TABLE dataset_pointers (
    id SERIAL PRIMARY KEY,
    dataset_id INT NOT NULL REFERENCES datasets(id),
    pointer_name VARCHAR(255) NOT NULL,
    dataset_version_id INT NOT NULL REFERENCES dataset_versions(id),
    is_tag BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE (dataset_id, pointer_name)
);
```

### Implementation Steps

1. **Updated Version Model**
```python
# src/modules/versioning/domain/version.py
@dataclass
class DatasetVersion(Entity):
    # ... existing fields ...
    parent_version_id: Optional[int]
    
    def create_branch(self, message: str, user_id: int) -> 'DatasetVersion':
        return DatasetVersion(
            dataset_id=self.dataset_id,
            parent_version_id=self.id,
            message=message,
            created_by=UserId(user_id)
        )
```

2. **Branch Management**
```python
# src/modules/versioning/service/branch_service.py
class BranchService:
    async def create_branch(
        self,
        dataset_id: int,
        branch_name: str,
        from_version_id: int
    ) -> None:
        # Create branch pointer
        # Point to specific version
        
    async def update_branch(
        self,
        dataset_id: int,
        branch_name: str,
        version_id: int
    ) -> None:
        # Move branch pointer to new version
```

### Validation
- [ ] Can create branches from any version
- [ ] Version DAG correctly represents history
- [ ] Branch pointers work correctly

## Phase 9: Data Migration (Day 17-18)

### Goals
- Migrate existing data to new schema
- Maintain data integrity
- Zero downtime migration

### Migration Steps

1. **Analysis Script**
```python
# scripts/analyze_migration.py
- Count existing datasets and versions
- Check for data inconsistencies
- Generate migration report
```

2. **Migration Script**
```python
# scripts/migrate_data.py
- Migrate users and roles
- Migrate files with hash calculation
- Migrate datasets
- Migrate versions with parent relationships
- Update reference counts
```

3. **Validation Script**
```python
# scripts/validate_migration.py
- Compare row counts
- Verify file integrity
- Check reference counts
- Validate relationships
```

## Phase 10: API Adapter Layer (Day 19-20)

### Goals
- Replace old endpoints with adapters
- Maintain backward compatibility
- Gradual deprecation strategy

### Implementation Steps

1. **Adapter Pattern**
```python
# src/modules/api/adapters/legacy_adapter.py
class LegacyDatasetAdapter:
    def __init__(self, new_services: dict):
        self.dataset_service = new_services['dataset']
        self.version_service = new_services['version']
        self.file_service = new_services['file']
        
    async def upload_dataset(self, legacy_request) -> LegacyResponse:
        # Transform legacy request
        # Call new services
        # Transform response to legacy format
```

2. **Route Updates for Testing**
```python
# Update routes incrementally for easy UI testing
# Each phase should update relevant routes to use new services
# This allows step-by-step testing through the UI without legacy adapters

# Example: Phase 3 - Update dataset creation route
@router.post("/api/datasets")
async def create_dataset(request: CreateDatasetRequest):
    # Direct call to new service, no adapter needed
    return await dataset_service.create_dataset(request)

# Example: Phase 5 - Update file upload route
@router.post("/api/datasets/{dataset_id}/versions")
async def create_version(dataset_id: int, file: UploadFile):
    # Direct integration with new version service
    return await version_service.create_version_with_file(dataset_id, file)
```

### Testing Strategy
- Update routes as each phase is completed
- Test directly through UI without adapter complexity
- Keep old routes temporarily if needed, but prefer direct updates
- Each phase should have fully functional routes for testing

