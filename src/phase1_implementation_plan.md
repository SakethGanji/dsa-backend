# Phase 1 Implementation Plan: Storage Vertical Slice & Core Interfaces

## Overview

This document provides a detailed implementation plan for Phase 1 of the vertical slice architecture migration, focusing on:
1. Establishing core interfaces (IArtifactProducer)
2. Refactoring the storage vertical slice
3. Updating all dependent slices to use the new abstractions
4. Setting up proper dependency injection

## Current State Analysis

### File Creation Across Slices
Currently, file creation logic is scattered across multiple slices:
- **Datasets**: Creates files when uploading datasets
- **Analysis**: Creates output files from analysis jobs
- **Sampling**: Creates sampled dataset files
- **Storage**: Provides low-level file operations

### Key Issues
1. **Code Duplication**: Each slice implements its own file creation logic
2. **Inconsistent Handling**: Different approaches to hashing, deduplication, and storage
3. **Circular Dependencies**: Storage imports from datasets, creating tight coupling
4. **No Streaming Support**: Large files loaded entirely into memory
5. **Race Conditions**: No protection against concurrent file creation

## Implementation Steps

### Step 1: Create Core Interfaces Module
**Location**: `app/core/interfaces.py`

#### 1.1 Create Core Module Structure
```
app/
├── core/
│   ├── __init__.py
│   ├── interfaces.py      # Core interface definitions
│   ├── types.py          # Shared type definitions
│   └── exceptions.py     # Shared exception classes
```

#### 1.2 Define IArtifactProducer Interface
```python
# app/core/interfaces.py
from typing import Protocol, BinaryIO, Dict, Any, Optional

# Critical: FileId MUST be int to match database schema
FileId = int  

class IArtifactProducer(Protocol):
    """Single point of entry for creating deduplicated file artifacts"""
    
    async def create_artifact(
        self,
        content_stream: BinaryIO,
        file_type: str,
        mime_type: Optional[str] = None,
        compression_type: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> FileId:
        """Create or reference an existing file artifact"""
        ...
```

#### 1.3 Define Shared Types
```python
# app/core/types.py
from typing import NewType, Union
from pathlib import Path

DatasetId = NewType('DatasetId', str)
VersionId = NewType('VersionId', str)
FileId = int  # MUST be int to match SERIAL PRIMARY KEY
StoragePath = Union[str, Path]
```

### Step 2: Refactor Storage Slice

#### 2.1 Create Storage Interfaces
**Location**: `app/storage/interfaces.py`

```python
from abc import ABC, abstractmethod
from typing import BinaryIO, List, Dict, Any
from pathlib import Path

class IStorageBackend(ABC):
    """Core storage operations for raw byte handling"""
    
    @abstractmethod
    async def write_stream(self, path: str, stream: BinaryIO) -> None:
        """Write content from stream (memory-efficient)"""
        pass
    
    @abstractmethod
    async def read_stream(self, path: str) -> BinaryIO:
        """Read content as stream"""
        pass
    
    @abstractmethod
    async def delete_file(self, path: str) -> None:
        """Delete a file"""
        pass
    
    @abstractmethod
    async def exists(self, path: str) -> bool:
        """Check if file exists"""
        pass
```

#### 2.2 Implement ArtifactProducer Service
**Location**: `app/storage/services/artifact_producer.py`

Key implementation details:
- Stream-based hashing (8KB chunks)
- Database-first approach for race condition handling
- Proper transaction rollback on failures
- Reference counting for deduplication

#### 2.3 Update Existing Storage Components
- Remove circular imports from `storage/data_adapter.py`
- Update `LocalFilesystemBackend` to implement `IStorageBackend`
- Separate data format readers from storage backend

### Step 3: Update Datasets Slice

#### 3.1 Remove File Creation Logic
**Files to modify**:
- `app/datasets/service.py`
- `app/datasets/controller.py`

#### 3.2 Inject IArtifactProducer
```python
# app/datasets/service.py
class DatasetService:
    def __init__(
        self, 
        repository: DatasetRepository,
        artifact_producer: IArtifactProducer  # New dependency
    ):
        self._repository = repository
        self._artifact_producer = artifact_producer
    
    async def create_dataset_with_files(self, dataset_data, files):
        # Use artifact_producer instead of direct file creation
        file_ids = []
        for file in files:
            file_id = await self._artifact_producer.create_artifact(
                content_stream=file.file,
                file_type=self._determine_file_type(file.filename),
                metadata={"original_name": file.filename}
            )
            file_ids.append(file_id)
```

### Step 4: Update Analysis Slice

#### 4.1 Modify Analysis Service
**File**: `app/analysis/services.py`

Replace direct file creation with artifact producer:
```python
async def run_analysis_job(self, job_config):
    # Generate analysis output
    output_data = await self._perform_analysis(job_config)
    
    # Create artifact through producer
    output_file_id = await self._artifact_producer.create_artifact(
        content_stream=io.BytesIO(output_data),
        file_type="parquet",
        metadata={"job_id": job_config.id}
    )
    
    return output_file_id
```

### Step 5: Update Sampling Slice

#### 5.1 Modify Sampling Service
**File**: `app/sampling/services.py`

Similar pattern to analysis slice - use artifact producer for output files.

### Step 6: Set Up Dependency Injection

#### 6.1 Create Dependencies Module
**Location**: `app/core/dependencies.py`

```python
from typing import Annotated
from fastapi import Depends
from sqlalchemy.orm import Session

from app.core.interfaces import IArtifactProducer
from app.storage.interfaces import IStorageBackend
from app.database import get_db

def get_storage_backend() -> IStorageBackend:
    """Get configured storage backend"""
    from app.storage.backends import LocalFilesystemBackend
    return LocalFilesystemBackend()

def get_artifact_producer(
    db: Annotated[Session, Depends(get_db)],
    storage: Annotated[IStorageBackend, Depends(get_storage_backend)]
) -> IArtifactProducer:
    """Get artifact producer instance"""
    from app.storage.services import ArtifactProducer
    return ArtifactProducer(db, storage)
```

#### 6.2 Update FastAPI Routes
Update all route handlers to use dependency injection:
```python
# app/datasets/routes.py
@router.post("/datasets")
async def create_dataset(
    dataset_service: Annotated[DatasetService, Depends(get_dataset_service)]
):
    # Route implementation
```

### Step 7: Testing Strategy

#### 7.1 Unit Tests for ArtifactProducer
**Location**: `tests/storage/test_artifact_producer.py`

Test cases:
- New file creation
- Duplicate file handling
- Stream processing
- Race condition handling
- Transaction rollback

#### 7.2 Integration Tests
**Location**: `tests/integration/test_artifact_production.py`

Test cases:
- End-to-end file creation
- Large file streaming
- Concurrent access
- Cross-slice usage

### Step 8: Migration Execution

#### 8.1 Database Considerations
- Ensure `files` table has proper constraints
- Add index on `content_hash` for performance
- Verify `reference_count` column exists

#### 8.2 Backward Compatibility
- Maintain existing APIs during transition
- Use feature flags if needed
- Plan for gradual rollout

## Detailed Task Breakdown

### Day 1-2: Core Interfaces & Types
- [ ] Create `app/core` module structure
- [ ] Define `IArtifactProducer` interface
- [ ] Define shared types (`FileId`, etc.)
- [ ] Create shared exceptions
- [ ] Add interface documentation

### Day 3-5: Storage Slice Refactoring
- [ ] Create storage interfaces (`IStorageBackend`)
- [ ] Implement `ArtifactProducer` service
- [ ] Add streaming support to backends
- [ ] Remove circular dependencies
- [ ] Add comprehensive logging

### Day 6-7: Datasets Slice Update
- [ ] Remove file creation logic from service
- [ ] Inject `IArtifactProducer` dependency
- [ ] Update controller to use new service
- [ ] Update tests
- [ ] Verify API compatibility

### Day 8-9: Analysis & Sampling Slices
- [ ] Update analysis service
- [ ] Update sampling service
- [ ] Ensure consistent error handling
- [ ] Add metadata tracking

### Day 10-11: Dependency Injection Setup
- [ ] Create central dependencies module
- [ ] Update all route handlers
- [ ] Configure proper scoping
- [ ] Add dependency overrides for testing

### Day 12-14: Testing & Documentation
- [ ] Write unit tests for all new components
- [ ] Create integration test suite
- [ ] Performance benchmarking
- [ ] Update API documentation
- [ ] Create migration guide

## Risk Mitigation

### Technical Risks
1. **Database Migration**: Test thoroughly in staging
2. **Performance Impact**: Benchmark streaming vs. current approach
3. **Concurrent Access**: Stress test race condition handling

### Process Risks
1. **API Changes**: Maintain backward compatibility
2. **Team Coordination**: Daily sync on progress
3. **Rollback Plan**: Keep old code paths available

## Success Criteria

1. **Zero Code Duplication**: File creation logic exists only in `ArtifactProducer`
2. **Clean Dependencies**: No circular imports between slices
3. **Memory Efficiency**: Can handle 10GB+ files without OOM
4. **Concurrent Safety**: No data corruption under load
5. **Test Coverage**: >90% coverage on new code
6. **Performance**: No regression in file upload times

## Next Steps

After Phase 1 completion:
- Phase 2: Event-driven communication between slices
- Phase 3: Additional interface extraction
- Phase 4: Cloud storage backend implementation

## Appendix: Code Examples

### Example: Using IArtifactProducer in Routes
```python
@router.post("/upload")
async def upload_file(
    file: UploadFile,
    producer: Annotated[IArtifactProducer, Depends(get_artifact_producer)]
):
    file_id = await producer.create_artifact(
        content_stream=file.file,
        file_type=file.content_type.split('/')[-1],
        mime_type=file.content_type
    )
    return {"file_id": file_id}
```

### Example: Testing with Mocks
```python
async def test_artifact_creation():
    mock_producer = Mock(spec=IArtifactProducer)
    mock_producer.create_artifact.return_value = 123
    
    service = AnalysisService(mock_producer)
    result = await service.run_job()
    
    assert result.output_file_id == 123
```