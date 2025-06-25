# Vertical Slice Architecture: Datasets & Storage

## Executive Summary

This document outlines a clean architecture approach for the datasets and storage vertical slices, incorporating the approved Core Services Interface Pattern (v1.2) for shared artifact production. The architecture emphasizes:

- **Clear responsibility boundaries** between vertical slices
- **Interface-driven design** with explicit contracts
- **Memory-efficient streaming** for large file handling
- **Transactional safety** with race condition protection
- **Single point of responsibility** for file artifact creation via IArtifactProducer

The Core Services Interface Pattern eliminates code duplication and ensures consistent file handling across all slices while maintaining clean architectural boundaries.

**Critical Note**: FileId is defined as `int` to match the database schema (`files.id SERIAL PRIMARY KEY`). This ensures type consistency across all layers.

## Current State Analysis

### Datasets Slice
**Location**: `app/src/datasets`

**Current Responsibilities**:
- Dataset CRUD operations
- Version management with overlay-based system
- File component management
- Tag-based categorization
- Dataset statistics computation
- Search functionality
- HTTP API exposure

**Key Strengths**:
- Clear layered architecture (routes → controller → service → repository)
- Well-defined domain models using Pydantic
- Separated search sub-module
- Good use of dependency injection

**Issues Identified**:
- No explicit interface definitions
- Direct coupling to storage implementation
- Mixed concerns in service layer
- File creation logic duplicated across slices

### Storage Slice
**Location**: `app/src/storage`

**Current Responsibilities**:
- Abstract storage operations
- File I/O operations
- Dataset reading/writing
- Storage backend management
- Factory-based backend creation
- **NEW**: Artifact production via IArtifactProducer implementation

**Key Strengths**:
- Abstract base classes defining contracts
- Protocol-based design for extensibility
- Factory pattern for backend creation
- Clear separation of concerns
- **NEW**: Streaming file handling for memory efficiency
- **NEW**: Transactional safety with race condition handling

**Issues Identified**:
- Circular dependency with datasets slice (resolved with core interfaces)
- Singleton pattern may limit testability
- Adapter imports from datasets services (resolved with IArtifactProducer)

## Proposed Architecture

### 1. Clear Responsibility Boundaries

#### Datasets Slice Responsibilities
```
DATASETS SLICE
├── API Layer
│   ├── HTTP endpoint definitions
│   ├── Request/response validation
│   └── OpenAPI documentation
├── Business Logic
│   ├── Dataset lifecycle management
│   ├── Version control logic
│   ├── Tag management
│   ├── Search orchestration
│   └── Statistics computation
├── Domain Models
│   ├── Dataset entities
│   ├── Version entities
│   ├── File component entities
│   └── Business rules validation
└── Data Access
    ├── Database operations
    ├── Query optimization
    └── Transaction management
```

#### Storage Slice Responsibilities
```
STORAGE SLICE
├── Storage Abstraction (IStorageBackend)
│   ├── Raw byte stream operations
│   ├── File I/O interface
│   ├── Stream handling
│   └── Path management
├── Backend Implementations
│   ├── Local filesystem
│   ├── S3 (future)
│   ├── Azure Blob (future)
│   └── GCS (future)
├── Data Format Readers (IDatasetReader implementations)
│   ├── CSV Reader (separate from storage backend)
│   ├── Parquet Reader (separate from storage backend)
│   ├── JSON Reader (separate from storage backend)
│   └── Format-specific parsing logic
├── Artifact Production (IArtifactProducer)
│   ├── Content hashing
│   ├── Deduplication logic
│   ├── Transactional file creation
│   └── Reference counting
└── Storage Management
    ├── Backend lifecycle
    ├── Connection pooling
    └── Resource cleanup
```

**Note**: The Storage slice maintains separation of concerns between raw byte storage (IStorageBackend) and format-specific parsing (IDatasetReader). Storage backends handle bytes; readers handle parsing.

### 2. Interface Definitions

#### Core Interfaces (Shared Contracts)

```python
# app/core/interfaces.py
from typing import Protocol, BinaryIO, Dict, Any, Optional

# Type aliases
FileId = int

class IArtifactProducer(Protocol):
    """
    Handles the creation and registration of new, deduplicated file artifacts.
    This is the single point of entry for adding files to the system.
    """
    async def create_artifact(
        self,
        content_stream: BinaryIO,
        file_type: str,  # e.g., 'parquet', 'json'. Corresponds to files.file_type
        mime_type: Optional[str] = None,
        compression_type: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> FileId:
        """
        Creates a new artifact from a stream.

        This method is designed to be both memory-efficient and concurrency-safe.
        It handles hashing, checks for duplicates, performs the physical storage write,
        and registers the file record in the database.

        Args:
            content_stream: A binary stream-like object (e.g., from an open file or io.BytesIO).
            file_type: The type of the file, e.g., 'parquet', 'csv', 'json'.
            mime_type: The standard MIME type, e.g., 'application/vnd.apache.parquet'.
            compression_type: The compression used, e.g., 'snappy', 'gzip'.
            metadata: An arbitrary JSON-serializable dictionary for additional context.

        Returns:
            The stable integer ID of the file record from the 'files' table.
        """
        ...
```

#### Storage Interfaces

```python
# storage/interfaces.py
from abc import ABC, abstractmethod
from typing import Protocol, Any, Dict, List, Optional, BinaryIO
from pathlib import Path

class IStorageBackend(ABC):
    """Core storage operations interface"""
    
    @abstractmethod
    async def read_file(self, path: Path) -> bytes:
        """Read file content"""
        pass
    
    @abstractmethod
    async def write_stream(self, path: str, stream: BinaryIO) -> None:
        """Write file content from stream (memory-efficient)"""
        pass
    
    @abstractmethod
    async def delete_file(self, path: Path) -> None:
        """Delete a file"""
        pass
    
    @abstractmethod
    async def exists(self, path: Path) -> bool:
        """Check if file exists"""
        pass
    
    @abstractmethod
    async def list_files(self, prefix: Path) -> List[Path]:
        """List files with prefix"""
        pass
    
    @abstractmethod
    async def get_file_info(self, path: Path) -> Dict[str, Any]:
        """Get file metadata"""
        pass

class IDatasetReader(Protocol):
    """Dataset reading operations interface"""
    
    # Note: In a real implementation, these would return pd.DataFrame
    # or a similar concrete type based on your data processing library
    def read_csv(self, path: Path, **kwargs) -> Any:  # e.g., -> pd.DataFrame
        """Read CSV file into a DataFrame"""
        ...
    
    def read_parquet(self, path: Path, **kwargs) -> Any:  # e.g., -> pd.DataFrame
        """Read Parquet file into a DataFrame"""
        ...
    
    def read_json(self, path: Path, **kwargs) -> Any:  # e.g., -> pd.DataFrame
        """Read JSON file into a DataFrame"""
        ...

class IStorageFactory(ABC):
    """Storage backend factory interface"""
    
    @abstractmethod
    def create_backend(self, backend_type: str, **config) -> IStorageBackend:
        """Create storage backend instance"""
        pass
```

#### Dataset Interfaces

```python
# datasets/interfaces.py
from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any
from datetime import datetime
from .models import Dataset, DatasetVersion, File, Tag

class IDatasetRepository(ABC):
    """Dataset data access interface"""
    
    @abstractmethod
    async def create_dataset(self, dataset: Dataset) -> Dataset:
        """Create a new dataset"""
        pass
    
    @abstractmethod
    async def get_dataset(self, dataset_id: str) -> Optional[Dataset]:
        """Get dataset by ID"""
        pass
    
    @abstractmethod
    async def update_dataset(self, dataset_id: str, updates: Dict[str, Any]) -> Dataset:
        """Update dataset"""
        pass
    
    @abstractmethod
    async def delete_dataset(self, dataset_id: str) -> None:
        """Delete dataset"""
        pass

class IDatasetService(ABC):
    """Dataset business logic interface"""
    
    @abstractmethod
    async def create_dataset_with_files(
        self, 
        dataset_data: Dict[str, Any], 
        files: List[Any]
    ) -> Dataset:
        """Create dataset with initial files"""
        pass
    
    @abstractmethod
    async def create_version(
        self, 
        dataset_id: str, 
        version_data: Dict[str, Any]
    ) -> DatasetVersion:
        """Create new dataset version"""
        pass
    
    @abstractmethod
    async def compute_statistics(self, dataset_id: str) -> Dict[str, Any]:
        """Compute dataset statistics"""
        pass

class ISearchService(ABC):
    """Search operations interface"""
    
    @abstractmethod
    async def search_datasets(
        self, 
        query: str, 
        filters: Dict[str, Any]
    ) -> List[Dataset]:
        """Search datasets"""
        pass
```

### 3. Dependency Injection Configuration

```python
# app/core/dependencies.py
from typing import Annotated
from fastapi import Depends
from sqlalchemy.orm import Session

from app.core.interfaces import IArtifactProducer
from app.storage.interfaces import IStorageFactory, IStorageBackend
from app.datasets.interfaces import IDatasetRepository, IDatasetService
from app.database import get_db

# Core dependencies
def get_artifact_producer(
    db: Annotated[Session, Depends(get_db)],
    storage: Annotated[IStorageBackend, Depends(get_storage_backend)]
) -> IArtifactProducer:
    """Get artifact producer instance"""
    from app.storage.services import ArtifactProducer
    return ArtifactProducer(db, storage)

# Storage dependencies
def get_storage_factory() -> IStorageFactory:
    """Get storage factory instance"""
    from app.storage.factory import StorageFactory
    return StorageFactory()

def get_storage_backend(
    factory: Annotated[IStorageFactory, Depends(get_storage_factory)]
) -> IStorageBackend:
    """Get storage backend instance"""
    return factory.create_backend("local")

# Dataset dependencies
def get_dataset_repository(
    db: Annotated[Session, Depends(get_db)]
) -> IDatasetRepository:
    """Get dataset repository instance"""
    from app.datasets.repository import DatasetRepository
    return DatasetRepository(db)

def get_dataset_service(
    repository: Annotated[IDatasetRepository, Depends(get_dataset_repository)],
    artifact_producer: Annotated[IArtifactProducer, Depends(get_artifact_producer)]
) -> IDatasetService:
    """Get dataset service instance"""
    from app.datasets.service import DatasetService
    return DatasetService(repository, artifact_producer)
```

### 4. Cross-Slice Communication

#### Event-Driven Communication
```python
# app/core/events.py
from typing import Protocol, Dict, Any, Callable
from enum import Enum

class EventType(Enum):
    DATASET_CREATED = "dataset.created"
    DATASET_UPDATED = "dataset.updated"
    DATASET_DELETED = "dataset.deleted"
    FILE_UPLOADED = "file.uploaded"
    FILE_DELETED = "file.deleted"

class IEventBus(Protocol):
    """Event bus for cross-slice communication"""
    
    async def publish(self, event_type: EventType, data: Dict[str, Any]) -> None:
        """Publish an event"""
        ...
    
    async def subscribe(self, event_type: EventType, handler: Callable) -> None:
        """Subscribe to an event"""
        ...
```

#### Shared Types
```python
# app/core/types.py
from typing import NewType, Union
from pathlib import Path

DatasetId = NewType('DatasetId', str)
VersionId = NewType('VersionId', str)
FileId = NewType('FileId', int)  # MUST be int to match SERIAL PRIMARY KEY
StoragePath = Union[str, Path]
```

### 5. Implementation Guidelines

#### SOLID Principles

1. **Single Responsibility**: Each class should have one reason to change
2. **Open/Closed**: Open for extension, closed for modification
3. **Liskov Substitution**: Implementations must be substitutable for their interfaces
4. **Interface Segregation**: Clients shouldn't depend on interfaces they don't use
5. **Dependency Inversion**: Depend on abstractions, not concretions

#### Best Practices

1. **Use Dependency Injection**: All dependencies should be injected
2. **Favor Composition**: Compose behaviors rather than inherit
3. **Explicit Interfaces**: Define clear contracts between slices
4. **Immutable Data**: Use immutable data structures where possible
5. **Error Handling**: Use custom exceptions for domain errors
6. **Streaming for Large Files**: Use streaming APIs to handle large files without memory exhaustion
7. **Transactional Safety**: Implement proper transaction handling with rollback on failures
8. **Race Condition Handling**: Use database constraints and proper exception handling for concurrent operations

### 6. Testing Strategy

#### Unit Testing
```python
# tests/storage/test_artifact_producer.py
class TestArtifactProducer:
    async def test_create_artifact_new_file(self, mock_db, mock_storage):
        producer = ArtifactProducer(mock_db, mock_storage)
        # Test new file creation
        
    async def test_create_artifact_duplicate_file(self, mock_db, mock_storage):
        producer = ArtifactProducer(mock_db, mock_storage)
        # Test duplicate file handling
        
    async def test_handle_race_condition(self, mock_db, mock_storage):
        producer = ArtifactProducer(mock_db, mock_storage)
        # Test concurrent file creation
```

#### Integration Testing
```python
# tests/integration/test_artifact_production.py
class TestArtifactProductionIntegration:
    async def test_large_file_streaming(self, test_db, test_storage):
        # Test streaming large files without memory issues
        
    async def test_concurrent_artifact_creation(self, test_db, test_storage):
        # Test multiple processes creating same file
```

### 7. Migration Plan

#### Phase 1: Interface Definition (Week 1)
- [ ] Create interface files for both slices
- [ ] Define shared types and events
- [ ] Document interface contracts

#### Phase 2: Refactor Storage Slice (Week 2)
- [ ] Implement interfaces in storage slice
- [ ] Remove circular dependencies
- [ ] Add comprehensive tests

#### Phase 3: Refactor Dataset Slice (Week 3)
- [ ] Implement interfaces in dataset slice
- [ ] Update dependency injection
- [ ] Refactor service layer

#### Phase 4: Integration Testing (Week 4)
- [ ] Comprehensive integration tests
- [ ] Performance benchmarking
- [ ] Documentation updates

### 8. Architecture Decision Records (ADRs)

#### ADR-001: Use Interface Segregation
**Status**: Approved
**Context**: Need clear boundaries between vertical slices
**Decision**: Use Python ABC and Protocol for interface definitions
**Consequences**: Better testability, clear contracts, easier mocking

#### ADR-002: Core Services Interface Pattern
**Status**: Approved
**Context**: Multiple slices need to create file artifacts
**Decision**: Implement IArtifactProducer in core module, with storage providing implementation
**Consequences**: Single responsibility, no code duplication, clean dependencies

#### ADR-003: Streaming File Handling
**Status**: Approved
**Context**: System must handle large files without memory exhaustion
**Decision**: Use streaming APIs with chunked processing
**Consequences**: Can handle multi-GB files, slightly more complex implementation

#### ADR-004: Transactional File Creation
**Status**: Approved
**Context**: Need to handle concurrent file creation safely
**Decision**: Insert DB record first, then upload, with proper rollback
**Consequences**: Data consistency guaranteed, race conditions handled gracefully

#### ADR-005: Overlay-Based Versioning
**Status**: Accepted
**Context**: Need efficient dataset versioning
**Decision**: Use overlay files to track changes
**Consequences**: Space efficient, complex queries, good performance

### 9. Performance Considerations

1. **Lazy Loading**: Load data only when needed
2. **Caching**: Cache frequently accessed data
3. **Batch Operations**: Support batch operations for efficiency
4. **Async Operations**: Use async/await for I/O operations
5. **Connection Pooling**: Pool database and storage connections

### 10. Security Considerations

1. **Input Validation**: Validate all inputs at boundaries
2. **Path Traversal**: Prevent path traversal attacks
3. **Access Control**: Implement proper access control
4. **Audit Logging**: Log all critical operations
5. **Secrets Management**: Never store secrets in code

### 11. Example Implementation

#### Storage Slice: ArtifactProducer
```python
# app/storage/services.py
import hashlib
from typing import BinaryIO
from sqlalchemy.exc import IntegrityError
from app.core.interfaces import IArtifactProducer, FileId

class ArtifactProducer(IArtifactProducer):
    """Production-ready artifact producer with streaming and transaction support"""
    
    def __init__(self, db: Session, storage_backend: IStorageBackend):
        self._db = db
        self._storage = storage_backend
    
    async def create_artifact(
        self, content_stream, file_type, mime_type=None, 
        compression_type=None, metadata=None
    ) -> FileId:
        # Stream content to calculate hash without loading into memory
        hasher = hashlib.sha256()
        content_size = 0
        while chunk := content_stream.read(8192):  # 8KB chunks
            hasher.update(chunk)
            content_size += len(chunk)
        
        content_hash = hasher.hexdigest()
        content_stream.seek(0)  # Rewind for upload
        
        # Check for existing file
        existing_file = self._db.query(File).filter(
            File.content_hash == content_hash
        ).first()
        
        if existing_file:
            existing_file.reference_count += 1
            self._db.commit()
            return existing_file.id
        
        # Handle new file with race condition protection
        storage_path = f"artifacts/{content_hash}"
        
        try:
            # Insert DB record first
            new_file = File(
                content_hash=content_hash,
                file_path=storage_path,
                file_size=content_size,
                file_type=file_type,
                mime_type=mime_type,
                compression_type=compression_type,
                reference_count=1,
                metadata=metadata or {}
            )
            self._db.add(new_file)
            self._db.commit()
            self._db.refresh(new_file)
        except IntegrityError:
            # Lost the race, another process created it
            self._db.rollback()
            conflicting_file = self._db.query(File).filter(
                File.content_hash == content_hash
            ).one()
            conflicting_file.reference_count += 1
            self._db.commit()
            return conflicting_file.id
        
        # Upload to storage
        try:
            await self._storage.write_stream(storage_path, content_stream)
        except Exception as e:
            # Rollback DB record on storage failure
            self._db.delete(new_file)
            self._db.commit()
            raise IOError(f"Storage upload failed for {content_hash}") from e
        
        return new_file.id
```

#### Consumer Slice: Using IArtifactProducer
```python
# app/analysis/services.py
import io
import pandas as pd
from app.core.interfaces import IArtifactProducer, FileId

class AnalysisService:
    """Service that produces file artifacts without knowing storage details"""
    
    def __init__(self, artifact_producer: IArtifactProducer):
        self._producer = artifact_producer
    
    async def run_sampling_job(self, source_version_id: int) -> FileId:
        """Generate analysis output using the artifact producer"""
        # Generate data
        sample_df = pd.DataFrame({
            'product_id': [101, 102, 103],
            'category': ['A', 'B', 'A'],
            'sales': [150.50, 200.00, 75.25]
        })
        
        # Convert to Parquet in memory
        sample_bytes = sample_df.to_parquet()
        
        # Use artifact producer - no knowledge of storage implementation
        output_file_id = await self._producer.create_artifact(
            content_stream=io.BytesIO(sample_bytes),
            file_type="parquet",
            mime_type="application/vnd.apache.parquet",
            compression_type="snappy",
            metadata={
                "analysis_type": "sampling",
                "source_version_id": source_version_id,
                "generated_by": "AnalysisService.run_sampling_job"
            }
        )
        
        return output_file_id
```

## Conclusion

This architecture, enhanced with the Core Services Interface Pattern v1.2, provides:

- **Clear separation of concerns** with well-defined vertical slices
- **Interface-driven design** ensuring loose coupling
- **Production-ready file handling** with streaming and transactional safety
- **Single point of responsibility** for artifact creation
- **Testable components** through dependency injection
- **Scalable design** supporting large files and concurrent operations
- **Maintainable codebase** with clear contracts and minimal duplication

The IArtifactProducer pattern exemplifies how shared functionality can be centralized without violating architectural boundaries, providing a blueprint for future cross-slice requirements.