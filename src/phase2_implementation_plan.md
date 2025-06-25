# Phase 2 Implementation Plan: Event-Driven Architecture & Dataset Interfaces

## Overview

Building on the successful Phase 1 implementation of the Storage vertical slice and Core interfaces, Phase 2 focuses on:

1. **Event-Driven Communication** - Implementing cross-slice communication via event bus
2. **Dataset Slice Interfaces** - Formalizing dataset slice contracts

## Objectives

- Enable loose coupling between slices through event-driven architecture
- Complete interface extraction for dataset slice
- Establish clear contracts for dataset operations
- Implement shared types for better type safety

## Architecture Alignment

This phase directly addresses key goals from the vertical slice architecture:
- **Cross-slice communication** via IEventBus
- **Interface-driven design** for dataset operations
- **Clear separation of concerns** between vertical slices
- **Type safety** through shared type definitions

## Implementation Components

### 1. Event-Driven Architecture

#### 1.1 Event Bus Interface
**Location**: `app/core/events.py`

```python
from typing import Protocol, Dict, Any, Callable, List
from enum import Enum
from dataclasses import dataclass
from datetime import datetime

class EventType(Enum):
    # Dataset events
    DATASET_CREATED = "dataset.created"
    DATASET_UPDATED = "dataset.updated"
    DATASET_DELETED = "dataset.deleted"
    VERSION_CREATED = "dataset.version.created"
    
    # File events
    FILE_UPLOADED = "file.uploaded"
    FILE_DELETED = "file.deleted"
    FILE_DEDUPLICATED = "file.deduplicated"
    
    # Sampling events
    SAMPLE_CREATED = "sample.created"
    SAMPLE_COMPLETED = "sample.completed"
    SAMPLE_FAILED = "sample.failed"

@dataclass
class Event:
    """Base event structure"""
    event_type: EventType
    timestamp: datetime
    data: Dict[str, Any]
    correlation_id: Optional[str] = None
    source: Optional[str] = None

class IEventBus(Protocol):
    """Event bus for cross-slice communication"""
    
    async def publish(self, event: Event) -> None:
        """Publish an event"""
        ...
    
    async def subscribe(self, event_type: EventType, handler: Callable[[Event], None]) -> None:
        """Subscribe to an event type"""
        ...
    
    async def unsubscribe(self, event_type: EventType, handler: Callable[[Event], None]) -> None:
        """Unsubscribe from an event type"""
        ...
```

#### 1.2 Event Bus Implementation
**Location**: `app/core/services/event_bus.py`

```python
from typing import Dict, List, Callable
from collections import defaultdict
import asyncio
import logging

from app.core.events import IEventBus, Event, EventType

logger = logging.getLogger(__name__)

class InMemoryEventBus(IEventBus):
    """Simple in-memory event bus implementation"""
    
    def __init__(self):
        self._handlers: Dict[EventType, List[Callable]] = defaultdict(list)
        self._lock = asyncio.Lock()
    
    async def publish(self, event: Event) -> None:
        """Publish an event to all registered handlers"""
        handlers = self._handlers.get(event.event_type, [])
        
        if not handlers:
            logger.debug(f"No handlers for event type {event.event_type}")
            return
        
        # Execute handlers concurrently
        tasks = []
        for handler in handlers:
            task = asyncio.create_task(self._execute_handler(handler, event))
            tasks.append(task)
        
        # Wait for all handlers to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Log any handler errors
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(
                    f"Handler {handlers[i].__name__} failed for event {event.event_type}: {result}"
                )
    
    async def subscribe(self, event_type: EventType, handler: Callable[[Event], None]) -> None:
        """Subscribe a handler to an event type"""
        async with self._lock:
            self._handlers[event_type].append(handler)
            logger.info(f"Handler {handler.__name__} subscribed to {event_type}")
    
    async def unsubscribe(self, event_type: EventType, handler: Callable[[Event], None]) -> None:
        """Unsubscribe a handler from an event type"""
        async with self._lock:
            if handler in self._handlers[event_type]:
                self._handlers[event_type].remove(handler)
                logger.info(f"Handler {handler.__name__} unsubscribed from {event_type}")
    
    async def _execute_handler(self, handler: Callable, event: Event) -> None:
        """Execute a single handler with error isolation"""
        try:
            if asyncio.iscoroutinefunction(handler):
                await handler(event)
            else:
                handler(event)
        except Exception as e:
            # Re-raise to be caught by gather()
            raise Exception(f"Handler error: {e}") from e
```

#### 1.3 Event Handler Registration
**Location**: `app/core/events/registration.py`

```python
from typing import List, Tuple, Callable
from app.core.events import EventType, IEventBus

# Registry of event handlers
EVENT_HANDLERS: List[Tuple[EventType, Callable]] = []

def register_handler(event_type: EventType):
    """Decorator to register an event handler"""
    def decorator(func: Callable) -> Callable:
        EVENT_HANDLERS.append((event_type, func))
        return func
    return decorator

async def setup_event_handlers(event_bus: IEventBus) -> None:
    """Register all event handlers with the event bus"""
    for event_type, handler in EVENT_HANDLERS:
        await event_bus.subscribe(event_type, handler)
```

#### 1.4 Slice-Specific Event Handlers

**Dataset Slice Handler** - `app/datasets/events.py`:
```python
from app.core.events import Event, EventType, register_handler
from app.core.dependencies import get_dataset_repository

@register_handler(EventType.FILE_UPLOADED)
async def handle_file_uploaded(event: Event):
    """Update dataset metadata when a file is uploaded"""
    file_id = event.data.get("file_id")
    dataset_id = event.data.get("dataset_id")
    
    if dataset_id:
        repo = await get_dataset_repository()
        await repo.update_file_count(dataset_id)

@register_handler(EventType.FILE_DEDUPLICATED)
async def handle_file_deduplicated(event: Event):
    """Log deduplication events for dataset statistics"""
    # Implementation here
    pass
```

**Storage Slice Handler** - `app/storage/events.py`:
```python
from app.core.events import Event, EventType, register_handler

@register_handler(EventType.DATASET_DELETED)
async def handle_dataset_deleted(event: Event):
    """Clean up orphaned files when dataset is deleted"""
    dataset_id = event.data.get("dataset_id")
    # Implementation for cleanup logic
    pass
```

### 2. Shared Types Module

**Location**: `app/core/types.py`

```python
from typing import NewType, Union, Any, Dict
from pathlib import Path

# Type aliases for better type safety
DatasetId = NewType('DatasetId', str)
VersionId = NewType('VersionId', str)
FileId = int  # Must match database SERIAL PRIMARY KEY
StoragePath = Union[str, Path]
TagName = NewType('TagName', str)

# Common DTOs
Metadata = Dict[str, Any]
```

### 3. Dataset Slice Interfaces

#### 3.1 Repository Interface
**Location**: `app/datasets/interfaces.py`

```python
from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any
from datetime import datetime

from app.core.types import DatasetId, VersionId, FileId, TagName
from app.datasets.models import Dataset, DatasetVersion, File, Tag

class IDatasetRepository(ABC):
    """Dataset data access interface"""
    
    @abstractmethod
    async def create(self, dataset: Dataset) -> Dataset:
        """Create a new dataset"""
        pass
    
    @abstractmethod
    async def get_by_id(self, dataset_id: DatasetId) -> Optional[Dataset]:
        """Get dataset by ID"""
        pass
    
    @abstractmethod
    async def get_by_name(self, name: str) -> Optional[Dataset]:
        """Get dataset by name"""
        pass
    
    @abstractmethod
    async def update(self, dataset_id: DatasetId, updates: Dict[str, Any]) -> Dataset:
        """Update dataset fields"""
        pass
    
    @abstractmethod
    async def delete(self, dataset_id: DatasetId) -> None:
        """Soft delete dataset"""
        pass
    
    @abstractmethod
    async def list_datasets(
        self, 
        offset: int = 0, 
        limit: int = 100,
        tags: Optional[List[TagName]] = None,
        created_after: Optional[datetime] = None,
        created_before: Optional[datetime] = None
    ) -> List[Dataset]:
        """List datasets with filters"""
        pass
    
    @abstractmethod
    async def add_tag(self, dataset_id: DatasetId, tag: TagName) -> None:
        """Add tag to dataset"""
        pass
    
    @abstractmethod
    async def remove_tag(self, dataset_id: DatasetId, tag: TagName) -> None:
        """Remove tag from dataset"""
        pass
    
    @abstractmethod
    async def get_version(self, version_id: VersionId) -> Optional[DatasetVersion]:
        """Get specific version"""
        pass
    
    @abstractmethod
    async def list_versions(self, dataset_id: DatasetId) -> List[DatasetVersion]:
        """List all versions of a dataset"""
        pass
    
    @abstractmethod
    async def update_file_count(self, dataset_id: DatasetId) -> None:
        """Update the file count for a dataset"""
        pass
```

#### 3.2 Service Interface
**Location**: `app/datasets/interfaces.py` (continued)

```python
from dataclasses import dataclass
from fastapi import UploadFile

@dataclass
class CreateDatasetDTO:
    """Data transfer object for dataset creation"""
    name: str
    description: Optional[str] = None
    tags: List[TagName] = field(default_factory=list)
    metadata: Optional[Dict[str, Any]] = None

@dataclass
class CreateVersionDTO:
    """Data transfer object for version creation"""
    version_tag: str
    description: Optional[str] = None
    parent_version_id: Optional[VersionId] = None
    file_operations: List[Dict[str, Any]] = field(default_factory=list)

@dataclass
class DatasetStatistics:
    """Dataset statistics result"""
    total_files: int
    total_size_bytes: int
    file_types: Dict[str, int]
    version_count: int
    last_modified: datetime
    unique_content_hashes: int

class IDatasetService(ABC):
    """Dataset business logic interface"""
    
    @abstractmethod
    async def create_dataset_with_files(
        self, 
        dataset_data: CreateDatasetDTO, 
        files: List[UploadFile]
    ) -> Dataset:
        """Create dataset with initial files"""
        pass
    
    @abstractmethod
    async def create_version(
        self, 
        dataset_id: DatasetId, 
        version_data: CreateVersionDTO
    ) -> DatasetVersion:
        """Create new dataset version"""
        pass
    
    @abstractmethod
    async def add_files_to_version(
        self,
        dataset_id: DatasetId,
        version_id: VersionId,
        files: List[UploadFile]
    ) -> List[FileId]:
        """Add files to an existing version"""
        pass
    
    @abstractmethod
    async def remove_files_from_version(
        self,
        dataset_id: DatasetId,
        version_id: VersionId,
        file_ids: List[FileId]
    ) -> None:
        """Remove files from a version"""
        pass
    
    @abstractmethod
    async def compute_statistics(
        self, 
        dataset_id: DatasetId,
        version_id: Optional[VersionId] = None
    ) -> DatasetStatistics:
        """Compute dataset statistics"""
        pass
    
    @abstractmethod
    async def export_dataset_metadata(
        self,
        dataset_id: DatasetId,
        include_versions: bool = True
    ) -> Dict[str, Any]:
        """Export dataset metadata for backup or transfer"""
        pass
```

#### 3.3 Search Interface
**Location**: `app/datasets/interfaces.py` (continued)

```python
@dataclass
class SearchFilters:
    """Search filter options"""
    tags: Optional[List[TagName]] = None
    file_types: Optional[List[str]] = None
    min_size_bytes: Optional[int] = None
    max_size_bytes: Optional[int] = None
    created_after: Optional[datetime] = None
    created_before: Optional[datetime] = None

@dataclass
class SearchResult:
    """Individual search result"""
    dataset: Dataset
    relevance_score: float
    matched_fields: List[str]

@dataclass
class SearchResults:
    """Search results container"""
    results: List[SearchResult]
    total_count: int
    query_time_ms: float

class IDatasetSearchService(ABC):
    """Search operations interface"""
    
    @abstractmethod
    async def search(
        self, 
        query: str, 
        filters: Optional[SearchFilters] = None,
        offset: int = 0,
        limit: int = 20
    ) -> SearchResults:
        """Full-text search across datasets"""
        pass
    
    @abstractmethod
    async def search_by_content_hash(
        self,
        content_hash: str
    ) -> List[Dataset]:
        """Find datasets containing a specific file by hash"""
        pass
    
    @abstractmethod
    async def suggest(
        self, 
        prefix: str,
        max_suggestions: int = 10
    ) -> List[str]:
        """Autocomplete suggestions for dataset names"""
        pass
```

### 4. Data Format Readers

**Location**: `app/storage/interfaces.py` (addition)

```python
class IDatasetReader(Protocol):
    """Dataset reading operations interface"""
    
    def read_csv(self, path: StoragePath, **kwargs) -> Any:
        """Read CSV file into a DataFrame"""
        ...
    
    def read_parquet(self, path: StoragePath, **kwargs) -> Any:
        """Read Parquet file into a DataFrame"""
        ...
    
    def read_json(self, path: StoragePath, **kwargs) -> Any:
        """Read JSON file into a DataFrame"""
        ...
    
    def infer_schema(self, path: StoragePath, file_type: str) -> Dict[str, Any]:
        """Infer schema from file"""
        ...
```

### 5. Dependency Injection Updates

**Location**: `app/core/dependencies.py` (additions)

```python
# Event bus dependency
def get_event_bus() -> IEventBus:
    """Get event bus instance"""
    from app.core.services.event_bus import InMemoryEventBus
    # In production, this could return a Redis-backed event bus
    return InMemoryEventBus()

# Dataset dependencies
def get_dataset_repository(
    db: Annotated[Session, Depends(get_db)]
) -> IDatasetRepository:
    """Get dataset repository instance"""
    from app.datasets.repository import DatasetRepository
    return DatasetRepository(db)

def get_dataset_service(
    repository: Annotated[IDatasetRepository, Depends(get_dataset_repository)],
    artifact_producer: Annotated[IArtifactProducer, Depends(get_artifact_producer)],
    event_bus: Annotated[IEventBus, Depends(get_event_bus)]
) -> IDatasetService:
    """Get dataset service instance"""
    from app.datasets.service import DatasetService
    return DatasetService(repository, artifact_producer, event_bus)

def get_dataset_search_service(
    repository: Annotated[IDatasetRepository, Depends(get_dataset_repository)]
) -> IDatasetSearchService:
    """Get dataset search service instance"""
    from app.datasets.search.service import DatasetSearchService
    return DatasetSearchService(repository)
```

### 6. Example Implementation Updates

#### Dataset Service with Events
```python
# app/datasets/service.py
from app.core.events import Event, EventType
from app.core.types import DatasetId
from datetime import datetime

class DatasetService(IDatasetService):
    def __init__(
        self, 
        repository: IDatasetRepository,
        artifact_producer: IArtifactProducer,
        event_bus: IEventBus
    ):
        self._repository = repository
        self._artifact_producer = artifact_producer
        self._event_bus = event_bus
    
    async def create_dataset_with_files(
        self, 
        dataset_data: CreateDatasetDTO, 
        files: List[UploadFile]
    ) -> Dataset:
        # Create dataset
        dataset = await self._repository.create(
            Dataset(
                name=dataset_data.name,
                description=dataset_data.description,
                metadata=dataset_data.metadata
            )
        )
        
        # Add tags
        for tag in dataset_data.tags:
            await self._repository.add_tag(dataset.id, tag)
        
        # Process files...
        # (file processing logic here)
        
        # Publish event
        await self._event_bus.publish(Event(
            event_type=EventType.DATASET_CREATED,
            timestamp=datetime.utcnow(),
            data={
                "dataset_id": dataset.id,
                "dataset_name": dataset.name,
                "file_count": len(files)
            },
            source="DatasetService"
        ))
        
        return dataset
```

## Implementation Timeline

### Week 1: Core Infrastructure
- [ ] Day 1: Implement shared types module
- [ ] Day 2: Create event bus interface and implementation
- [ ] Day 3: Set up event handler registration system
- [ ] Day 4-5: Create dataset interfaces

### Week 2: Integration
- [ ] Day 1-2: Update dataset repository to implement interfaces
- [ ] Day 3-4: Update dataset service to use event bus
- [ ] Day 5: Implement cross-slice event handlers

### Week 3: Data Readers & Testing
- [ ] Day 1-2: Implement IDatasetReader for different formats
- [ ] Day 3: Update dependency injection configuration
- [ ] Day 4-5: Integration testing and documentation

## Success Criteria

1. **Event-Driven Architecture**
   - All major operations publish appropriate events
   - Event handlers properly isolated with error handling
   - No direct cross-slice dependencies

2. **Dataset Interfaces**
   - Complete interface coverage for dataset operations
   - All implementations follow interface contracts
   - Type safety through shared types

3. **Code Quality**
   - Clean separation of concerns maintained
   - No circular dependencies
   - Comprehensive documentation

## Dependencies

- No new external dependencies required
- Uses Python's built-in asyncio for event handling
- Leverages existing FastAPI dependency injection

## Conclusion

Phase 2 focuses on establishing the foundational patterns for cross-slice communication and formalizing the dataset slice contracts. This creates a solid base for future enhancements while maintaining the clean architecture principles established in Phase 1.