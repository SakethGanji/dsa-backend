"""Dataset slice interface definitions.

This module defines the interfaces for dataset operations including
repository, service, and search functionality. These interfaces ensure
clean separation between the dataset slice and other vertical slices.
"""

from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any
from datetime import datetime
from dataclasses import dataclass, field
from fastapi import UploadFile

from app.core.types import TagName, Metadata
from app.datasets.models import (
    Dataset, DatasetVersion, File, Tag,
    DatasetCreate, DatasetUpdate, DatasetStatistics,
    VersionCreateRequest, VersionCreateResponse
)


class IDatasetRepository(ABC):
    """Dataset data access interface.
    
    This interface defines all database operations for datasets,
    versions, and related entities.
    """
    
    @abstractmethod
    async def create(self, dataset: Dataset) -> Dataset:
        """Create a new dataset."""
        pass
    
    @abstractmethod
    async def get_by_id(self, dataset_id: int) -> Optional[Dataset]:
        """Get dataset by ID."""
        pass
    
    @abstractmethod
    async def get_by_name(self, name: str) -> Optional[Dataset]:
        """Get dataset by name."""
        pass
    
    @abstractmethod
    async def update(self, dataset_id: int, updates: Dict[str, Any]) -> Dataset:
        """Update dataset fields."""
        pass
    
    @abstractmethod
    async def delete(self, dataset_id: int) -> None:
        """Soft delete dataset."""
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
        """List datasets with filters."""
        pass
    
    @abstractmethod
    async def add_tag(self, dataset_id: int, tag: TagName) -> None:
        """Add tag to dataset."""
        pass
    
    @abstractmethod
    async def remove_tag(self, dataset_id: int, tag: TagName) -> None:
        """Remove tag from dataset."""
        pass
    
    @abstractmethod
    async def get_version(self, version_id: int) -> Optional[DatasetVersion]:
        """Get specific version."""
        pass
    
    @abstractmethod
    async def list_versions(self, dataset_id: int) -> List[DatasetVersion]:
        """List all versions of a dataset."""
        pass
    
    @abstractmethod
    async def update_file_count(self, dataset_id: int) -> None:
        """Update the file count for a dataset."""
        pass




class IDatasetService(ABC):
    """Dataset business logic interface.
    
    This interface defines the high-level business operations for datasets,
    coordinating between the repository, storage, and event bus.
    """
    
    @abstractmethod
    async def create_dataset_with_files(
        self, 
        dataset_data: DatasetCreate, 
        files: List[UploadFile]
    ) -> Dataset:
        """Create dataset with initial files."""
        pass
    
    @abstractmethod
    async def create_version(
        self, 
        version_data: VersionCreateRequest
    ) -> VersionCreateResponse:
        """Create new dataset version."""
        pass
    
    @abstractmethod
    async def add_files_to_version(
        self,
        dataset_id: int,
        version_id: int,
        files: List[UploadFile]
    ) -> List[int]:
        """Add files to an existing version."""
        pass
    
    @abstractmethod
    async def remove_files_from_version(
        self,
        dataset_id: int,
        version_id: int,
        file_ids: List[int]
    ) -> None:
        """Remove files from a version."""
        pass
    
    @abstractmethod
    async def compute_statistics(
        self, 
        dataset_id: int,
        version_id: Optional[int] = None
    ) -> DatasetStatistics:
        """Compute dataset statistics."""
        pass
    
    @abstractmethod
    async def export_dataset_metadata(
        self,
        dataset_id: int,
        include_versions: bool = True
    ) -> Dict[str, Any]:
        """Export dataset metadata for backup or transfer."""
        pass


@dataclass
class SearchFilters:
    """Search filter options."""
    tags: Optional[List[TagName]] = None
    file_types: Optional[List[str]] = None
    min_size_bytes: Optional[int] = None
    max_size_bytes: Optional[int] = None
    created_after: Optional[datetime] = None
    created_before: Optional[datetime] = None


@dataclass
class SearchResult:
    """Individual search result."""
    dataset: Dataset
    relevance_score: float
    matched_fields: List[str]


@dataclass
class SearchResults:
    """Search results container."""
    results: List[SearchResult]
    total_count: int
    query_time_ms: float


class IDatasetSearchService(ABC):
    """Search operations interface.
    
    This interface defines search and discovery operations for datasets.
    """
    
    @abstractmethod
    async def search(
        self, 
        query: str, 
        filters: Optional[SearchFilters] = None,
        offset: int = 0,
        limit: int = 20
    ) -> SearchResults:
        """Full-text search across datasets."""
        pass
    
    @abstractmethod
    async def search_by_content_hash(
        self,
        content_hash: str
    ) -> List[Dataset]:
        """Find datasets containing a specific file by hash."""
        pass
    
    @abstractmethod
    async def suggest(
        self, 
        prefix: str,
        max_suggestions: int = 10
    ) -> List[str]:
        """Autocomplete suggestions for dataset names."""
        pass