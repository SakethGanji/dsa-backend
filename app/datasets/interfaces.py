"""Dataset slice interface definitions.

This module defines the interfaces for dataset operations including
repository, service, and search functionality. These interfaces ensure
clean separation between the dataset slice and other vertical slices.
"""

from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any
from datetime import datetime
from dataclasses import dataclass
from fastapi import UploadFile

from app.core.types import TagName
from app.core.repository import ITaggableRepository, ISoftDeleteRepository
from app.datasets.models import (
    Dataset, DatasetVersion,
    DatasetCreate, DatasetStatistics,
    VersionCreateRequest, VersionCreateResponse
)


class IDatasetRepository(ISoftDeleteRepository[Dataset, int], ITaggableRepository[Dataset, int], ABC):
    """Dataset data access interface.
    
    This interface defines all database operations for datasets,
    versions, and related entities.
    """
    
    # Methods from parent interfaces:
    # - create, get_by_id, update, delete (from IRepository)
    # - list, count (from IPaginatedRepository)
    # - soft_delete, restore, list_deleted, purge (from ISoftDeleteRepository)
    # - add_tag, remove_tag, get_tags, find_by_tags (from ITaggableRepository)
    
    @abstractmethod
    async def get_by_name(self, name: str) -> Optional[Dataset]:
        """Get dataset by name."""
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


from app.core.search import BaseSearchFilters, ISearchService, SearchResults


@dataclass
class DatasetSearchFilters(BaseSearchFilters):
    """Dataset-specific search filter options."""
    tags: Optional[List[TagName]] = None
    file_types: Optional[List[str]] = None
    min_size_bytes: Optional[int] = None
    max_size_bytes: Optional[int] = None


class IDatasetSearchService(ISearchService[Dataset]):
    """Search operations interface for datasets.
    
    This interface extends the core search interface with dataset-specific
    operations.
    """
    
    @abstractmethod
    async def search(
        self, 
        query: str, 
        filters: Optional[DatasetSearchFilters] = None
    ) -> SearchResults[Dataset]:
        """Full-text search across datasets."""
        pass
    
    @abstractmethod
    async def search_by_content_hash(
        self,
        content_hash: str
    ) -> List[Dataset]:
        """Find datasets containing a specific file by hash."""
        pass