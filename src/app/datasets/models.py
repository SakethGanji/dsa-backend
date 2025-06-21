from datetime import datetime
from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel, Field

class TagBase(BaseModel):
    name: str

class TagCreate(TagBase):
    pass

class Tag(TagBase):
    id: int
    usage_count: Optional[int] = None

    class Config:
        from_attributes = True

class FileBase(BaseModel):
    storage_type: str
    file_type: str
    mime_type: Optional[str] = None
    file_path: Optional[str] = None
    file_size: Optional[int] = None
    content_hash: Optional[str] = None
    reference_count: Optional[int] = 0
    compression_type: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

class FileCreate(FileBase):
    pass

class File(FileBase):
    id: int
    created_at: datetime

    class Config:
        from_attributes = True


from enum import Enum

class VersionStatus(str, Enum):
    ACTIVE = "active"
    ARCHIVED = "archived"
    DELETED = "deleted"


class FileOperation(str, Enum):
    ADD = "add"
    REMOVE = "remove"
    UPDATE = "update"

class OverlayFileAction(BaseModel):
    operation: FileOperation
    file_id: int
    component_name: str
    component_type: Optional[str] = "primary"
    metadata: Optional[Dict[str, Any]] = None

class OverlayData(BaseModel):
    parent_version: Optional[int] = None
    version_number: int
    actions: List[OverlayFileAction]
    created_at: datetime
    created_by: int
    message: Optional[str] = None

class DatasetVersionBase(BaseModel):
    dataset_id: int
    version_number: int
    overlay_file_id: int
    created_by: int

class DatasetVersionCreate(DatasetVersionBase):
    message: Optional[str] = None
    status: Optional[VersionStatus] = VersionStatus.ACTIVE

class DatasetVersion(DatasetVersionBase):
    id: int
    message: Optional[str] = None
    status: VersionStatus = VersionStatus.ACTIVE
    created_at: datetime
    updated_at: datetime
    file_type: Optional[str] = None
    file_size: Optional[int] = None

    class Config:
        from_attributes = True

class DatasetBase(BaseModel):
    name: str
    description: Optional[str] = None

class DatasetCreate(DatasetBase):
    created_by: int
    tags: Optional[List[str]] = None

class DatasetUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    tags: Optional[List[str]] = None

class Dataset(DatasetBase):
    id: int
    created_by: int
    created_at: datetime
    updated_at: datetime
    current_version: Optional[int] = None
    file_type: Optional[str] = None
    file_size: Optional[int] = None
    versions: Optional[List[DatasetVersion]] = None
    tags: Optional[List[Tag]] = None

    class Config:
        from_attributes = True

class DatasetUploadRequest(BaseModel):
    dataset_id: Optional[int] = None  # Optional for new datasets
    name: str
    description: Optional[str] = None
    tags: Optional[List[str]] = None

class SheetInfo(BaseModel):
    """Represents a sheet as a component in dataset_version_files"""
    name: str  # component_name
    index: int  # component_index
    description: Optional[str] = None  # from metadata
    file_id: Optional[int] = None  # references files table

class DatasetUploadResponse(BaseModel):
    dataset_id: int
    version_id: int
    sheets: List[SheetInfo]

class SchemaVersionBase(BaseModel):
    dataset_version_id: int
    schema_data: Dict[str, Any] = Field(..., description="JSON schema for the dataset")

class SchemaVersionCreate(SchemaVersionBase):
    pass

class SchemaVersion(SchemaVersionBase):
    id: int
    created_at: datetime

    class Config:
        from_attributes = True

class VersionFileBase(BaseModel):
    version_id: int
    file_id: int
    component_type: str = Field(..., description="Type of component: primary, metadata, schema, etc.")
    component_name: Optional[str] = Field(None, description="Name/identifier for the component")
    component_index: Optional[int] = Field(None, description="Index for ordering multiple files")
    metadata: Optional[Dict[str, Any]] = None

class VersionFileCreate(VersionFileBase):
    pass

class VersionFile(VersionFileBase):
    file: Optional[File] = None

    class Config:
        from_attributes = True


class VersionTagBase(BaseModel):
    dataset_id: int
    tag_name: str = Field(..., description="Tag name e.g. 'v1.0', 'latest-stable'")
    dataset_version_id: int

class VersionTagCreate(VersionTagBase):
    pass

class VersionTag(VersionTagBase):
    id: int

    class Config:
        from_attributes = True

class VersionResolutionType(str, Enum):
    NUMBER = "number"
    TAG = "tag"
    LATEST = "latest"

class VersionResolution(BaseModel):
    type: VersionResolutionType
    value: Optional[Union[int, str]] = None

class VersionCreateRequest(BaseModel):
    dataset_id: int
    file_changes: List[OverlayFileAction]
    message: Optional[str] = None
    parent_version: Optional[int] = None

class VersionCreateResponse(BaseModel):
    version_id: int
    version_number: int
    overlay_file_id: int


class DatasetListParams(BaseModel):
    limit: Optional[int] = Field(10, ge=1, le=100)
    offset: Optional[int] = Field(0, ge=0)
    sort_by: Optional[str] = Field(None, pattern=r'^(name|created_at|updated_at|file_size|current_version)$')
    sort_order: Optional[str] = Field(None, pattern=r'^(asc|desc)$')
    name: Optional[str] = None
    description: Optional[str] = None
    created_by: Optional[int] = None
    tag: Optional[List[str]] = None
    file_type: Optional[str] = None
    file_size_min: Optional[int] = Field(None, ge=0)
    file_size_max: Optional[int] = Field(None, ge=0)
    version_min: Optional[int] = Field(None, ge=1)
    version_max: Optional[int] = Field(None, ge=1)
    created_at_from: Optional[datetime] = None
    created_at_to: Optional[datetime] = None
    updated_at_from: Optional[datetime] = None
    updated_at_to: Optional[datetime] = None

