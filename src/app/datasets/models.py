from datetime import datetime
from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel, Field

class TagBase(BaseModel):
    name: str
    description: Optional[str] = None

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

class FileCreate(FileBase):
    file_data: Optional[bytes] = None

class File(FileBase):
    id: int
    created_at: datetime
    file_data: Optional[bytes] = None

    class Config:
        from_attributes = True

class SheetBase(BaseModel):
    name: str
    sheet_index: int
    description: Optional[str] = None

class SheetCreate(SheetBase):
    dataset_version_id: int

class SheetMetadata(BaseModel):
    metadata: Dict[str, Any]
    profiling_report_file_id: Optional[int] = None

class Sheet(SheetBase):
    id: int
    dataset_version_id: int
    metadata: Optional[SheetMetadata] = None

    class Config:
        from_attributes = True

class DatasetVersionBase(BaseModel):
    dataset_id: int
    version_number: int
    file_id: int
    uploaded_by: int

class DatasetVersionCreate(DatasetVersionBase):
    pass

class DatasetVersion(DatasetVersionBase):
    id: int
    ingestion_timestamp: datetime
    last_updated_timestamp: datetime
    uploaded_by_soeid: Optional[str] = None
    file_type: Optional[str] = None
    file_size: Optional[int] = None
    sheets: Optional[List[Sheet]] = None

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
    name: str
    index: int
    description: Optional[str] = None
    id: Optional[int] = None

class DatasetUploadResponse(BaseModel):
    dataset_id: int
    version_id: int
    sheets: List[SheetInfo]

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

