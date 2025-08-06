"""API Response models."""

from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, List, Dict, Any, Type, TypeVar
from uuid import UUID

T = TypeVar('T', bound=BaseModel)

# Import types from common to avoid forward references
from .common import (
    DatasetSummary, UserSummary, JobSummary, JobDetail,
    DataRow, SheetSchema, CommitInfo, RefInfo,
    TableInfo, RefWithTables, SearchResult
)


# ============================================
# Dataset Response Models
# ============================================

class CreateDatasetResponse(BaseModel):
    """Response after creating a dataset."""
    dataset_id: int
    name: str
    description: Optional[str] = None
    tags: List[str]
    created_at: datetime


class CreateDatasetWithFileResponse(BaseModel):
    """Response after creating dataset with file."""
    dataset: CreateDatasetResponse
    commit_id: str
    import_job: 'QueueImportResponse'  # Keep as forward ref - defined later in file
    
    class Config:
        """Allow forward references."""
        arbitrary_types_allowed = True


class DatasetDetailResponse(BaseModel):
    """Detailed dataset information."""
    id: int
    name: str
    description: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime
    created_by: int
    permission_type: Optional[str] = None
    import_status: Optional[str] = None
    import_job_id: Optional[str] = None  # UUID as string


class ListDatasetsResponse(BaseModel):
    """Paginated list of datasets."""
    datasets: List[DatasetSummary]
    total: int
    offset: int
    limit: int


class UpdateDatasetResponse(BaseModel):
    """Response after updating a dataset."""
    dataset_id: int
    name: str
    description: Optional[str] = None
    tags: List[str]
    updated_at: datetime


class DeleteDatasetResponse(BaseModel):
    """Response after deleting a dataset."""
    success: bool = True
    message: str = "Dataset deleted successfully"


class DatasetOverviewResponse(BaseModel):
    """Complete dataset overview with all branches."""
    dataset_id: int
    name: str
    description: Optional[str] = None
    branches: List[RefWithTables]


# ============================================
# User Response Models
# ============================================

class CreateUserResponse(BaseModel):
    """Response after creating a user."""
    user_id: int
    soeid: str
    role_id: int
    role_name: Optional[str] = None
    is_active: bool
    created_at: datetime


class UpdateUserResponse(BaseModel):
    """Response after updating a user."""
    user_id: int
    soeid: str
    role_id: int
    role_name: Optional[str] = None
    is_active: bool
    updated_at: datetime


class DeleteUserResponse(BaseModel):
    """Response after deleting a user."""
    success: bool = True
    message: str = "User deleted successfully"


class ListUsersResponse(BaseModel):
    """Paginated list of users."""
    users: List[UserSummary]
    total: int
    offset: int
    limit: int
    
    class Config:
        """Configuration for the model."""
        json_schema_extra = {
            "example": {
                "users": [{"user_id": 1, "soeid": "ab12345", "role_id": 2, "role_name": "analyst"}],
                "total": 1,
                "offset": 0,
                "limit": 10
            }
        }


class LoginResponse(BaseModel):
    """Response after successful login."""
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    user_id: int
    soeid: str
    role_id: int
    role_name: Optional[str] = None


# ============================================
# Job Response Models
# ============================================

class QueueImportResponse(BaseModel):
    """Response after queuing an import job."""
    job_id: str  # UUID as string
    status: str = "pending"
    message: str = "Import job queued successfully"


class JobListResponse(BaseModel):
    """Paginated list of jobs."""
    jobs: List[JobSummary]
    total: int
    offset: int
    limit: int


class JobDetailResponse(BaseModel):
    """Detailed job information."""
    job: JobDetail


class CancelJobResponse(BaseModel):
    """Response after cancelling a job."""
    job_id: str  # UUID as string
    status: str = "cancelled"
    message: str = "Job cancelled successfully"
    cancelled_at: datetime


# ============================================
# Version Control Response Models
# ============================================

class CreateCommitResponse(BaseModel):
    """Response after creating a commit."""
    commit_id: str
    message: str
    created_at: datetime


class GetCommitHistoryResponse(BaseModel):
    """Commit history for a dataset."""
    dataset_id: int
    ref_name: str
    commits: List[CommitInfo]
    total: int
    offset: int
    limit: int


class CheckoutResponse(BaseModel):
    """Response after checking out a ref."""
    dataset_id: int
    ref_name: str
    commit_id: str
    message: str = "Checkout successful"
    tables: List[TableInfo]


class CreateBranchResponse(BaseModel):
    """Response after creating a branch."""
    dataset_id: int
    ref_name: str
    commit_id: str
    created_at: datetime


class ListRefsResponse(BaseModel):
    """List of refs for a dataset."""
    refs: List[RefInfo]
    default_branch: str = "main"


# ============================================
# Data Access Response Models
# ============================================

class GetDataResponse(BaseModel):
    """Response with dataset data."""
    dataset_id: int
    ref_name: str
    commit_id: str
    rows: List[DataRow]
    total_rows: int
    offset: int
    limit: int
    # Cursor pagination for infinite scroll
    next_cursor: Optional[str] = None
    has_more: bool = False
    # Applied filters/sorts for client reference
    filters_applied: Optional[Dict[str, Any]] = None
    sort_applied: Optional[List[Dict[str, str]]] = None


class GetSchemaResponse(BaseModel):
    """Response with schema information."""
    dataset_id: int
    ref_name: str
    commit_id: str
    sheets: List[SheetSchema]


class CommitSchemaResponse(BaseModel):
    """Schema for a specific commit."""
    commit_id: str
    sheets: List[SheetSchema]


class TableAnalysisResponse(BaseModel):
    """Table analysis information."""
    table_key: str
    sheet_name: str
    column_stats: Dict[str, Any]
    sample_data: List[Dict[str, Any]]
    row_count: int
    null_counts: Dict[str, int]
    unique_counts: Dict[str, int]
    data_types: Dict[str, str]
    columns: List[Dict[str, str]]  # List of {"name": "col_name", "type": "col_type"}


# ============================================
# Permission Response Models
# ============================================

class GrantPermissionResponse(BaseModel):
    """Response after granting permission."""
    dataset_id: int
    user_id: int
    permission_type: str
    granted_at: datetime
    message: str = "Permission granted successfully"


# ============================================
# Search Response Models
# ============================================

class SearchResponse(BaseModel):
    """Search results."""
    results: List[SearchResult]
    total: int
    offset: int
    limit: int
    query: str


# ============================================
# Common Response Models and Builders
# (Migrated from api/common/responses.py)
# ============================================

class SuccessResponse(BaseModel):
    """Standard success response format."""
    success: bool = True
    data: Any
    message: Optional[str] = None


class ErrorResponse(BaseModel):
    """Standard error response format."""
    success: bool = False
    error: str
    detail: Optional[str] = None
    code: Optional[str] = None


class ResponseBuilder:
    """Helper class for building consistent API responses."""
    
    @staticmethod
    def success(
        data: Any,
        message: Optional[str] = None
    ) -> SuccessResponse:
        """
        Build a success response.
        
        Args:
            data: The response data
            message: Optional success message
            
        Returns:
            SuccessResponse object
        """
        return SuccessResponse(
            data=data,
            message=message
        )
    
    @staticmethod
    def error(
        error: str,
        detail: Optional[str] = None,
        code: Optional[str] = None
    ) -> ErrorResponse:
        """
        Build an error response.
        
        Args:
            error: Error type/category
            detail: Detailed error message
            code: Optional error code
            
        Returns:
            ErrorResponse object
        """
        return ErrorResponse(
            error=error,
            detail=detail,
            code=code
        )
    
    @staticmethod
    def created(
        data: Any,
        message: str = "Resource created successfully"
    ) -> SuccessResponse:
        """Build a response for successful creation."""
        return SuccessResponse(
            data=data,
            message=message
        )
    
    @staticmethod
    def updated(
        data: Any,
        message: str = "Resource updated successfully"
    ) -> SuccessResponse:
        """Build a response for successful update."""
        return SuccessResponse(
            data=data,
            message=message
        )
    
    @staticmethod
    def deleted(
        message: str = "Resource deleted successfully"
    ) -> SuccessResponse:
        """Build a response for successful deletion."""
        return SuccessResponse(
            data=None,
            message=message
        )


class BatchResponse(BaseModel):
    """Response for batch operations."""
    success_count: int
    failure_count: int
    total: int
    results: List[Dict[str, Any]]
    
    @property
    def all_successful(self) -> bool:
        """Check if all operations were successful."""
        return self.failure_count == 0


# ============================================
# Response Factory
# (Migrated from api/factories/response.py)
# ============================================

class ResponseFactory:
    """Factory for creating consistent responses."""
    
    @staticmethod
    def from_entity(entity: Dict[str, Any], response_class: Type[T], **overrides) -> T:
        """
        Create response model from entity dict.
        
        Handles common field mappings and allows overrides.
        
        Args:
            entity: Entity dictionary from database
            response_class: Pydantic model class to create
            **overrides: Additional fields or overrides
            
        Returns:
            Instance of response_class
        """
        # Create a copy to avoid modifying original
        data = entity.copy()
        
        # Handle common field mappings
        if 'id' in data:
            # Map id to specific ID fields based on response class fields
            if 'dataset_id' in response_class.__fields__ and 'dataset_id' not in data:
                data['dataset_id'] = data['id']
            elif 'user_id' in response_class.__fields__ and 'user_id' not in data:
                data['user_id'] = data['id']
            elif 'job_id' in response_class.__fields__ and 'job_id' not in data:
                data['job_id'] = str(data['id'])  # Convert UUID to string
        
        # Apply overrides
        data.update(overrides)
        
        return response_class(**data)
    
    @staticmethod
    def create_list_response(
        items: List[Dict[str, Any]],
        total: int,
        offset: int,
        limit: int,
        item_class: Type[T],
        response_class: Optional[Type[BaseModel]] = None,
        **item_overrides
    ) -> Dict[str, Any]:
        """
        Create paginated list response.
        
        Args:
            items: List of entity dictionaries
            total: Total number of items
            offset: Number of items skipped
            limit: Maximum items per page
            item_class: Pydantic model class for items
            response_class: Optional custom response class
            **item_overrides: Overrides applied to each item
            
        Returns:
            Dictionary or response model instance
        """
        # Convert items to response models
        response_items = [
            ResponseFactory.from_entity(item, item_class, **item_overrides) 
            for item in items
        ]
        
        response_data = {
            "items": response_items,
            "total": total,
            "offset": offset,
            "limit": limit,
            "has_more": total > offset + len(items)
        }
        
        if response_class:
            return response_class(**response_data)
        else:
            return response_data
    
    @staticmethod
    def create_operation_response(
        success: bool,
        message: str,
        entity_type: Optional[str] = None,
        entity_id: Optional[Any] = None,
        **additional_fields
    ) -> Dict[str, Any]:
        """
        Create operation response (create, update, delete).
        
        Args:
            success: Whether operation succeeded
            message: Response message
            entity_type: Type of entity operated on
            entity_id: ID of entity operated on
            **additional_fields: Additional response fields
            
        Returns:
            Response dictionary
        """
        response = {
            "success": success,
            "message": message
        }
        
        if entity_type:
            response["entity_type"] = entity_type
        if entity_id is not None:
            response["entity_id"] = entity_id
            
        response.update(additional_fields)
        return response
    
    @staticmethod
    def create_delete_response(
        entity_type: str,
        entity_id: Any,
        custom_message: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create standardized delete response.
        
        Args:
            entity_type: Type of deleted entity
            entity_id: ID of deleted entity
            custom_message: Optional custom message
            
        Returns:
            Delete response dictionary
        """
        message = custom_message or f"{entity_type} {entity_id} deleted successfully"
        return ResponseFactory.create_operation_response(
            success=True,
            message=message,
            entity_type=entity_type,
            entity_id=entity_id
        )
    
    @staticmethod
    def map_entity_list(
        entities: List[Dict[str, Any]],
        response_class: Type[T],
        **overrides
    ) -> List[T]:
        """
        Map a list of entities to response models.
        
        Args:
            entities: List of entity dictionaries
            response_class: Pydantic model class
            **overrides: Overrides for each entity
            
        Returns:
            List of response model instances
        """
        return [
            ResponseFactory.from_entity(entity, response_class, **overrides)
            for entity in entities
        ]
    
    @staticmethod
    def enrich_with_relations(
        entity: Dict[str, Any],
        relations: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Enrich entity with related data.
        
        Args:
            entity: Base entity dictionary
            relations: Related data to merge
            
        Returns:
            Enriched entity dictionary
        """
        enriched = entity.copy()
        enriched.update(relations)
        return enriched