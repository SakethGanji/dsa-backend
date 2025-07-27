"""Consolidated service for all dataset operations."""

from typing import List, Tuple, Optional, Dict, Any
from datetime import datetime
from dataclasses import dataclass

from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.core.permissions import PermissionService
from src.core.events.publisher import EventBus, DatasetCreatedEvent, DatasetUpdatedEvent, DatasetDeletedEvent, PermissionGrantedEvent
from src.core.domain_exceptions import EntityNotFoundException, ValidationException
from src.api.models import (
    CreateDatasetResponse, DatasetDetailResponse, UpdateDatasetResponse,
    DatasetSummary, GrantPermissionResponse, CreateDatasetWithFileResponse
)
from ...base_handler import with_transaction, with_error_handling
from ...base_update_handler import BaseUpdateHandler
from ..models import *
from ..models.dataset import Dataset


@dataclass
class DeleteDatasetResponse:
    """Standardized delete response."""
    entity_type: str = "Dataset"
    entity_id: int = None
    success: bool = True
    message: str = None
    
    def __post_init__(self):
        if self.entity_id and not self.message:
            self.message = f"{self.entity_type} {self.entity_id} deleted successfully"


class DatasetService:
    """Consolidated service for all dataset operations."""
    
    def __init__(
        self,
        uow: PostgresUnitOfWork,
        permissions: PermissionService,
        event_bus: Optional[EventBus] = None
    ):
        self._uow = uow
        self._permissions = permissions
        self._event_bus = event_bus
        self._dataset_repo = uow.datasets
        self._commit_repo = uow.commits
        self._job_repo = uow.jobs if hasattr(uow, 'jobs') else None
    
    @with_transaction
    @with_error_handling
    async def create_dataset(
        self, 
        command: CreateDatasetCommand
    ) -> CreateDatasetResponse:
        """
        Create a new dataset with initial empty commit.
        
        Steps:
        1. Create dataset domain model
        2. Persist dataset
        3. Grant admin permission to creator
        4. Create initial empty commit
        5. Create ref pointing to initial commit
        6. Publish domain event
        """
        # Create dataset domain model
        dataset = Dataset(
            name=command.name,
            description=command.description,
            default_branch=command.default_branch
        )
        
        # Add tags using domain logic
        for tag in command.tags:
            dataset.add_tag(tag)
        
        # Persist dataset
        dataset_id = await self._dataset_repo.create_dataset(
            name=dataset.name,
            description=dataset.description or "",
            created_by=command.created_by
        )
        
        # Grant admin permission to creator
        await self._dataset_repo.grant_permission(
            dataset_id=dataset_id,
            user_id=command.created_by,
            permission_type='admin'
        )
        
        # Add tags if any
        if dataset.tags:
            tag_values = [tag.value for tag in dataset.tags]
            await self._dataset_repo.add_dataset_tags(dataset_id, tag_values)
        
        # Create initial empty commit
        initial_commit_id = await self._commit_repo.create_commit_and_manifest(
            dataset_id=dataset_id,
            parent_commit_id=None,
            message="Initial commit",
            author_id=command.created_by,
            manifest=[]  # Empty manifest for initial commit
        )
        
        # Update the default branch ref
        if command.default_branch == "main":
            # Update existing ref from NULL to the initial commit
            await self._commit_repo.update_ref_atomically(
                dataset_id=dataset_id,
                ref_name=command.default_branch,
                expected_commit_id=None,  # Current value is NULL
                new_commit_id=initial_commit_id
            )
        else:
            # Create new ref for non-main branches
            await self._commit_repo.create_ref(
                dataset_id=dataset_id,
                ref_name=command.default_branch,
                commit_id=initial_commit_id
            )
        
        # Publish domain event
        if self._event_bus:
            event = DatasetCreatedEvent(
                dataset_id=dataset_id,
                user_id=command.created_by,
                name=dataset.name,
                description=dataset.description,
                tags=[tag.value for tag in dataset.tags]
            )
            await self._event_bus.publish(event)
        
        # Fetch the created dataset to get timestamps
        created_dataset = await self._dataset_repo.get_dataset_by_id(dataset_id)
        
        return CreateDatasetResponse(
            dataset_id=dataset_id,
            name=dataset.name,
            description=dataset.description or "",
            tags=[tag.value for tag in dataset.tags],
            created_at=created_dataset['created_at']
        )
    
    @with_transaction
    @with_error_handling
    async def create_dataset_with_file(
        self,
        command: CreateDatasetWithFileCommand
    ) -> CreateDatasetWithFileResponse:
        """
        Create dataset with initial file upload.
        
        This method:
        1. Creates the dataset
        2. Stores the file in temporary storage
        3. Creates an import job
        4. Returns immediately (import happens asynchronously)
        """
        # First create the dataset
        create_command = CreateDatasetCommand(
            name=command.name,
            created_by=command.created_by,
            description=command.description,
            tags=command.tags,
            default_branch=command.default_branch
        )
        
        dataset_response = await self.create_dataset(create_command)
        
        # Store file in temporary location
        import tempfile
        import os
        
        # Create temp file
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            # Write file content
            if hasattr(command.file_content, 'read'):
                content = await command.file_content.read()
                tmp_file.write(content)
            else:
                tmp_file.write(command.file_content)
            temp_path = tmp_file.name
        
        # Create import job
        if self._job_repo:
            job_id = await self._job_repo.create_job(
                job_type='import',
                dataset_id=dataset_response.dataset_id,
                created_by=command.created_by,
                parameters={
                    'file_path': temp_path,
                    'original_filename': command.file_name,
                    'file_size': command.file_size,
                    'branch_name': command.branch_name,
                    'commit_message': command.commit_message
                }
            )
        else:
            job_id = None
        
        return CreateDatasetWithFileResponse(
            dataset_id=dataset_response.dataset_id,
            name=dataset_response.name,
            description=dataset_response.description,
            tags=dataset_response.tags,
            created_at=dataset_response.created_at,
            import_job_id=str(job_id) if job_id else None
        )
    
    @with_error_handling
    async def get_dataset(
        self,
        command: GetDatasetCommand
    ) -> DatasetDetailResponse:
        """
        Get detailed information about a dataset.
        
        Returns:
            DatasetDetailResponse with full dataset details
        """
        # Check read permission
        await self._permissions.require("dataset", command.dataset_id, command.user_id, "read")
        
        # Get dataset details
        dataset = await self._dataset_repo.get_dataset_by_id(command.dataset_id)
        if not dataset:
            raise EntityNotFoundException("Dataset", command.dataset_id)
        
        # Get tags
        tags = await self._dataset_repo.get_dataset_tags(command.dataset_id)
        
        # Get user's permission type for this dataset
        user_datasets = await self._dataset_repo.list_user_datasets(command.user_id)
        permission_type = None
        for ds in user_datasets:
            if ds['dataset_id'] == command.dataset_id:
                permission_type = ds['permission_type']
                break
        
        return DatasetDetailResponse(
            id=dataset['id'],
            name=dataset['name'],
            description=dataset['description'],
            created_by=dataset['created_by'],
            created_at=dataset['created_at'],
            updated_at=dataset['updated_at'],
            tags=tags,
            permission_type=permission_type,
            # Import status will be added by the endpoint
            import_status=None,
            import_job_id=None
        )
    
    @with_error_handling
    async def list_datasets(
        self,
        command: ListDatasetsCommand
    ) -> Tuple[List[DatasetSummary], int]:
        """
        List datasets that the user has access to.
        
        Returns:
            Tuple of (datasets, total_count)
        """
        # Validate pagination
        offset = max(0, command.offset)
        limit = min(max(1, command.limit), 1000)  # Cap at 1000
        
        # Get all datasets for the user
        all_datasets = await self._dataset_repo.list_user_datasets(command.user_id)
        
        # Apply pagination
        total = len(all_datasets)
        datasets = all_datasets[offset:offset + limit]
        
        # Convert to response models
        dataset_items = []
        for dataset in datasets:
            # Get tags for each dataset
            tags = await self._dataset_repo.get_dataset_tags(dataset['dataset_id'])
            
            dataset_items.append(DatasetSummary(
                dataset_id=dataset['dataset_id'],
                name=dataset['name'],
                description=dataset['description'],
                tags=tags,
                created_at=dataset['created_at'],
                updated_at=dataset['updated_at'],
                created_by=dataset['created_by'],
                permission_type=dataset['permission_type'],
                import_status=None,
                import_job_id=None
            ))
        
        return dataset_items, total
    
    @with_transaction
    @with_error_handling
    async def update_dataset(
        self,
        command: UpdateDatasetCommand
    ) -> UpdateDatasetResponse:
        """Update dataset metadata."""
        # Check write permission
        await self._permissions.require("dataset", command.dataset_id, command.user_id, "write")
        
        # Get existing dataset
        existing = await self._dataset_repo.get_dataset_by_id(command.dataset_id)
        if not existing:
            raise EntityNotFoundException("Dataset", command.dataset_id)
        
        # Validate name length if provided
        if command.name is not None and len(command.name) < 3:
            raise ValidationException("Dataset name must be at least 3 characters long", field="name")
        
        # Prepare update data
        update_data = {}
        if command.name is not None:
            update_data['name'] = command.name
        if command.description is not None:
            update_data['description'] = command.description
        
        # Update dataset
        if update_data:
            await self._dataset_repo.update_dataset(
                dataset_id=command.dataset_id,
                **update_data
            )
        
        # Handle tags update
        if command.tags is not None:
            await self._dataset_repo.remove_dataset_tags(command.dataset_id)
            if command.tags:
                await self._dataset_repo.add_dataset_tags(command.dataset_id, command.tags)
        
        # Get updated dataset
        updated = await self._dataset_repo.get_dataset_by_id(command.dataset_id)
        tags = await self._dataset_repo.get_dataset_tags(command.dataset_id)
        
        # Publish event if there were changes
        if self._event_bus:
            changes = {}
            if command.name is not None and existing.get('name') != command.name:
                changes['name'] = {'old': existing.get('name'), 'new': command.name}
            if command.description is not None and existing.get('description') != command.description:
                changes['description'] = {'old': existing.get('description'), 'new': command.description}
            if command.tags is not None:
                old_tags = await self._dataset_repo.get_dataset_tags(command.dataset_id)
                if set(old_tags) != set(command.tags):
                    changes['tags'] = {'old': old_tags, 'new': command.tags}
            
            if changes:
                event = DatasetUpdatedEvent(
                    dataset_id=command.dataset_id,
                    user_id=command.user_id,
                    changes=changes
                )
                await self._event_bus.publish(event)
        
        return UpdateDatasetResponse(
            dataset_id=updated['id'],
            name=updated['name'],
            description=updated['description'],
            tags=tags,
            updated_at=updated['updated_at']
        )
    
    @with_transaction
    @with_error_handling
    async def delete_dataset(
        self,
        command: DeleteDatasetCommand
    ) -> DeleteDatasetResponse:
        """
        Delete a dataset and all its associated data.
        
        This includes:
        - Dataset record
        - All permissions
        - All tags
        - All commits and refs
        - All rows and manifests
        """
        # Check admin permission (only admins can delete datasets)
        await self._permissions.require("dataset", command.dataset_id, command.user_id, "admin")
        
        # Check if dataset exists
        dataset = await self._dataset_repo.get_dataset_by_id(command.dataset_id)
        if not dataset:
            raise EntityNotFoundException("Dataset", command.dataset_id)
        
        # Delete all associated data
        # Note: The order matters due to foreign key constraints
        
        # 1. Delete tags
        await self._dataset_repo.remove_dataset_tags(command.dataset_id)
        
        # 2. Delete the dataset itself (cascade should handle related records)
        await self._dataset_repo.delete_dataset(command.dataset_id)
        
        # Note: Row data cleanup might be handled by a separate cleanup job
        # to avoid deleting rows that are shared across datasets
        
        # Publish deletion event
        if self._event_bus:
            event = DatasetDeletedEvent(
                dataset_id=command.dataset_id,
                user_id=command.user_id,
                name=dataset['name']
            )
            await self._event_bus.publish(event)
        
        # Return standardized response
        return DeleteDatasetResponse(
            entity_type="Dataset",
            entity_id=command.dataset_id,
            message=f"Dataset '{dataset['name']}' and all related data have been deleted successfully"
        )
    
    @with_transaction
    @with_error_handling
    async def grant_permission(
        self,
        command: GrantPermissionCommand
    ) -> GrantPermissionResponse:
        """
        Grant permission to a user on a dataset.
        
        Steps:
        1. Verify granting user has admin permission
        2. Grant requested permission to target user
        """
        # Check that the granting user has admin permission on the dataset
        await self._permissions.require("dataset", command.dataset_id, command.user_id, "admin")
        
        # Validate permission type
        valid_permissions = ['read', 'write', 'admin']
        if command.permission_type not in valid_permissions:
            raise ValueError(f"Invalid permission type. Must be one of: {valid_permissions}")
        
        # Grant permission
        await self._dataset_repo.grant_permission(
            dataset_id=command.dataset_id,
            user_id=command.target_user_id,
            permission_type=command.permission_type
        )
        
        # Publish event
        if self._event_bus:
            await self._event_bus.publish(PermissionGrantedEvent(
                dataset_id=command.dataset_id,
                user_id=command.user_id,
                target_user_id=command.target_user_id,
                permission_type=command.permission_type
            ))
        
        # Refresh search index to reflect permission changes
        if hasattr(self._uow, 'search_repository'):
            await self._uow.search_repository.refresh_search_index()
        
        return GrantPermissionResponse(
            dataset_id=command.dataset_id,
            user_id=command.target_user_id,
            permission_type=command.permission_type
        )
    
    @with_error_handling
    async def check_dataset_ready(
        self,
        command: CheckDatasetReadyCommand
    ) -> Dict[str, Any]:
        """
        Check if a dataset is ready for operations by examining import job status.
        
        Returns:
            Dict with ready status and details
        """
        # Check that the user has read permission on the dataset
        await self._permissions.require("dataset", command.dataset_id, command.user_id, "read")
        
        if not self._job_repo:
            return {
                "ready": True,
                "status": "no_job_repo",
                "message": "Job repository not available"
            }
        
        # Check for latest import job
        latest_import_job = await self._job_repo.get_latest_import_job(command.dataset_id)
        
        if not latest_import_job:
            # No import job found - dataset might be empty
            return {
                "ready": True,
                "status": "no_import",
                "message": "No import job found for this dataset"
            }
        
        status = latest_import_job['status']
        job_id = str(latest_import_job['job_id'])
        
        if status == 'completed':
            return {
                "ready": True,
                "status": status,
                "import_job_id": job_id,
                "message": "Dataset is ready for use"
            }
        elif status in ['pending', 'processing']:
            return {
                "ready": False,
                "status": status,
                "import_job_id": job_id,
                "message": "Dataset import is still in progress"
            }
        elif status == 'failed':
            return {
                "ready": False,
                "status": status,
                "import_job_id": job_id,
                "message": "Dataset import failed",
                "error": latest_import_job.get('error_message')
            }
        else:
            return {
                "ready": False,
                "status": status,
                "import_job_id": job_id,
                "message": f"Unknown import status: {status}"
            }