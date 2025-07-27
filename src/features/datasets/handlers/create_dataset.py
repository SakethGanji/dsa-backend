from typing import Optional
from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.infrastructure.postgres.dataset_repo import PostgresDatasetRepository
from src.infrastructure.postgres.versioning_repo import PostgresCommitRepository
from src.core.events.publisher import EventBus, DatasetCreatedEvent
from src.api.models import CreateDatasetResponse
from ...base_handler import BaseHandler, with_transaction
from ..models import CreateDatasetCommand, Dataset, DatasetMetadata
from datetime import datetime
from src.core.permissions import PermissionService


class CreateDatasetHandler(BaseHandler):
    """Handler for creating new datasets"""
    
    def __init__(
        self,
        uow: PostgresUnitOfWork,
        dataset_repo: PostgresDatasetRepository,
        commit_repo: PostgresCommitRepository,
        permissions: PermissionService,
        event_bus: Optional[EventBus] = None
    ):
        super().__init__(uow)
        self._dataset_repo = dataset_repo
        self._commit_repo = commit_repo
        self._permissions = permissions
        self._event_bus = event_bus
    
    @with_transaction
    async def handle(
        self,
        command: CreateDatasetCommand
    ) -> CreateDatasetResponse:
        """
        Create a new dataset with initial empty commit
        
        Steps:
        1. Create dataset domain model
        2. Persist dataset
        3. Grant admin permission to creator
        4. Create initial empty commit
        5. Create ref pointing to initial commit
        6. Publish domain event
        """
        # For dataset creation, we might want to check if user has appropriate role
        # But for now, we'll allow any authenticated user to create datasets
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