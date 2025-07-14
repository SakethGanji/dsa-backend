from src.core.abstractions import IUnitOfWork, IDatasetRepository, ICommitRepository
from src.api.models import CreateDatasetRequest, CreateDatasetResponse
from ...base_handler import BaseHandler, with_transaction
from src.core.events import EventBus, DatasetCreatedEvent, get_event_bus
from dataclasses import dataclass


@dataclass
class CreateDatasetCommand:
    user_id: int  # Must be first for decorator
    name: str
    description: str
    tags: list[str] = None
    default_branch: str = 'main'


class CreateDatasetHandler(BaseHandler):
    """Handler for creating new datasets"""
    
    def __init__(
        self,
        uow: IUnitOfWork,
        dataset_repo: IDatasetRepository,
        commit_repo: ICommitRepository,
        event_bus: EventBus = None
    ):
        super().__init__(uow)
        self._dataset_repo = dataset_repo
        self._commit_repo = commit_repo
        self._event_bus = event_bus or get_event_bus()
    
    @with_transaction
    async def handle(
        self,
        command: CreateDatasetCommand
    ) -> CreateDatasetResponse:
        """
        Create a new dataset with initial empty commit
        
        Steps:
        1. Create dataset record
        2. Grant admin permission to creator
        3. Create initial empty commit
        4. Create 'main' ref pointing to initial commit
        """
        # Transaction is handled by @with_transaction decorator
        # Permission check is handled by @requires_permission decorator
        
        # Create dataset
        dataset_id = await self._dataset_repo.create_dataset(
            name=command.name,
            description=command.description,
            created_by=command.user_id
        )
        
        # Grant admin permission to creator
        await self._dataset_repo.grant_permission(
            dataset_id=dataset_id,
            user_id=command.user_id,
            permission_type='admin'
        )
        
        # Add tags if provided
        if command.tags:
            await self._dataset_repo.add_dataset_tags(dataset_id, command.tags)
        
        # Create initial empty commit
        initial_commit_id = await self._commit_repo.create_commit_and_manifest(
            dataset_id=dataset_id,
            parent_commit_id=None,
            message="Initial commit",
            author_id=command.user_id,
            manifest=[]  # Empty manifest for initial commit
        )
        
        # Update the default branch ref
        # The dataset creation creates a 'main' ref with NULL commit_id
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
        await self._event_bus.publish(DatasetCreatedEvent(
            dataset_id=dataset_id,
            user_id=command.user_id,
            name=command.name,
            description=command.description,
            tags=command.tags if command.tags else []
        ))
        
        # Fetch the created dataset to get timestamps
        dataset = await self._dataset_repo.get_dataset_by_id(dataset_id)
        
        return CreateDatasetResponse(
            dataset_id=dataset_id,
            name=command.name,
            description=command.description,
            tags=command.tags if command.tags else [],
            created_at=dataset['created_at']
        )