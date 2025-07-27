"""Handler for creating new commits."""
from typing import Optional

from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.infrastructure.services.commit_preparation_service import CommitPreparationService
from src.core.events.publisher import EventBus, CommitCreatedEvent
from src.api.models import CreateCommitRequest, CreateCommitResponse
from ...base_handler import BaseHandler, with_error_handling, with_transaction
from src.core.permissions import PermissionService


class CreateCommitHandler(BaseHandler[CreateCommitResponse]):
    """Handler for creating new commits with direct data."""
    
    def __init__(
        self,
        uow: PostgresUnitOfWork,
        commit_service: CommitPreparationService,
        permissions: PermissionService,
        event_bus: Optional[EventBus] = None
    ):
        super().__init__(uow)
        self._commit_service = commit_service
        self._permissions = permissions
        self._event_bus = event_bus
    
    @with_error_handling
    @with_transaction
    async def handle(
        self,
        dataset_id: int,
        ref_name: str,
        request: CreateCommitRequest,
        user_id: int
    ) -> CreateCommitResponse:
        """Create a new commit with provided data."""
        # Check write permission
        await self._permissions.require("dataset", dataset_id, user_id, "write")
        
        # Get current commit
        current = await self._uow.commits.get_current_commit_for_ref(dataset_id, ref_name)
        
        # Prepare commit data
        commit_data = await self._commit_service.prepare_commit_data(
            dataset_id=dataset_id,
            parent_commit_id=request.parent_commit_id or current,
            changes={request.table_name or 'primary': {'data': request.data}},
            message=request.message,
            author=str(user_id)
        )
        
        # Create commit
        commit_id = await self._uow.commits.create_commit_and_manifest(
            dataset_id=dataset_id,
            parent_commit_id=commit_data.parent_commit_id,
            message=commit_data.message,
            author_id=user_id,
            manifest=[(f"{t}:{i}", h) for t, hs in commit_data.row_hashes.items() 
                      for i, h in enumerate(hs)]
        )
        
        # Store row hashes and schemas
        for table, hashes in commit_data.row_hashes.items():
            await self._uow.commits.add_rows_if_not_exist([(h, h) for h in hashes])
        await self._uow.commits.create_commit_schema(commit_id, commit_data.schemas)
        
        # Update ref atomically
        if not await self._uow.commits.update_ref_atomically(
            dataset_id, ref_name, commit_id, current
        ):
            raise ValueError("Concurrent modification detected. Please retry.")
        
        # Refresh search and publish event
        await self._uow.search_repository.refresh_search_index()
        if self._event_bus:
            await self._event_bus.publish(CommitCreatedEvent.from_commit(
                commit_id, dataset_id, request.message, user_id, 
                commit_data.parent_commit_id
            ))
        
        # Return response
        commit = await self._uow.commits.get_commit_by_id(commit_id)
        return CreateCommitResponse(
            commit_id=commit_id,
            message=request.message,
            created_at=commit['created_at'] if commit else None
        )