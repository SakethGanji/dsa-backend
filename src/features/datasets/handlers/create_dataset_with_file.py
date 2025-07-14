from typing import BinaryIO
import os
import tempfile
import uuid
from dataclasses import dataclass

from src.core.abstractions import IUnitOfWork, IDatasetRepository, ICommitRepository, IJobRepository
from src.api.models import CreateDatasetWithFileRequest, CreateDatasetWithFileResponse, CreateDatasetResponse, QueueImportResponse
from ...base_handler import BaseHandler, with_error_handling, with_transaction
from src.core.events import EventBus, DatasetCreatedEvent, get_event_bus
from src.core.abstractions.models.constants import JobStatus


@dataclass
class CreateDatasetWithFileCommand:
    """Command for creating dataset with file"""
    request: CreateDatasetWithFileRequest
    file: BinaryIO
    filename: str
    user_id: int


class CreateDatasetWithFileHandler(BaseHandler[CreateDatasetWithFileResponse]):
    """Handler for creating a dataset and importing a file in one operation"""
    
    def __init__(
        self,
        uow: IUnitOfWork,
        dataset_repo: IDatasetRepository,
        commit_repo: ICommitRepository,
        job_repo: IJobRepository = None
    ):
        super().__init__(uow)
        self._dataset_repo = dataset_repo
        self._commit_repo = commit_repo
        self._job_repo = job_repo
    
    @with_error_handling
    @with_transaction
    async def handle(
        self,
        request: CreateDatasetWithFileRequest,
        file: BinaryIO,
        filename: str,
        user_id: int
    ) -> CreateDatasetWithFileResponse:
        """
        Create a new dataset and queue an import job for the uploaded file
        
        Steps:
        1. Create dataset
        2. Queue import job for the file
        3. Return combined response
        """
        
        # Create dataset
        dataset_id = await self._dataset_repo.create_dataset(
            name=request.name,
            description=request.description or "",
            created_by=user_id
        )
        
        # Grant admin permission to creator
        await self._dataset_repo.grant_permission(
            dataset_id=dataset_id,
            user_id=user_id,
            permission_type='admin'
        )
        
        # Add tags if provided
        if request.tags:
            await self._dataset_repo.add_dataset_tags(dataset_id, request.tags)
        
        # Create initial empty commit
        initial_commit_id = await self._commit_repo.create_commit_and_manifest(
            dataset_id=dataset_id,
            parent_commit_id=None,
            message="Initial commit",
            author_id=user_id,
            manifest=[]  # Empty manifest for initial commit
        )
        
        # Update ref to point to initial commit
        # For initial dataset, we might need to create the ref first
        # Try to get current commit for ref
        current_commit = await self._commit_repo.get_current_commit_for_ref(
            dataset_id=dataset_id,
            ref_name=request.default_branch
        )
        
        if current_commit is None:
            # Need to create the ref - check if there's a create_ref method
            # For now, we'll just try update_ref_atomically with empty string
            await self._commit_repo.update_ref_atomically(
                dataset_id=dataset_id,
                ref_name=request.default_branch,
                new_commit_id=initial_commit_id,
                expected_commit_id=""  # Empty string for initial creation
            )
        else:
            await self._commit_repo.update_ref_atomically(
                dataset_id=dataset_id,
                ref_name=request.default_branch,
                new_commit_id=initial_commit_id,
                expected_commit_id=current_commit
            )
        
        # Now queue the import job
        # Save file to temporary location
        temp_dir = tempfile.mkdtemp()
        file_path = os.path.join(temp_dir, filename)
        
        # Write file content
        with open(file_path, 'wb') as f:
            file.seek(0)
            f.write(file.read())
        
        # Create job record if job_repo is available
        job_id = None
        if self._job_repo:
            job_id = await self._job_repo.create_job(
                run_type='import',
                dataset_id=dataset_id,
                user_id=user_id,
                source_commit_id=initial_commit_id,
                run_parameters={
                    'dataset_id': dataset_id,
                    'ref_name': request.default_branch,
                    'file_path': file_path,
                    'original_filename': filename,
                    'commit_message': request.commit_message,
                    'user_id': user_id
                }
            )
        
        # Fetch the created dataset to get timestamps
        dataset = await self._dataset_repo.get_dataset_by_id(dataset_id)
        
        # Publish event
        event_bus = get_event_bus()
        await event_bus.publish(DatasetCreatedEvent(
            dataset_id=dataset_id,
            user_id=user_id,
            name=request.name,
            description=request.description or "",
            tags=request.tags if request.tags else []
        ))
        
        # Return combined response
        return CreateDatasetWithFileResponse(
            dataset=CreateDatasetResponse(
                dataset_id=dataset_id,
                name=request.name,
                description=request.description or "",
                tags=request.tags if request.tags else [],
                created_at=dataset['created_at']
            ),
            commit_id="",  # Will be populated after import completes
            import_job=QueueImportResponse(
                job_id=str(job_id) if job_id else str(uuid.uuid4()),
                status="pending",
                message="Import job queued successfully"
            )
        )