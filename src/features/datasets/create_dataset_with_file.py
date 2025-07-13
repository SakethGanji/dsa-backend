from typing import BinaryIO
import os
import tempfile
from dataclasses import dataclass

from src.core.abstractions import IUnitOfWork, IDatasetRepository, ICommitRepository, IJobRepository
from src.models.pydantic_models import CreateDatasetWithFileRequest, CreateDatasetWithFileResponse
from src.features.datasets.create_dataset import CreateDatasetHandler
from src.features.versioning.queue_import_job import QueueImportJobHandler
from src.features.base_handler import BaseHandler, with_error_handling, with_transaction


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
        commit_repo: ICommitRepository
    ):
        super().__init__(uow)
        self._dataset_repo = dataset_repo
        self._commit_repo = commit_repo
        # Reuse existing handlers
        self._create_dataset_handler = CreateDatasetHandler(uow, dataset_repo, commit_repo)
        self._queue_import_handler = QueueImportJobHandler(uow)
    
    @with_error_handling
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
        1. Create dataset using existing handler
        2. Queue import job for the file
        3. Return combined response
        """
        
        # First create the dataset
        from src.models.pydantic_models import CreateDatasetRequest
        
        create_dataset_req = CreateDatasetRequest(
            name=request.name,
            description=request.description,
            tags=request.tags,
            default_branch=request.default_branch
        )
        
        dataset_response = await self._create_dataset_handler.handle(
            create_dataset_req,
            user_id
        )
        
        # Now queue the import job
        from src.models.pydantic_models import QueueImportRequest
        
        queue_import_req = QueueImportRequest(
            commit_message=request.commit_message
        )
        
        import_response = await self._queue_import_handler.handle(
            dataset_id=dataset_response.dataset_id,
            ref_name=request.default_branch,
            file=file,
            filename=filename,
            request=queue_import_req,
            user_id=user_id
        )
        
        # Return combined response
        return CreateDatasetWithFileResponse(
            dataset_id=dataset_response.dataset_id,
            name=dataset_response.name,
            description=dataset_response.description,
            tags=dataset_response.tags,
            import_job_id=import_response.job_id,
            status=import_response.status,
            message="Dataset created and import job queued successfully"
        )