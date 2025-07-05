from typing import BinaryIO
from uuid import UUID
import os
import tempfile

from src.core.abstractions import IUnitOfWork, IJobRepository, IDatasetRepository
from src.models.pydantic_models import QueueImportRequest, QueueImportResponse


class QueueImportJobHandler:
    """Handler for queuing dataset import jobs from uploaded files"""
    
    def __init__(self, uow: IUnitOfWork):
        self._uow = uow
    
    async def handle(
        self,
        dataset_id: int,
        ref_name: str,
        file: BinaryIO,
        filename: str,
        request: QueueImportRequest,
        user_id: int
    ) -> QueueImportResponse:
        """
        Queue an import job for processing uploaded file
        
        Steps:
        1. Validate user has write permission
        2. Save file to temporary storage
        3. Create job record with parameters
        4. Return job_id for status polling
        """
        # Check user permission
        has_permission = await self._uow.datasets.check_user_permission(
            dataset_id, user_id, 'write'
        )
        if not has_permission:
            raise PermissionError("User lacks write permission for this dataset")
        
        # TODO: Save file to temporary storage
        # In production, use S3 or similar with expiration policies
        temp_dir = tempfile.gettempdir()
        temp_path = os.path.join(temp_dir, f"import_{dataset_id}_{filename}")
        
        # Save uploaded file
        with open(temp_path, 'wb') as f:
            # TODO: Stream large files instead of loading in memory
            content = file.read()
            f.write(content)
        
        # Get current commit for the ref
        current_commit = await self._uow.commits.get_current_commit_for_ref(
            dataset_id, ref_name
        )
        
        # TODO: Create job record
        job_parameters = {
            "temp_file_path": temp_path,
            "filename": filename,
            "commit_message": request.commit_message,
            "target_ref": ref_name
        }
        
        await self._uow.begin()
        try:
            job_id = await self._uow.jobs.create_job(
                run_type='import',
                dataset_id=dataset_id,
                user_id=user_id,
                source_commit_id=current_commit,
                run_parameters=job_parameters
            )
            await self._uow.commit()
            
            return QueueImportResponse(job_id=job_id)
        except Exception:
            await self._uow.rollback()
            # TODO: Clean up temp file on failure
            raise