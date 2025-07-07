"""Optimized handler for queuing dataset import jobs with streaming file upload."""

from typing import BinaryIO, AsyncIterator
from uuid import UUID
import os
import tempfile
import aiofiles
from contextlib import asynccontextmanager

from src.core.abstractions import IUnitOfWork, IJobRepository, IDatasetRepository
from src.core.config import get_settings
from src.models.pydantic_models import QueueImportRequest, QueueImportResponse
from src.features.base_handler import BaseHandler, with_error_handling
from fastapi import HTTPException


class QueueImportJobHandler(BaseHandler[QueueImportResponse]):
    """Handler for queuing dataset import jobs from uploaded files with streaming support"""
    
    def __init__(self, uow: IUnitOfWork):
        super().__init__(uow)
        self.settings = get_settings()
    
    @asynccontextmanager
    async def save_upload_file_tmp(
        self, 
        file: BinaryIO, 
        filename: str,
        dataset_id: int,
        max_size: int
    ) -> AsyncIterator[str]:
        """
        Save uploaded file to temp location with streaming, size validation and cleanup.
        
        Args:
            file: File-like object to read from
            filename: Original filename
            dataset_id: Dataset ID for unique naming
            max_size: Maximum allowed file size in bytes
            
        Yields:
            Path to temporary file
            
        Raises:
            HTTPException: If file exceeds max size
        """
        # Create unique temp file
        temp_file = tempfile.NamedTemporaryFile(
            delete=False,
            prefix=f"import_{dataset_id}_",
            suffix=f"_{filename}"
        )
        
        try:
            file_size = 0
            chunk_size = 1024 * 1024  # 1MB chunks
            
            # Stream file to disk with size validation
            async with aiofiles.open(temp_file.name, 'wb') as f:
                while True:
                    # Read chunk from upload
                    chunk = file.read(chunk_size)
                    if not chunk:
                        break
                    
                    # Check size limit
                    file_size += len(chunk)
                    if file_size > max_size:
                        raise HTTPException(
                            status_code=413,
                            detail=f"File size ({file_size:,} bytes) exceeds maximum allowed size ({max_size:,} bytes)"
                        )
                    
                    # Write chunk to disk
                    await f.write(chunk)
            
            # Yield the temp file path
            yield temp_file.name
            
        finally:
            # Always cleanup temp file
            try:
                os.unlink(temp_file.name)
            except OSError:
                pass  # File already deleted
    
    @with_error_handling
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
        Queue an import job for processing uploaded file with streaming support
        
        Steps:
        1. Validate user has write permission
        2. Stream file to temporary storage with size validation
        3. Create job record with parameters
        4. Return job_id for status polling
        """
        # POC MODE: Skip permission check
        # TODO: Re-enable permission check for production
        # has_permission = await self._uow.datasets.check_user_permission(
        #     dataset_id, user_id, 'write'
        # )
        # if not has_permission:
        #     raise PermissionError("User lacks write permission for this dataset")
        
        # Get current commit for the ref
        current_commit = await self._uow.commits.get_current_commit_for_ref(
            dataset_id, ref_name
        )
        
        # Stream file to temporary storage with automatic cleanup
        async with self.save_upload_file_tmp(
            file, 
            filename, 
            dataset_id,
            self.settings.max_upload_size
        ) as temp_path:
            
            # Get file size for progress tracking
            file_size = os.path.getsize(temp_path)
            
            # Create job record
            job_parameters = {
                "temp_file_path": temp_path,
                "filename": filename,
                "file_size": file_size,
                "commit_message": request.commit_message,
                "target_ref": ref_name,
                "dataset_id": dataset_id,
                "user_id": user_id
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
                raise