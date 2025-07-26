"""Handler for checking if a dataset is ready for operations."""

from typing import Dict, Any
from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.infrastructure.postgres.job_repo import PostgresJobRepository
from ...base_handler import BaseHandler
from src.core.decorators import requires_permission
from ..models import CheckDatasetReadyCommand


class CheckDatasetReadyHandler(BaseHandler):
    """Handler for checking dataset readiness (import status)."""
    
    def __init__(
        self,
        uow: PostgresUnitOfWork,
        job_repo: PostgresJobRepository
    ):
        super().__init__(uow)
        self._job_repo = job_repo
    
    @requires_permission("datasets", "read")
    async def handle(self, command: CheckDatasetReadyCommand) -> Dict[str, Any]:
        """
        Check if a dataset is ready for operations by examining import job status.
        
        Returns:
            Dict with ready status and details
        """
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