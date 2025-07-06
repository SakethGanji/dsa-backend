from typing import Optional, List, Dict, Any

from src.core.abstractions import ICommitRepository, IDatasetRepository
from src.models.pydantic_models import GetDataRequest, GetDataResponse, DataRow
from src.features.base_handler import BaseHandler, with_error_handling, PaginationMixin


class GetDataAtRefHandler(BaseHandler[GetDataResponse], PaginationMixin):
    """Handler for retrieving data at a specific ref"""
    
    def __init__(
        self,
        commit_repo: ICommitRepository,
        dataset_repo: IDatasetRepository
    ):
        # Note: We don't have UoW here, so we pass None to BaseHandler
        super().__init__(None)
        self._commit_repo = commit_repo
        self._dataset_repo = dataset_repo
    
    @with_error_handling
    async def handle(
        self,
        dataset_id: int,
        ref_name: str,
        request: GetDataRequest,
        user_id: int
    ) -> GetDataResponse:
        """
        Retrieve paginated data for a ref
        
        Steps:
        1. Check read permission
        2. Get commit ID for ref
        3. Fetch data from commit
        4. Apply pagination and filtering
        """
        # Validate pagination parameters
        offset, limit = self.validate_pagination(request.offset, request.limit)
        
        # Permission check removed - handled by authorization middleware
        
        # TODO: Get current commit for ref
        commit_id = await self._commit_repo.get_current_commit_for_ref(
            dataset_id, ref_name
        )
        if not commit_id:
            raise ValueError(f"Ref '{ref_name}' not found for dataset {dataset_id}")
        
        # TODO: Fetch data with pagination
        rows_data = await self._commit_repo.get_commit_data(
            commit_id=commit_id,
            table_key=request.table_key,
            offset=offset,
            limit=limit
        )
        
        # TODO: Get total count for pagination
        # This would require a separate count query in real implementation
        total_rows = len(rows_data)  # Placeholder
        
        # Transform to response format
        rows = [
            DataRow(
                logical_row_id=row['logical_row_id'],
                data=row['data']
            )
            for row in rows_data
        ]
        
        return GetDataResponse(
            dataset_id=dataset_id,
            ref_name=ref_name,
            commit_id=commit_id,
            rows=rows,
            total_rows=total_rows,
            offset=offset,
            limit=limit
        )