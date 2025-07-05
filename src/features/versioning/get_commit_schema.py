from typing import Optional, Dict, Any, List

from src.core.abstractions import ICommitRepository, IDatasetRepository
from src.models.pydantic_models import CommitSchemaResponse, SheetSchema, ColumnSchema


class GetCommitSchemaHandler:
    """Handler for retrieving schema information for a commit"""
    
    def __init__(
        self,
        commit_repo: ICommitRepository,
        dataset_repo: IDatasetRepository
    ):
        self._commit_repo = commit_repo
        self._dataset_repo = dataset_repo
    
    async def handle(
        self,
        dataset_id: int,
        commit_id: str,
        user_id: int
    ) -> CommitSchemaResponse:
        """
        Retrieve schema for a specific commit
        
        Steps:
        1. Check read permission
        2. Fetch schema from commit_schemas table
        3. Transform to response format
        """
        # TODO: Verify commit belongs to dataset and user has permission
        has_permission = await self._dataset_repo.check_user_permission(
            dataset_id, user_id, 'read'
        )
        if not has_permission:
            raise PermissionError("User lacks read permission")
        
        # TODO: Get schema definition
        schema_data = await self._commit_repo.get_commit_schema(commit_id)
        if not schema_data:
            raise ValueError(f"Schema not found for commit {commit_id}")
        
        # Transform schema to response format
        sheets = self._transform_schema(schema_data)
        
        return CommitSchemaResponse(
            commit_id=commit_id,
            sheets=sheets
        )
    
    def _transform_schema(self, schema_data: Dict[str, Any]) -> List[SheetSchema]:
        """Transform raw schema data to SheetSchema models"""
        sheets = []
        
        for sheet_name, sheet_info in schema_data.items():
            columns = [
                ColumnSchema(
                    name=col_name,
                    type=col_info.get('type', 'string'),
                    nullable=col_info.get('nullable', True)
                )
                for col_name, col_info in sheet_info.get('columns', {}).items()
            ]
            
            sheets.append(SheetSchema(
                sheet_name=sheet_name,
                columns=columns,
                row_count=sheet_info.get('row_count', 0)
            ))
        
        return sheets