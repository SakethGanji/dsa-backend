from typing import List, Dict, Any
from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.infrastructure.postgres.table_reader import PostgresTableReader
from src.api.models import DatasetOverviewResponse, RefWithTables, TableInfo
from ...base_handler import BaseHandler, with_error_handling
from src.core.domain_exceptions import EntityNotFoundException
from src.core.permissions import PermissionService


class GetDatasetOverviewHandler(BaseHandler):
    """Optimized handler for getting dataset overview with refs and tables."""
    
    def __init__(self, uow: PostgresUnitOfWork, table_reader: PostgresTableReader, permissions: PermissionService):
        self.uow = uow
        self.table_reader = table_reader
        self._permissions = permissions
        
    @with_error_handling
    async def handle(self, dataset_id: int, user_id: int) -> DatasetOverviewResponse:
        """Get overview of dataset including all refs and their tables."""
        async with self.uow:
            # Check read permission
            await self._permissions.require("dataset", dataset_id, user_id, "read")
            # Get dataset info
            dataset = await self.uow.datasets.get_dataset_by_id(dataset_id)
            if not dataset:
                raise EntityNotFoundException("Dataset", dataset_id)
            
            # Get all refs for the dataset
            refs = await self.uow.commits.list_refs(dataset_id)
            
            # Extract all unique commit IDs
            commit_ids = []
            ref_by_commit = {}
            for ref in refs:
                commit_id = (ref["commit_id"] if isinstance(ref, dict) else ref.commit_id)
                if commit_id:
                    commit_id = commit_id.strip()
                    commit_ids.append(commit_id)
                    ref_by_commit[commit_id] = ref
            
            # Batch fetch all table metadata for all commits
            if commit_ids:
                table_metadata = await self.table_reader.batch_get_table_metadata(commit_ids)
            else:
                table_metadata = {}
            
            # Build ref with tables information
            refs_with_tables = []
            for ref in refs:
                ref_name = ref["name"] if isinstance(ref, dict) else ref.name
                commit_id = (ref["commit_id"] if isinstance(ref, dict) else ref.commit_id)
                created_at = ref["created_at"] if isinstance(ref, dict) else ref.created_at
                updated_at = ref["updated_at"] if isinstance(ref, dict) else ref.updated_at
                
                if commit_id:
                    commit_id = commit_id.strip()
                    # Get pre-fetched table data and convert to TableInfo objects
                    table_data_list = table_metadata.get(commit_id, [])
                    tables = [
                        TableInfo(
                            table_key=data['table_key'],
                            sheet_name=data['table_key'],
                            row_count=data['row_count'],
                            column_count=data['column_count'],
                            created_at=data['created_at'],
                            commit_id=commit_id
                        )
                        for data in table_data_list
                    ]
                else:
                    tables = []
                
                refs_with_tables.append(RefWithTables(
                    ref_name=ref_name,
                    commit_id=commit_id,
                    is_default=ref_name == "main",
                    tables=tables,
                    created_at=created_at,
                    updated_at=updated_at
                ))
            
            return DatasetOverviewResponse(
                dataset_id=dataset_id,
                name=dataset["name"],
                description=dataset.get("description"),
                branches=refs_with_tables
            )
