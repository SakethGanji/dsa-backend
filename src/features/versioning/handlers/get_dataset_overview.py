from typing import List
from src.core.abstractions import IUnitOfWork, ITableReader
from src.api.models import DatasetOverviewResponse, RefWithTables, TableInfo
from ...base_handler import BaseHandler, with_error_handling
from src.core.domain_exceptions import EntityNotFoundException


class GetDatasetOverviewHandler(BaseHandler):
    """Handler for getting dataset overview with refs and tables."""
    
    def __init__(self, uow: IUnitOfWork, table_reader: ITableReader):
        self.uow = uow
        self.table_reader = table_reader
        
    @with_error_handling
    async def handle(self, dataset_id: int, user_id: int) -> DatasetOverviewResponse:
        """Get overview of dataset including all refs and their tables."""
        async with self.uow:
            # Get dataset info
            dataset = await self.uow.datasets.get_dataset_by_id(dataset_id)
            if not dataset:
                raise EntityNotFoundException("Dataset", dataset_id)
            
            # Get all refs for the dataset
            refs = await self.uow.commits.list_refs(dataset_id)
            
            # Build ref with tables information
            refs_with_tables = []
            for ref in refs:
                commit_id = (ref["commit_id"] if isinstance(ref, dict) else ref.commit_id)
                if commit_id:
                    commit_id = commit_id.strip()  # Trim whitespace from commit_id
                ref_name = ref["name"] if isinstance(ref, dict) else ref.name
                created_at = ref["created_at"] if isinstance(ref, dict) else ref.created_at
                updated_at = ref["updated_at"] if isinstance(ref, dict) else ref.updated_at
                
                if commit_id:
                    # Get tables for this ref's commit
                    table_keys = await self.table_reader.list_table_keys(commit_id)
                    
                    # Get basic info for each table
                    tables = []
                    for table_key in table_keys:
                        # Get row count (reusing existing interface)
                        row_count = await self.table_reader.count_table_rows(commit_id, table_key)
                        
                        # Get column count and names from schema
                        schema = await self.table_reader.get_table_schema(commit_id, table_key)
                        if schema and isinstance(schema, dict):
                            # Schema has 'columns' key 
                            columns_data = schema.get('columns', [])
                            if isinstance(columns_data, list) and len(columns_data) > 0:
                                # Check if columns are strings (column names) or dicts (column objects)
                                if isinstance(columns_data[0], str):
                                    # Direct column names
                                    column_names = columns_data
                                elif isinstance(columns_data[0], dict):
                                    # Column objects with 'name' field
                                    column_names = [col.get('name', f'col_{i}') for i, col in enumerate(columns_data)]
                                else:
                                    column_names = []
                            else:
                                column_names = []
                            column_count = len(column_names)
                        else:
                            column_names = []
                            column_count = 0
                        
                        tables.append(TableInfo(
                            table_key=table_key,
                            sheet_name=table_key,  # Use table_key as sheet_name
                            row_count=row_count,
                            column_count=column_count,
                            created_at=created_at,  # Use ref's created_at
                            commit_id=commit_id
                        ))
                else:
                    # Empty ref (no commits yet)
                    tables = []
                
                refs_with_tables.append(RefWithTables(
                    ref_name=ref_name,
                    commit_id=commit_id,
                    is_default=ref_name == "main",  # Assuming 'main' is default
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