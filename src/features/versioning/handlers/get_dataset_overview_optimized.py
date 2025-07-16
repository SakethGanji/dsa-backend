from typing import List, Dict, Any
from src.core.abstractions import IUnitOfWork
from src.api.models import DatasetOverviewResponse, RefWithTables, TableInfo
from ...base_handler import BaseHandler, with_error_handling
from src.core.domain_exceptions import EntityNotFoundException
import asyncio
import json


class GetDatasetOverviewOptimizedHandler(BaseHandler):
    """Optimized handler for getting dataset overview with refs and tables."""
    
    def __init__(self, uow: IUnitOfWork):
        self.uow = uow
        
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
                table_metadata = await self._batch_fetch_table_metadata(commit_ids)
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
                    # Get pre-fetched table data
                    tables = table_metadata.get(commit_id, [])
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
    
    async def _batch_fetch_table_metadata(self, commit_ids: List[str]) -> Dict[str, List[TableInfo]]:
        """Fetch all table metadata for multiple commits in a single query."""
        # Get database connection from UOW
        # Access the connection through the commits repository
        conn = self.uow.commits._conn
        
        # First, get all schemas for the commits
        schema_query = """
            SELECT 
                cs.commit_id,
                cs.schema_definition,
                cs.created_at
            FROM dsa_core.commit_schemas cs
            WHERE cs.commit_id = ANY($1::text[])
        """
        
        schema_rows = await conn.fetch(schema_query, commit_ids)
        
        # Then get row counts for each table
        # We need to extract table keys from logical_row_ids
        count_query = """
            SELECT 
                commit_id,
                CASE 
                    WHEN logical_row_id LIKE '%:%' THEN SPLIT_PART(logical_row_id, ':', 1)
                    ELSE REGEXP_REPLACE(logical_row_id, '_[0-9]+$', '')
                END AS table_key,
                COUNT(*) as row_count
            FROM dsa_core.commit_rows
            WHERE commit_id = ANY($1::text[])
            GROUP BY commit_id, table_key
        """
        
        count_rows = await conn.fetch(count_query, commit_ids)
        
        # Build a lookup for row counts
        row_counts = {}
        for row in count_rows:
            commit_id = row['commit_id'].strip()
            table_key = row['table_key']
            if commit_id not in row_counts:
                row_counts[commit_id] = {}
            row_counts[commit_id][table_key] = row['row_count']
        
        # Process results
        result = {}
        for schema_row in schema_rows:
            commit_id = schema_row['commit_id'].strip()
            schema_def = schema_row['schema_definition']
            created_at = schema_row['created_at']
            
            if commit_id not in result:
                result[commit_id] = []
            
            # Parse schema to get table information
            if schema_def:
                # Handle different schema formats
                if isinstance(schema_def, str):
                    schema_def = json.loads(schema_def)
                
                # Extract table keys and column info
                tables_to_process = []
                
                # Check for direct table keys (newer format)
                if isinstance(schema_def, dict):
                    for table_key, table_schema in schema_def.items():
                        if isinstance(table_schema, dict) and 'columns' in table_schema:
                            tables_to_process.append({
                                'table_key': table_key,
                                'columns': table_schema['columns']
                            })
                    
                    # Also check for 'sheets' format (Excel files)
                    if 'sheets' in schema_def:
                        for sheet in schema_def['sheets']:
                            if 'sheet_name' in sheet and 'columns' in sheet:
                                tables_to_process.append({
                                    'table_key': sheet['sheet_name'],
                                    'columns': sheet['columns']
                                })
                
                # Create TableInfo for each table
                for table_data in tables_to_process:
                    table_key = table_data['table_key']
                    columns = table_data['columns']
                    
                    # Get column count
                    column_count = len(columns) if columns else 0
                    
                    # Get row count from our lookup
                    row_count = row_counts.get(commit_id, {}).get(table_key, 0)
                    
                    table_info = TableInfo(
                        table_key=table_key,
                        sheet_name=table_key,
                        row_count=row_count,
                        column_count=column_count,
                        created_at=created_at,
                        commit_id=commit_id
                    )
                    
                    result[commit_id].append(table_info)
        
        return result