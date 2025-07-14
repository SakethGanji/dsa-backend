"""Handler for comprehensive table analysis including schema, statistics, and sample values."""

from typing import Dict, Any, List
from collections import defaultdict

from src.core.abstractions import IUnitOfWork, ITableReader
from src.api.models import TableAnalysisResponse
from ...base_handler import BaseHandler, with_error_handling
from fastapi import HTTPException


class GetTableAnalysisHandler(BaseHandler[TableAnalysisResponse]):
    """Handler for retrieving comprehensive table analysis."""
    
    def __init__(self, uow: IUnitOfWork, table_reader: ITableReader):
        super().__init__(uow)
        self._table_reader = table_reader
    
    @with_error_handling
    async def handle(
        self,
        dataset_id: int,
        ref_name: str,
        table_key: str,
        user_id: int
    ) -> TableAnalysisResponse:
        """
        Get comprehensive table analysis including schema, statistics, and sample values.
        
        This handler reuses existing components:
        - ITableReader for schema and statistics
        - Extends with sample value collection
        
        Args:
            dataset_id: The dataset ID
            ref_name: The ref name (e.g., 'main')
            table_key: The table key to analyze
            user_id: The requesting user's ID
            
        Returns:
            TableAnalysisResponse with complete analysis
        """
        async with self._uow:
            # Permission check removed - handled by authorization middleware
            
            # Get the current commit for the ref
            ref = await self._uow.commits.get_ref(dataset_id, ref_name)
            if not ref:
                raise HTTPException(
                    status_code=404,
                    detail=f"Ref '{ref_name}' not found"
                )
            
            if not ref['commit_id']:
                raise HTTPException(
                    status_code=404,
                    detail=f"No commit found for ref '{ref_name}'"
                )
            
            commit_id = ref['commit_id']
            
            # Verify table exists
            available_tables = await self._table_reader.list_table_keys(commit_id)
            if table_key not in available_tables:
                raise HTTPException(
                    status_code=404,
                    detail=f"Table '{table_key}' not found. Available tables: {available_tables}"
                )
            
            # Get schema using existing table reader
            schema = await self._table_reader.get_table_schema(commit_id, table_key)
            if not schema:
                raise HTTPException(
                    status_code=500,
                    detail=f"Schema not found for table '{table_key}'"
                )
            
            # Parse schema if it's a string
            import json
            if isinstance(schema, str):
                schema = json.loads(schema)
            
            # Get statistics using existing table reader
            stats = await self._table_reader.get_table_statistics(commit_id, table_key)
            
            # Parse stats if it's a string
            if isinstance(stats, str):
                stats = json.loads(stats)
            
            # Extract column information from schema
            columns = []
            column_types = {}
            
            # Handle different schema formats
            schema_columns = schema.get('columns', [])
            if schema_columns and isinstance(schema_columns[0], str):
                # Simple format: columns as list of strings
                columns = schema_columns
                # Try to infer types from data later
                column_types = {col: 'unknown' for col in columns}
            else:
                # Complex format: columns as list of objects
                for col in schema_columns:
                    col_name = col['name']
                    columns.append(col_name)
                    column_types[col_name] = col.get('type', 'unknown')
            
            # First, try to get pre-computed analysis from table_analysis table
            analysis_result = await self._uow.connection.fetch(
                """
                SELECT analysis 
                FROM dsa_core.table_analysis 
                WHERE commit_id = $1 AND table_key = $2
                """,
                commit_id, table_key
            )
            
            if analysis_result:
                # Use pre-computed analysis
                analysis = json.loads(analysis_result[0]['analysis'])
                # Get sample data
                sample_data = await self._table_reader.get_table_data(
                    commit_id=commit_id,
                    table_key=table_key,
                    offset=0,
                    limit=10  # Just get 10 rows for sample
                )
                # Extract columns list from analysis
                column_types = analysis.get('column_types', {})
                columns_list = [{"name": name, "type": dtype} for name, dtype in column_types.items()]
                
                return TableAnalysisResponse(
                    table_key=table_key,
                    sheet_name=table_key,  # Use table_key as sheet_name
                    column_stats=analysis.get('statistics', {}),
                    sample_data=sample_data,
                    row_count=analysis.get('total_rows', 0),
                    null_counts=analysis.get('null_counts', {}),
                    unique_counts=analysis.get('unique_counts', {}),
                    data_types=analysis.get('column_types', {}),
                    columns=columns_list
                )
            
            # Fallback to computing analysis on-the-fly if not pre-computed
            # Get total row count
            total_rows = await self._table_reader.count_table_rows(commit_id, table_key)
            
            # Get null counts from statistics (if available)
            null_counts = {}
            if stats and 'null_counts' in stats:
                null_counts = stats['null_counts']
            else:
                # If statistics don't have null counts, initialize with zeros
                null_counts = {col: 0 for col in columns}
            
            # Get sample values - fetch more rows to ensure variety
            sample_size = min(1000, total_rows) if total_rows > 0 else 0
            sample_data = []
            if sample_size > 0:
                sample_data = await self._table_reader.get_table_data(
                    commit_id=commit_id,
                    table_key=table_key,
                    offset=0,
                    limit=sample_size
                )
            
            # Extract unique sample values per column and infer types
            sample_values = defaultdict(set)
            inferred_types = {}
            
            for row in sample_data:
                for col in columns:
                    if col in row and row[col] is not None:
                        value = row[col]
                        sample_values[col].add(value)
                        
                        # Infer type if not already known
                        if col not in inferred_types and column_types.get(col) == 'unknown':
                            if isinstance(value, bool):
                                inferred_types[col] = 'boolean'
                            elif isinstance(value, int):
                                inferred_types[col] = 'integer'
                            elif isinstance(value, float):
                                inferred_types[col] = 'float'
                            elif isinstance(value, str):
                                # Try to detect if it's a number stored as string
                                try:
                                    float(value)
                                    inferred_types[col] = 'numeric'
                                except ValueError:
                                    inferred_types[col] = 'string'
                            else:
                                inferred_types[col] = 'string'
            
            # Update column types with inferred types
            for col, dtype in inferred_types.items():
                if column_types[col] == 'unknown':
                    column_types[col] = dtype
            
            # Convert sets to lists and limit to 20 unique samples per column
            sample_values_dict = {}
            for col, values in sample_values.items():
                # Sort values for consistency (if possible)
                try:
                    sorted_values = sorted(list(values))[:20]
                except TypeError:
                    # If values aren't sortable, just convert to list
                    sorted_values = list(values)[:20]
                sample_values_dict[col] = sorted_values
            
            # Ensure all columns have an entry in sample_values
            for col in columns:
                if col not in sample_values_dict:
                    sample_values_dict[col] = []
            
            # Include additional statistics if available
            additional_stats = None
            if stats:
                # Extract useful statistics beyond null counts
                additional_stats = {
                    k: v for k, v in stats.items()
                    if k not in ['null_counts', 'row_count']
                }
            
            # Get unique counts
            unique_counts = {}
            for col in columns:
                unique_counts[col] = len(sample_values_dict.get(col, []))
            
            # Convert sample data to list format
            sample_data_list = sample_data[:10] if sample_data else []
            
            # Build column stats from available statistics
            column_stats = {}
            if stats:
                # Try to extract per-column statistics if available
                for col in columns:
                    col_stat = {}
                    if 'column_stats' in stats and col in stats['column_stats']:
                        col_stat = stats['column_stats'][col]
                    elif col in stats:
                        col_stat = stats[col]
                    column_stats[col] = col_stat
            
            # Extract columns list
            columns_list = [{"name": name, "type": dtype} for name, dtype in column_types.items()]
            
            return TableAnalysisResponse(
                table_key=table_key,
                sheet_name=table_key,  # Use table_key as sheet_name
                column_stats=column_stats,
                sample_data=sample_data_list,
                row_count=total_rows,
                null_counts=null_counts,
                unique_counts=unique_counts,
                data_types=column_types,
                columns=columns_list
            )