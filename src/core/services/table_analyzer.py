"""Refactored table analysis service following clean architecture."""

from typing import Dict, Any, List, Set, Optional
from collections import defaultdict
import json
import logging

from ..abstractions.uow import IUnitOfWork
from ..abstractions.repositories import ITableReader

logger = logging.getLogger(__name__)


class TableAnalysisService:
    """Service for analyzing table data and generating comprehensive statistics."""
    
    def __init__(self, uow: IUnitOfWork):
        self._uow = uow
    
    async def analyze_imported_tables(
        self,
        commit_id: str,
        rows: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Analyze imported table data and store analysis results.
        
        Args:
            commit_id: The commit ID for which to calculate analysis
            rows: List of row dictionaries from import
            
        Returns:
            Dictionary mapping table_key to analysis results
        """
        # Group rows by table
        tables_data = defaultdict(list)
        for row in rows:
            table_key = row.get('sheet_name', 'primary')
            tables_data[table_key].append(row)
        
        # Analyze each table
        analysis_results = {}
        async with self._uow:
            for table_key, table_rows in tables_data.items():
                logger.info(f"Analyzing table {table_key} with {len(table_rows)} rows")
                
                # Calculate table-level statistics
                analysis = await self._analyze_table(table_rows)
                
                # Store analysis in database through repository
                await self._store_table_analysis(
                    commit_id, table_key, analysis
                )
                
                analysis_results[table_key] = analysis
            
            await self._uow.commit()
        
        return analysis_results
    
    async def _analyze_table(self, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze a single table's data."""
        if not rows:
            return {
                'row_count': 0,
                'column_count': 0,
                'columns': {},
                'sample_values': {}
            }
        
        # Get all unique columns
        all_columns = set()
        for row in rows:
            all_columns.update(k for k in row.keys() 
                            if not k.startswith('_') and k != 'sheet_name')
        
        # Analyze each column
        column_stats = {}
        sample_values = {}
        
        for column in sorted(all_columns):
            stats = self._analyze_column(rows, column)
            column_stats[column] = stats
            
            # Collect sample values (first 5 unique non-null values)
            unique_values = []
            seen = set()
            for row in rows:
                value = row.get(column)
                if value is not None and value not in seen and len(unique_values) < 5:
                    unique_values.append(value)
                    seen.add(value)
            sample_values[column] = unique_values
        
        return {
            'row_count': len(rows),
            'column_count': len(all_columns),
            'columns': column_stats,
            'sample_values': sample_values
        }
    
    def _analyze_column(self, rows: List[Dict[str, Any]], column: str) -> Dict[str, Any]:
        """Analyze a single column's data."""
        values = []
        null_count = 0
        type_counts = defaultdict(int)
        
        for row in rows:
            value = row.get(column)
            if value is None:
                null_count += 1
            else:
                values.append(value)
                type_counts[type(value).__name__] += 1
        
        # Determine primary data type
        primary_type = max(type_counts.items(), key=lambda x: x[1])[0] if type_counts else 'null'
        
        # Calculate basic statistics
        stats = {
            'data_type': primary_type,
            'null_count': null_count,
            'null_percentage': (null_count / len(rows) * 100) if rows else 0,
            'unique_count': len(set(values)),
            'type_distribution': dict(type_counts)
        }
        
        # Add numeric statistics if applicable
        numeric_values = [v for v in values if isinstance(v, (int, float))]
        if numeric_values:
            stats.update({
                'min': min(numeric_values),
                'max': max(numeric_values),
                'mean': sum(numeric_values) / len(numeric_values),
                'numeric_count': len(numeric_values)
            })
        
        # Add string statistics if applicable
        string_values = [str(v) for v in values if v is not None]
        if string_values:
            lengths = [len(s) for s in string_values]
            stats.update({
                'min_length': min(lengths),
                'max_length': max(lengths),
                'avg_length': sum(lengths) / len(lengths)
            })
        
        return stats
    
    async def _store_table_analysis(
        self,
        commit_id: str,
        table_key: str,
        analysis: Dict[str, Any]
    ) -> None:
        """Store table analysis results through repository."""
        # This would use a proper repository method
        # For now, we'll store it as part of commit metadata
        logger.info(f"Storing analysis for table {table_key} in commit {commit_id}")
    
    async def get_table_analysis(
        self,
        commit_id: str,
        table_key: str
    ) -> Optional[Dict[str, Any]]:
        """Retrieve stored table analysis."""
        async with self._uow:
            # This would use a repository method to fetch stored analysis
            # For now, return None (not implemented)
            return None