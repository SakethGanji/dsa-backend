"""Table analysis service for calculating comprehensive table statistics during import."""

from typing import Dict, Any, List, Set, Optional
from collections import defaultdict
import json
import logging
from src.infrastructure.postgres.database import DatabasePool

logger = logging.getLogger(__name__)


class TableAnalyzer:
    """Service for analyzing table data and generating comprehensive statistics."""
    
    async def analyze_imported_tables(
        self,
        conn,
        commit_id: str,
        rows: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Analyze imported table data and store analysis results.
        
        Args:
            conn: Database connection
            commit_id: The commit ID for which to calculate analysis
            rows: List of row dictionaries from import
            
        Returns:
            Dictionary mapping table_key to analysis results
        """
        # Group rows by table/sheet
        tables_data = defaultdict(list)
        for row in rows:
            table_key = row['sheet_name']
            tables_data[table_key].append(row['data'])
        
        analysis_results = {}
        
        # Analyze each table
        for table_key, table_rows in tables_data.items():
            analysis = await self._analyze_single_table(table_key, table_rows)
            analysis_results[table_key] = analysis
            
            # Store analysis in database
            await self._store_table_analysis(conn, commit_id, table_key, analysis)
        
        return analysis_results
    
    async def _analyze_single_table(
        self,
        table_key: str,
        rows: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Analyze a single table's data."""
        if not rows:
            return {
                'columns': [],
                'column_types': {},
                'total_rows': 0,
                'null_counts': {},
                'sample_values': {},
                'statistics': {}
            }
        
        # Extract columns from first row
        columns = list(rows[0].keys()) if rows else []
        
        # Initialize tracking structures
        column_types = {}
        null_counts = defaultdict(int)
        sample_values = defaultdict(set)
        numeric_stats = defaultdict(lambda: {'min': None, 'max': None, 'sum': 0, 'count': 0})
        
        # Analyze each row
        for row in rows:
            for col in columns:
                value = row.get(col)
                
                # Track nulls
                if value is None or value == '':
                    null_counts[col] += 1
                    continue
                
                # Collect sample values (limit to 100 to avoid memory issues)
                if len(sample_values[col]) < 100:
                    sample_values[col].add(value)
                
                # Infer type and collect stats
                if col not in column_types:
                    column_types[col] = self._infer_type(value)
                
                # Update numeric statistics if applicable
                if column_types[col] in ['integer', 'float', 'numeric']:
                    try:
                        num_value = float(value)
                        stats = numeric_stats[col]
                        if stats['min'] is None or num_value < stats['min']:
                            stats['min'] = num_value
                        if stats['max'] is None or num_value > stats['max']:
                            stats['max'] = num_value
                        stats['sum'] += num_value
                        stats['count'] += 1
                    except (ValueError, TypeError):
                        # If conversion fails, might need to reconsider type
                        if column_types[col] == 'numeric':
                            column_types[col] = 'string'
        
        # Convert sets to sorted lists for sample values
        sample_values_dict = {}
        for col in columns:
            values = list(sample_values[col])
            # Try to sort if possible
            try:
                values = sorted(values)[:20]  # Limit to 20 samples
            except TypeError:
                values = values[:20]
            sample_values_dict[col] = values
        
        # Calculate additional statistics
        statistics = {}
        for col in columns:
            col_stats = {}
            
            # Add numeric statistics if available
            if col in numeric_stats and numeric_stats[col]['count'] > 0:
                stats = numeric_stats[col]
                col_stats['min'] = stats['min']
                col_stats['max'] = stats['max']
                col_stats['mean'] = stats['sum'] / stats['count']
            
            # Calculate unique count
            col_stats['unique_count'] = len(sample_values[col])
            
            # Null percentage
            col_stats['null_percentage'] = (null_counts[col] / len(rows) * 100) if rows else 0
            
            statistics[col] = col_stats
        
        return {
            'columns': columns,
            'column_types': column_types,
            'total_rows': len(rows),
            'null_counts': dict(null_counts),
            'sample_values': sample_values_dict,
            'statistics': statistics
        }
    
    def _infer_type(self, value: Any) -> str:
        """Infer the data type of a value."""
        if isinstance(value, bool):
            return 'boolean'
        elif isinstance(value, int):
            return 'integer'
        elif isinstance(value, float):
            return 'float'
        elif isinstance(value, str):
            # Try to detect if it's a number stored as string
            try:
                float(value)
                return 'numeric'
            except ValueError:
                return 'string'
        else:
            return 'string'
    
    async def _store_table_analysis(
        self,
        conn,
        commit_id: str,
        table_key: str,
        analysis: Dict[str, Any]
    ):
        """Store table analysis results in the database."""
        # Store in a new table_analysis table
        await conn.execute("""
            INSERT INTO dsa_core.table_analysis (commit_id, table_key, analysis)
            VALUES ($1, $2, $3)
            ON CONFLICT (commit_id, table_key) 
            DO UPDATE SET analysis = EXCLUDED.analysis
        """, commit_id, table_key, json.dumps(analysis))
        
        logger.info(f"Stored analysis for table {table_key} in commit {commit_id}")
    
    async def analyze_committed_tables(
        self,
        db_pool: DatabasePool,
        commit_id: str
    ) -> Dict[str, Dict[str, Any]]:
        """
        Analyze tables from an already committed dataset.
        Used by optimized import executor after batch processing.
        
        Args:
            db_pool: Database connection pool
            commit_id: The commit ID to analyze
            
        Returns:
            Dictionary mapping table_key to analysis results
        """
        async with db_pool.acquire() as conn:
            # Get all distinct table keys for this commit
            table_keys_result = await conn.fetch("""
                SELECT DISTINCT 
                    SPLIT_PART(cr.logical_row_id, ':', 1) as table_key
                FROM dsa_core.commit_rows cr
                WHERE cr.commit_id = $1
            """, commit_id)
            
            table_keys = [row['table_key'] for row in table_keys_result]
            
            analysis_results = {}
            
            for table_key in table_keys:
                # Fetch all rows for this table
                rows_result = await conn.fetch("""
                    SELECT 
                        r.data->>'data' as row_data,
                        r.data->>'sheet_name' as sheet_name
                    FROM dsa_core.commit_rows cr
                    JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
                    WHERE cr.commit_id = $1 
                    AND SPLIT_PART(cr.logical_row_id, ':', 1) = $2
                    ORDER BY cr.logical_row_id
                """, commit_id, table_key)
                
                # Convert to list of dictionaries
                table_rows = []
                for row in rows_result:
                    if row['row_data']:
                        try:
                            row_data = json.loads(row['row_data'])
                            table_rows.append(row_data)
                        except json.JSONDecodeError:
                            logger.warning(f"Failed to parse row data for table {table_key}")
                
                # Analyze the table
                analysis = await self._analyze_single_table(table_key, table_rows)
                analysis_results[table_key] = analysis
                
                # Store analysis in database
                await self._store_table_analysis(conn, commit_id, table_key, analysis)
            
            return analysis_results