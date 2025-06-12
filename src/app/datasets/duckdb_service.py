"""DuckDB service for dataset operations - handles data query operations using DuckDB"""
import duckdb
import os
import logging
from typing import List, Dict, Any, Optional
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class DuckDBService:
    """Service for DuckDB operations on dataset files"""
    
    @staticmethod
    @contextmanager
    def get_connection(mode: str = ':memory:'):
        """Context manager for DuckDB connections"""
        conn = duckdb.connect(mode)
        try:
            yield conn
        finally:
            conn.close()
    
    @staticmethod
    async def query_parquet_file(
        file_path: str,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Execute a query on a Parquet file"""
        with DuckDBService.get_connection() as conn:
            # Create view from Parquet file
            conn.execute(f"CREATE VIEW data AS SELECT * FROM read_parquet('{file_path}')")
            
            # Execute query
            if params:
                result = conn.execute(query, params)
            else:
                result = conn.execute(query)
            
            # Get column names
            columns = [desc[0] for desc in result.description]
            
            # Convert to list of dicts
            rows = []
            for row in result.fetchall():
                row_dict = {}
                for i, value in enumerate(row):
                    row_dict[columns[i]] = value
                rows.append(row_dict)
            
            return rows
    
    @staticmethod
    async def get_file_metadata(file_path: str) -> Dict[str, Any]:
        """Get metadata about a Parquet file"""
        with DuckDBService.get_connection() as conn:
            # Create view from Parquet file
            conn.execute(f"CREATE VIEW data AS SELECT * FROM read_parquet('{file_path}')")
            
            # Get metadata
            columns_info = conn.execute("PRAGMA table_info('data')").fetchall()
            column_names = [col[1] for col in columns_info]
            column_types = {col[1]: col[2] for col in columns_info}
            
            # Get row count
            row_count = conn.execute("SELECT COUNT(*) FROM data").fetchone()[0]
            
            # Get file size
            file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
            
            return {
                "columns": len(column_names),
                "rows": row_count,
                "column_names": column_names,
                "column_types": column_types,
                "file_size": file_size,
                "file_format": "parquet"
            }
    
    @staticmethod
    async def export_to_format(
        file_path: str,
        output_format: str,
        output_path: str,
        filters: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Export Parquet file to another format"""
        try:
            with DuckDBService.get_connection() as conn:
                # Build query with optional filters
                base_query = f"SELECT * FROM read_parquet('{file_path}')"
                
                if filters:
                    where_clauses = []
                    for col, value in filters.items():
                        where_clauses.append(f"{col} = '{value}'")
                    if where_clauses:
                        base_query += " WHERE " + " AND ".join(where_clauses)
                
                # Export based on format
                if output_format.lower() == 'csv':
                    conn.execute(f"COPY ({base_query}) TO '{output_path}' (FORMAT CSV, HEADER)")
                elif output_format.lower() == 'json':
                    conn.execute(f"COPY ({base_query}) TO '{output_path}' (FORMAT JSON)")
                elif output_format.lower() == 'parquet':
                    conn.execute(f"COPY ({base_query}) TO '{output_path}' (FORMAT PARQUET)")
                else:
                    raise ValueError(f"Unsupported export format: {output_format}")
                
                return True
                
        except Exception as e:
            logger.error(f"Error exporting file: {str(e)}")
            return False