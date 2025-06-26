"""DuckDB service for data operations - handles data query operations using DuckDB.

This service provides generic data processing capabilities including:
- Querying Parquet, CSV, and other data files
- Extracting schema information
- Converting between data formats
- Getting file metadata
"""
import duckdb
import os
import logging
from typing import List, Dict, Any, Optional
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class DuckDBService:
    """Service for DuckDB operations on data files.
    
    This is a generic data processing service that can be used across
    different vertical slices for working with structured data files.
    """
    
    @staticmethod
    @contextmanager
    def get_connection(mode: str = ':memory:'):
        """Context manager for DuckDB connections.
        
        Args:
            mode: Connection mode (':memory:' for in-memory, or file path)
            
        Yields:
            duckdb.DuckDBPyConnection: Active DuckDB connection
        """
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
        """Execute a query on a Parquet file.
        
        Args:
            file_path: Path to the Parquet file
            query: SQL query to execute
            params: Optional query parameters
            
        Returns:
            List of dictionaries representing query results
        """
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
        """Get metadata about a Parquet file.
        
        Args:
            file_path: Path to the Parquet file
            
        Returns:
            Dictionary containing file metadata
        """
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
    async def extract_schema(file_path: str, file_type: str) -> Dict[str, Any]:
        """Extract schema information from a file.
        
        Args:
            file_path: Path to the data file
            file_type: Type of the file (parquet, csv, tsv, etc.)
            
        Returns:
            Dictionary containing schema information in JSON schema format
        """
        try:
            with DuckDBService.get_connection() as conn:
                # Create view based on file type
                if file_type.lower() in ['parquet', 'pq']:
                    conn.execute(f"CREATE VIEW data AS SELECT * FROM read_parquet('{file_path}')")
                elif file_type.lower() in ['csv', 'tsv']:
                    conn.execute(f"CREATE VIEW data AS SELECT * FROM read_csv_auto('{file_path}')")
                elif file_type.lower() in ['xlsx', 'xls']:
                    # DuckDB doesn't natively support Excel, would need conversion
                    # For now, we'll skip Excel support in schema extraction
                    return {"error": "Excel format not supported for schema extraction"}
                else:
                    return {"error": f"Unsupported file type: {file_type}"}
                
                # Get column information
                columns_info = conn.execute("PRAGMA table_info('data')").fetchall()
                
                # Build schema in JSON schema format
                schema = {
                    "type": "object",
                    "properties": {},
                    "columns": []
                }
                
                # Map DuckDB types to JSON schema types
                type_mapping = {
                    'INTEGER': 'integer',
                    'BIGINT': 'integer',
                    'DOUBLE': 'number',
                    'FLOAT': 'number',
                    'DECIMAL': 'number',
                    'VARCHAR': 'string',
                    'DATE': 'string',
                    'TIMESTAMP': 'string',
                    'BOOLEAN': 'boolean',
                    'BLOB': 'string'
                }
                
                for col_info in columns_info:
                    col_name = col_info[1]
                    col_type = col_info[2]
                    is_nullable = col_info[3] == 0  # 0 means nullable
                    
                    # Map to JSON schema type
                    json_type = type_mapping.get(col_type.upper().split('(')[0], 'string')
                    
                    column_schema = {
                        "type": json_type,
                        "nullable": is_nullable,
                        "duckdb_type": col_type
                    }
                    
                    schema["properties"][col_name] = column_schema
                    schema["columns"].append({
                        "name": col_name,
                        "type": json_type,
                        "nullable": is_nullable,
                        "original_type": col_type
                    })
                
                # Get sample statistics
                try:
                    row_count = conn.execute("SELECT COUNT(*) FROM data").fetchone()[0]
                    schema["row_count"] = row_count
                except:
                    schema["row_count"] = None
                
                return schema
                
        except Exception as e:
            logger.error(f"Error extracting schema: {str(e)}")
            return {"error": str(e)}
    
    @staticmethod
    async def export_to_format(
        file_path: str,
        output_format: str,
        output_path: str,
        filters: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Export data file to another format.
        
        Args:
            file_path: Path to the source file
            output_format: Target format (csv, json, parquet)
            output_path: Path for the output file
            filters: Optional filters to apply during export
            
        Returns:
            True if successful, False otherwise
        """
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