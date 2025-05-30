"""
File Service - Abstraction layer for file operations across the application
"""
import os
from typing import Optional, Dict, Any, List, Tuple
import duckdb
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class FileService:
    """
    Unified file service for all file operations in the application.
    Provides a consistent interface for reading/writing files across datasets and sampling modules.
    """
    
    def __init__(self, base_path: str = "/home/saketh/Projects/dsa/data"):
        self.base_path = Path(base_path)
        self.datasets_path = self.base_path / "datasets"
        self.samples_path = self.base_path / "samples"
        self._ensure_directories()
    
    def _ensure_directories(self):
        """Ensure all required directories exist"""
        self.datasets_path.mkdir(parents=True, exist_ok=True)
        self.samples_path.mkdir(parents=True, exist_ok=True)
    
    async def read_dataset_file(
        self, 
        file_path: str,
        columns: Optional[List[str]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Read a dataset file (always in Parquet format) using DuckDB.
        
        Args:
            file_path: Path to the Parquet file
            columns: Optional list of columns to read
            limit: Optional number of rows to return
            offset: Optional number of rows to skip
            
        Returns:
            List of dictionaries with the requested data
        """
        conn = duckdb.connect(':memory:')
        try:
            # Build column list
            col_list = ', '.join([f'"{col}"' for col in columns]) if columns else '*'
            
            # Build query with pagination
            query = f"SELECT {col_list} FROM read_parquet('{file_path}')"
            if limit:
                query += f" LIMIT {limit}"
                if offset:
                    query += f" OFFSET {offset}"
            
            result = conn.execute(query).fetchall()
            cols = [desc[0] for desc in conn.description]
            
            # Return as list of dicts
            return [dict(zip(cols, row)) for row in result]
        except Exception as e:
            logger.error(f"Error reading dataset file {file_path}: {str(e)}")
            raise ValueError(f"Failed to read dataset file: {str(e)}")
        finally:
            conn.close()
    
    async def get_file_metadata(self, file_path: str) -> Dict[str, Any]:
        """
        Get metadata about a Parquet file without loading the data using DuckDB.
        
        Args:
            file_path: Path to the Parquet file
            
        Returns:
            Dictionary with file metadata
        """
        conn = duckdb.connect(':memory:')
        try:
            # Create a view to inspect metadata
            conn.execute(f"CREATE VIEW metadata AS SELECT * FROM read_parquet('{file_path}') LIMIT 0")
            
            # Get column information
            columns_info = conn.execute("PRAGMA table_info('metadata')").fetchall()
            columns = []
            column_types = {}
            for col_info in columns_info:
                col_name = col_info[1]
                col_type = col_info[2]
                columns.append(col_name)
                column_types[col_name] = col_type
            
            # Get row count
            row_count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{file_path}')").fetchone()[0]
            
            return {
                "num_rows": row_count,
                "num_columns": len(columns),
                "columns": columns,
                "column_types": column_types,
                "file_size": os.path.getsize(file_path),
                "created": os.path.getctime(file_path),
                "modified": os.path.getmtime(file_path)
            }
        except Exception as e:
            logger.error(f"Error getting file metadata for {file_path}: {str(e)}")
            raise ValueError(f"Failed to get file metadata: {str(e)}")
        finally:
            conn.close()
    
    def get_dataset_path(self, dataset_id: int, version_id: int, filename: str) -> Path:
        """Get the standard path for a dataset file"""
        return self.datasets_path / str(dataset_id) / str(version_id) / filename
    
    def get_sample_path(self, dataset_id: int, version_id: int, job_id: str) -> Path:
        """Get the standard path for a sample file"""
        return self.samples_path / str(dataset_id) / str(version_id) / f"{job_id}.parquet"
    
    async def file_exists(self, file_path: str) -> bool:
        """Check if a file exists"""
        return Path(file_path).exists()
    
    async def delete_file(self, file_path: str) -> bool:
        """Delete a file"""
        try:
            if await self.file_exists(file_path):
                os.unlink(file_path)
                return True
            return False
        except Exception as e:
            logger.error(f"Error deleting file {file_path}: {str(e)}")
            return False
    
    async def list_dataset_files(self, dataset_id: int, version_id: Optional[int] = None) -> List[str]:
        """List all files for a dataset or specific version"""
        if version_id:
            path = self.datasets_path / str(dataset_id) / str(version_id)
        else:
            path = self.datasets_path / str(dataset_id)
        
        if not path.exists():
            return []
        
        files = []
        for file_path in path.rglob("*.parquet"):
            files.append(str(file_path))
        
        return files
    
    async def list_sample_files(self, dataset_id: int, version_id: int) -> List[str]:
        """List all sample files for a dataset version"""
        path = self.samples_path / str(dataset_id) / str(version_id)
        
        if not path.exists():
            return []
        
        files = []
        for file_path in path.glob("*.parquet"):
            files.append(str(file_path))
        
        return files