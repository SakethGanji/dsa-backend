"""
DEPRECATED: This module is deprecated and will be removed in a future version.
Please use LocalStorageBackend from app.storage.backend instead.

Migration guide:
- Replace LocalFileStorage with LocalStorageBackend
- Use StorageFactory.get_instance() to get a storage backend instance
- The new backend provides the same functionality with a cleaner interface
"""

import os
import uuid
import logging
from pathlib import Path
from typing import Optional, Tuple
import duckdb
import tempfile
from fastapi import UploadFile
import warnings

logger = logging.getLogger(__name__)

# Issue deprecation warning when module is imported
warnings.warn(
    "LocalFileStorage is deprecated. Use LocalStorageBackend from app.storage.backend instead.",
    DeprecationWarning,
    stacklevel=2
)

class LocalFileStorage:
    """Service for managing local file storage with Parquet conversion"""
    
    def __init__(self, base_path: str = "/home/saketh/Projects/dsa/data"):
        self.base_path = Path(base_path)
        self.ensure_directories()
    
    def ensure_directories(self):
        """Ensure required directories exist"""
        (self.base_path / "uploads").mkdir(parents=True, exist_ok=True)
        (self.base_path / "datasets").mkdir(parents=True, exist_ok=True)
        (self.base_path / "samples").mkdir(parents=True, exist_ok=True)
    
    async def save_dataset_file(
        self, 
        file: UploadFile, 
        dataset_id: int, 
        version_id: int,
        sheet_name: Optional[str] = None
    ) -> Tuple[str, int]:
        """
        Save uploaded file as Parquet format using DuckDB
        Returns: (file_path, file_size)
        """
        # Read file content
        content = await file.read()
        await file.seek(0)
        
        # Determine file type
        file_type = os.path.splitext(file.filename)[1].lower()[1:]
        
        # Create unique filename
        file_uuid = str(uuid.uuid4())
        if version_id == 0:
            # Temporary path before we know the version ID
            parquet_filename = f"{dataset_id}_temp_{file_uuid}.parquet"
            parquet_path = self.base_path / "datasets" / str(dataset_id) / "temp" / parquet_filename
        else:
            parquet_filename = f"{dataset_id}_{version_id}_{file_uuid}.parquet"
            parquet_path = self.base_path / "datasets" / str(dataset_id) / str(version_id) / parquet_filename
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Create temporary file for input
        with tempfile.NamedTemporaryFile(delete=False, suffix=f".{file_type}") as tmp_file:
            tmp_file.write(content)
            tmp_file_path = tmp_file.name
        
        try:
            # Use DuckDB to convert to Parquet
            conn = duckdb.connect(':memory:')
            
            if file_type == "csv":
                # Read CSV with lenient parsing options to handle edge cases
                conn.execute(f"""
                    COPY (SELECT * FROM read_csv_auto('{tmp_file_path}', 
                        ignore_errors=true,
                        quote='"',
                        escape='"',
                        header=true)) 
                    TO '{parquet_path}' (FORMAT PARQUET)
                """)
            
            elif file_type == "parquet":
                # Already in Parquet format, just copy
                with open(parquet_path, 'wb') as f:
                    f.write(content)
            
            else:
                # Try to read as CSV for other formats with lenient parsing
                try:
                    conn.execute(f"""
                        COPY (SELECT * FROM read_csv_auto('{tmp_file_path}', 
                            ignore_errors=true,
                            quote='"',
                            escape='"',
                            header=true)) 
                        TO '{parquet_path}' (FORMAT PARQUET)
                    """)
                except Exception as e:
                    logger.error(f"Failed to convert {file_type} to Parquet: {str(e)}")
                    raise ValueError(f"Unsupported file type: {file_type}")
            
            conn.close()
            file_size = os.path.getsize(parquet_path)
            return str(parquet_path), file_size
            
        finally:
            # Clean up temp file
            if os.path.exists(tmp_file_path):
                os.unlink(tmp_file_path)
    
    async def read_parquet_file(
        self, 
        file_path: str, 
        columns: Optional[list] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> list:
        """Read Parquet file with optional column selection and pagination using DuckDB"""
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
        finally:
            conn.close()
    
    async def save_sample_data_from_query(
        self,
        conn: duckdb.DuckDBPyConnection,
        query: str,
        dataset_id: int,
        version_id: int,
        job_id: str
    ) -> Tuple[str, int]:
        """Save sampled data directly from DuckDB query as Parquet file"""
        sample_filename = f"{job_id}.parquet"
        sample_path = self.base_path / "samples" / str(dataset_id) / str(version_id) / sample_filename
        sample_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Execute query and save directly as Parquet with compression for better storage
        # Using ZSTD compression for good balance of speed and compression ratio
        conn.execute(f"""
            COPY ({query}) 
            TO '{sample_path}' 
            (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
        """)
        
        file_size = os.path.getsize(sample_path)
        return str(sample_path), file_size
    
    def delete_file(self, file_path: str) -> bool:
        """Delete a file from local storage"""
        try:
            if os.path.exists(file_path):
                os.unlink(file_path)
                return True
            return False
        except Exception as e:
            logger.error(f"Error deleting file {file_path}: {str(e)}")
            return False
    
    def get_file_info(self, file_path: str) -> Optional[dict]:
        """Get information about a stored file"""
        if not os.path.exists(file_path):
            return None
        
        stats = os.stat(file_path)
        return {
            "path": file_path,
            "size": stats.st_size,
            "created": stats.st_ctime,
            "modified": stats.st_mtime
        }