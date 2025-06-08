"""Local file system storage backend implementation."""
import os
import uuid
import logging
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd
import duckdb

from .backend import StorageBackend, DatasetReader

logger = logging.getLogger(__name__)


class LocalDatasetReader:
    """Local file system dataset reader with enhanced functionality."""
    
    def __init__(self, file_path: str):
        self.file_path = file_path
    
    def to_pandas(self) -> pd.DataFrame:
        """Read dataset as pandas DataFrame."""
        return pd.read_parquet(self.file_path)
    
    def to_duckdb(self, conn: duckdb.DuckDBPyConnection, view_name: str = "main_data") -> None:
        """Create a DuckDB view from the dataset."""
        conn.execute(f"CREATE VIEW {view_name} AS SELECT * FROM read_parquet('{self.file_path}')")
    
    def get_path(self) -> str:
        """Get the path of the dataset."""
        return self.file_path
    
    def read_with_selection(
        self,
        columns: Optional[List[str]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Read dataset with column selection and pagination using DuckDB."""
        conn = duckdb.connect(':memory:')
        try:
            # Build column list
            col_list = ', '.join([f'"{col}"' for col in columns]) if columns else '*'
            
            # Build query with pagination
            query = f"SELECT {col_list} FROM read_parquet('{self.file_path}')"
            if limit:
                query += f" LIMIT {limit}"
                if offset:
                    query += f" OFFSET {offset}"
            
            result = conn.execute(query).fetchall()
            cols = [desc[0] for desc in conn.description]
            
            # Return as list of dicts
            return [dict(zip(cols, row)) for row in result]
        except Exception as e:
            logger.error(f"Error reading dataset file {self.file_path}: {str(e)}")
            raise ValueError(f"Failed to read dataset file: {str(e)}")
        finally:
            conn.close()


class LocalStorageBackend(StorageBackend):
    """Local file system storage backend."""
    
    def __init__(self, base_path: str = "/data"):
        """Initialize local storage backend.
        
        Args:
            base_path: Base directory for all storage operations
        """
        self.base_path = Path(base_path)
        self.datasets_dir = self.base_path / "datasets"
        self.samples_dir = self.base_path / "samples"
        self.ensure_directories()
    
    def ensure_directories(self) -> None:
        """Ensure required directories exist."""
        self.datasets_dir.mkdir(parents=True, exist_ok=True)
        self.samples_dir.mkdir(parents=True, exist_ok=True)
        (self.base_path / "uploads").mkdir(parents=True, exist_ok=True)
    
    def read_dataset(self, dataset_id: int, version_id: int, file_path: str) -> DatasetReader:
        """Read a dataset by its ID and version.
        
        Args:
            dataset_id: The dataset identifier
            version_id: The version identifier
            file_path: The file path from database
            
        Returns:
            LocalDatasetReader instance
        """
        # For now, we use the file_path directly from the database
        # In future, we could construct paths based on dataset_id/version_id
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Dataset file not found: {file_path}")
        
        return LocalDatasetReader(file_path)
    
    def save_sample(
        self, 
        conn: duckdb.DuckDBPyConnection,
        query: str,
        dataset_id: int,
        sample_id: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Save a sample from a DuckDB query.
        
        Args:
            conn: DuckDB connection with the data
            query: SQL query to execute for the sample
            dataset_id: The dataset identifier
            sample_id: Unique identifier for the sample
            metadata: Optional metadata to store with the sample
            
        Returns:
            Dictionary with sample information
        """
        # Create dataset-specific sample directory
        dataset_samples_dir = self.samples_dir / str(dataset_id)
        dataset_samples_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate sample file path
        sample_file = dataset_samples_dir / f"{sample_id}.parquet"
        
        # Execute query and save to parquet
        conn.execute(f"COPY ({query}) TO '{sample_file}' (FORMAT PARQUET)")
        
        # Get file info
        file_size = sample_file.stat().st_size
        
        return {
            "path": str(sample_file),
            "size": file_size,
            "format": "parquet",
            "metadata": metadata or {}
        }
    
    def list_samples(self, dataset_id: int) -> List[Dict[str, Any]]:
        """List all samples for a dataset.
        
        Args:
            dataset_id: The dataset identifier
            
        Returns:
            List of sample information dictionaries
        """
        dataset_samples_dir = self.samples_dir / str(dataset_id)
        if not dataset_samples_dir.exists():
            return []
        
        samples = []
        for sample_file in dataset_samples_dir.glob("*.parquet"):
            samples.append({
                "sample_id": sample_file.stem,
                "path": str(sample_file),
                "size": sample_file.stat().st_size,
                "format": "parquet"
            })
        
        return samples
    
    def delete_sample(self, dataset_id: int, sample_id: str) -> bool:
        """Delete a sample.
        
        Args:
            dataset_id: The dataset identifier
            sample_id: The sample identifier
            
        Returns:
            True if deleted successfully, False otherwise
        """
        sample_file = self.samples_dir / str(dataset_id) / f"{sample_id}.parquet"
        if sample_file.exists():
            sample_file.unlink()
            return True
        return False
    
    def get_sample_path(self, dataset_id: int, sample_id: str) -> str:
        """Get the path for a sample.
        
        Args:
            dataset_id: The dataset identifier
            sample_id: The sample identifier
            
        Returns:
            Path to the sample file
        """
        return str(self.samples_dir / str(dataset_id) / f"{sample_id}.parquet")
    
    def get_sample_save_path(self, dataset_id: int, version_id: int, job_id: str) -> str:
        """Get the path where a sample should be saved.
        
        Args:
            dataset_id: The dataset identifier
            version_id: The version identifier
            job_id: The sampling job identifier
            
        Returns:
            Path where the sample should be saved
        """
        # Create the directory structure if it doesn't exist
        sample_dir = self.samples_dir / str(dataset_id) / str(version_id)
        sample_dir.mkdir(parents=True, exist_ok=True)
        return str(sample_dir / f"{job_id}.parquet")
    
    async def save_dataset_file(
        self,
        file_content: bytes,
        dataset_id: int,
        version_id: int,
        file_name: str
    ) -> Dict[str, Any]:
        """Save a dataset file with automatic conversion to Parquet format.
        
        Args:
            file_content: The file content as bytes
            dataset_id: The dataset identifier
            version_id: The version identifier
            file_name: Original filename
            
        Returns:
            Dictionary with file information (path, size, etc.)
        """
        # Determine file type from extension
        file_type = os.path.splitext(file_name)[1].lower()[1:]
        
        # Create unique filename
        file_uuid = str(uuid.uuid4())
        if version_id == 0:
            # Temporary path before we know the version ID
            parquet_filename = f"{dataset_id}_temp_{file_uuid}.parquet"
            parquet_path = self.datasets_dir / str(dataset_id) / "temp" / parquet_filename
        else:
            parquet_filename = f"{dataset_id}_{version_id}_{file_uuid}.parquet"
            parquet_path = self.datasets_dir / str(dataset_id) / str(version_id) / parquet_filename
        
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Create temporary file for input
        with tempfile.NamedTemporaryFile(delete=False, suffix=f".{file_type}") as tmp_file:
            tmp_file.write(file_content)
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
                    f.write(file_content)
            
            elif file_type in ["xls", "xlsx", "xlsm"]:
                # Handle Excel files
                df = pd.read_excel(tmp_file_path)
                df.to_parquet(parquet_path)
            
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
            
            return {
                "path": str(parquet_path),
                "size": file_size,
                "format": "parquet"
            }
            
        finally:
            # Clean up temp file
            if os.path.exists(tmp_file_path):
                os.unlink(tmp_file_path)
    
    def get_file_metadata(self, file_path: str) -> Dict[str, Any]:
        """Get metadata about a Parquet file without loading the data.
        
        Args:
            file_path: Path to the Parquet file
            
        Returns:
            Dictionary with file metadata including columns, types, row count, etc.
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
    
    def get_dataset_path(self, dataset_id: int, version_id: int, filename: str) -> str:
        """Get the standard path for a dataset file.
        
        Args:
            dataset_id: The dataset identifier
            version_id: The version identifier
            filename: The filename
            
        Returns:
            Full path to the dataset file
        """
        return str(self.datasets_dir / str(dataset_id) / str(version_id) / filename)
    
    async def list_dataset_files(self, dataset_id: int, version_id: Optional[int] = None) -> List[str]:
        """List all files for a dataset or specific version.
        
        Args:
            dataset_id: The dataset identifier
            version_id: Optional version identifier
            
        Returns:
            List of file paths
        """
        if version_id:
            path = self.datasets_dir / str(dataset_id) / str(version_id)
        else:
            path = self.datasets_dir / str(dataset_id)
        
        if not path.exists():
            return []
        
        files = []
        for file_path in path.rglob("*.parquet"):
            files.append(str(file_path))
        
        return files
    
    async def list_sample_files(self, dataset_id: int, version_id: int) -> List[str]:
        """List all sample files for a dataset version.
        
        Args:
            dataset_id: The dataset identifier
            version_id: The version identifier
            
        Returns:
            List of sample file paths
        """
        path = self.samples_dir / str(dataset_id) / str(version_id)
        
        if not path.exists():
            return []
        
        files = []
        for file_path in path.glob("*.parquet"):
            files.append(str(file_path))
        
        return files
    
    async def file_exists(self, file_path: str) -> bool:
        """Check if a file exists.
        
        Args:
            file_path: Path to check
            
        Returns:
            True if file exists, False otherwise
        """
        return Path(file_path).exists()
    
    async def delete_file(self, file_path: str) -> bool:
        """Delete a file.
        
        Args:
            file_path: Path to the file to delete
            
        Returns:
            True if deleted successfully, False otherwise
        """
        try:
            if await self.file_exists(file_path):
                os.unlink(file_path)
                return True
            return False
        except Exception as e:
            logger.error(f"Error deleting file {file_path}: {str(e)}")
            return False