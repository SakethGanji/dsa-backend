"""In-memory storage backend using Polars for dataset storage"""
import os
import io
import logging
import polars as pl
from typing import Dict, Any, Optional, List, Tuple, BinaryIO
from datetime import datetime
import json
import tempfile

from app.storage.backend import StorageBackend
from app.storage.interfaces import IStorageBackend

logger = logging.getLogger(__name__)


class InMemoryStorageBackend(StorageBackend, IStorageBackend):
    """In-memory storage backend using Polars DataFrames"""
    
    def __init__(self):
        self._datasets: Dict[str, pl.DataFrame] = {}
        self._metadata: Dict[str, Dict[str, Any]] = {}
        self._temp_files: Dict[str, bytes] = {}
        logger.info("Initialized in-memory storage backend with Polars")
    
    async def upload_file(self, file: BinaryIO, key: str, metadata: Optional[Dict[str, Any]] = None) -> str:
        """Upload a file to memory"""
        content = file.read()
        self._temp_files[key] = content
        
        if metadata:
            self._metadata[key] = metadata
        
        logger.info(f"Uploaded file to memory: {key}")
        return key
    
    async def download_file(self, key: str) -> bytes:
        """Download a file from memory"""
        if key in self._temp_files:
            return self._temp_files[key]
        
        # Check if it's a dataset stored as DataFrame
        if key in self._datasets:
            # Convert DataFrame to Parquet bytes
            buffer = io.BytesIO()
            self._datasets[key].write_parquet(buffer)
            buffer.seek(0)
            return buffer.read()
        
        raise FileNotFoundError(f"File not found: {key}")
    
    async def delete_file(self, key: str) -> bool:
        """Delete a file from memory"""
        deleted = False
        
        if key in self._temp_files:
            del self._temp_files[key]
            deleted = True
        
        if key in self._datasets:
            del self._datasets[key]
            deleted = True
        
        if key in self._metadata:
            del self._metadata[key]
        
        return deleted
    
    async def file_exists(self, key: str) -> bool:
        """Check if a file exists in memory"""
        return key in self._temp_files or key in self._datasets
    
    async def get_file_metadata(self, key: str) -> Dict[str, Any]:
        """Get metadata for a file"""
        metadata = self._metadata.get(key, {})
        
        if key in self._datasets:
            df = self._datasets[key]
            metadata.update({
                "rows": df.height,
                "columns": df.width,
                "columns_names": df.columns,
                "dtypes": {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)},
                "size_bytes": df.estimated_size()
            })
        elif key in self._temp_files:
            metadata["size_bytes"] = len(self._temp_files[key])
        
        return metadata
    
    async def list_files(self, prefix: str = "") -> List[str]:
        """List all files in memory"""
        all_keys = list(self._temp_files.keys()) + list(self._datasets.keys())
        if prefix:
            return [k for k in all_keys if k.startswith(prefix)]
        return all_keys
    
    async def save_dataset_file(
        self,
        file_content: bytes,
        dataset_id: int,
        version_id: int,
        file_name: str
    ) -> Dict[str, Any]:
        """Save a dataset file to memory as a Polars DataFrame"""
        # Determine file type from extension
        file_type = os.path.splitext(file_name)[1].lower()[1:]
        
        # Create memory key
        if version_id == 0:
            # Temporary key before we know the version ID
            key = f"memory://datasets/{dataset_id}/temp/{file_name}"
        else:
            key = f"memory://datasets/{dataset_id}/{version_id}/{file_name}"
        
        try:
            # Load data into Polars DataFrame based on file type
            if file_type == "csv":
                df = pl.read_csv(io.BytesIO(file_content))
            elif file_type == "parquet":
                df = pl.read_parquet(io.BytesIO(file_content))
            elif file_type in ["xlsx", "xls", "xlsm"]:
                # Use pandas to read Excel, then convert to Polars
                import pandas as pd
                pd_df = pd.read_excel(io.BytesIO(file_content))
                df = pl.from_pandas(pd_df)
            else:
                # Try to read as CSV for other formats
                try:
                    df = pl.read_csv(io.BytesIO(file_content))
                except Exception as e:
                    logger.error(f"Failed to load {file_type} into Polars: {str(e)}")
                    raise ValueError(f"Unsupported file type: {file_type}")
            
            # Store in memory
            self._datasets[key] = df
            
            # Calculate size (estimated)
            file_size = df.estimated_size()
            
            # Store metadata
            self._metadata[key] = {
                "dataset_id": dataset_id,
                "version_id": version_id,
                "original_filename": file_name,
                "file_type": file_type,
                "created_at": datetime.utcnow().isoformat(),
                "rows": df.height,
                "columns": df.width
            }
            
            logger.info(f"Stored dataset in memory: {key} ({df.height} rows, {df.width} columns)")
            
            return {
                "path": key,
                "size": file_size,
                "format": "polars_dataframe"
            }
            
        except Exception as e:
            logger.error(f"Error loading dataset into memory: {str(e)}")
            raise
    
    def get_file_metadata(self, file_path: str) -> Dict[str, Any]:
        """Get metadata about a dataset in memory"""
        if file_path in self._datasets:
            df = self._datasets[file_path]
            
            # Get column info
            columns = []
            for col, dtype in zip(df.columns, df.dtypes):
                columns.append({
                    "name": col,
                    "type": str(dtype),
                    "nullable": df[col].null_count() > 0
                })
            
            return {
                "row_count": df.height,
                "column_count": df.width,
                "columns": columns,
                "size_bytes": df.estimated_size(),
                "format": "polars_dataframe"
            }
        
        return {}
    
    async def read_dataset_sample(
        self,
        file_path: str,
        num_rows: int = 1000
    ) -> pl.DataFrame:
        """Read a sample of the dataset directly from memory"""
        if file_path in self._datasets:
            df = self._datasets[file_path]
            return df.head(num_rows)
        
        raise FileNotFoundError(f"Dataset not found in memory: {file_path}")
    
    async def read_dataset_paginated(
        self,
        file_path: str,
        limit: int,
        offset: int
    ) -> Tuple[List[str], List[Dict[str, Any]], bool]:
        """Read paginated data from in-memory dataset"""
        if file_path not in self._datasets:
            raise FileNotFoundError(f"Dataset not found in memory: {file_path}")
        
        df = self._datasets[file_path]
        
        # Get headers
        headers = df.columns
        
        # Get total row count
        total_rows = df.height
        
        # Get paginated data
        end_idx = min(offset + limit, total_rows)
        slice_df = df.slice(offset, end_idx - offset)
        
        # Convert to list of dicts
        rows = slice_df.to_dicts()
        
        # Check if there are more rows
        has_more = end_idx < total_rows
        
        return headers, rows, has_more
    
    async def finalize_file_location(
        self,
        temp_key: str,
        version_id: int
    ) -> str:
        """Move dataset from temp location to final location with version ID"""
        if temp_key not in self._datasets:
            return temp_key
        
        # Extract dataset_id from temp key
        parts = temp_key.split("/")
        if len(parts) >= 4 and parts[3] == "temp":
            dataset_id = parts[2]
            filename = parts[-1]
            
            # Create new key with version ID
            new_key = f"memory://datasets/{dataset_id}/{version_id}/{filename}"
            
            # Move the DataFrame
            self._datasets[new_key] = self._datasets.pop(temp_key)
            
            # Update metadata
            if temp_key in self._metadata:
                self._metadata[new_key] = self._metadata.pop(temp_key)
                self._metadata[new_key]["version_id"] = version_id
            
            logger.info(f"Moved dataset from {temp_key} to {new_key}")
            return new_key
        
        return temp_key
    
    def get_dataframe(self, key: str) -> Optional[pl.DataFrame]:
        """Get the Polars DataFrame directly (for advanced operations)"""
        return self._datasets.get(key)
    
    def clear_all(self):
        """Clear all data from memory (useful for testing)"""
        self._datasets.clear()
        self._metadata.clear()
        self._temp_files.clear()
        logger.info("Cleared all data from in-memory storage")
    
    # Sample-related methods (required by abstract base class)
    
    def save_sample(
        self, 
        conn: Any,  # duckdb.DuckDBPyConnection
        query: str,
        dataset_id: int,
        sample_id: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Save a sample from a DuckDB query"""
        # Execute query and convert to Polars
        result = conn.execute(query).fetchall()
        columns = [desc[0] for desc in conn.description]
        
        # Create Polars DataFrame
        df = pl.DataFrame({col: [row[i] for row in result] for i, col in enumerate(columns)})
        
        # Store in memory
        key = f"memory://samples/{dataset_id}/{sample_id}.parquet"
        self._datasets[key] = df
        
        if metadata:
            self._metadata[key] = metadata
        
        return {
            "path": key,
            "size": df.estimated_size(),
            "format": "polars_dataframe"
        }
    
    def list_samples(self, dataset_id: int) -> List[Dict[str, Any]]:
        """List all samples for a dataset"""
        prefix = f"memory://samples/{dataset_id}/"
        samples = []
        
        for key in self._datasets:
            if key.startswith(prefix):
                sample_id = key[len(prefix):].replace('.parquet', '')
                samples.append({
                    "sample_id": sample_id,
                    "path": key,
                    "metadata": self._metadata.get(key, {})
                })
        
        return samples
    
    def delete_sample(self, dataset_id: int, sample_id: str) -> bool:
        """Delete a sample"""
        key = f"memory://samples/{dataset_id}/{sample_id}.parquet"
        
        if key in self._datasets:
            del self._datasets[key]
            if key in self._metadata:
                del self._metadata[key]
            return True
        
        return False
    
    def get_sample_path(self, dataset_id: int, sample_id: str) -> str:
        """Get the path/URI for a sample"""
        return f"memory://samples/{dataset_id}/{sample_id}.parquet"
    
    def get_sample_save_path(self, dataset_id: int, version_id: int, job_id: str) -> str:
        """Get the path where a sample should be saved"""
        return f"memory://samples/{dataset_id}/{version_id}/{job_id}.parquet"
    
    def get_multi_round_sample_path(
        self, 
        dataset_id: int, 
        version_id: int, 
        job_id: str, 
        round_number: int
    ) -> str:
        """Get the path where a multi-round sample should be saved"""
        return f"memory://samples/{dataset_id}/{version_id}/{job_id}/round_{round_number}.parquet"
    
    def get_multi_round_residual_path(
        self,
        dataset_id: int,
        version_id: int,
        job_id: str
    ) -> str:
        """Get the path where multi-round residuals should be saved"""
        return f"memory://samples/{dataset_id}/{version_id}/{job_id}/residual.parquet"
    
    async def read_dataset(
        self,
        file_path: str,
        limit: Optional[int] = None,
        offset: int = 0
    ) -> pl.DataFrame:
        """Read a dataset from memory"""
        if file_path in self._datasets:
            df = self._datasets[file_path]
            
            if limit is not None:
                end_idx = min(offset + limit, df.height)
                return df.slice(offset, end_idx - offset)
            else:
                return df.slice(offset)
        
        raise FileNotFoundError(f"Dataset not found in memory: {file_path}")
    
    def ensure_directories(self, *paths: str) -> None:
        """No-op for in-memory storage - directories don't need to be created"""
        pass
    
    # IStorageBackend interface implementation
    
    async def write_stream(self, path: str, stream: BinaryIO) -> None:
        """Write content from stream to memory"""
        content = stream.read()
        self._temp_files[path] = content
        logger.info(f"Wrote {len(content)} bytes to memory path: {path}")
    
    async def read_stream(self, path: str) -> io.BytesIO:
        """Read content as stream from memory"""
        if path in self._temp_files:
            return io.BytesIO(self._temp_files[path])
        
        # Try to read from datasets
        if path in self._datasets:
            buffer = io.BytesIO()
            self._datasets[path].write_parquet(buffer)
            buffer.seek(0)
            return buffer
        
        raise FileNotFoundError(f"File not found in memory: {path}")
    
    async def exists(self, path: str) -> bool:
        """Check if path exists in memory"""
        return path in self._temp_files or path in self._datasets
    
    async def delete(self, path: str) -> None:
        """Delete from memory"""
        if path in self._temp_files:
            del self._temp_files[path]
        if path in self._datasets:
            del self._datasets[path]
        if path in self._metadata:
            del self._metadata[path]
    
    async def list_dir(self, path: str) -> List[str]:
        """List all entries under a path prefix"""
        entries = []
        prefix = path if path.endswith("/") else path + "/"
        
        # Check temp files
        for key in self._temp_files:
            if key.startswith(prefix):
                relative = key[len(prefix):]
                if "/" not in relative:  # Direct child
                    entries.append(relative)
        
        # Check datasets
        for key in self._datasets:
            if key.startswith(prefix):
                relative = key[len(prefix):]
                if "/" not in relative:  # Direct child
                    entries.append(relative)
        
        return sorted(list(set(entries)))
    
    async def get_size(self, path: str) -> int:
        """Get size of file in memory"""
        if path in self._temp_files:
            return len(self._temp_files[path])
        
        if path in self._datasets:
            return self._datasets[path].estimated_size()
        
        raise FileNotFoundError(f"File not found in memory: {path}")
    
    async def get_file_info(self, path: str) -> Dict[str, Any]:
        """Get file metadata"""
        info = {
            "path": path,
            "exists": await self.exists(path)
        }
        
        if path in self._temp_files:
            info.update({
                "size": len(self._temp_files[path]),
                "type": "file",
                "format": "bytes"
            })
        elif path in self._datasets:
            df = self._datasets[path]
            info.update({
                "size": df.estimated_size(),
                "type": "dataset",
                "format": "polars_dataframe",
                "rows": df.height,
                "columns": df.width
            })
        
        if path in self._metadata:
            info["metadata"] = self._metadata[path]
        
        return info