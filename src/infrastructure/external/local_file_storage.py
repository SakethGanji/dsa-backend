"""Local file system storage implementation."""

import os
import json
from pathlib import Path
from typing import Optional, Dict, Any
import aiofiles
from src.core.abstractions.external import IFileStorage


class LocalFileStorage(IFileStorage):
    """Local file system implementation of IFileStorage."""
    
    def __init__(self, base_path: str):
        self._base_path = Path(base_path)
        self._base_path.mkdir(parents=True, exist_ok=True)
        self._metadata_suffix = ".metadata.json"
    
    def _get_file_path(self, file_id: str) -> Path:
        """Get the full path for a file ID."""
        # Use first 2 chars as subdirectory for better file system performance
        if len(file_id) >= 2:
            subdir = file_id[:2]
            return self._base_path / subdir / file_id
        return self._base_path / file_id
    
    def _get_metadata_path(self, file_id: str) -> Path:
        """Get the metadata file path for a file ID."""
        file_path = self._get_file_path(file_id)
        return file_path.parent / f"{file_path.name}{self._metadata_suffix}"
    
    async def store(self, file_path: str, content: bytes, metadata: Optional[Dict[str, Any]] = None) -> str:
        """Store a file and return its identifier."""
        # Generate file ID from path
        file_id = file_path.replace('/', '_').replace('\\', '_')
        
        # Create full path
        full_path = self._get_file_path(file_id)
        full_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Write file content
        async with aiofiles.open(full_path, 'wb') as f:
            await f.write(content)
        
        # Write metadata if provided
        if metadata:
            metadata_path = self._get_metadata_path(file_id)
            async with aiofiles.open(metadata_path, 'w') as f:
                await f.write(json.dumps(metadata))
        
        return file_id
    
    async def retrieve(self, file_id: str) -> bytes:
        """Retrieve a file by its identifier."""
        file_path = self._get_file_path(file_id)
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_id}")
        
        async with aiofiles.open(file_path, 'rb') as f:
            return await f.read()
    
    async def delete(self, file_id: str) -> None:
        """Delete a file by its identifier."""
        file_path = self._get_file_path(file_id)
        metadata_path = self._get_metadata_path(file_id)
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_id}")
        
        # Delete file
        file_path.unlink()
        
        # Delete metadata if exists
        if metadata_path.exists():
            metadata_path.unlink()
        
        # Try to remove empty parent directory
        try:
            file_path.parent.rmdir()
        except OSError:
            pass  # Directory not empty
    
    async def exists(self, file_id: str) -> bool:
        """Check if a file exists."""
        file_path = self._get_file_path(file_id)
        return file_path.exists()
    
    async def get_metadata(self, file_id: str) -> Dict[str, Any]:
        """Get metadata for a file."""
        if not await self.exists(file_id):
            raise FileNotFoundError(f"File not found: {file_id}")
        
        metadata_path = self._get_metadata_path(file_id)
        
        if not metadata_path.exists():
            return {}
        
        async with aiofiles.open(metadata_path, 'r') as f:
            content = await f.read()
            return json.loads(content)


class S3FileStorage(IFileStorage):
    """
    S3 implementation of IFileStorage.
    
    This is a placeholder for S3 storage implementation.
    Would require boto3 or aioboto3 for actual implementation.
    """
    
    def __init__(self, bucket: str, region: str, access_key: str, secret_key: str):
        self._bucket = bucket
        self._region = region
        # Initialize S3 client here
        raise NotImplementedError("S3 storage not yet implemented")
    
    async def store(self, file_path: str, content: bytes, metadata: Optional[Dict[str, Any]] = None) -> str:
        raise NotImplementedError()
    
    async def retrieve(self, file_id: str) -> bytes:
        raise NotImplementedError()
    
    async def delete(self, file_id: str) -> None:
        raise NotImplementedError()
    
    async def exists(self, file_id: str) -> bool:
        raise NotImplementedError()
    
    async def get_metadata(self, file_id: str) -> Dict[str, Any]:
        raise NotImplementedError()