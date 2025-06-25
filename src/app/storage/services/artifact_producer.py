"""ArtifactProducer implementation for centralized file creation.

This module implements the IArtifactProducer interface, providing a single
point of entry for creating deduplicated file artifacts with streaming support
and transactional safety.
"""

import hashlib
import logging
import json
from typing import BinaryIO, Dict, Any, Optional
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError

from app.core.interfaces import IArtifactProducer, FileId
from app.core.exceptions import (
    ArtifactCreationError,
    StorageWriteError,
    InvalidFileTypeError,
    InvalidStreamError,
    RaceConditionError,
)
from app.storage.interfaces import IStorageBackend
from app.datasets.models import File  # Using Pydantic model for type hints

logger = logging.getLogger(__name__)


class ArtifactProducer(IArtifactProducer):
    """Production-ready artifact producer with streaming and transaction support.
    
    This implementation provides:
    - Stream-based content hashing without loading files into memory
    - Content-based deduplication
    - Database-first approach for race condition handling
    - Proper transaction rollback on failures
    - Reference counting for safe garbage collection
    """
    
    # Supported file types
    VALID_FILE_TYPES = {'parquet', 'csv', 'json', 'avro', 'orc'}
    
    # Chunk size for streaming (8KB)
    CHUNK_SIZE = 8192
    
    def __init__(self, db: AsyncSession, storage_backend: IStorageBackend):
        """Initialize the artifact producer.
        
        Args:
            db: SQLAlchemy database session.
            storage_backend: Storage backend for file operations.
        """
        self._db = db
        self._storage = storage_backend
    
    async def create_artifact(
        self,
        content_stream: BinaryIO,
        file_type: str,
        mime_type: Optional[str] = None,
        compression_type: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> FileId:
        """Create or reference an existing file artifact.
        
        Implements the IArtifactProducer interface with full production features.
        """
        # Validate inputs
        self._validate_inputs(content_stream, file_type)
        
        try:
            # Calculate content hash and size while streaming
            content_hash, content_size = await self._hash_stream(content_stream)
            
            # Check for existing file
            existing_file = await self._find_existing_file(content_hash)
            
            if existing_file:
                # Increment reference count for existing file
                return await self._increment_reference_count(existing_file)
            
            # Create new file with race condition protection
            return await self._create_new_file(
                content_stream=content_stream,
                content_hash=content_hash,
                content_size=content_size,
                file_type=file_type,
                mime_type=mime_type,
                compression_type=compression_type,
                metadata=metadata
            )
            
        except Exception as e:
            logger.error(f"Failed to create artifact: {str(e)}")
            raise ArtifactCreationError(f"Failed to create artifact: {str(e)}") from e
    
    def _validate_inputs(self, content_stream: BinaryIO, file_type: str) -> None:
        """Validate input parameters.
        
        Args:
            content_stream: The binary stream to validate.
            file_type: The file type to validate.
        
        Raises:
            InvalidStreamError: If the stream is invalid.
            InvalidFileTypeError: If the file type is not supported.
        """
        if not content_stream or not hasattr(content_stream, 'read'):
            raise InvalidStreamError("Invalid content stream provided")
        
        if file_type not in self.VALID_FILE_TYPES:
            raise InvalidFileTypeError(
                f"Invalid file type '{file_type}'. "
                f"Supported types: {', '.join(self.VALID_FILE_TYPES)}"
            )
    
    async def _hash_stream(self, content_stream: BinaryIO) -> tuple[str, int]:
        """Calculate SHA256 hash and size of stream content.
        
        This method reads the stream in chunks to support large files
        without loading them entirely into memory.
        
        Args:
            content_stream: The binary stream to hash.
        
        Returns:
            Tuple of (content_hash, content_size).
        """
        hasher = hashlib.sha256()
        content_size = 0
        
        # Read and hash in chunks
        while True:
            chunk = content_stream.read(self.CHUNK_SIZE)
            if not chunk:
                break
            hasher.update(chunk)
            content_size += len(chunk)
        
        # Reset stream position for subsequent reads
        content_stream.seek(0)
        
        return hasher.hexdigest(), content_size
    
    async def _find_existing_file(self, content_hash: str) -> Optional[Dict[str, Any]]:
        """Find an existing file by content hash.
        
        Args:
            content_hash: The SHA256 hash to search for.
        
        Returns:
            Dictionary with file data if found, None otherwise.
        """
        result = await self._db.execute(
            text("SELECT * FROM files WHERE content_hash = :hash"),
            {"hash": content_hash}
        )
        row = result.fetchone()
        
        if row:
            # Convert to dictionary
            return dict(row._mapping)
        return None
    
    async def _increment_reference_count(self, file: Dict[str, Any]) -> FileId:
        """Increment reference count for an existing file.
        
        Args:
            file: Dictionary containing existing file data.
        
        Returns:
            The file ID.
        """
        await self._db.execute(
            text("UPDATE files SET reference_count = reference_count + 1 WHERE id = :id"),
            {"id": file["id"]}
        )
        await self._db.commit()
        
        new_count = file["reference_count"] + 1
        logger.info(
            f"Incremented reference count for existing file {file['id']} "
            f"(hash: {file['content_hash']}, new count: {new_count})"
        )
        return file["id"]
    
    async def _create_new_file(
        self,
        content_stream: BinaryIO,
        content_hash: str,
        content_size: int,
        file_type: str,
        mime_type: Optional[str],
        compression_type: Optional[str],
        metadata: Optional[Dict[str, Any]]
    ) -> FileId:
        """Create a new file with race condition protection.
        
        Uses a database-first approach where we insert the DB record first,
        then upload to storage. If storage fails, we rollback the DB record.
        """
        storage_path = f"artifacts/{content_hash}"
        
        # First, try to insert the database record
        try:
            result = await self._db.execute(
                text("""
                    INSERT INTO files (
                        storage_type, file_type, mime_type, file_path, 
                        file_size, content_hash, reference_count, 
                        compression_type, metadata
                    ) VALUES (
                        :storage_type, :file_type, :mime_type, :file_path,
                        :file_size, :content_hash, :reference_count,
                        :compression_type, :metadata
                    ) RETURNING id
                """),
                {
                    "storage_type": "local",  # TODO: Get from storage backend
                    "file_type": file_type,
                    "mime_type": mime_type,
                    "file_path": storage_path,
                    "file_size": content_size,
                    "content_hash": content_hash,
                    "reference_count": 1,
                    "compression_type": compression_type,
                    "metadata": json.dumps(metadata or {})
                }
            )
            await self._db.commit()
            new_file_id = result.scalar()
            logger.info(f"Created new file record {new_file_id} (hash: {content_hash})")
            
        except IntegrityError:
            # Lost the race - another process created the file
            await self._db.rollback()
            logger.info(f"Race condition detected for hash {content_hash}, using existing file")
            
            # Find the file that was created by the other process
            conflicting_file = await self._find_existing_file(content_hash)
            if not conflicting_file:
                raise RuntimeError(f"Failed to find conflicting file with hash {content_hash}")
            
            # Increment its reference count
            return await self._increment_reference_count(conflicting_file)
        
        # Now upload to storage
        try:
            await self._storage.write_stream(storage_path, content_stream)
            logger.info(f"Successfully uploaded file to storage: {storage_path}")
            
        except Exception as e:
            # Storage failed - rollback the database record
            logger.error(f"Storage upload failed for {content_hash}: {str(e)}")
            await self._db.execute(
                text("DELETE FROM files WHERE id = :id"),
                {"id": new_file_id}
            )
            await self._db.commit()
            raise StorageWriteError(f"Failed to upload file to storage: {str(e)}") from e
        
        return new_file_id