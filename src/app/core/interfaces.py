"""Core interfaces for the application.

This module defines the core interfaces that enable clean architecture
and proper separation of concerns between vertical slices.
"""

from typing import Protocol, BinaryIO, Dict, Any, Optional

# Critical: FileId MUST be int to match database schema (files.id SERIAL PRIMARY KEY)
FileId = int


class IArtifactProducer(Protocol):
    """Single point of entry for creating deduplicated file artifacts.
    
    This interface handles the creation and registration of new, deduplicated file artifacts.
    It is the single point of entry for adding files to the system, ensuring:
    - Content-based deduplication through hashing
    - Memory-efficient streaming for large files
    - Transactional safety with race condition protection
    - Consistent file handling across all slices
    """
    
    async def create_artifact(
        self,
        content_stream: BinaryIO,
        file_type: str,
        mime_type: Optional[str] = None,
        compression_type: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> FileId:
        """Create or reference an existing file artifact.
        
        This method is designed to be both memory-efficient and concurrency-safe.
        It handles hashing, checks for duplicates, performs the physical storage write,
        and registers the file record in the database.
        
        Args:
            content_stream: A binary stream-like object (e.g., from an open file or io.BytesIO).
                           The stream will be read in chunks to support large files.
            file_type: The type of the file, e.g., 'parquet', 'csv', 'json'.
                      This corresponds to the files.file_type column.
            mime_type: The standard MIME type, e.g., 'application/vnd.apache.parquet'.
                      Optional but recommended for proper content type handling.
            compression_type: The compression used, e.g., 'snappy', 'gzip', 'brotli'.
                            This helps consumers understand how to decompress the file.
            metadata: An arbitrary JSON-serializable dictionary for additional context.
                     This can include source information, processing parameters, etc.
        
        Returns:
            The stable integer ID of the file record from the 'files' table.
            If the file already exists (based on content hash), returns the existing ID
            and increments the reference count.
        
        Raises:
            IOError: If the storage backend fails to write the file.
            ValueError: If the file_type is invalid or required parameters are missing.
        """
        ...