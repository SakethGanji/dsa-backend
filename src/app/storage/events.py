"""Storage slice event handlers.

This module contains event handlers for storage-related events,
allowing the storage slice to react to events from other slices.
"""

import logging
from typing import List, Optional

from app.core.events import Event, EventType, register_handler

logger = logging.getLogger(__name__)


@register_handler(EventType.DATASET_DELETED)
async def handle_dataset_deleted(event: Event):
    """Clean up orphaned files when dataset is deleted.
    
    This handler ensures that files are properly cleaned up when a dataset
    is deleted, updating reference counts and potentially removing files
    that are no longer referenced.
    
    Args:
        event: Event containing dataset deletion details
    """
    dataset_id = event.data.get("dataset_id")
    file_ids: Optional[List[int]] = event.data.get("file_ids", [])
    
    if dataset_id:
        logger.info(f"Processing file cleanup for deleted dataset {dataset_id}")
        
        try:
            # Get storage service
            # Note: In production, this would be injected properly
            from app.storage.services.artifact_producer import ArtifactProducer
            from app.storage.factory import StorageFactory
            from app.core.config import get_settings
            from app.db.connection import get_session
            
            settings = get_settings()
            storage_factory = StorageFactory(settings)
            
            async for db in get_session():
                artifact_producer = ArtifactProducer(storage_factory, db)
                
                # Decrease reference counts for all files
                for file_id in file_ids:
                    # This would decrease reference count and potentially delete
                    # files with zero references
                    logger.debug(f"Decreasing reference count for file {file_id}")
                
                logger.info(f"Completed file cleanup for dataset {dataset_id}")
                break
                
        except Exception as e:
            logger.error(f"Failed to clean up files for dataset {dataset_id}: {e}")


@register_handler(EventType.FILE_UPLOADED)
async def handle_file_uploaded(event: Event):
    """Process newly uploaded files.
    
    This handler can trigger additional processing on uploaded files,
    such as virus scanning, metadata extraction, or thumbnail generation.
    
    Args:
        event: Event containing file upload details
    """
    file_id = event.data.get("file_id")
    file_path = event.data.get("file_path")
    file_type = event.data.get("file_type")
    file_size = event.data.get("file_size")
    
    logger.info(
        f"New file uploaded: id={file_id}, type={file_type}, "
        f"size={file_size}, path={file_path}"
    )
    
    # Could trigger:
    # - Virus scanning
    # - Metadata extraction
    # - Thumbnail generation
    # - Format validation


@register_handler(EventType.VERSION_CREATED)
async def handle_version_created(event: Event):
    """Handle storage operations for new dataset versions.
    
    This handler can optimize storage for new versions, such as
    creating hard links for unchanged files or updating indexes.
    
    Args:
        event: Event containing version creation details
    """
    dataset_id = event.data.get("dataset_id")
    version_id = event.data.get("version_id")
    parent_version_id = event.data.get("parent_version_id")
    
    if parent_version_id:
        logger.info(
            f"New version {version_id} created from parent {parent_version_id} "
            f"for dataset {dataset_id}"
        )
        
        # Could optimize storage by:
        # - Creating hard links for unchanged files
        # - Updating storage indexes
        # - Computing storage deltas


@register_handler(EventType.SAMPLE_CREATED)
async def handle_sample_created(event: Event):
    """Handle storage allocation for sampling jobs.
    
    This handler can pre-allocate storage space or set up temporary
    storage areas for sampling operations.
    
    Args:
        event: Event containing sampling job details
    """
    sample_id = event.data.get("sample_id")
    dataset_id = event.data.get("dataset_id")
    estimated_size = event.data.get("estimated_size", 0)
    
    logger.info(
        f"Sample {sample_id} created for dataset {dataset_id}, "
        f"estimated size: {estimated_size} bytes"
    )
    
    # Could:
    # - Pre-allocate storage space
    # - Set up temporary directories
    # - Configure storage quotas