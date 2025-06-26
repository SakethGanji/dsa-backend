"""Dataset slice event handlers.

This module contains event handlers for dataset-related events,
allowing the dataset slice to react to events from other slices.
"""

import logging
from typing import Optional

from app.core.events import Event, EventType, register_handler
from app.core.dependencies import get_dataset_repository

logger = logging.getLogger(__name__)


@register_handler(EventType.FILE_UPLOADED)
async def handle_file_uploaded(event: Event):
    """Update dataset metadata when a file is uploaded.
    
    This handler updates the dataset's file count and other metadata
    when a new file is associated with a dataset.
    
    Args:
        event: Event containing file_id and dataset_id
    """
    file_id = event.data.get("file_id")
    dataset_id = event.data.get("dataset_id")
    
    if dataset_id:
        try:
            # Get repository instance
            # Note: In production, this would be injected properly
            from app.datasets.repository import DatasetsRepository
            from app.db.connection import get_session
            
            async for db in get_session():
                repo = DatasetsRepository(db)
                await repo.update_file_count(dataset_id)
                logger.info(f"Updated file count for dataset {dataset_id} after file {file_id} upload")
                break
        except Exception as e:
            logger.error(f"Failed to update dataset {dataset_id} after file upload: {e}")


@register_handler(EventType.FILE_DEDUPLICATED)
async def handle_file_deduplicated(event: Event):
    """Log deduplication events for dataset statistics.
    
    This handler tracks when files are deduplicated to help maintain
    accurate dataset statistics and storage efficiency metrics.
    
    Args:
        event: Event containing deduplication details
    """
    original_file_id = event.data.get("original_file_id")
    duplicate_file_id = event.data.get("duplicate_file_id")
    dataset_id = event.data.get("dataset_id")
    saved_bytes = event.data.get("saved_bytes", 0)
    
    logger.info(
        f"File deduplication for dataset {dataset_id}: "
        f"duplicate {duplicate_file_id} -> original {original_file_id}, "
        f"saved {saved_bytes} bytes"
    )
    
    # In a production system, this could update deduplication statistics
    # or trigger analytics updates


@register_handler(EventType.SAMPLE_COMPLETED)
async def handle_sample_completed(event: Event):
    """Handle sampling job completion.
    
    This handler updates dataset metadata when a sampling job completes,
    potentially adding sample information to the dataset.
    
    Args:
        event: Event containing sampling job results
    """
    dataset_id = event.data.get("dataset_id")
    sample_id = event.data.get("sample_id")
    sample_size = event.data.get("sample_size")
    
    if dataset_id and sample_id:
        logger.info(
            f"Sample {sample_id} completed for dataset {dataset_id} "
            f"with {sample_size} samples"
        )
        
        # Could update dataset metadata with sampling information
        # or create links to sample datasets


@register_handler(EventType.VERSION_CREATED)
async def handle_version_created(event: Event):
    """Handle dataset version creation.
    
    This handler can trigger additional processes when a new version
    is created, such as schema validation or statistics computation.
    
    Args:
        event: Event containing version creation details
    """
    dataset_id = event.data.get("dataset_id")
    version_id = event.data.get("version_id")
    version_number = event.data.get("version_number")
    
    logger.info(
        f"New version {version_number} (id: {version_id}) "
        f"created for dataset {dataset_id}"
    )
    
    # Could trigger automatic statistics computation
    # or schema evolution checks