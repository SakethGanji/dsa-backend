"""Data download API endpoints."""

import io
from typing import Optional
from fastapi import APIRouter, Depends, Query, Path
from fastapi.responses import StreamingResponse

from ..api.models import CurrentUser
from ..core.authorization import get_current_user_info, require_dataset_read
from .dependencies import get_uow
from ..infrastructure.postgres.uow import PostgresUnitOfWork
from ..infrastructure.postgres.table_reader import PostgresTableReader


router = APIRouter(prefix="/datasets", tags=["downloads"])


# Local dependency helpers
async def get_table_reader(
    uow: PostgresUnitOfWork = Depends(get_uow)
) -> PostgresTableReader:
    """Get table reader."""
    return uow.table_reader


@router.get("/{dataset_id}/refs/{ref_name}/download")
async def download_dataset(
    dataset_id: int = Path(..., description="Dataset ID"),
    ref_name: str = Path(..., description="Ref/branch name"),
    format: str = Query("csv", description="Export format (csv, excel, json)"),
    current_user: CurrentUser = Depends(get_current_user_info),
    uow: PostgresUnitOfWork = Depends(get_uow),
    table_reader: PostgresTableReader = Depends(get_table_reader),
    _: CurrentUser = Depends(require_dataset_read)
):
    """Download entire dataset in specified format."""
    from ..features.downloads.services import DownloadService
    from ..features.downloads.models import DownloadDatasetCommand
    
    # Create command
    command = DownloadDatasetCommand(
        user_id=current_user.user_id,
        dataset_id=dataset_id,
        ref_name=ref_name,
        format=format
    )
    
    # Create service and execute  
    from src.services.data_export_service import DataExportService
    export_service = DataExportService(table_reader)
    service = DownloadService(uow, table_reader, export_service)
    result = await service.download_dataset(command)
    
    # Return streaming response
    return StreamingResponse(
        io.BytesIO(result.content),
        media_type=result.content_type,
        headers={
            "Content-Disposition": f'attachment; filename="{result.filename}"'
        }
    )


@router.get("/{dataset_id}/refs/{ref_name}/tables/{table_key}/download")
async def download_table(
    dataset_id: int = Path(..., description="Dataset ID"),
    ref_name: str = Path(..., description="Ref/branch name"),
    table_key: str = Path(..., description="Table key"),
    format: str = Query("csv", description="Export format (csv, json)"),
    columns: Optional[str] = Query(None, description="Comma-separated column names"),
    current_user: CurrentUser = Depends(get_current_user_info),
    uow: PostgresUnitOfWork = Depends(get_uow),
    table_reader: PostgresTableReader = Depends(get_table_reader),
    _: CurrentUser = Depends(require_dataset_read)
):
    """Download a specific table in specified format."""
    from ..features.downloads.services import DownloadService
    from ..features.downloads.models import DownloadTableCommand
    
    # Parse columns
    column_list = [col.strip() for col in columns.split(",")] if columns else None
    
    # Create command
    command = DownloadTableCommand(
        user_id=current_user.user_id,
        dataset_id=dataset_id,
        ref_name=ref_name,
        table_key=table_key,
        format=format,
        columns=column_list
    )
    
    # Create service and execute
    service = DownloadService(uow, table_reader)
    result = await service.download_table(command)
    
    # Return streaming response
    return StreamingResponse(
        io.BytesIO(result.content),
        media_type=result.content_type,
        headers={
            "Content-Disposition": f'attachment; filename="{result.filename}"'
        }
    )

