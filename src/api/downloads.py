"""Data download API endpoints."""

import io
import csv
from typing import Optional, Dict, Any
from fastapi import APIRouter, Depends, Query, Path
from fastapi.responses import StreamingResponse
import openpyxl
from openpyxl import Workbook

from ..api.models import CurrentUser
from ..core.authorization import get_current_user_info, require_dataset_read
from ..core.exceptions import resource_not_found
from .dependencies import get_uow, get_db_pool
from ..core.abstractions import IUnitOfWork
from ..infrastructure.postgres.table_reader import PostgresTableReader
from ..infrastructure.postgres.database import DatabasePool


router = APIRouter(prefix="/datasets", tags=["downloads"])


# Local dependency helpers
async def get_table_reader(
    uow: IUnitOfWork = Depends(get_uow)
) -> PostgresTableReader:
    """Get table reader."""
    return PostgresTableReader(uow.connection)


@router.get("/{dataset_id}/refs/{ref_name}/download")
async def download_dataset(
    dataset_id: int = Path(..., description="Dataset ID"),
    ref_name: str = Path(..., description="Ref/branch name"),
    format: str = Query("csv", description="Export format (csv, excel, json)"),
    current_user: CurrentUser = Depends(get_current_user_info),
    uow: IUnitOfWork = Depends(get_uow),
    table_reader: PostgresTableReader = Depends(get_table_reader),
    _: CurrentUser = Depends(require_dataset_read)
):
    """Download entire dataset in specified format."""
    from ..features.downloads.handlers.download_dataset import DownloadDatasetHandler, DownloadDatasetCommand
    
    # Create command
    command = DownloadDatasetCommand(
        user_id=current_user.user_id,
        dataset_id=dataset_id,
        ref_name=ref_name,
        format=format
    )
    
    # Create handler and execute
    handler = DownloadDatasetHandler(uow, table_reader)
    result = await handler.handle(command)
    
    # Return streaming response
    return StreamingResponse(
        io.BytesIO(result["content"]),
        media_type=result["content_type"],
        headers={
            "Content-Disposition": f'attachment; filename="{result["filename"]}"'
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
    uow: IUnitOfWork = Depends(get_uow),
    table_reader: PostgresTableReader = Depends(get_table_reader),
    _: CurrentUser = Depends(require_dataset_read)
):
    """Download a specific table in specified format."""
    from ..features.downloads.handlers.download_table import DownloadTableHandler, DownloadTableCommand
    
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
    
    # Create handler and execute
    handler = DownloadTableHandler(uow, table_reader)
    result = await handler.handle(command)
    
    # Return streaming response
    return StreamingResponse(
        io.BytesIO(result["content"]),
        media_type=result["content_type"],
        headers={
            "Content-Disposition": f'attachment; filename="{result["filename"]}"'
        }
    )

