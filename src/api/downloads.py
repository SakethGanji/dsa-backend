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
from ..features.file_conversion.services.file_conversion_service import FileConversionService
from ..features.file_conversion.models.file_format import FileFormat, ConversionOptions
from ..core.domain_exceptions import EntityNotFoundException


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
    # Map string format to FileFormat enum
    format_map = {
        "csv": FileFormat.CSV,
        "excel": FileFormat.EXCEL,
        "json": FileFormat.JSON,
        "parquet": FileFormat.PARQUET
    }
    
    file_format = format_map.get(format)
    if not file_format:
        raise ValueError(f"Unsupported format: {format}")
    
    # Get dataset and ref
    dataset = await uow.datasets.get_dataset_by_id(dataset_id)
    if not dataset:
        raise EntityNotFoundException("Dataset", dataset_id)
    
    ref = await uow.commits.get_ref(dataset_id, ref_name)
    if not ref:
        raise EntityNotFoundException("Ref", ref_name)
    
    # Create service and export
    service = FileConversionService(uow, table_reader)
    result = await service.export_data(
        dataset_id=dataset_id,
        commit_id=ref['commit_id'],
        table_name="primary",
        format=file_format
    )
    
    # Read file content
    with open(result.file_path, 'rb') as f:
        content = f.read()
    
    # Return streaming response
    return StreamingResponse(
        io.BytesIO(content),
        media_type=result.content_type,
        headers={
            "Content-Disposition": f'attachment; filename="{dataset["name"]}.{format}"'
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
    # Map string format to FileFormat enum
    format_map = {
        "csv": FileFormat.CSV,
        "json": FileFormat.JSON
    }
    
    file_format = format_map.get(format)
    if not file_format:
        raise ValueError(f"Unsupported format for table download: {format}")
    
    # Parse columns
    column_list = [col.strip() for col in columns.split(",")] if columns else None
    
    # Get dataset and ref
    dataset = await uow.datasets.get_dataset_by_id(dataset_id)
    if not dataset:
        raise EntityNotFoundException("Dataset", dataset_id)
    
    ref = await uow.commits.get_ref(dataset_id, ref_name)
    if not ref:
        raise EntityNotFoundException("Ref", ref_name)
    
    # Create service and export
    service = FileConversionService(uow, table_reader)
    options = ConversionOptions(columns=column_list)
    
    result = await service.export_data(
        dataset_id=dataset_id,
        commit_id=ref['commit_id'],
        table_name=table_key,
        format=file_format,
        options=options
    )
    
    # Read file content
    with open(result.file_path, 'rb') as f:
        content = f.read()
    
    # Return streaming response
    return StreamingResponse(
        io.BytesIO(content),
        media_type=result.content_type,
        headers={
            "Content-Disposition": f'attachment; filename="{dataset["name"]}_{table_key}.{format}"'
        }
    )

