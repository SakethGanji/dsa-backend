"""Data download API endpoints."""

import io
import csv
from typing import Optional, Dict, Any
from fastapi import APIRouter, Depends, Query, Path, HTTPException
from fastapi.responses import StreamingResponse
import openpyxl
from openpyxl import Workbook

from ..models.pydantic_models import CurrentUser
from ..core.authorization import get_current_user_info, require_dataset_read
from ..core.dependencies import get_uow, get_db_pool
from ..core.abstractions import IUnitOfWork
from ..core.infrastructure.postgres.table_reader import PostgresTableReader
from ..core.database import DatabasePool


router = APIRouter(prefix="/datasets", tags=["downloads"])


# Dependency injection helpers (will be overridden in main.py)
def get_db_pool() -> DatabasePool:
    """Get database pool."""
    raise NotImplementedError("Database pool not configured")


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
    
    # Get dataset info
    dataset = await uow.datasets.get_dataset_by_id(dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    # Get ref
    ref = await uow.commits.get_ref(dataset_id, ref_name)
    if not ref:
        raise HTTPException(status_code=404, detail=f"Ref '{ref_name}' not found")
    
    commit_id = ref['commit_id']
    
    # Get all tables
    tables = await table_reader.get_table_keys(commit_id)
    
    if format == "excel":
        return await _export_excel(
            dataset_name=dataset['name'],
            commit_id=commit_id,
            tables=tables,
            table_reader=table_reader
        )
    elif format == "json":
        return await _export_json(
            dataset_name=dataset['name'],
            commit_id=commit_id,
            tables=tables,
            table_reader=table_reader
        )
    else:
        # CSV format - if multiple tables, export the first one
        if not tables:
            raise HTTPException(status_code=404, detail="No tables found in dataset")
        
        return await _export_csv(
            dataset_name=dataset['name'],
            commit_id=commit_id,
            table_key=tables[0],
            table_reader=table_reader
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
    
    # Get dataset info
    dataset = await uow.datasets.get_dataset_by_id(dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    # Get ref
    ref = await uow.commits.get_ref(dataset_id, ref_name)
    if not ref:
        raise HTTPException(status_code=404, detail=f"Ref '{ref_name}' not found")
    
    commit_id = ref['commit_id']
    
    # Parse columns
    column_list = columns.split(",") if columns else None
    
    if format == "json":
        return await _export_table_json(
            dataset_name=dataset['name'],
            commit_id=commit_id,
            table_key=table_key,
            table_reader=table_reader,
            columns=column_list
        )
    else:  # Default to CSV
        return await _export_csv(
            dataset_name=dataset['name'],
            commit_id=commit_id,
            table_key=table_key,
            table_reader=table_reader,
            columns=column_list
        )


async def _export_csv(
    dataset_name: str,
    commit_id: str,
    table_key: str,
    table_reader: PostgresTableReader,
    columns: Optional[list] = None
) -> StreamingResponse:
    """Export table data as CSV."""
    output = io.StringIO()
    
    # Get all data in batches
    all_data = []
    offset = 0
    batch_size = 1000
    
    while True:
        result = await table_reader.get_table_data(
            commit_id=commit_id,
            table_key=table_key,
            offset=offset,
            limit=batch_size
        )
        
        # Filter columns if specified
        if columns and result:
            filtered_result = []
            for row in result:
                filtered_row = {k: v for k, v in row.items() if k in columns or k == '_logical_row_id'}
                filtered_result.append(filtered_row)
            result = filtered_result
        
        all_data.extend(result)
        offset += batch_size
        
        if len(result) < batch_size:
            break
    
    # Write CSV
    if all_data:
        writer = csv.DictWriter(output, fieldnames=all_data[0].keys())
        writer.writeheader()
        writer.writerows(all_data)
    
    # Return streaming response
    output.seek(0)
    filename = f"{dataset_name}_{table_key}_{commit_id[:8]}.csv"
    
    return StreamingResponse(
        io.BytesIO(output.getvalue().encode()),
        media_type="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename={filename}"
        }
    )


async def _export_excel(
    dataset_name: str,
    commit_id: str,
    tables: list,
    table_reader: PostgresTableReader
) -> StreamingResponse:
    """Export all tables as Excel file with multiple sheets."""
    wb = Workbook()
    
    # Remove default sheet
    if 'Sheet' in wb.sheetnames:
        wb.remove(wb['Sheet'])
    
    for table_key in tables:
        # Create sheet for each table
        ws = wb.create_sheet(title=table_key[:31])  # Excel sheet name limit
        
        # Get all data for this table
        all_data = []
        offset = 0
        batch_size = 1000
        
        while True:
            result = await table_reader.get_table_data(
                commit_id=commit_id,
                table_key=table_key,
                offset=offset,
                limit=batch_size
            )
            
            all_data.extend(result)
            offset += batch_size
            
            if len(result) < batch_size:
                break
        
        # Write data to sheet
        if all_data:
            # Write headers
            headers = list(all_data[0].keys())
            ws.append(headers)
            
            # Write rows
            for row in all_data:
                ws.append([row.get(h) for h in headers])
    
    # Save to bytes buffer
    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    
    filename = f"{dataset_name}_{commit_id[:8]}.xlsx"
    
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename={filename}"
        }
    )


async def _export_json(
    dataset_name: str,
    commit_id: str,
    tables: list,
    table_reader: PostgresTableReader
) -> Dict[str, Any]:
    """Export all tables as JSON."""
    result = {
        "dataset_name": dataset_name,
        "commit_id": commit_id,
        "tables": {}
    }
    
    for table_key in tables:
        # Get all data for this table
        all_data = []
        offset = 0
        batch_size = 1000
        
        while True:
            table_result = await table_reader.get_table_data(
                commit_id=commit_id,
                table_key=table_key,
                offset=offset,
                limit=batch_size
            )
            
            all_data.extend(table_result)
            offset += batch_size
            
            if len(table_result) < batch_size:
                break
        
        # Get schema
        schema = await table_reader.get_table_schema(commit_id, table_key)
        
        result["tables"][table_key] = {
            "schema": schema,
            "row_count": len(all_data),
            "data": all_data
        }
    
    return result


async def _export_table_json(
    dataset_name: str,
    commit_id: str,
    table_key: str,
    table_reader: PostgresTableReader,
    columns: Optional[list] = None
) -> Dict[str, Any]:
    """Export single table as JSON."""
    # Get all data
    all_data = []
    offset = 0
    batch_size = 1000
    
    while True:
        result = await table_reader.get_table_data(
            commit_id=commit_id,
            table_key=table_key,
            offset=offset,
            limit=batch_size
        )
        
        # Filter columns if specified
        if columns and result:
            filtered_result = []
            for row in result:
                filtered_row = {k: v for k, v in row.items() if k in columns or k == '_logical_row_id'}
                filtered_result.append(filtered_row)
            result = filtered_result
        
        all_data.extend(result)
        offset += batch_size
        
        if len(result) < batch_size:
            break
    
    # Get schema
    schema = await table_reader.get_table_schema(commit_id, table_key)
    
    return {
        "dataset_name": dataset_name,
        "commit_id": commit_id,
        "table_key": table_key,
        "schema": schema,
        "row_count": len(all_data),
        "data": all_data
    }