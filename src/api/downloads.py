"""Data download API endpoints - True streaming implementation."""

from typing import Optional, AsyncIterator, List
from fastapi import APIRouter, Depends, Query, Path, HTTPException, status
from fastapi.responses import StreamingResponse
import json
import io
import csv

from ..api.models import CurrentUser
from ..core.authorization import get_current_user_info, require_dataset_read
from .dependencies import get_uow
from ..infrastructure.postgres.uow import PostgresUnitOfWork
from ..core.domain_exceptions import EntityNotFoundException


router = APIRouter(prefix="/datasets", tags=["downloads"])


def _get_raw_connection_from_uow(uow: PostgresUnitOfWork):
    """Pragmatic helper to get the raw asyncpg pool.
    
    TODO: A cleaner long-term solution would be to add a method to 
    PostgresUnitOfWork like `async def get_streaming_connection()` 
    that returns a raw connection for streaming use cases.
    """
    # This is a bit of a hack, but necessary if the UoW hides the pool.
    return uow._pool._pool if hasattr(uow._pool, '_pool') else uow._pool


def _parse_db_row(db_row: dict) -> dict:
    """Parse potentially nested and stringified JSON from the database."""
    row_data = db_row['data']
    if isinstance(row_data, str):
        row_data = json.loads(row_data)
    
    # Standardized data is nested under a 'data' key
    return row_data.get('data', row_data) if isinstance(row_data, dict) else row_data


async def _get_schema_headers(
    conn,
    commit_id: str,
    table_key: Optional[str] = None
) -> Optional[List[str]]:
    """Fetch the canonical column headers from the commit schema.
    
    Returns:
        List of column names in order, or None if schema not found
    """
    schema_row = await conn.fetchval(
        "SELECT schema_definition FROM dsa_core.commit_schemas WHERE commit_id = $1",
        commit_id
    )
    
    if not schema_row:
        return None
        
    schema_def = json.loads(schema_row) if isinstance(schema_row, str) else schema_row
    
    # If looking for a specific table
    if table_key and table_key in schema_def:
        columns = schema_def[table_key].get('columns', [])
        return [col['name'] for col in columns]
    
    # For full dataset download, we need to combine all tables' columns
    # or use the first table if there's only one
    if not table_key:
        all_columns = []
        for table_data in schema_def.values():
            if isinstance(table_data, dict) and 'columns' in table_data:
                for col in table_data['columns']:
                    if col['name'] not in all_columns:
                        all_columns.append(col['name'])
        return all_columns if all_columns else None
    
    return None


async def _stream_csv_generator(
    conn,
    commit_id: str,
    table_key: Optional[str] = None
) -> AsyncIterator[bytes]:
    """
    CORRECTLY streams CSV data using a server-side cursor with stable headers from schema.
    
    Args:
        conn: Database connection
        commit_id: Commit to export from
        table_key: Optional table key to filter by
        
    Yields:
        CSV data chunks as bytes
    """
    # Build query based on whether we're filtering by table
    if table_key:
        # Handle both formats: "table_key:hash" and "table_key_hash"
        if ':' in table_key:
            pattern = f"{table_key}:%"
        else:
            pattern = f"{table_key}_%"
            
        query = """
            SELECT r.data, cr.logical_row_id
            FROM dsa_core.commit_rows cr
            JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
            WHERE cr.commit_id = $1 
            AND (cr.logical_row_id LIKE $2 OR r.data->>'sheet_name' = $3)
            ORDER BY cr.logical_row_id
        """
        # TODO: For better performance on large datasets, consider:
        # 1. Ensure logical_row_id always starts with sheet/table name
        # 2. Create a GIN index on r.data->>'sheet_name' if needed
        # 3. Then simplify to just: WHERE cr.logical_row_id LIKE $2
        params = [commit_id, pattern, table_key]
    else:
        # Get all data for the commit
        query = """
            SELECT r.data, cr.logical_row_id
            FROM dsa_core.commit_rows cr
            JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
            WHERE cr.commit_id = $1
            ORDER BY cr.logical_row_id
        """
        params = [commit_id]
    
    # First, fetch the schema headers before starting the cursor
    headers = await _get_schema_headers(conn, commit_id, table_key)
    
    if not headers:
        # Fallback: if no schema found, we'll use headers from first row
        # This handles legacy data or schema-less imports
        headers = None
        use_first_row_headers = True
    else:
        use_first_row_headers = False
    
    # THIS IS THE CRITICAL FIX: Use a transaction and a cursor
    async with conn.transaction():
        # Create cursor without prefetch - asyncpg will handle buffering
        cursor = conn.cursor(query, *params)
        
        output = io.StringIO()
        writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)
        headers_written = False
        
        async for db_row in cursor:
            actual_data = _parse_db_row(db_row)
            
            if not headers_written:
                if use_first_row_headers:
                    # Fallback: use keys from first row
                    headers = list(actual_data.keys())
                
                writer.writerow(headers)
                yield output.getvalue().encode('utf-8')
                output.seek(0)
                output.truncate(0)
                headers_written = True
            
            # Use the stable headers from schema (or first row fallback)
            writer.writerow([actual_data.get(h) for h in headers])
            yield output.getvalue().encode('utf-8')
            output.seek(0)
            output.truncate(0)
        
        if not headers_written:
            # If we have headers from schema but no data
            if headers:
                writer.writerow(headers)
                yield output.getvalue().encode('utf-8')
            else:
                yield b"No data found for this dataset/table\n"


async def _create_streaming_download_response(
    dataset: dict,
    ref: dict,
    uow: PostgresUnitOfWork,
    table_key: Optional[str] = None
) -> StreamingResponse:
    """Helper to create the streaming response, removing code duplication.
    
    Note: Compression should be handled by FastAPI's GZipMiddleware at the
    application level, which will automatically compress responses when the
    client sends 'Accept-Encoding: gzip' header.
    """
    
    async def generate_csv_stream():
        pool = _get_raw_connection_from_uow(uow)
        async with pool.acquire() as conn:
            async for chunk in _stream_csv_generator(conn, ref['commit_id'], table_key):
                yield chunk
    
    filename = f"{dataset['name']}.csv"
    if table_key:
        filename = f"{dataset['name']}_{table_key}.csv"
    
    return StreamingResponse(
        generate_csv_stream(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )


@router.get("/{dataset_id}/refs/{ref_name}/tables/{table_key}/download")
async def download_table(
    dataset_id: int = Path(..., description="Dataset ID"),
    ref_name: str = Path(..., description="Ref/branch name"),
    table_key: str = Path(..., description="Table key"),
    format: str = Query("csv", description="Export format (currently only csv supported)"),
    columns: Optional[str] = Query(None, description="Column selection not yet supported"),
    current_user: CurrentUser = Depends(get_current_user_info),
    uow: PostgresUnitOfWork = Depends(get_uow),
    _: CurrentUser = Depends(require_dataset_read)
):
    """Download specific table using true, memory-efficient streaming."""
    if format != "csv":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Format '{format}' not supported. Only 'csv' is currently available."
        )
    
    if columns:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Column selection not yet supported in streaming implementation"
        )
    
    # Get dataset and ref
    dataset = await uow.datasets.get_dataset_by_id(dataset_id)
    if not dataset:
        raise EntityNotFoundException("Dataset", dataset_id)
    
    ref = await uow.commits.get_ref(dataset_id, ref_name)
    if not ref:
        raise EntityNotFoundException("Ref", ref_name)
    
    return await _create_streaming_download_response(dataset, ref, uow, table_key)


@router.get("/{dataset_id}/refs/{ref_name}/download")
async def download_dataset(
    dataset_id: int = Path(..., description="Dataset ID"),
    ref_name: str = Path(..., description="Ref/branch name"),
    format: str = Query("csv", description="Export format (currently only csv supported)"),
    current_user: CurrentUser = Depends(get_current_user_info),
    uow: PostgresUnitOfWork = Depends(get_uow),
    _: CurrentUser = Depends(require_dataset_read)
):
    """Download entire dataset using true, memory-efficient streaming."""
    if format != "csv":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Format '{format}' not supported. Only 'csv' is currently available."
        )
    
    # Get dataset and ref
    dataset = await uow.datasets.get_dataset_by_id(dataset_id)
    if not dataset:
        raise EntityNotFoundException("Dataset", dataset_id)
    
    ref = await uow.commits.get_ref(dataset_id, ref_name)
    if not ref:
        raise EntityNotFoundException("Ref", ref_name)
    
    return await _create_streaming_download_response(dataset, ref, uow)

