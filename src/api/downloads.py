"""Data download API endpoints - Streaming implementation."""

from typing import Optional, AsyncIterator
from fastapi import APIRouter, Depends, Query, Path
from fastapi.responses import StreamingResponse

from ..api.models import CurrentUser
from ..core.authorization import get_current_user_info, require_dataset_read
from .dependencies import get_uow
from ..infrastructure.postgres.uow import PostgresUnitOfWork
from ..core.domain_exceptions import EntityNotFoundException


router = APIRouter(prefix="/datasets", tags=["downloads"])


async def stream_dataset_as_csv(
    conn,
    commit_id: str,
    table_key: Optional[str] = None
) -> AsyncIterator[bytes]:
    """
    Stream CSV data directly from database using server-side cursors.
    
    Args:
        conn: Database connection
        commit_id: Commit to export from
        table_key: Optional table key to filter by
        
    Yields:
        CSV data chunks as bytes
    """
    import io
    import csv
    
    # Get the raw connection if it's wrapped
    if hasattr(conn, '_conn'):
        conn = conn._conn
    elif hasattr(conn, 'raw_connection'):
        conn = conn.raw_connection
    
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
        params = (commit_id, pattern, table_key)
    else:
        # Get all data for the commit
        query = """
            SELECT r.data, cr.logical_row_id
            FROM dsa_core.commit_rows cr
            JOIN dsa_core.rows r ON cr.row_hash = r.row_hash
            WHERE cr.commit_id = $1
            ORDER BY cr.logical_row_id
        """
        params = (commit_id,)
    
    # Simple version without server-side cursors for debugging
    try:
        # Fetch all data (not ideal for large datasets, but let's test)
        rows = await conn.fetch(query, *params)
        
        if not rows:
            yield b"No data found for this dataset/table\n"
            return
            
        # Get first row to determine headers
        first_row = rows[0]
        
        # Extract data from the row
        row_data = first_row['data']
        
        # Handle different data structures
        if isinstance(row_data, str):
            # If data is a JSON string, parse it
            import json
            row_data = json.loads(row_data)
        
        if isinstance(row_data, dict) and 'data' in row_data:
            # Handle nested data structure
            actual_data = row_data['data']
        else:
            actual_data = row_data
        
        headers = list(actual_data.keys())
        
        # Setup CSV formatting with proper edge case handling
        output = io.StringIO()
        writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)
        
        # Write headers
        writer.writerow(headers)
        yield output.getvalue().encode('utf-8')
        output.seek(0)
        output.truncate(0)
        
        # Write all rows
        for row in rows:
            row_data = row['data']
            
            # Handle different data structures
            if isinstance(row_data, str):
                # If data is a JSON string, parse it
                row_data = json.loads(row_data)
            
            if isinstance(row_data, dict) and 'data' in row_data:
                actual_data = row_data['data']
            else:
                actual_data = row_data
                
            writer.writerow([actual_data.get(h) for h in headers])
            yield output.getvalue().encode('utf-8')
            output.seek(0)
            output.truncate(0)
            
    except Exception as e:
        yield f"Error during export: {str(e)}\n".encode('utf-8')


@router.get("/{dataset_id}/refs/{ref_name}/download")
async def download_dataset(
    dataset_id: int = Path(..., description="Dataset ID"),
    ref_name: str = Path(..., description="Ref/branch name"),
    format: str = Query("csv", description="Export format (currently only csv supported)"),
    current_user: CurrentUser = Depends(get_current_user_info),
    uow: PostgresUnitOfWork = Depends(get_uow),
    _: CurrentUser = Depends(require_dataset_read)
):
    """Download entire dataset using streaming (low memory usage)."""
    if format != "csv":
        raise ValueError(f"Format {format} not yet supported. Only CSV is currently available.")
    
    # Get dataset and ref
    dataset = await uow.datasets.get_dataset_by_id(dataset_id)
    if not dataset:
        raise EntityNotFoundException("Dataset", dataset_id)
    
    ref = await uow.commits.get_ref(dataset_id, ref_name)
    if not ref:
        raise EntityNotFoundException("Ref", ref_name)
    
    # Create streaming response
    async def generate():
        try:
            # Get a dedicated connection for streaming
            # We need to get the pool from somewhere accessible
            pool = uow._pool._pool if hasattr(uow._pool, '_pool') else uow._pool
            async with pool.acquire() as conn:
                async for chunk in stream_dataset_as_csv(conn, ref['commit_id']):
                    yield chunk
        except Exception as e:
            yield f"Error in generate: {str(e)}\n".encode('utf-8')
    
    # Return streaming response
    return StreamingResponse(
        generate(),
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{dataset["name"]}.csv"'
        }
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
    """Download specific table using streaming (low memory usage)."""
    if format != "csv":
        raise ValueError(f"Format {format} not yet supported. Only CSV is currently available.")
    
    if columns:
        raise ValueError("Column selection not yet supported in streaming implementation")
    
    # Get dataset and ref
    dataset = await uow.datasets.get_dataset_by_id(dataset_id)
    if not dataset:
        raise EntityNotFoundException("Dataset", dataset_id)
    
    ref = await uow.commits.get_ref(dataset_id, ref_name)
    if not ref:
        raise EntityNotFoundException("Ref", ref_name)
    
    # Create streaming response
    async def generate():
        try:
            # Get a dedicated connection for streaming
            # We need to get the pool from somewhere accessible
            pool = uow._pool._pool if hasattr(uow._pool, '_pool') else uow._pool
            async with pool.acquire() as conn:
                async for chunk in stream_dataset_as_csv(conn, ref['commit_id'], table_key):
                    yield chunk
        except Exception as e:
            yield f"Error in generate: {str(e)}\n".encode('utf-8')
    
    # Return streaming response
    return StreamingResponse(
        generate(),
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{dataset["name"]}_{table_key}.csv"'
        }
    )

