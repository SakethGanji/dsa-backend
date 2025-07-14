"""Handler for getting exploration history."""

from dataclasses import dataclass
from typing import List, Dict, Any, Optional
import json

from src.infrastructure.postgres.database import DatabasePool


@dataclass
class GetExplorationHistoryCommand:
    """Command to get exploration history."""
    dataset_id: Optional[int] = None
    user_id: Optional[int] = None
    requesting_user_id: Optional[int] = None  # The user making the request
    offset: int = 0
    limit: int = 20


@dataclass 
class ExplorationHistoryItem:
    """Single item in exploration history."""
    job_id: str
    dataset_id: int
    dataset_name: str
    user_id: int
    username: str
    status: str
    created_at: str
    updated_at: Optional[str]
    run_parameters: Dict[str, Any]
    has_result: bool


@dataclass
class ExplorationHistoryResponse:
    """Response for exploration history."""
    items: List[ExplorationHistoryItem]
    total: int
    offset: int
    limit: int


class GetExplorationHistoryHandler:
    """Handler for getting exploration history."""
    
    def __init__(self, pool: DatabasePool):
        self._pool = pool
    
    async def handle(self, command: GetExplorationHistoryCommand) -> ExplorationHistoryResponse:
        """Get exploration history."""
        # Build query based on filters
        base_query = """
            SELECT 
                ar.id as job_id,
                ar.dataset_id,
                ar.user_id,
                ar.status,
                ar.created_at,
                ar.completed_at as updated_at,
                ar.run_parameters,
                ar.output_summary,
                d.name as dataset_name,
                u.soeid as username
            FROM dsa_jobs.analysis_runs ar
            JOIN dsa_core.datasets d ON ar.dataset_id = d.id
            JOIN dsa_auth.users u ON ar.user_id = u.id
            WHERE ar.run_type = 'exploration'
        """
        
        count_query = """
            SELECT COUNT(*) 
            FROM dsa_jobs.analysis_runs ar
            WHERE ar.run_type = 'exploration'
        """
        
        params = []
        param_count = 0
        
        # Add filters
        if command.dataset_id:
            param_count += 1
            base_query += f" AND ar.dataset_id = ${param_count}"
            count_query += f" AND ar.dataset_id = ${param_count}"
            params.append(command.dataset_id)
        
        if command.user_id:
            param_count += 1
            base_query += f" AND ar.user_id = ${param_count}"
            count_query += f" AND ar.user_id = ${param_count}"
            params.append(command.user_id)
        
        # Add ordering and pagination
        base_query += " ORDER BY ar.created_at DESC"
        param_count += 1
        base_query += f" OFFSET ${param_count}"
        params.append(command.offset)
        
        param_count += 1
        base_query += f" LIMIT ${param_count}"
        params.append(command.limit)
        
        # Get data
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(base_query, *params)
            
            items = [
                ExplorationHistoryItem(
                    job_id=str(row["job_id"]),
                    dataset_id=row["dataset_id"],
                    dataset_name=row["dataset_name"],
                    user_id=row["user_id"],
                    username=row["username"],
                    status=row["status"],
                    created_at=row["created_at"].isoformat(),
                    updated_at=row["updated_at"].isoformat() if row["updated_at"] else None,
                    run_parameters=json.loads(row["run_parameters"]) if isinstance(row["run_parameters"], str) else row["run_parameters"] or {},
                    has_result=bool(row["output_summary"])
                )
                for row in rows
            ]
        
        # Get total count (without offset/limit params)
        count_params = params[:len(params)-2] if params else []
        async with self._pool.acquire() as conn:
            total = await conn.fetchval(count_query, *count_params)
        
        return ExplorationHistoryResponse(
            items=items,
            total=total,
            offset=command.offset,
            limit=command.limit
        )