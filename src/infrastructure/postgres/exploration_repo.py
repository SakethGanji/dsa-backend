"""PostgreSQL implementation of IExplorationRepository."""

from typing import Optional, List, Dict, Any
from uuid import UUID
import json
from datetime import datetime

from src.core.abstractions.repositories import IExplorationRepository
from .base_repository import BasePostgresRepository


class PostgresExplorationRepository(BasePostgresRepository[UUID], IExplorationRepository):
    """PostgreSQL implementation of exploration repository."""
    
    def __init__(self, connection):
        """Initialize with analysis_runs table."""
        super().__init__(
            connection=connection,
            table_name="dsa_jobs.analysis_runs",
            id_column="id",
            id_type=UUID
        )
    
    async def save_exploration(
        self,
        dataset_id: int,
        user_id: int,
        query: str,
        ref_name: str,
        commit_id: str,
        results_summary: Dict[str, Any]
    ) -> UUID:
        """Save an exploration query and its results summary."""
        query_sql = """
            INSERT INTO dsa_jobs.analysis_runs (
                dataset_id, user_id, run_type, status,
                run_parameters, output_summary, created_at
            )
            VALUES ($1, $2, 'exploration', 'completed', $3, $4, $5)
            RETURNING id
        """
        
        run_parameters = {
            "query": query,
            "ref_name": ref_name,
            "commit_id": commit_id
        }
        
        result = await self._conn.fetchval(
            query_sql,
            dataset_id,
            user_id,
            json.dumps(run_parameters),
            json.dumps(results_summary),
            datetime.utcnow()
        )
        
        return result
    
    async def get_exploration_by_id(self, exploration_id: UUID) -> Optional[Dict[str, Any]]:
        """Get exploration details by ID."""
        query = """
            SELECT 
                ar.id,
                ar.dataset_id,
                ar.user_id,
                ar.status,
                ar.created_at,
                ar.completed_at,
                ar.run_parameters,
                ar.output_summary,
                ar.error_message,
                d.name as dataset_name,
                u.soeid as username
            FROM dsa_jobs.analysis_runs ar
            JOIN dsa_core.datasets d ON ar.dataset_id = d.id
            JOIN dsa_auth.users u ON ar.user_id = u.id
            WHERE ar.id = $1 AND ar.run_type = 'exploration'
        """
        
        row = await self._conn.fetchrow(query, exploration_id)
        if not row:
            return None
        
        return {
            "id": row["id"],
            "dataset_id": row["dataset_id"],
            "dataset_name": row["dataset_name"],
            "user_id": row["user_id"],
            "username": row["username"],
            "status": row["status"],
            "created_at": row["created_at"],
            "completed_at": row["completed_at"],
            "run_parameters": json.loads(row["run_parameters"]) if isinstance(row["run_parameters"], str) else row["run_parameters"],
            "output_summary": json.loads(row["output_summary"]) if isinstance(row["output_summary"], str) else row["output_summary"],
            "error_message": row["error_message"]
        }
    
    async def get_exploration_history(
        self,
        dataset_id: int,
        user_id: Optional[int] = None,
        limit: int = 50,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Get exploration history for a dataset."""
        query = """
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
            AND ar.dataset_id = $1
        """
        
        params = [dataset_id]
        param_count = 1
        
        if user_id is not None:
            param_count += 1
            query += f" AND ar.user_id = ${param_count}"
            params.append(user_id)
        
        query += " ORDER BY ar.created_at DESC"
        
        param_count += 1
        query += f" OFFSET ${param_count}"
        params.append(offset)
        
        param_count += 1
        query += f" LIMIT ${param_count}"
        params.append(limit)
        
        rows = await self._conn.fetch(query, *params)
        
        return [
            {
                "job_id": str(row["job_id"]),
                "dataset_id": row["dataset_id"],
                "dataset_name": row["dataset_name"],
                "user_id": row["user_id"],
                "username": row["username"],
                "status": row["status"],
                "created_at": row["created_at"].isoformat(),
                "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
                "run_parameters": json.loads(row["run_parameters"]) if isinstance(row["run_parameters"], str) else row["run_parameters"] or {},
                "has_result": bool(row["output_summary"])
            }
            for row in rows
        ]
    
    async def get_exploration_result(self, exploration_id: UUID) -> Optional[Dict[str, Any]]:
        """Get the full results of an exploration."""
        query = """
            SELECT output_summary
            FROM dsa_jobs.analysis_runs
            WHERE id = $1 AND run_type = 'exploration' AND status = 'completed'
        """
        
        row = await self._conn.fetchrow(query, exploration_id)
        
        if not row or not row["output_summary"]:
            return None
        
        output_summary = json.loads(row["output_summary"]) if isinstance(row["output_summary"], str) else row["output_summary"]
        
        return output_summary
    
    async def delete_old_explorations(self, days_to_keep: int = 30) -> int:
        """Delete explorations older than specified days."""
        query = """
            DELETE FROM dsa_jobs.analysis_runs
            WHERE run_type = 'exploration'
            AND created_at < NOW() - INTERVAL '%s days'
            RETURNING id
        """
        
        result = await self._conn.fetch(query, days_to_keep)
        return len(result)
    
    async def count_explorations(self, dataset_id: Optional[int] = None, user_id: Optional[int] = None) -> int:
        """Count explorations with optional filters."""
        query = """
            SELECT COUNT(*) 
            FROM dsa_jobs.analysis_runs
            WHERE run_type = 'exploration'
        """
        
        params = []
        param_count = 0
        
        if dataset_id is not None:
            param_count += 1
            query += f" AND dataset_id = ${param_count}"
            params.append(dataset_id)
        
        if user_id is not None:
            param_count += 1
            query += f" AND user_id = ${param_count}"
            params.append(user_id)
        
        return await self._conn.fetchval(query, *params)