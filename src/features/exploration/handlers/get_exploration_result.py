"""Handler for getting exploration job results."""

from dataclasses import dataclass
from typing import Dict, Any, Optional
from uuid import UUID
import json

from src.core.abstractions import IUnitOfWork
from src.infrastructure.postgres.database import DatabasePool
from src.core.domain_exceptions import EntityNotFoundException, ValidationException, BusinessRuleViolation


@dataclass
class GetExplorationResultCommand:
    """Command to get exploration result."""
    user_id: int
    job_id: UUID
    format: str = "html"  # html, json, info


class GetExplorationResultHandler:
    """Handler for getting exploration results."""
    
    def __init__(self, uow: IUnitOfWork, pool: DatabasePool):
        self._uow = uow
        self._pool = pool
    
    async def handle(self, command: GetExplorationResultCommand) -> Dict[str, Any]:
        """Get exploration job result."""
        # Validate format
        if command.format not in ["html", "json", "info"]:
            raise ValidationException(f"Invalid format: {command.format}", field="format")
        
        # Get job details
        job = await self._uow.jobs.get_job_by_id(command.job_id)
        if not job:
            raise EntityNotFoundException("Job", command.job_id)
        
        if job["run_type"] != "exploration":
            raise ValidationException("Not an exploration job", field="run_type")
        
        if job["status"] != "completed":
            raise BusinessRuleViolation(
                f"Job is {job['status']}, not completed", 
                rule="job_must_be_completed"
            )
        
        # Get result from output_summary
        async with self._pool.acquire() as conn:
            query = """
                SELECT output_summary
                FROM dsa_jobs.analysis_runs
                WHERE id = $1 AND run_type = 'exploration' AND status = 'completed'
            """
            
            row = await conn.fetchrow(query, command.job_id)
            
            if not row or not row["output_summary"]:
                raise EntityNotFoundException("Result", command.job_id)
            
            output_summary = json.loads(row["output_summary"]) if isinstance(row["output_summary"], str) else row["output_summary"]
            
            # Return appropriate response based on format
            if command.format == "html":
                return {
                    "content": output_summary.get("profile_html", ""),
                    "content_type": "text/html"
                }
            elif command.format == "json":
                return {
                    "content": json.loads(output_summary.get("profile_json", "{}")),
                    "content_type": "application/json"
                }
            else:  # info
                return {
                    "content": output_summary.get("dataset_info", {}),
                    "content_type": "application/json"
                }