"""Handler for getting exploration job results."""

from dataclasses import dataclass
from typing import Dict, Any
from uuid import UUID
import json

from src.features.base_handler import BaseHandler
from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.core.domain_exceptions import EntityNotFoundException, ValidationException, BusinessRuleViolation
from ..models import GetExplorationResultCommand


class GetExplorationResultHandler(BaseHandler[Dict[str, Any]]):
    """Handler for getting exploration results."""
    
    def __init__(self, uow: PostgresUnitOfWork):
        super().__init__(uow)
    
    async def handle(self, command: GetExplorationResultCommand) -> Dict[str, Any]:
        """Get exploration job result."""
        # Validate format
        if command.format not in ["html", "json", "info"]:
            raise ValidationException(f"Invalid format: {command.format}", field="format")
        
        async with self._uow:
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
            
            # Get exploration result
            output_summary = await self._uow.explorations.get_exploration_result(command.job_id)
            
            if not output_summary:
                raise EntityNotFoundException("Result", command.job_id)
            
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