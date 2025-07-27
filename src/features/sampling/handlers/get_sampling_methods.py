"""Handler for getting available sampling methods."""

from typing import Dict, Any, List
from dataclasses import dataclass
from src.infrastructure.postgres.uow import PostgresUnitOfWork
from enum import Enum

# Sampling method enum
class SamplingMethod(Enum):
    """Supported sampling methods."""
    RANDOM = "random"
    STRATIFIED = "stratified"
    CLUSTER = "cluster"
    SYSTEMATIC = "systematic"
    LLM_BASED = "llm_based"
    MULTI_ROUND = "multi_round"
from src.core.services.sampling_service import SamplingService
from ...base_handler import BaseHandler
from src.core.permissions import PermissionService
from src.core.domain_exceptions import EntityNotFoundException
from ..models import GetSamplingMethodsCommand


class GetSamplingMethodsHandler(BaseHandler):
    """Handler for retrieving available sampling methods."""
    
    def __init__(self, uow: PostgresUnitOfWork, permissions: PermissionService):
        super().__init__(uow)
        self._permissions = permissions
    
    async def handle(self, command: GetSamplingMethodsCommand) -> Dict[str, Any]:
        """
        Get available sampling methods and their parameters.
        
        Returns:
            Dict with methods and supported operators
        """
        # Check permissions - read permission needed
        await self._permissions.require("dataset", command.dataset_id, command.user_id, "read")
        
        # Check dataset exists
        dataset = await self._uow.datasets.get_dataset_by_id(command.dataset_id)
        if not dataset:
            raise EntityNotFoundException("Dataset", command.dataset_id)
        
        # Get available methods
        sampling_service = SamplingService(self._uow)
        methods = sampling_service.list_available_methods()
        
        return {
            "methods": [
                {
                    "name": method.value,
                    "description": self._get_method_description(method),
                    "parameters": self._get_method_parameters(method)
                }
                for method in methods
            ],
            "supported_operators": [
                ">", ">=", "<", "<=", "=", "!=", "in", "not_in", 
                "like", "ilike", "is_null", "is_not_null"
            ]
        }
    
    def _get_method_description(self, method: SamplingMethod) -> str:
        """Get description for sampling method."""
        descriptions = {
            SamplingMethod.RANDOM: "Simple random sampling with optional seed for reproducibility",
            SamplingMethod.STRATIFIED: "Stratified sampling ensuring representation from all strata",
            SamplingMethod.SYSTEMATIC: "Systematic sampling with fixed intervals",
            SamplingMethod.CLUSTER: "Cluster sampling selecting entire groups",
            SamplingMethod.MULTI_ROUND: "Multiple sampling rounds with exclusion"
        }
        return descriptions.get(method, "")
    
    def _get_method_parameters(self, method: SamplingMethod) -> List[Dict[str, Any]]:
        """Get required and optional parameters for each method."""
        base_params = [
            {"name": "sample_size", "type": "integer", "required": True, "description": "Number of samples"},
            {"name": "seed", "type": "integer", "required": False, "description": "Random seed"}
        ]
        
        method_specific = {
            SamplingMethod.STRATIFIED: [
                {"name": "strata_columns", "type": "array", "required": True, "description": "Columns to stratify by"},
                {"name": "min_per_stratum", "type": "integer", "required": False, "description": "Minimum samples per stratum"},
                {"name": "proportional", "type": "boolean", "required": False, "description": "Use proportional allocation"}
            ],
            SamplingMethod.CLUSTER: [
                {"name": "cluster_column", "type": "string", "required": True, "description": "Column defining clusters"},
                {"name": "num_clusters", "type": "integer", "required": True, "description": "Number of clusters to select"},
                {"name": "samples_per_cluster", "type": "integer", "required": False, "description": "Samples per cluster"}
            ],
            SamplingMethod.SYSTEMATIC: [
                {"name": "interval", "type": "integer", "required": True, "description": "Sampling interval"},
                {"name": "start", "type": "integer", "required": False, "description": "Starting position"}
            ]
        }
        
        return base_params + method_specific.get(method, [])