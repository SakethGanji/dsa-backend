"""Handler for comprehensive table analysis using service abstraction."""

from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.infrastructure.services.table_analysis import TableAnalysisService
from src.api.models import TableAnalysisResponse
from ...base_handler import BaseHandler, with_error_handling
from fastapi import HTTPException


class GetTableAnalysisHandler(BaseHandler[TableAnalysisResponse]):
    """Handler for retrieving comprehensive table analysis."""
    
    def __init__(self, uow: PostgresUnitOfWork, table_analysis_service: TableAnalysisService):
        super().__init__(uow)
        self._table_analysis_service = table_analysis_service
    
    @with_error_handling
    async def handle(
        self,
        dataset_id: int,
        ref_name: str,
        table_key: str,
        user_id: int
    ) -> TableAnalysisResponse:
        """Get comprehensive table analysis using table analysis service."""
        async with self._uow:
            # Get the current commit for the ref
            ref = await self._uow.commits.get_ref(dataset_id, ref_name)
            if not ref:
                raise HTTPException(status_code=404, detail=f"Ref '{ref_name}' not found")
            
            if not ref['commit_id']:
                raise HTTPException(status_code=404, detail=f"No commit found for ref '{ref_name}'")
            
            commit_id = ref['commit_id']
            
            # Use table analysis service
            analysis = await self._table_analysis_service.analyze_table(
                commit_id=commit_id,
                table_key=table_key,
                sample_size=1000,
                compute_statistics=True,
                infer_types=True
            )
            
            # Get sample data for display
            sample_data = await self._table_analysis_service._table_reader.get_table_data(
                commit_id=commit_id,
                table_key=table_key,
                limit=10
            )
            
            # Convert service response to API response
            return TableAnalysisResponse(
                table_key=table_key,
                sheet_name=table_key,
                column_stats={stat.column_name: stat.__dict__ for stat in analysis.statistics},
                sample_data=sample_data,
                row_count=analysis.schema.row_count,
                null_counts={stat.column_name: stat.null_count for stat in analysis.statistics},
                unique_counts={stat.column_name: stat.unique_count for stat in analysis.statistics},
                data_types={stat.column_name: stat.data_type for stat in analysis.statistics},
                columns=[{"name": stat.column_name, "type": stat.data_type} for stat in analysis.statistics]
            )