"""Consolidated service for all versioning operations including refs."""

import json
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

from src.infrastructure.postgres.uow import PostgresUnitOfWork
from src.core.permissions import PermissionService
from src.core.events.publisher import EventBus
from src.core.domain_exceptions import EntityNotFoundException, ValidationException, BusinessRuleViolation
from src.features.versioning.services.commit_preparation_service import CommitPreparationService
from src.features.table_analysis.services.table_analysis import TableAnalysisService, DataTypeInferenceService, ColumnStatisticsService
from ...base_handler import with_transaction, with_error_handling
from src.core.common.pagination import PaginationMixin
from src.api.models import (
    CreateCommitRequest, CreateCommitResponse,
    GetDataRequest, GetDataResponse,
    CreateBranchRequest, CreateBranchResponse,
    ListRefsResponse, RefInfo,
    CommitSchemaResponse, TableAnalysisResponse,
    DatasetOverviewResponse, QueueImportResponse,
    GetCommitHistoryResponse, CommitInfo
)


@dataclass
class DeleteBranchResponse:
    """Standardized delete response."""
    entity_type: str = "Branch"
    entity_id: str = None
    success: bool = True
    message: str = None
    
    def __post_init__(self):
        if self.entity_id and not self.message:
            self.message = f"{self.entity_type} '{self.entity_id}' deleted successfully"


class VersioningService(PaginationMixin):
    """Consolidated service for all versioning operations including refs."""
    
    def __init__(
        self,
        uow: PostgresUnitOfWork,
        permissions: PermissionService,
        commit_service: Optional[CommitPreparationService] = None,
        table_analysis_service: Optional[TableAnalysisService] = None,
        event_bus: Optional[EventBus] = None
    ):
        self._uow = uow
        self._permissions = permissions
        self._commit_service = commit_service
        self._table_analysis_service = table_analysis_service
        self._event_bus = event_bus
        self._commit_repo = uow.commits
        self._table_reader = uow.table_reader if hasattr(uow, 'table_reader') else None
    
    # ========== Commit Operations ==========
    
    @with_error_handling
    @with_transaction
    async def create_commit(
        self,
        dataset_id: int,
        ref_name: str,
        request: CreateCommitRequest,
        user_id: int
    ) -> CreateCommitResponse:
        """Create a new commit with provided data."""
        # Check write permission
        await self._permissions.require("dataset", dataset_id, user_id, "write")
        
        # Get current commit
        current = await self._uow.commits.get_current_commit_for_ref(dataset_id, ref_name)
        
        # Prepare commit data
        commit_data = await self._commit_service.prepare_commit_data(
            dataset_id=dataset_id,
            parent_commit_id=request.parent_commit_id or current,
            changes={request.table_name or 'primary': {'data': request.data}},
            message=request.message,
            author=str(user_id)
        )
        
        # Create commit
        commit_id = await self._uow.commits.create_commit_and_manifest(
            dataset_id=dataset_id,
            parent_commit_id=commit_data.parent_commit_id,
            message=commit_data.message,
            author_id=user_id,
            manifest=[(f"{t}:{i}", h) for t, hs in commit_data.row_hashes.items() 
                      for i, h in enumerate(hs)]
        )
        
        # Store row hashes and schemas
        for table, hashes in commit_data.row_hashes.items():
            await self._uow.commits.add_rows_if_not_exist([(h, h) for h in hashes])
        await self._uow.commits.create_commit_schema(commit_id, commit_data.schemas)
        
        # Update ref atomically
        if not await self._uow.commits.update_ref_atomically(
            dataset_id, ref_name, commit_id, current
        ):
            raise ValueError("Concurrent modification detected. Please retry.")
        
        # Refresh search and publish event
        await self._uow.search_repository.refresh_search_index()
        if self._event_bus:
            from src.core.events.publisher import CommitCreatedEvent
            await self._event_bus.publish(CommitCreatedEvent.from_commit(
                commit_id, dataset_id, request.message, user_id, 
                commit_data.parent_commit_id
            ))
        
        # Return response
        commit = await self._uow.commits.get_commit_by_id(commit_id)
        return CreateCommitResponse(
            commit_id=commit_id,
            message=request.message,
            created_at=commit['created_at'] if commit else None
        )
    
    @with_error_handling
    async def get_commit_history(
        self,
        dataset_id: int,
        ref_name: str,
        user_id: int,
        offset: int = 0,
        limit: int = 20
    ) -> GetCommitHistoryResponse:
        """Get commit history for a dataset."""
        # Check read permission
        await self._permissions.require("dataset", dataset_id, user_id, "read")
        
        # Get current commit for ref
        current_commit = await self._uow.commits.get_current_commit_for_ref(dataset_id, ref_name)
        if not current_commit:
            raise EntityNotFoundException("Ref", ref_name)
        
        # Get commit history
        commits = await self._uow.commits.get_commit_history(
            dataset_id, 
            ref_name,
            offset=offset,
            limit=limit
        )
        total = len(commits)  # For now, total is the number of commits returned
        
        # Convert to response model
        commit_infos = []
        for i, commit in enumerate(commits):
            commit_info = CommitInfo(
                commit_id=commit['commit_id'],
                message=commit['message'],
                author_id=commit['author_id'],
                author_soeid=commit.get('author_soeid', 'unknown'),
                created_at=commit['created_at'],
                parent_commit_id=commit.get('parent_commit_id'),
                table_count=commit.get('table_count', 0),
                is_head=(i == 0)  # First commit in the list is the head
            )
            commit_infos.append(commit_info)
        
        return GetCommitHistoryResponse(
            dataset_id=dataset_id,
            ref_name=ref_name,
            commits=commit_infos,
            total=total,
            offset=offset,
            limit=limit
        )
    
    @with_error_handling
    async def get_commit_schema(
        self,
        dataset_id: int,
        commit_id: str,
        user_id: int
    ) -> CommitSchemaResponse:
        """Get schema for a specific commit."""
        # Check read permission
        await self._permissions.require("dataset", dataset_id, user_id, "read")
        
        # Verify commit belongs to dataset
        commit = await self._uow.commits.get_commit_by_id(commit_id)
        if not commit or commit.get('dataset_id') != dataset_id:
            raise EntityNotFoundException("Commit", commit_id)
        
        # Get schema
        schema = await self._uow.commits.get_commit_schema(commit_id)
        
        # If no schema in commit_schemas, try to build from table analysis
        if not schema:
            # Get all tables for this commit
            if self._table_reader:
                table_keys = await self._table_reader.list_table_keys(commit_id)
                if table_keys:
                    schema = {}
                    for table_key in table_keys:
                        table_schema = await self._table_reader.get_table_schema(commit_id, table_key)
                        if table_schema:
                            schema[table_key] = table_schema
                
        if not schema:
            raise EntityNotFoundException("Schema", commit_id)
        
        # Convert schema to sheets format
        from src.api.models import SheetSchema, ColumnSchema
        sheets = []
        
        if isinstance(schema, dict):
            for table_key, table_schema in schema.items():
                if isinstance(table_schema, dict):
                    columns = []
                    # Handle different schema formats
                    if 'columns' in table_schema:
                        # New format with columns array
                        for col in table_schema['columns']:
                            column = ColumnSchema(
                                name=col.get('name', ''),
                                type=col.get('data_type', col.get('type', 'string')),
                                nullable=col.get('nullable', True)
                            )
                            columns.append(column)
                    elif 'fields' in table_schema:
                        # Legacy format with fields array
                        for field in table_schema['fields']:
                            column = ColumnSchema(
                                name=field.get('name', ''),
                                type=field.get('type', 'string'),
                                nullable=True
                            )
                            columns.append(column)
                    
                    sheet = SheetSchema(
                        sheet_name=table_key,
                        columns=columns,
                        row_count=table_schema.get('row_count', 0)
                    )
                    sheets.append(sheet)
        
        return CommitSchemaResponse(
            commit_id=commit_id,
            sheets=sheets
        )
    
    @with_error_handling
    async def checkout_commit(
        self,
        dataset_id: int,
        commit_id: str,
        user_id: int,
        table_key: str = "primary",
        offset: int = 0,
        limit: int = 100
    ) -> GetDataResponse:
        """Checkout data at a specific commit."""
        # Check read permission
        await self._permissions.require("dataset", dataset_id, user_id, "read")
        
        # Verify commit belongs to dataset
        commit = await self._uow.commits.get_commit_by_id(commit_id)
        if not commit or commit.get('dataset_id') != dataset_id:
            raise EntityNotFoundException("Commit", commit_id)
        
        # Use table reader to get data
        if not self._table_reader:
            raise ValueError("Table reader not available")
            
        # Get data from the table
        raw_rows = await self._table_reader.get_table_data(
            commit_id=commit_id,
            table_key=table_key,
            offset=offset,
            limit=limit
        )
        
        # Convert to DataRow objects
        from src.api.models import DataRow
        rows = []
        for i, row in enumerate(raw_rows):
            # Extract logical_row_id if present
            logical_row_id = row.get('_logical_row_id') or row.get('logical_row_id') or f"{table_key}:{i}"
            # Remove system columns
            clean_row = {k: v for k, v in row.items() 
                        if not k.startswith('_') and k != 'logical_row_id'}
            data_row = DataRow(
                sheet_name=table_key,
                logical_row_id=logical_row_id,
                data=clean_row
            )
            rows.append(data_row)
        
        # Get total count
        total_count = await self._table_reader.count_table_rows(commit_id, table_key)
        
        # Get schema
        schema = None
        if self._table_reader:
            schema = await self._table_reader.get_table_schema(commit_id, table_key)
        
        # For checkout, we don't have a ref_name, use "checkout" as placeholder
        return GetDataResponse(
            dataset_id=dataset_id,
            ref_name="checkout",
            commit_id=commit_id,
            rows=rows,
            total_rows=total_count,
            offset=offset,
            limit=limit
        )
    
    # ========== Ref Operations (formerly in refs feature) ==========
    
    @with_transaction
    @with_error_handling
    async def create_branch(
        self,
        dataset_id: int,
        request: CreateBranchRequest,
        user_id: int
    ) -> CreateBranchResponse:
        """Create a new branch from an existing ref."""
        # Check write permission
        await self._permissions.require("dataset", dataset_id, user_id, "write")
        
        # Use the commit_id from the request directly
        commit_id = request.commit_id.strip()
        
        # Verify the commit exists
        commit_exists = await self._uow.commits.get_commit_by_id(commit_id)
        if not commit_exists:
            raise EntityNotFoundException("Commit", commit_id)
        
        # Verify the commit belongs to this dataset
        if commit_exists.get('dataset_id') != dataset_id:
            raise ValidationException("Commit does not belong to this dataset")
        
        # Create the new ref pointing to the specified commit
        await self._uow.commits.create_ref(
            dataset_id=dataset_id,
            ref_name=request.ref_name,
            commit_id=commit_id
        )
        
        # Get commit details for timestamp
        commit = await self._uow.commits.get_commit_by_id(commit_id)
        
        return CreateBranchResponse(
            dataset_id=dataset_id,
            ref_name=request.ref_name,
            commit_id=commit_id,
            created_at=commit.get('created_at') if commit else None
        )
    
    @with_transaction
    @with_error_handling
    async def delete_branch(
        self,
        dataset_id: int,
        ref_name: str,
        user_id: int
    ) -> DeleteBranchResponse:
        """Delete a branch/ref."""
        # Check write permission
        await self._permissions.require("dataset", dataset_id, user_id, "write")
        
        # Get default branch
        default_branch = await self._uow.commits.get_default_branch(dataset_id)
        
        # Prevent deletion of default branch
        if ref_name == default_branch:
            raise BusinessRuleViolation(
                f"Cannot delete the default branch '{default_branch}'",
                rule="protect_default_branch"
            )
        
        # Delete the ref
        deleted = await self._uow.commits.delete_ref(dataset_id, ref_name)
        
        if not deleted:
            raise EntityNotFoundException("Branch", ref_name)
        
        # Publish event
        if self._event_bus:
            # Define event inline since handlers are removed
            from src.core.events.publisher import DomainEvent
            from dataclasses import dataclass
            
            @dataclass  
            class BranchDeletedEvent(DomainEvent):
                """Event raised when a branch is deleted."""
                dataset_id: int
                branch_name: str
                deleted_by: int
                
                def __post_init__(self):
                    super().__init__()
            
            await self._event_bus.publish(BranchDeletedEvent(
                dataset_id=dataset_id,
                branch_name=ref_name,
                deleted_by=user_id
            ))
        
        # Return standardized response
        return DeleteBranchResponse(
            entity_type="Branch",
            entity_id=ref_name,
            message=f"Branch '{ref_name}' deleted successfully from dataset {dataset_id}"
        )
    
    @with_error_handling
    async def list_refs(
        self,
        dataset_id: int,
        user_id: int
    ) -> ListRefsResponse:
        """List all refs for a dataset."""
        # Check read permission
        await self._permissions.require("dataset", dataset_id, user_id, "read")
        
        # Get all refs for the dataset
        refs = await self._uow.commits.list_refs(dataset_id)
        
        # Convert to response model
        ref_infos = [
            RefInfo(
                ref_name=ref['name'],
                commit_id=ref['commit_id'].strip() if ref['commit_id'] else '',
                dataset_id=dataset_id,
                is_default=ref.get('name') == 'main',
                created_at=ref['created_at'],
                updated_at=ref['updated_at']
            )
            for ref in refs
        ]
        
        return ListRefsResponse(
            refs=ref_infos,
            dataset_id=dataset_id
        )
    
    # ========== Data Operations ==========
    
    @with_error_handling
    async def get_data_at_ref(
        self,
        dataset_id: int,
        ref_name: str,
        request: GetDataRequest,
        user_id: int
    ) -> GetDataResponse:
        """Get data at a specific ref."""
        # Validate pagination parameters
        offset, limit = self.validate_pagination(request.offset, request.limit)
        
        # Check read permission
        await self._permissions.require("dataset", dataset_id, user_id, "read")
        
        # Get current commit for ref
        ref = await self._uow.commits.get_ref(dataset_id, ref_name)
        if not ref:
            raise EntityNotFoundException("Ref", ref_name)
        
        commit_id = ref['commit_id']
        
        # Use table reader to get data
        if not self._table_reader:
            raise ValueError("Table reader not available")
        
        # Get data from the table
        table_key = request.sheet_name or "primary"
        
        # Get total row count first
        total_rows = await self._table_reader.count_table_rows(commit_id, table_key)
        
        # Then get paginated data
        rows = await self._table_reader.get_table_data(
            commit_id=commit_id,
            table_key=table_key,
            offset=offset,
            limit=limit
        )
        
        # Get schema
        schema = await self._table_reader.get_table_schema(commit_id, table_key)
        
        # Convert rows to DataRow objects
        from src.api.models import DataRow
        data_rows = []
        for i, row in enumerate(rows):
            # Extract logical_row_id if present
            logical_row_id = row.get('_logical_row_id') or row.get('logical_row_id') or f"{table_key}:{i}"
            # Remove system columns
            clean_row = {k: v for k, v in row.items() 
                        if not k.startswith('_') and k != 'logical_row_id'}
            data_row = DataRow(
                sheet_name=table_key,
                logical_row_id=logical_row_id,
                data=clean_row
            )
            data_rows.append(data_row)
        
        return GetDataResponse(
            dataset_id=dataset_id,
            ref_name=ref_name,
            commit_id=commit_id,
            rows=data_rows,
            total_rows=total_rows,
            offset=offset,
            limit=limit,
            has_more=(offset + len(rows)) < total_rows,
            schema=schema
        )
    
    @with_error_handling
    async def get_table_data(
        self,
        dataset_id: int,
        commit_id: str,
        table_key: str,
        user_id: int,
        offset: int = 0,
        limit: int = 100,
        columns: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Get table data from a commit."""
        # Check read permission
        await self._permissions.require("dataset", dataset_id, user_id, "read")
        
        # Verify commit belongs to dataset
        commit = await self._uow.commits.get_commit_by_id(commit_id)
        if not commit or commit.get('dataset_id') != dataset_id:
            raise EntityNotFoundException("Commit", commit_id)
        
        if not self._table_reader:
            raise ValueError("Table reader not available")
        
        # Get total row count
        total_rows = await self._table_reader.count_table_rows(commit_id, table_key)
        
        # Get table data
        rows = await self._table_reader.get_table_data(
            commit_id=commit_id,
            table_key=table_key,
            offset=offset,
            limit=limit
        )
        
        # Filter columns if specified
        if columns:
            filtered_rows = []
            for row in rows:
                filtered_row = {k: v for k, v in row.items() if k in columns}
                filtered_rows.append(filtered_row)
            rows = filtered_rows
        
        # Get schema
        schema = await self._table_reader.get_table_schema(commit_id, table_key)
        
        return {
            'dataset_id': dataset_id,
            'commit_id': commit_id,
            'table_key': table_key,
            'data': rows,
            'total_rows': total_rows,
            'offset': offset,
            'limit': limit,
            'has_more': offset + len(rows) < total_rows,
            'schema': schema
        }
    
    @with_error_handling
    async def list_tables(
        self,
        dataset_id: int,
        commit_id: str,
        user_id: int
    ) -> Dict[str, Any]:
        """List all tables in a commit."""
        # Check read permission
        await self._permissions.require("dataset", dataset_id, user_id, "read")
        
        # Verify commit belongs to dataset
        commit = await self._uow.commits.get_commit_by_id(commit_id)
        if not commit or commit.get('dataset_id') != dataset_id:
            raise EntityNotFoundException("Commit", commit_id)
        
        # Get schema which contains all tables
        schema = await self._uow.commits.get_commit_schema(commit_id)
        if not schema:
            return {'tables': []}
        
        # Extract table information
        tables = []
        for table_key, table_schema in schema.items():
            if isinstance(table_schema, dict) and ('fields' in table_schema or 'columns' in table_schema):
                tables.append(table_key)
        
        return {'tables': tables}
    
    @with_error_handling
    async def get_table_schema(
        self,
        dataset_id: int,
        commit_id: str,
        table_key: str,
        user_id: int
    ) -> Dict[str, Any]:
        """Get schema for a specific table."""
        # Check read permission
        await self._permissions.require("dataset", dataset_id, user_id, "read")
        
        # Verify commit belongs to dataset
        commit = await self._uow.commits.get_commit_by_id(commit_id)
        if not commit or commit.get('dataset_id') != dataset_id:
            raise EntityNotFoundException("Commit", commit_id)
        
        # Get table schema
        if not self._table_reader:
            raise ValueError("Table reader not available")
            
        schema = await self._table_reader.get_table_schema(commit_id, table_key)
        if not schema:
            raise EntityNotFoundException("Table", table_key)
        
        return {
            'dataset_id': dataset_id,
            'commit_id': commit_id,
            'table_key': table_key,
            'schema': schema
        }
    
    @with_error_handling
    async def get_table_analysis(
        self,
        dataset_id: int,
        ref_name: str,
        table_key: str,
        user_id: int
    ) -> TableAnalysisResponse:
        """Analyze table statistics."""
        # Check read permission
        await self._permissions.require("dataset", dataset_id, user_id, "read")
        
        # Get current commit for ref
        ref = await self._uow.commits.get_ref(dataset_id, ref_name)
        if not ref:
            raise EntityNotFoundException("Ref", ref_name)
        
        commit_id = ref['commit_id']
        
        if not self._table_analysis_service:
            # Create one if not provided
            self._table_analysis_service = TableAnalysisService(
                table_reader=self._table_reader,
                type_inference_service=DataTypeInferenceService(),
                statistics_service=ColumnStatisticsService()
            )
        
        # Analyze table
        analysis = await self._table_analysis_service.analyze_table(
            commit_id=commit_id,
            table_key=table_key,
            sample_size=1000
        )
        
        # Convert TableAnalysis to dict format expected by TableAnalysisResponse
        # Get row count
        row_count = await self._uow.commits.count_commit_rows(commit_id, table_key)
        
        # Convert statistics to column_stats format
        column_stats = {}
        null_counts = {}
        unique_counts = {}
        data_types = {}
        columns = []
        
        for stat in analysis.statistics:
            column_stats[stat.column_name] = {
                "min": stat.min_value,
                "max": stat.max_value,
                "mean": stat.mean_value,
                "median": stat.median_value,
                "mode": stat.mode_value,
                "std_dev": stat.std_dev,
                "null_count": stat.null_count,
                "unique_count": stat.unique_count
            }
            null_counts[stat.column_name] = stat.null_count or 0
            unique_counts[stat.column_name] = stat.unique_count or 0
            data_types[stat.column_name] = stat.data_type
            columns.append({"name": stat.column_name, "type": stat.data_type})
        
        # Convert sample data to list of dicts
        sample_data = []
        if analysis.sample_values:
            # Get column names
            col_names = list(analysis.sample_values.keys())
            # Get max number of samples
            max_samples = max(len(vals) for vals in analysis.sample_values.values()) if analysis.sample_values else 0
            # Build sample rows
            for i in range(max_samples):
                row = {}
                for col in col_names:
                    if i < len(analysis.sample_values[col]):
                        row[col] = analysis.sample_values[col][i]
                sample_data.append(row)
        
        return TableAnalysisResponse(
            table_key=table_key,
            sheet_name=table_key,  # Using table_key as sheet_name
            column_stats=column_stats,
            sample_data=sample_data,
            row_count=row_count,
            null_counts=null_counts,
            unique_counts=unique_counts,
            data_types=data_types,
            columns=columns
        )
    
    # ========== Overview and Import ==========
    
    @with_error_handling
    async def get_dataset_overview(
        self,
        dataset_id: int,
        ref_name: str,
        user_id: int
    ) -> DatasetOverviewResponse:
        """Get overview of a dataset."""
        # Check read permission
        await self._permissions.require("dataset", dataset_id, user_id, "read")
        
        # Get dataset info
        dataset = await self._uow.datasets.get_dataset_by_id(dataset_id)
        if not dataset:
            raise EntityNotFoundException("Dataset", dataset_id)
        
        # Get current commit for ref
        ref = await self._uow.commits.get_ref(dataset_id, ref_name)
        if not ref:
            raise EntityNotFoundException("Ref", ref_name)
        
        commit_id = ref['commit_id']
        
        # Get commit info
        commit = await self._uow.commits.get_commit_by_id(commit_id)
        
        # Get schema to count tables
        schema = await self._uow.commits.get_commit_schema(commit_id)
        
        # Parse schema if it's a string
        if schema and isinstance(schema, str):
            schema_dict = json.loads(schema)
        else:
            schema_dict = schema if schema else {}
            
        table_count = len(schema_dict)
        
        # Get total row count across all tables
        total_rows = 0
        if schema_dict:
            for table_key in schema_dict.keys():
                row_count = await self._uow.commits.get_commit_table_row_count(commit_id, table_key)
                total_rows += row_count
        
        # Get refs count
        refs = await self._uow.commits.list_refs(dataset_id)
        ref_count = len(refs)
        
        # Get recent commits
        commits = await self._uow.commits.get_commit_history(
            dataset_id, ref_name, offset=0, limit=5
        )
        
        # Get all branches with their tables
        branches = []
        for ref_info in refs:
            ref_commit_id = ref_info['commit_id']
            ref_schema = await self._uow.commits.get_commit_schema(ref_commit_id)
            
            tables = []
            if ref_schema:
                # Parse schema if it's a string
                if isinstance(ref_schema, str):
                    ref_schema_dict = json.loads(ref_schema)
                else:
                    ref_schema_dict = ref_schema
                    
                for table_key, table_schema in ref_schema_dict.items():
                    # Get row count for this table
                    table_row_count = await self._uow.commits.get_commit_table_row_count(ref_commit_id, table_key)
                    tables.append({
                        'table_key': table_key,
                        'sheet_name': table_key,  # Using table_key as sheet name
                        'row_count': table_row_count,
                        'column_count': len(table_schema.get('columns', [])),
                        'created_at': ref_info['created_at'],  # Using ref creation time
                        'commit_id': ref_commit_id
                    })
            
            branches.append({
                'ref_name': ref_info['name'],
                'commit_id': ref_commit_id,
                'is_default': ref_info['name'] == 'main',
                'created_at': ref_info['created_at'],
                'updated_at': ref_info['updated_at'],
                'tables': tables
            })
        
        return DatasetOverviewResponse(
            dataset_id=dataset_id,
            name=dataset['name'],
            description=dataset.get('description', ''),
            branches=branches
        )
    
    @with_transaction
    @with_error_handling
    async def queue_import_job(
        self,
        dataset_id: int,
        file_path: str,
        file_name: str,
        branch_name: str,
        user_id: int,
        append_mode: bool = False,
        commit_message: Optional[str] = None
    ) -> QueueImportResponse:
        """Queue an import job."""
        # Check write permission
        await self._permissions.require("dataset", dataset_id, user_id, "write")
        
        # Get current commit for branch
        current_commit = await self._uow.commits.get_current_commit_for_ref(dataset_id, branch_name)
        
        # Create import job
        job_params = {
            'file_path': file_path,
            'file_name': file_name,
            'branch_name': branch_name,
            'append_mode': append_mode,
            'commit_message': commit_message or f"Import {file_name}"
        }
        
        job_id = await self._uow.jobs.create_job(
            run_type='import',
            dataset_id=dataset_id,
            source_commit_id=current_commit,
            user_id=user_id,
            run_parameters=job_params
        )
        
        return QueueImportResponse(
            job_id=str(job_id),
            status='pending',
            message=f"Import job queued for file {file_name}"
        )