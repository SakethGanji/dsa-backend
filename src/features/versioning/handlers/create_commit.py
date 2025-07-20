from typing import List, Dict, Any, Tuple, Set
import hashlib
import json

from src.core.abstractions import IUnitOfWork, ICommitRepository, IDatasetRepository
from src.api.models import CreateCommitRequest, CreateCommitResponse
from ...base_handler import BaseHandler, with_error_handling, with_transaction


class CreateCommitHandler(BaseHandler[CreateCommitResponse]):
    """Handler for creating new commits with direct data (not file imports)"""
    
    def __init__(
        self,
        uow: IUnitOfWork,
        commit_repo: ICommitRepository,
        dataset_repo: IDatasetRepository
    ):
        super().__init__(uow)
        self._commit_repo = commit_repo
        self._dataset_repo = dataset_repo
    
    @with_error_handling
    @with_transaction
    async def handle(
        self,
        dataset_id: int,
        ref_name: str,
        request: CreateCommitRequest,
        user_id: int
    ) -> CreateCommitResponse:
        """
        Create a new commit with provided data
        
        Steps:
        1. Validate permissions
        2. Prepare rows (canonicalize, hash)
        3. Build manifest
        4. Create commit atomically
        5. Update ref
        """
        # Permission check removed - handled by authorization middleware
        
        # TODO: Get current commit for optimistic locking
        current_commit = await self._commit_repo.get_current_commit_for_ref(
            dataset_id, ref_name
        )
        
        # TODO: Prepare data
        rows_to_store, manifest, schema = self._prepare_data(request.data)
        
        # Add rows to content-addressable store
        await self._commit_repo.add_rows_if_not_exist(rows_to_store)
        
        # Create commit with manifest
        new_commit_id = await self._commit_repo.create_commit_and_manifest(
            dataset_id=dataset_id,
            parent_commit_id=request.parent_commit_id or current_commit,
            message=request.message,
            author_id=user_id,
            manifest=manifest
        )
        
        # Store schema for the commit
        await self._commit_repo.create_commit_schema(new_commit_id, schema)
        
        # Update ref atomically
        success = await self._commit_repo.update_ref_atomically(
            dataset_id=dataset_id,
            ref_name=ref_name,
            new_commit_id=new_commit_id,
            expected_commit_id=current_commit
        )
        
        if not success:
            raise ValueError("Concurrent modification detected. Please retry.")
        
        # Refresh search index to update dataset's updated_at timestamp
        await self._uow.search_repository.refresh_search_index()
        
        # Get the commit details to include created_at
        commit_details = await self._commit_repo.get_commit_by_id(new_commit_id)
        
        return CreateCommitResponse(
            commit_id=new_commit_id,
            message=request.message,
            created_at=commit_details['created_at'] if commit_details else None
        )
    
    def _prepare_data(self, data: List[Dict[str, Any]]) -> Tuple[Set[Tuple[str, str]], List[Tuple[str, str]], Dict[str, Any]]:
        """
        Prepare rows for storage
        Returns: (rows_to_store, manifest, schema)
        """
        rows_to_store = set()
        manifest = []
        schema = {}
        
        # Group rows by sheet_name if present
        sheet_counters = {}
        sheet_columns = {}
        
        for row in data:
            # Extract sheet_name if present, otherwise use 'default'
            sheet_name = row.get('sheet_name', 'default')
            
            # Initialize counter for this sheet if not exists
            if sheet_name not in sheet_counters:
                sheet_counters[sheet_name] = 0
                sheet_columns[sheet_name] = set()
            
            # Track columns for schema
            for key in row.keys():
                if key != 'sheet_name':  # Exclude sheet_name from data columns
                    sheet_columns[sheet_name].add(key)
            
            # TODO: [TECH DEBT - POC IMPLEMENTATION]
            # This logical ID generation is based on a hash of the entire row's content.
            #
            # PROS: It correctly handles file re-sorting without creating a new commit.
            # CONS: It CANNOT distinguish a row update from a DELETE and ADD operation.
            #       This will cause significant performance degradation and database bloat
            #       at scale when datasets are frequently updated.
            #
            # ACTION: This MUST be migrated to a Business Key-based hash generation
            #         before moving to a production environment to enable true updates
            #         and ensure system scalability. See ticket [POC-HASH-ID].
            
            # Extract data (exclude sheet_name from the data that gets hashed)
            row_data = {k: v for k, v in row.items() if k != 'sheet_name'}
            
            # Canonicalize and hash data only
            data_canonical_json = self._canonicalize_json(row_data)
            data_hash = hashlib.sha256(data_canonical_json.encode()).hexdigest()
            
            # Generate logical row ID using sheet_name:hash format
            logical_row_id = f"{sheet_name}:{data_hash}"
            sheet_counters[sheet_name] += 1
            
            # Create standardized row format for storage
            # This matches the format used by import_executor.py
            row_wrapper = {
                "sheet_name": sheet_name,
                "row_number": sheet_counters[sheet_name],  # 1-indexed
                "data": row_data
            }
            canonical_json = self._canonicalize_json(row_wrapper)
            row_hash = data_hash
            
            rows_to_store.add((row_hash, canonical_json))
            manifest.append((logical_row_id, row_hash))
        
        # Build schema
        for sheet_name, columns in sheet_columns.items():
            schema[sheet_name] = {
                "columns": sorted(list(columns)),  # Sort for consistency
                "row_count": sheet_counters[sheet_name]
            }
        
        return rows_to_store, manifest, schema
    
    def _canonicalize_json(self, data: Dict[str, Any]) -> str:
        """Create canonical JSON representation for consistent hashing"""
        # TODO: Implement proper canonicalization
        # Sort keys, handle None values, etc.
        return json.dumps(data, sort_keys=True, separators=(',', ':'))