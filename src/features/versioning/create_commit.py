from typing import List, Dict, Any, Tuple, Set
import hashlib
import json

from src.core.abstractions import IUnitOfWork, ICommitRepository, IDatasetRepository
from src.models.pydantic_models import CreateCommitRequest, CreateCommitResponse


class CreateCommitHandler:
    """Handler for creating new commits with direct data (not file imports)"""
    
    def __init__(
        self,
        uow: IUnitOfWork,
        commit_repo: ICommitRepository,
        dataset_repo: IDatasetRepository
    ):
        self._uow = uow
        self._commit_repo = commit_repo
        self._dataset_repo = dataset_repo
    
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
        # TODO: Check write permission
        has_permission = await self._dataset_repo.check_user_permission(
            dataset_id, user_id, 'write'
        )
        if not has_permission:
            raise PermissionError("User lacks write permission")
        
        # TODO: Get current commit for optimistic locking
        current_commit = await self._commit_repo.get_current_commit_for_ref(
            dataset_id, ref_name
        )
        
        # TODO: Prepare data
        rows_to_store, manifest = self._prepare_data(request.data)
        
        await self._uow.begin()
        try:
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
            
            # Update ref atomically
            success = await self._commit_repo.update_ref_atomically(
                dataset_id=dataset_id,
                ref_name=ref_name,
                new_commit_id=new_commit_id,
                expected_commit_id=current_commit
            )
            
            if not success:
                raise Exception("Concurrent modification detected. Please retry.")
            
            await self._uow.commit()
            
            return CreateCommitResponse(
                commit_id=new_commit_id,
                dataset_id=dataset_id,
                message=request.message
            )
        except Exception:
            await self._uow.rollback()
            raise
    
    def _prepare_data(self, data: List[Dict[str, Any]]) -> Tuple[Set[Tuple[str, str]], List[Tuple[str, str]]]:
        """
        Prepare rows for storage
        Returns: (rows_to_store, manifest)
        """
        rows_to_store = set()
        manifest = []
        
        for idx, row in enumerate(data):
            # Generate logical row ID
            logical_row_id = f"default:{idx}"
            
            # Canonicalize and hash
            canonical_json = self._canonicalize_json(row)
            row_hash = hashlib.sha256(canonical_json.encode()).hexdigest()
            
            rows_to_store.add((row_hash, canonical_json))
            manifest.append((logical_row_id, row_hash))
        
        return rows_to_store, manifest
    
    def _canonicalize_json(self, data: Dict[str, Any]) -> str:
        """Create canonical JSON representation for consistent hashing"""
        # TODO: Implement proper canonicalization
        # Sort keys, handle None values, etc.
        return json.dumps(data, sort_keys=True, separators=(',', ':'))