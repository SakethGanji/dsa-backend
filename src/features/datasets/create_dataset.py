from core.services.interfaces import IUnitOfWork, IDatasetRepository, ICommitRepository
from models.pydantic_models import CreateDatasetRequest, CreateDatasetResponse


class CreateDatasetHandler:
    """Handler for creating new datasets"""
    
    def __init__(
        self,
        uow: IUnitOfWork,
        dataset_repo: IDatasetRepository,
        commit_repo: ICommitRepository
    ):
        self._uow = uow
        self._dataset_repo = dataset_repo
        self._commit_repo = commit_repo
    
    async def handle(
        self,
        request: CreateDatasetRequest,
        user_id: int
    ) -> CreateDatasetResponse:
        """
        Create a new dataset with initial empty commit
        
        Steps:
        1. Create dataset record
        2. Grant admin permission to creator
        3. Create initial empty commit
        4. Create 'main' ref pointing to initial commit
        """
        await self._uow.begin()
        try:
            # Create dataset
            dataset_id = await self._dataset_repo.create_dataset(
                name=request.name,
                description=request.description,
                created_by=user_id
            )
            
            # Grant admin permission to creator
            await self._dataset_repo.grant_permission(
                dataset_id=dataset_id,
                user_id=user_id,
                permission_type='admin'
            )
            
            # Create initial empty commit
            initial_commit_id = await self._commit_repo.create_commit_and_manifest(
                dataset_id=dataset_id,
                parent_commit_id=None,
                message="Initial commit",
                author_id=user_id,
                manifest=[]  # Empty manifest for initial commit
            )
            
            # Create main ref
            # Since this is a new dataset, we don't need optimistic locking
            await self._commit_repo.update_ref_atomically(
                dataset_id=dataset_id,
                ref_name='main',
                new_commit_id=initial_commit_id,
                expected_commit_id=None  # No previous commit
            )
            
            await self._uow.commit()
            
            return CreateDatasetResponse(
                dataset_id=dataset_id,
                name=request.name
            )
        except Exception:
            await self._uow.rollback()
            raise