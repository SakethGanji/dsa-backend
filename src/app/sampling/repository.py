"""In-memory repository for sampling jobs - HOLLOWED OUT FOR BACKEND RESET"""
from typing import Dict, List, Optional, Any
from datetime import datetime
import json
import logging
from app.sampling.models import SamplingJob, JobStatus, MultiRoundSamplingJob

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SamplingRepository:
    """
    In-memory repository for sampling jobs - DEPRECATED.
    
    NOTE: This is legacy code for backward compatibility.
    New implementation should use SamplingDBRepository with analysis_runs table.
    """
    def __init__(self):
        # In-memory storage for jobs
        self.jobs: Dict[str, SamplingJob] = {}
        # In-memory storage for multi-round jobs
        self.multi_round_jobs: Dict[str, MultiRoundSamplingJob] = {}
        
    async def create_job(self, job: SamplingJob) -> SamplingJob:
        """
        Create a new job.
        
        DEPRECATED: Use SamplingDBRepository.create_analysis_run instead
        
        Implementation Notes:
        This method is kept for backward compatibility only.
        New code should create analysis_runs records in database.
        
        Request:
        - job: SamplingJob
        
        Response:
        - SamplingJob with generated ID
        """
        raise NotImplementedError("Use SamplingDBRepository instead")
        
    async def get_job(self, job_id: str) -> Optional[SamplingJob]:
        """
        Get a job by ID.
        
        DEPRECATED: Use SamplingDBRepository.get_analysis_run instead
        
        Request:
        - job_id: str
        
        Response:
        - Optional[SamplingJob]
        """
        raise NotImplementedError("Use SamplingDBRepository instead")
        
    async def update_job(self, job: SamplingJob) -> SamplingJob:
        """
        Update an existing job.
        
        DEPRECATED: Use SamplingDBRepository.update_analysis_run instead
        
        Request:
        - job: SamplingJob
        
        Response:
        - Updated SamplingJob
        """
        raise NotImplementedError("Use SamplingDBRepository instead")
        
    async def list_jobs(
        self, 
        user_id: Optional[int] = None,
        dataset_id: Optional[int] = None,
        status: Optional[JobStatus] = None,
        limit: int = 10,
        offset: int = 0
    ) -> List[SamplingJob]:
        """
        List jobs with optional filtering.
        
        DEPRECATED: Use SamplingDBRepository query methods instead
        
        Request:
        - user_id: Optional[int]
        - dataset_id: Optional[int]
        - status: Optional[JobStatus]
        - limit: int
        - offset: int
        
        Response:
        - List[SamplingJob] paginated
        """
        raise NotImplementedError("Use SamplingDBRepository instead")
    
    # Multi-round sampling methods
    async def create_multi_round_job(self, job: MultiRoundSamplingJob) -> MultiRoundSamplingJob:
        """
        Create a new multi-round job.
        
        DEPRECATED: Use SamplingDBRepository.create_analysis_run instead
        
        Implementation Notes:
        Multi-round jobs are now stored as analysis_runs with
        analysis_type='multi_round_sampling' and request stored
        in run_parameters JSONB field.
        
        Request:
        - job: MultiRoundSamplingJob
        
        Response:
        - MultiRoundSamplingJob with ID
        """
        raise NotImplementedError("Use SamplingDBRepository instead")
    
    async def get_multi_round_job(self, job_id: str) -> Optional[MultiRoundSamplingJob]:
        """
        Get a multi-round job by ID.
        
        DEPRECATED: Use SamplingDBRepository.get_analysis_run instead
        
        Request:
        - job_id: str
        
        Response:
        - Optional[MultiRoundSamplingJob]
        """
        raise NotImplementedError("Use SamplingDBRepository instead")
    
    async def update_multi_round_job(self, job: MultiRoundSamplingJob) -> MultiRoundSamplingJob:
        """
        Update an existing multi-round job.
        
        DEPRECATED: Use SamplingDBRepository.update_analysis_run instead
        
        Request:
        - job: MultiRoundSamplingJob
        
        Response:
        - Updated MultiRoundSamplingJob
        """
        raise NotImplementedError("Use SamplingDBRepository instead")
    
    async def list_multi_round_jobs(
        self,
        user_id: Optional[int] = None,
        dataset_id: Optional[int] = None,
        status: Optional[JobStatus] = None,
        limit: int = 10,
        offset: int = 0
    ) -> List[MultiRoundSamplingJob]:
        """
        List multi-round jobs with optional filtering.
        
        DEPRECATED: Use SamplingDBRepository query methods instead
        
        Request:
        - user_id: Optional[int]
        - dataset_id: Optional[int]
        - status: Optional[JobStatus]
        - limit: int
        - offset: int
        
        Response:
        - List[MultiRoundSamplingJob] paginated
        """
        raise NotImplementedError("Use SamplingDBRepository instead")