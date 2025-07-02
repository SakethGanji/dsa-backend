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
    In-memory repository for sampling jobs.
    
    In a production environment, this would be replaced with
    a database-backed repository.
    """
    def __init__(self):
        # In-memory storage for jobs
        self.jobs: Dict[str, SamplingJob] = {}
        # In-memory storage for multi-round jobs
        self.multi_round_jobs: Dict[str, MultiRoundSamplingJob] = {}
        
    async def create_job(self, job: SamplingJob) -> SamplingJob:
        """Create a new job"""
        self.jobs[job.id] = job
        return job
        
    async def get_job(self, job_id: str) -> Optional[SamplingJob]:
        """Get a job by ID"""
        return self.jobs.get(job_id)
        
    async def update_job(self, job: SamplingJob) -> SamplingJob:
        """Update an existing job"""
        self.jobs[job.id] = job
        return job
        
    async def list_jobs(
        self, 
        user_id: Optional[int] = None,
        dataset_id: Optional[int] = None,
        status: Optional[JobStatus] = None,
        limit: int = 10,
        offset: int = 0
    ) -> List[SamplingJob]:
        """List jobs with optional filtering"""
        jobs = list(self.jobs.values())
        
        # Apply filters
        if user_id is not None:
            jobs = [j for j in jobs if j.user_id == user_id]
            
        if dataset_id is not None:
            jobs = [j for j in jobs if j.dataset_id == dataset_id]
            
        if status is not None:
            jobs = [j for j in jobs if j.status == status]
            
        # Sort by creation time (newest first)
        jobs.sort(key=lambda j: j.created_at, reverse=True)
        
        # Apply pagination
        return jobs[offset:offset+limit]
    
    # Multi-round sampling methods
    async def create_multi_round_job(self, job: MultiRoundSamplingJob) -> MultiRoundSamplingJob:
        """Create a new multi-round job"""
        self.multi_round_jobs[job.id] = job
        return job
    
    async def get_multi_round_job(self, job_id: str) -> Optional[MultiRoundSamplingJob]:
        """Get a multi-round job by ID"""
        return self.multi_round_jobs.get(job_id)
    
    async def update_multi_round_job(self, job: MultiRoundSamplingJob) -> MultiRoundSamplingJob:
        """Update an existing multi-round job"""
        self.multi_round_jobs[job.id] = job
        return job
    
    async def list_multi_round_jobs(
        self,
        user_id: Optional[int] = None,
        dataset_id: Optional[int] = None,
        status: Optional[JobStatus] = None,
        limit: int = 10,
        offset: int = 0
    ) -> List[MultiRoundSamplingJob]:
        """List multi-round jobs with optional filtering"""
        jobs = list(self.multi_round_jobs.values())
        
        # Apply filters
        if user_id is not None:
            jobs = [j for j in jobs if j.user_id == user_id]
        
        if dataset_id is not None:
            jobs = [j for j in jobs if j.dataset_id == dataset_id]
        
        if status is not None:
            jobs = [j for j in jobs if j.status == status]
        
        # Sort by creation time (newest first)
        jobs.sort(key=lambda j: j.created_at, reverse=True)
        
        # Apply pagination
        return jobs[offset:offset+limit]