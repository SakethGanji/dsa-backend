from typing import Dict, List, Optional
from app.explore.models import ExploreJob, JobStatus
import asyncio

class ExploreRepository:
    """
    Repository for managing exploration jobs.
    Stores jobs in memory (can be replaced with database later).
    """
    
    def __init__(self):
        self.jobs: Dict[str, ExploreJob] = {}
        self._lock = asyncio.Lock()
    
    async def create_job(self, job: ExploreJob) -> ExploreJob:
        """Create a new exploration job"""
        async with self._lock:
            self.jobs[job.id] = job
            return job
    
    async def get_job(self, job_id: str) -> Optional[ExploreJob]:
        """Get a job by ID"""
        return self.jobs.get(job_id)
    
    async def update_job_status(
        self, 
        job_id: str, 
        status: JobStatus,
        **kwargs
    ) -> Optional[ExploreJob]:
        """Update job status and other fields"""
        async with self._lock:
            job = self.jobs.get(job_id)
            if job:
                job.status = status
                for key, value in kwargs.items():
                    if hasattr(job, key):
                        setattr(job, key, value)
            return job
    
    async def list_jobs(
        self, 
        user_id: Optional[int] = None,
        dataset_id: Optional[int] = None,
        status: Optional[JobStatus] = None
    ) -> List[ExploreJob]:
        """List jobs with optional filters"""
        jobs = list(self.jobs.values())
        
        if user_id is not None:
            jobs = [j for j in jobs if j.user_id == user_id]
        
        if dataset_id is not None:
            jobs = [j for j in jobs if j.dataset_id == dataset_id]
        
        if status is not None:
            jobs = [j for j in jobs if j.status == status]
        
        # Sort by created_at descending
        jobs.sort(key=lambda x: x.created_at, reverse=True)
        
        return jobs
    
    async def delete_job(self, job_id: str) -> bool:
        """Delete a job"""
        async with self._lock:
            if job_id in self.jobs:
                del self.jobs[job_id]
                return True
            return False

# Global instance
explore_repository = ExploreRepository()