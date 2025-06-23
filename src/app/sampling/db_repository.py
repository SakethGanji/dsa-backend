from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
import json
import logging
from sqlalchemy.ext.asyncio import AsyncSession
import sqlalchemy as sa
from app.sampling.models import MultiRoundSamplingRequest

logger = logging.getLogger(__name__)

class SamplingDBRepository:
    """
    Database repository for sampling jobs using the analysis_runs table.
    
    This replaces the in-memory repository with persistent storage.
    """
    
    def __init__(self, session: AsyncSession):
        self.session = session
    
    async def create_analysis_run(
        self,
        dataset_version_id: int,
        user_id: int,
        request: MultiRoundSamplingRequest
    ) -> int:
        """
        Create a new analysis run for multi-round sampling
        
        Args:
            dataset_version_id: The dataset version to sample from
            user_id: The user creating the job
            request: The multi-round sampling request configuration
            
        Returns:
            The ID of the created analysis run
        """
        # Prepare the run parameters with the full request configuration
        run_parameters = {
            "request": request.dict(),
            "total_rounds": len(request.rounds),
            "completed_rounds": 0,
            "round_results": [],
            "job_type": "multi_round_sampling"
        }
        
        query = sa.text("""
            INSERT INTO analysis_runs (
                dataset_version_id,
                user_id,
                run_type,
                run_parameters,
                status,
                run_timestamp
            )
            VALUES (
                :dataset_version_id,
                :user_id,
                CAST('sampling' AS analysis_run_type),
                :run_parameters,
                CAST('pending' AS analysis_run_status),
                NOW()
            )
            RETURNING id;
        """)
        
        values = {
            "dataset_version_id": dataset_version_id,
            "user_id": user_id,
            "run_parameters": json.dumps(run_parameters)
        }
        
        result = await self.session.execute(query, values)
        await self.session.commit()
        
        run_id = result.scalar_one()
        logger.info(f"Created analysis run {run_id} for multi-round sampling")
        
        return run_id
    
    async def update_analysis_run(
        self,
        run_id: int,
        status: Optional[str] = None,
        output_file_id: Optional[int] = None,
        output_summary: Optional[dict] = None,
        notes: Optional[str] = None,
        execution_time_ms: Optional[int] = None,
        run_parameters_update: Optional[dict] = None
    ) -> None:
        """
        Update an existing analysis run
        
        Args:
            run_id: The ID of the run to update
            status: New status (pending, running, completed, failed)
            output_file_id: ID of the output file if completed
            output_summary: Summary data for the run
            notes: Any notes or error messages
            execution_time_ms: Execution time in milliseconds
            run_parameters_update: Updates to merge into run_parameters
        """
        # Build dynamic update query
        update_fields = []
        values = {"run_id": run_id}
        
        if status is not None:
            update_fields.append("status = CAST(:status AS analysis_run_status)")
            values["status"] = status
            
        if output_file_id is not None:
            update_fields.append("output_file_id = :output_file_id")
            values["output_file_id"] = output_file_id
            
        if output_summary is not None:
            update_fields.append("output_summary = :output_summary")
            values["output_summary"] = json.dumps(output_summary)
            
        if notes is not None:
            update_fields.append("notes = :notes")
            values["notes"] = notes
            
        if execution_time_ms is not None:
            update_fields.append("execution_time_ms = :execution_time_ms")
            values["execution_time_ms"] = execution_time_ms
            
        if run_parameters_update is not None:
            # Merge updates into existing run_parameters
            update_fields.append("""
                run_parameters = run_parameters || :run_parameters_update
            """)
            values["run_parameters_update"] = json.dumps(run_parameters_update)
        
        if not update_fields:
            return
            
        query = sa.text(f"""
            UPDATE analysis_runs
            SET {', '.join(update_fields)}
            WHERE id = :run_id;
        """)
        
        await self.session.execute(query, values)
        await self.session.commit()
        
        logger.info(f"Updated analysis run {run_id}")
    
    async def get_analysis_run(self, run_id: int) -> Optional[dict]:
        """
        Get an analysis run by ID
        
        Args:
            run_id: The ID of the run to retrieve
            
        Returns:
            Dictionary with run data or None if not found
        """
        query = sa.text("""
            SELECT 
                ar.id,
                ar.dataset_version_id,
                ar.user_id,
                ar.run_type,
                ar.run_parameters,
                ar.status,
                ar.run_timestamp,
                ar.execution_time_ms,
                ar.notes,
                ar.output_file_id,
                ar.output_summary,
                dv.dataset_id,
                f.file_path as output_file_path
            FROM 
                analysis_runs ar
            JOIN 
                dataset_versions dv ON ar.dataset_version_id = dv.id
            LEFT JOIN
                files f ON ar.output_file_id = f.id
            WHERE 
                ar.id = :run_id
                AND ar.run_type = 'sampling';
        """)
        
        result = await self.session.execute(query, {"run_id": run_id})
        row = result.fetchone()
        
        if not row:
            return None
            
        return {
            "id": row.id,
            "dataset_id": row.dataset_id,
            "dataset_version_id": row.dataset_version_id,
            "user_id": row.user_id,
            "run_type": row.run_type,
            "run_parameters": row.run_parameters,
            "status": row.status,
            "run_timestamp": row.run_timestamp,
            "execution_time_ms": row.execution_time_ms,
            "notes": row.notes,
            "output_file_id": row.output_file_id,
            "output_file_path": row.output_file_path,
            "output_summary": row.output_summary
        }
    
    async def list_analysis_runs(
        self,
        user_id: Optional[int] = None,
        dataset_id: Optional[int] = None,
        dataset_version_id: Optional[int] = None,
        status: Optional[str] = None,
        limit: int = 10,
        offset: int = 0
    ) -> List[dict]:
        """
        List analysis runs with optional filtering
        
        Args:
            user_id: Filter by user
            dataset_id: Filter by dataset
            dataset_version_id: Filter by dataset version
            status: Filter by status
            limit: Maximum number of results
            offset: Number of results to skip
            
        Returns:
            List of analysis run dictionaries
        """
        # Build dynamic query with filters
        where_clauses = ["ar.run_type = 'sampling'"]
        values = {"limit": limit, "offset": offset}
        
        if user_id is not None:
            where_clauses.append("ar.user_id = :user_id")
            values["user_id"] = user_id
            
        if dataset_version_id is not None:
            where_clauses.append("ar.dataset_version_id = :dataset_version_id")
            values["dataset_version_id"] = dataset_version_id
        elif dataset_id is not None:
            where_clauses.append("dv.dataset_id = :dataset_id")
            values["dataset_id"] = dataset_id
            
        if status is not None:
            where_clauses.append("ar.status = CAST(:status AS analysis_run_status)")
            values["status"] = status
        
        where_clause = " AND ".join(where_clauses)
        
        query = sa.text(f"""
            SELECT 
                ar.id,
                ar.dataset_version_id,
                ar.user_id,
                ar.run_type,
                ar.run_parameters,
                ar.status,
                ar.run_timestamp,
                ar.execution_time_ms,
                ar.notes,
                ar.output_file_id,
                ar.output_summary,
                dv.dataset_id
            FROM 
                analysis_runs ar
            JOIN 
                dataset_versions dv ON ar.dataset_version_id = dv.id
            WHERE 
                {where_clause}
            ORDER BY 
                ar.run_timestamp DESC
            LIMIT :limit
            OFFSET :offset;
        """)
        
        result = await self.session.execute(query, values)
        rows = result.fetchall()
        
        return [
            {
                "id": row.id,
                "dataset_id": row.dataset_id,
                "dataset_version_id": row.dataset_version_id,
                "user_id": row.user_id,
                "run_type": row.run_type,
                "run_parameters": row.run_parameters,
                "status": row.status,
                "run_timestamp": row.run_timestamp,
                "execution_time_ms": row.execution_time_ms,
                "notes": row.notes,
                "output_file_id": row.output_file_id,
                "output_summary": row.output_summary
            }
            for row in rows
        ]
    
    async def get_running_jobs(self) -> List[dict]:
        """
        Get all jobs that are currently running.
        Used for recovery after server restart.
        
        Returns:
            List of running analysis runs
        """
        return await self.list_analysis_runs(status="running", limit=1000)
    
    async def update_round_progress(
        self,
        run_id: int,
        current_round: int,
        completed_rounds: int,
        round_results: List[dict]
    ) -> None:
        """
        Update the progress of multi-round sampling
        
        Args:
            run_id: The analysis run ID
            current_round: The currently executing round number
            completed_rounds: Number of completed rounds
            round_results: Results from completed rounds
        """
        run_parameters_update = {
            "current_round": current_round,
            "completed_rounds": completed_rounds,
            "round_results": round_results
        }
        
        await self.update_analysis_run(
            run_id=run_id,
            run_parameters_update=run_parameters_update
        )
    
    async def get_samplings_by_user(
        self,
        user_id: int,
        limit: int = 10,
        offset: int = 0
    ) -> Tuple[List[dict], int]:
        """
        Get all sampling runs created by a specific user
        
        Args:
            user_id: The user ID to filter by
            limit: Maximum number of results
            offset: Number of results to skip
            
        Returns:
            Tuple of (list of sampling runs, total count)
        """
        # Count total matching records
        count_query = sa.text("""
            SELECT COUNT(*) as total
            FROM analysis_runs ar
            WHERE ar.run_type = 'sampling'
                AND ar.user_id = :user_id;
        """)
        
        count_result = await self.session.execute(count_query, {"user_id": user_id})
        total_count = count_result.scalar()
        
        # Get paginated results with all required fields
        query = sa.text("""
            SELECT 
                ar.id,
                ar.dataset_version_id,
                ar.user_id,
                ar.run_type,
                ar.run_parameters,
                ar.status,
                ar.run_timestamp,
                ar.execution_time_ms,
                ar.notes,
                ar.output_file_id,
                ar.output_summary,
                dv.dataset_id,
                d.name as dataset_name,
                dv.version_number,
                u.soeid as user_soeid,
                f.file_path as output_file_path,
                f.file_size as output_file_size
            FROM 
                analysis_runs ar
            JOIN 
                dataset_versions dv ON ar.dataset_version_id = dv.id
            JOIN
                datasets d ON dv.dataset_id = d.id
            LEFT JOIN
                users u ON ar.user_id = u.id
            LEFT JOIN
                files f ON ar.output_file_id = f.id
            WHERE 
                ar.run_type = 'sampling'
                AND ar.user_id = :user_id
            ORDER BY 
                ar.run_timestamp DESC
            LIMIT :limit
            OFFSET :offset;
        """)
        
        values = {
            "user_id": user_id,
            "limit": limit,
            "offset": offset
        }
        
        result = await self.session.execute(query, values)
        rows = result.fetchall()
        
        runs = [
            {
                "id": row.id,
                "dataset_id": row.dataset_id,
                "dataset_name": row.dataset_name,
                "dataset_version_id": row.dataset_version_id,
                "version_number": row.version_number,
                "user_id": row.user_id,
                "user_soeid": row.user_soeid,
                "run_type": row.run_type,
                "run_parameters": row.run_parameters,
                "status": row.status,
                "run_timestamp": row.run_timestamp,
                "execution_time_ms": row.execution_time_ms,
                "notes": row.notes,
                "output_file_id": row.output_file_id,
                "output_file_path": row.output_file_path,
                "output_file_size": row.output_file_size,
                "output_summary": row.output_summary
            }
            for row in rows
        ]
        
        return runs, total_count
    
    async def get_samplings_by_dataset(
        self,
        dataset_id: int,
        limit: int = 10,
        offset: int = 0
    ) -> Tuple[List[dict], int]:
        """
        Get all sampling runs for a specific dataset (across all versions)
        
        Args:
            dataset_id: The dataset ID to filter by
            limit: Maximum number of results
            offset: Number of results to skip
            
        Returns:
            Tuple of (list of sampling runs, total count)
        """
        # Count total matching records
        count_query = sa.text("""
            SELECT COUNT(*) as total
            FROM analysis_runs ar
            JOIN dataset_versions dv ON ar.dataset_version_id = dv.id
            WHERE ar.run_type = 'sampling'
                AND dv.dataset_id = :dataset_id;
        """)
        
        count_result = await self.session.execute(count_query, {"dataset_id": dataset_id})
        total_count = count_result.scalar()
        
        # Get paginated results
        query = sa.text("""
            SELECT 
                ar.id,
                ar.dataset_version_id,
                ar.user_id,
                ar.run_type,
                ar.run_parameters,
                ar.status,
                ar.run_timestamp,
                ar.execution_time_ms,
                ar.notes,
                ar.output_file_id,
                ar.output_summary,
                dv.dataset_id,
                d.name as dataset_name,
                dv.version_number,
                u.soeid as user_soeid,
                f.file_path as output_file_path,
                f.file_size as output_file_size
            FROM 
                analysis_runs ar
            JOIN 
                dataset_versions dv ON ar.dataset_version_id = dv.id
            JOIN
                datasets d ON dv.dataset_id = d.id
            LEFT JOIN
                users u ON ar.user_id = u.id
            LEFT JOIN
                files f ON ar.output_file_id = f.id
            WHERE 
                ar.run_type = 'sampling'
                AND dv.dataset_id = :dataset_id
            ORDER BY 
                ar.run_timestamp DESC
            LIMIT :limit
            OFFSET :offset;
        """)
        
        values = {
            "dataset_id": dataset_id,
            "limit": limit,
            "offset": offset
        }
        
        result = await self.session.execute(query, values)
        rows = result.fetchall()
        
        runs = [
            {
                "id": row.id,
                "dataset_id": row.dataset_id,
                "dataset_name": row.dataset_name,
                "dataset_version_id": row.dataset_version_id,
                "version_number": row.version_number,
                "user_id": row.user_id,
                "user_soeid": row.user_soeid,
                "run_type": row.run_type,
                "run_parameters": row.run_parameters,
                "status": row.status,
                "run_timestamp": row.run_timestamp,
                "execution_time_ms": row.execution_time_ms,
                "notes": row.notes,
                "output_file_id": row.output_file_id,
                "output_file_path": row.output_file_path,
                "output_file_size": row.output_file_size,
                "output_summary": row.output_summary
            }
            for row in rows
        ]
        
        return runs, total_count
    
    async def get_samplings_by_dataset_version(
        self,
        dataset_version_id: int,
        limit: int = 10,
        offset: int = 0
    ) -> Tuple[List[dict], int]:
        """
        Get all sampling runs for a specific dataset version
        
        Args:
            dataset_version_id: The dataset version ID to filter by
            limit: Maximum number of results
            offset: Number of results to skip
            
        Returns:
            Tuple of (list of sampling runs, total count)
        """
        # Count total matching records
        count_query = sa.text("""
            SELECT COUNT(*) as total
            FROM analysis_runs ar
            WHERE ar.run_type = 'sampling'
                AND ar.dataset_version_id = :dataset_version_id;
        """)
        
        count_result = await self.session.execute(count_query, {"dataset_version_id": dataset_version_id})
        total_count = count_result.scalar()
        
        # Get paginated results
        query = sa.text("""
            SELECT 
                ar.id,
                ar.dataset_version_id,
                ar.user_id,
                ar.run_type,
                ar.run_parameters,
                ar.status,
                ar.run_timestamp,
                ar.execution_time_ms,
                ar.notes,
                ar.output_file_id,
                ar.output_summary,
                dv.dataset_id,
                d.name as dataset_name,
                dv.version_number,
                u.soeid as user_soeid,
                f.file_path as output_file_path,
                f.file_size as output_file_size
            FROM 
                analysis_runs ar
            JOIN 
                dataset_versions dv ON ar.dataset_version_id = dv.id
            JOIN
                datasets d ON dv.dataset_id = d.id
            LEFT JOIN
                users u ON ar.user_id = u.id
            LEFT JOIN
                files f ON ar.output_file_id = f.id
            WHERE 
                ar.run_type = 'sampling'
                AND ar.dataset_version_id = :dataset_version_id
            ORDER BY 
                ar.run_timestamp DESC
            LIMIT :limit
            OFFSET :offset;
        """)
        
        values = {
            "dataset_version_id": dataset_version_id,
            "limit": limit,
            "offset": offset
        }
        
        result = await self.session.execute(query, values)
        rows = result.fetchall()
        
        runs = [
            {
                "id": row.id,
                "dataset_id": row.dataset_id,
                "dataset_name": row.dataset_name,
                "dataset_version_id": row.dataset_version_id,
                "version_number": row.version_number,
                "user_id": row.user_id,
                "user_soeid": row.user_soeid,
                "run_type": row.run_type,
                "run_parameters": row.run_parameters,
                "status": row.status,
                "run_timestamp": row.run_timestamp,
                "execution_time_ms": row.execution_time_ms,
                "notes": row.notes,
                "output_file_id": row.output_file_id,
                "output_file_path": row.output_file_path,
                "output_file_size": row.output_file_size,
                "output_summary": row.output_summary
            }
            for row in rows
        ]
        
        return runs, total_count
    
    async def get_samplings_by_dataset(
        self,
        dataset_id: int,
        limit: int = 10,
        offset: int = 0
    ) -> Tuple[List[dict], int]:
        """
        Get all sampling runs for a specific dataset (across all versions)
        
        Args:
            dataset_id: The dataset ID to filter by
            limit: Maximum number of results
            offset: Number of results to skip
            
        Returns:
            Tuple of (list of sampling runs, total count)
        """
        # Count total matching records
        count_query = sa.text("""
            SELECT COUNT(*) as total
            FROM analysis_runs ar
            JOIN dataset_versions dv ON ar.dataset_version_id = dv.id
            WHERE ar.run_type = 'sampling'
                AND dv.dataset_id = :dataset_id;
        """)
        
        count_result = await self.session.execute(count_query, {"dataset_id": dataset_id})
        total_count = count_result.scalar()
        
        # Get paginated results
        query = sa.text("""
            SELECT 
                ar.id,
                ar.dataset_version_id,
                ar.user_id,
                ar.run_type,
                ar.run_parameters,
                ar.status,
                ar.run_timestamp,
                ar.execution_time_ms,
                ar.notes,
                ar.output_file_id,
                ar.output_summary,
                dv.dataset_id,
                d.name as dataset_name,
                dv.version_number,
                u.soeid as user_soeid,
                f.file_path as output_file_path,
                f.file_size as output_file_size
            FROM 
                analysis_runs ar
            JOIN 
                dataset_versions dv ON ar.dataset_version_id = dv.id
            JOIN
                datasets d ON dv.dataset_id = d.id
            LEFT JOIN
                users u ON ar.user_id = u.id
            LEFT JOIN
                files f ON ar.output_file_id = f.id
            WHERE 
                ar.run_type = 'sampling'
                AND dv.dataset_id = :dataset_id
            ORDER BY 
                ar.run_timestamp DESC
            LIMIT :limit
            OFFSET :offset;
        """)
        
        values = {
            "dataset_id": dataset_id,
            "limit": limit,
            "offset": offset
        }
        
        result = await self.session.execute(query, values)
        rows = result.fetchall()
        
        runs = [
            {
                "id": row.id,
                "dataset_id": row.dataset_id,
                "dataset_name": row.dataset_name,
                "dataset_version_id": row.dataset_version_id,
                "version_number": row.version_number,
                "user_id": row.user_id,
                "user_soeid": row.user_soeid,
                "run_type": row.run_type,
                "run_parameters": row.run_parameters,
                "status": row.status,
                "run_timestamp": row.run_timestamp,
                "execution_time_ms": row.execution_time_ms,
                "notes": row.notes,
                "output_file_id": row.output_file_id,
                "output_file_path": row.output_file_path,
                "output_file_size": row.output_file_size,
                "output_summary": row.output_summary
            }
            for row in rows
        ]
        
        return runs, total_count