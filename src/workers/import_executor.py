"""Executor for import jobs."""

import os
import csv
import json
import hashlib
from typing import Dict, Any, List
from datetime import datetime

from src.workers.job_worker import JobExecutor
from src.core.database import DatabasePool
from src.core.infrastructure.services import FileParserFactory
from src.core.services.table_analyzer import TableAnalyzer


class ImportJobExecutor(JobExecutor):
    """Executes import jobs by processing uploaded files."""
    
    def __init__(self):
        self.parser_factory = FileParserFactory()
        self.table_analyzer = TableAnalyzer()
    
    async def execute(self, job_id: str, parameters: Dict[str, Any], db_pool: DatabasePool) -> Dict[str, Any]:
        """Execute import job."""
        import logging
        logger = logging.getLogger(__name__)
        
        logger.info(f"Import job {job_id} - parameters type: {type(parameters)}, value: {parameters}")
        
        # Handle case where parameters come as string
        if isinstance(parameters, str):
            import json
            parameters = json.loads(parameters)
        
        # Check if this is actually a SQL transform job
        if 'workbench_context' in parameters and parameters.get('workbench_context', {}).get('operation_type') == 'sql_transform':
            # Delegate to SQL transform executor
            from .sql_transform_executor import SqlTransformExecutor
            sql_executor = SqlTransformExecutor()
            return await sql_executor.execute(job_id, parameters, db_pool)
        
        # Extract parameters
        temp_file_path = parameters['temp_file_path']
        filename = parameters['filename']
        commit_message = parameters['commit_message']
        target_ref = parameters['target_ref']
        
        # Get job details from database
        async with db_pool.acquire() as conn:
            job = await conn.fetchrow(
                "SELECT dataset_id, user_id, source_commit_id FROM dsa_jobs.analysis_runs WHERE id = $1",
                job_id
            )
            
            dataset_id = job['dataset_id']
            user_id = job['user_id']
            parent_commit_id = job['source_commit_id']
        
        try:
            # Parse the file
            rows = await self._parse_file(temp_file_path, filename)
            
            # Create new commit
            async with db_pool.acquire() as conn:
                # Begin transaction
                async with conn.transaction():
                    # Create commit
                    commit_id = await self._create_commit(
                        conn, dataset_id, parent_commit_id, user_id, commit_message
                    )
                    
                    # Process and store rows
                    stats = await self._store_rows(conn, commit_id, rows)
                    
                    # Store schema
                    schema = self._extract_schema(rows)
                    await self._store_schema(conn, commit_id, schema)
                    
                    # Update ref to point to new commit
                    await self._update_ref(conn, dataset_id, target_ref, commit_id)
                    
                    # Analyze tables and store comprehensive analysis
                    await self.table_analyzer.analyze_imported_tables(conn, commit_id, rows)
            
            # Refresh search index after successful import
            from src.core.infrastructure.postgres.search_repository import PostgresSearchRepository
            async with db_pool.acquire() as conn:
                search_repo = PostgresSearchRepository(conn)
                await search_repo.refresh_search_index()
            
            # Clean up temp file
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
            
            return {
                "commit_id": commit_id,
                "rows_imported": stats['row_count'],
                "message": f"Successfully imported {stats['row_count']} rows"
            }
            
        except Exception as e:
            # Clean up temp file on error
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
            raise
    
    async def _parse_file(self, file_path: str, filename: str) -> List[Dict[str, Any]]:
        """Parse uploaded file into row dictionaries."""
        rows = []
        
        # Determine file type
        file_ext = os.path.splitext(filename)[1].lower()
        
        if file_ext == '.csv':
            # Parse CSV file
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                sheet_name = os.path.splitext(filename)[0]
                for idx, row_data in enumerate(reader):
                    # Create row dict
                    row = {
                        "sheet_name": sheet_name,
                        "row_number": idx + 2,  # +2 because row 1 is header
                        "data": row_data
                    }
                    rows.append(row)
        
        elif file_ext in ['.xlsx', '.xls']:
            # Parse Excel file
            import pandas as pd
            
            # Read all sheets
            excel_file = pd.ExcelFile(file_path)
            
            for sheet_name in excel_file.sheet_names:
                # Read each sheet
                df = pd.read_excel(excel_file, sheet_name=sheet_name)
                
                # Convert NaN to None and ensure all values are JSON-serializable
                df = df.where(pd.notnull(df), None)
                
                # Convert each row to dictionary
                for idx, row_data in df.iterrows():
                    # Convert row to dict and ensure all values are serializable
                    row_dict = {}
                    for col, value in row_data.items():
                        if pd.isna(value):
                            row_dict[str(col)] = None
                        elif isinstance(value, (pd.Timestamp, datetime)):
                            row_dict[str(col)] = value.isoformat()
                        else:
                            row_dict[str(col)] = value
                    
                    row = {
                        "sheet_name": sheet_name,
                        "row_number": idx + 2,  # +2 because row 1 is header
                        "data": row_dict
                    }
                    rows.append(row)
        
        else:
            raise ValueError(f"Unsupported file format: {file_ext}")
        
        return rows
    
    async def _create_commit(
        self, conn, dataset_id: int, parent_commit_id: str, 
        user_id: int, message: str
    ) -> str:
        """Create a new commit."""
        # Generate commit ID (timestamp + random for simplicity)
        import uuid
        commit_id = f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"
        
        await conn.execute("""
            INSERT INTO dsa_core.commits (commit_id, dataset_id, parent_commit_id, author_id, message)
            VALUES ($1, $2, $3, $4, $5)
        """, commit_id, dataset_id, parent_commit_id, user_id, message)
        
        return commit_id
    
    async def _store_rows(self, conn, commit_id: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Store rows and create commit_rows associations."""
        row_count = 0
        unique_sheets = set()
        
        for row in rows:
            # Calculate row hash
            row_json = json.dumps(row, sort_keys=True)
            row_hash = hashlib.sha256(row_json.encode()).hexdigest()
            
            # Insert or get existing row
            existing = await conn.fetchrow(
                "SELECT row_hash FROM dsa_core.rows WHERE row_hash = $1",
                row_hash
            )
            
            if not existing:
                await conn.execute("""
                    INSERT INTO dsa_core.rows (row_hash, data)
                    VALUES ($1, $2)
                """, row_hash, json.dumps(row))
            
            # Create commit_row association with logical_row_id
            # Use sheet_name:row_index format for proper table separation
            sheet_name = row['sheet_name']
            row_index = row['row_number'] - 2  # Convert back to 0-based index
            logical_row_id = f"{sheet_name}:{row_index}"
            await conn.execute("""
                INSERT INTO dsa_core.commit_rows (commit_id, logical_row_id, row_hash)
                VALUES ($1, $2, $3)
                ON CONFLICT DO NOTHING
            """, commit_id, logical_row_id, row_hash)
            
            row_count += 1
            unique_sheets.add(row["sheet_name"])
        
        return {
            "row_count": row_count,
            "sheet_count": len(unique_sheets)
        }
    
    def _extract_schema(self, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Extract schema from rows."""
        schema = {}
        
        for row in rows:
            sheet = row["sheet_name"]
            if sheet not in schema:
                schema[sheet] = {
                    "columns": list(row["data"].keys()),
                    "row_count": 0
                }
            schema[sheet]["row_count"] += 1
        
        return schema
    
    async def _store_schema(self, conn, commit_id: str, schema: Dict[str, Any]):
        """Store commit schema."""
        await conn.execute("""
            INSERT INTO dsa_core.commit_schemas (commit_id, schema_definition)
            VALUES ($1, $2)
        """, commit_id, json.dumps(schema))
    
    async def _update_ref(self, conn, dataset_id: int, ref_name: str, commit_id: str):
        """Update ref to point to new commit."""
        await conn.execute("""
            UPDATE dsa_core.refs
            SET commit_id = $1
            WHERE dataset_id = $2 AND name = $3
        """, commit_id, dataset_id, ref_name)