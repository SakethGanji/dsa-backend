from typing import List, Optional, Dict, Any, Union, Tuple
from datetime import datetime
import json
from sqlalchemy.ext.asyncio import AsyncSession
import sqlalchemy as sa
from app.datasets.models import (
    Dataset, DatasetCreate, DatasetUpdate, DatasetVersion, DatasetVersionCreate,
    File, FileCreate, Sheet, SheetCreate, SheetMetadata, Tag, TagCreate
)
import os

class DatasetsRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create_dataset(self, dataset: DatasetCreate) -> int:
        query = sa.text(open(os.path.join(os.path.dirname(__file__), "sql/create_dataset.sql")).read())
        values = {
            "name": dataset.name,
            "description": dataset.description,
            "created_by": dataset.created_by
        }
        result = await self.session.execute(query, values)
        await self.session.commit()
        return result.scalar_one()

    async def upsert_dataset(self, dataset_id: Optional[int], dataset: DatasetCreate) -> int:
        if dataset_id is None:
            return await self.create_dataset(dataset)

        query = sa.text(open(os.path.join(os.path.dirname(__file__), "sql/upsert_dataset.sql")).read())
        values = {
            "id": dataset_id,
            "name": dataset.name,
            "description": dataset.description,
            "created_by": dataset.created_by
        }
        result = await self.session.execute(query, values)
        await self.session.commit()
        return result.scalar_one()

    async def create_file(self, file: FileCreate) -> int:
        query = sa.text(open(os.path.join(os.path.dirname(__file__), "sql/create_file.sql")).read())
        values = {
            "storage_type": file.storage_type,
            "file_type": file.file_type,
            "mime_type": file.mime_type,
            "file_data": file.file_data,
            "file_path": file.file_path,
            "file_size": file.file_size
        }
        result = await self.session.execute(query, values)
        await self.session.commit()
        return result.scalar_one()

    async def get_next_version_number(self, dataset_id: int) -> int:
        query = sa.text(open(os.path.join(os.path.dirname(__file__), "sql/get_next_version_number.sql")).read())
        values = {"dataset_id": dataset_id}
        result = await self.session.execute(query, values)
        return result.scalar_one()

    async def create_dataset_version(self, version: DatasetVersionCreate) -> int:
        query = sa.text(open(os.path.join(os.path.dirname(__file__), "sql/create_dataset_version.sql")).read())
        values = {
            "dataset_id": version.dataset_id,
            "version_number": version.version_number,
            "file_id": version.file_id,
            "uploaded_by": version.uploaded_by
        }
        result = await self.session.execute(query, values)
        await self.session.commit()
        return result.scalar_one()

    async def update_dataset_timestamp(self, dataset_id: int) -> None:
        query = sa.text(open(os.path.join(os.path.dirname(__file__), "sql/update_dataset_timestamp.sql")).read())
        values = {"dataset_id": dataset_id}
        await self.session.execute(query, values)
        await self.session.commit()

    async def upsert_tag(self, tag_name: str, description: Optional[str] = None) -> int:
        query = sa.text(open(os.path.join(os.path.dirname(__file__), "sql/upsert_tag.sql")).read())
        values = {
            "name": tag_name,
            "description": description
        }
        result = await self.session.execute(query, values)
        await self.session.commit()
        return result.scalar_one()

    async def create_dataset_tag(self, dataset_id: int, tag_id: int) -> None:
        query = sa.text(open(os.path.join(os.path.dirname(__file__), "sql/create_dataset_tag.sql")).read())
        values = {
            "dataset_id": dataset_id,
            "tag_id": tag_id
        }
        await self.session.execute(query, values)
        await self.session.commit()

    async def create_sheet(self, sheet: SheetCreate) -> int:
        query = sa.text(open(os.path.join(os.path.dirname(__file__), "sql/create_sheet.sql")).read())
        values = {
            "dataset_version_id": sheet.dataset_version_id,
            "name": sheet.name,
            "sheet_index": sheet.sheet_index,
            "description": sheet.description
        }
        result = await self.session.execute(query, values)
        await self.session.commit()
        return result.scalar_one()

    async def create_sheet_metadata(self, sheet_id: int, metadata: Dict[str, Any], profiling_report_file_id: Optional[int] = None) -> int:
        query = sa.text(open(os.path.join(os.path.dirname(__file__), "sql/create_sheet_metadata.sql")).read())
        values = {
            "sheet_id": sheet_id,
            "metadata": json.dumps(metadata),
            "profiling_report_file_id": profiling_report_file_id
        }
        result = await self.session.execute(query, values)
        await self.session.commit()
        return result.scalar_one()

    # Simple method to list datasets with minimal filtering
    async def list_datasets_very_simple(
        self,
        limit: int = 10,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        query = sa.text("""
        WITH dataset_data AS (
            SELECT 
                d.id,
                d.name,
                d.description,
                d.created_by,
                d.created_at,
                d.updated_at,
                array_agg(DISTINCT t.id) FILTER (WHERE t.id IS NOT NULL) AS tag_ids,
                array_agg(DISTINCT t.name) FILTER (WHERE t.name IS NOT NULL) AS tag_names
            FROM 
                datasets d
            LEFT JOIN dataset_tags dt ON d.id = dt.dataset_id
            LEFT JOIN tags t ON dt.tag_id = t.id
            GROUP BY 
                d.id, d.name, d.description, d.created_by, d.created_at, d.updated_at
        )
        SELECT 
            dd.id,
            dd.name,
            dd.description,
            dd.created_by,
            dd.created_at,
            dd.updated_at,
            dd.tag_ids,
            dd.tag_names,
            dv.version_number as current_version,
            f.file_type,
            f.file_size
        FROM 
            dataset_data dd
        LEFT JOIN LATERAL (
            SELECT version_number, file_id
            FROM dataset_versions
            WHERE dataset_id = dd.id
            ORDER BY version_number DESC
            LIMIT 1
        ) dv ON true
        LEFT JOIN files f ON dv.file_id = f.id
        ORDER BY 
            dd.updated_at DESC
        LIMIT :limit
        OFFSET :offset;
        """)
        result = await self.session.execute(query, {"limit": limit, "offset": offset})
        return [dict(row) for row in result.mappings()]
        
    async def list_datasets_simple(
        self,
        limit: int = 10,
        offset: int = 0,
        name: Optional[str] = None,
        created_by: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        # Create base query
        sql = """
        WITH dataset_data AS (
            SELECT 
                d.id,
                d.name,
                d.description,
                d.created_by,
                d.created_at,
                d.updated_at,
                array_agg(DISTINCT t.id) FILTER (WHERE t.id IS NOT NULL) AS tag_ids,
                array_agg(DISTINCT t.name) FILTER (WHERE t.name IS NOT NULL) AS tag_names
            FROM 
                datasets d
            LEFT JOIN dataset_tags dt ON d.id = dt.dataset_id
            LEFT JOIN tags t ON dt.tag_id = t.id
        """
        
        # Add WHERE clause conditionally
        where_clauses = []
        params = {"limit": limit, "offset": offset}
        
        if name is not None:
            where_clauses.append("d.name ILIKE '%' || :name || '%'")
            params["name"] = name
            
        if created_by is not None:
            where_clauses.append("d.created_by = :created_by")
            params["created_by"] = created_by
            
        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)
            
        # Complete the query
        sql += """
            GROUP BY 
                d.id, d.name, d.description, d.created_by, d.created_at, d.updated_at
        )
        SELECT 
            dd.id,
            dd.name,
            dd.description,
            dd.created_by,
            dd.created_at,
            dd.updated_at,
            dd.tag_ids,
            dd.tag_names,
            dv.version_number as current_version,
            f.file_type,
            f.file_size
        FROM 
            dataset_data dd
        LEFT JOIN LATERAL (
            SELECT version_number, file_id
            FROM dataset_versions
            WHERE dataset_id = dd.id
            ORDER BY version_number DESC
            LIMIT 1
        ) dv ON true
        LEFT JOIN files f ON dv.file_id = f.id
        ORDER BY 
            dd.updated_at DESC
        LIMIT :limit
        OFFSET :offset;
        """
        
        query = sa.text(sql)
        result = await self.session.execute(query, params)
        return [dict(row) for row in result.mappings()]

    # Original methods for listing and retrieving datasets and versions
    async def list_datasets(
        self, 
        limit: int = 10, 
        offset: int = 0,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = None,
        name: Optional[str] = None,
        description: Optional[str] = None,
        created_by: Optional[int] = None,
        tags: Optional[List[str]] = None,
        file_type: Optional[str] = None,
        file_size_min: Optional[int] = None,
        file_size_max: Optional[int] = None,
        version_min: Optional[int] = None,
        version_max: Optional[int] = None,
        created_at_from: Optional[datetime] = None,
        created_at_to: Optional[datetime] = None,
        updated_at_from: Optional[datetime] = None,
        updated_at_to: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        query = sa.text(open(os.path.join(os.path.dirname(__file__), "sql/list_datasets.sql")).read())
        values = {
            "limit": limit,
            "offset": offset,
            "sort_by": sort_by,
            "sort_order": sort_order.lower() if sort_order else "desc",
            "name": name,
            "description": description,
            "created_by": created_by,
            "tag": tags,
            "file_type": file_type,
            "file_size_min": file_size_min,
            "file_size_max": file_size_max,
            "version_min": version_min,
            "version_max": version_max,
            "created_at_from": created_at_from,
            "created_at_to": created_at_to,
            "updated_at_from": updated_at_from,
            "updated_at_to": updated_at_to
        }
        result = await self.session.execute(query, values)
        return [dict(row) for row in result.mappings()]

    async def get_dataset_simple(self, dataset_id: int) -> Optional[Dict[str, Any]]:
        query = sa.text(open(os.path.join(os.path.dirname(__file__), "sql/get_dataset_simple.sql")).read())
        values = {"dataset_id": dataset_id}
        result = await self.session.execute(query, values)
        row = result.mappings().first()
        if not row:
            return None

        # Get versions separately
        versions_query = sa.text(open(os.path.join(os.path.dirname(__file__), "sql/get_dataset_versions.sql")).read())
        versions_result = await self.session.execute(versions_query, {"dataset_id": dataset_id})
        versions = [dict(v) for v in versions_result.mappings()]

        # Combine results
        dataset = dict(row)
        dataset["versions"] = versions

        # Transform tag arrays into objects
        tags = []
        if dataset.get("tag_ids") and dataset.get("tag_names"):
            for tag_id, tag_name in zip(dataset["tag_ids"], dataset["tag_names"]):
                tags.append({"id": tag_id, "name": tag_name})
        dataset["tags"] = tags

        return dataset

    async def get_dataset(self, dataset_id: int) -> Optional[Dict[str, Any]]:
        # Use the simple version instead
        return await self.get_dataset_simple(dataset_id)
        
    async def update_dataset(
        self, 
        dataset_id: int, 
        data: DatasetUpdate
    ) -> Optional[int]:
        query = sa.text(open(os.path.join(os.path.dirname(__file__), "sql/update_dataset.sql")).read())
        values = {
            "dataset_id": dataset_id,
            "name": data.name,
            "description": data.description
        }
        result = await self.session.execute(query, values)
        await self.session.commit()
        return result.scalar_one_or_none()
        
    async def delete_dataset_tags(self, dataset_id: int) -> None:
        query = sa.text(open(os.path.join(os.path.dirname(__file__), "sql/delete_dataset_tags.sql")).read())
        values = {"dataset_id": dataset_id}
        await self.session.execute(query, values)
        await self.session.commit()
        
    async def list_dataset_versions(self, dataset_id: int) -> List[Dict[str, Any]]:
        query = sa.text(open(os.path.join(os.path.dirname(__file__), "sql/list_dataset_versions_simple.sql")).read())
        values = {"dataset_id": dataset_id}
        result = await self.session.execute(query, values)
        return [dict(row) for row in result.mappings()]
        
    async def get_dataset_version(self, version_id: int) -> Optional[Dict[str, Any]]:
        query = sa.text(open(os.path.join(os.path.dirname(__file__), "sql/get_dataset_version_simple.sql")).read())
        values = {"version_id": version_id}
        result = await self.session.execute(query, values)
        row = result.mappings().first()
        if not row:
            return None

        version = dict(row)

        # Get sheets for this version
        sheets_query = sa.text(open(os.path.join(os.path.dirname(__file__), "sql/get_version_sheets.sql")).read())
        sheets_result = await self.session.execute(sheets_query, {"version_id": version_id})
        sheets = [dict(s) for s in sheets_result.mappings()]

        # Parse metadata JSON if needed
        for sheet in sheets:
            if sheet.get("metadata") and isinstance(sheet["metadata"], str):
                try:
                    sheet["metadata"] = json.loads(sheet["metadata"])
                except:
                    pass

        version["sheets"] = sheets
        return version
        
    async def delete_dataset_version(self, version_id: int) -> Optional[int]:
        query = sa.text(open(os.path.join(os.path.dirname(__file__), "sql/delete_dataset_version.sql")).read())
        values = {"version_id": version_id}
        result = await self.session.execute(query, values)
        await self.session.commit()
        return result.scalar_one_or_none()
        
    async def list_tags(self) -> List[Dict[str, Any]]:
        query = sa.text(open(os.path.join(os.path.dirname(__file__), "sql/list_tags.sql")).read())
        result = await self.session.execute(query)
        return [dict(row) for row in result.mappings()]
        
    async def get_file(self, file_id: int) -> Optional[Dict[str, Any]]:
        query = sa.text("""
        SELECT * FROM files WHERE id = :file_id
        """)
        result = await self.session.execute(query, {"file_id": file_id})
        row = result.mappings().first()
        return dict(row) if row else None

    async def list_version_sheets(self, version_id: int) -> List[Dict[str, Any]]:
        """Get all sheets for a dataset version"""
        query = sa.text(open(os.path.join(os.path.dirname(__file__), "sql/list_version_sheets.sql")).read())
        values = {"version_id": version_id}
        result = await self.session.execute(query, values)
        return [dict(row) for row in result.mappings()]