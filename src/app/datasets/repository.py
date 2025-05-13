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

    # Dataset listing methods
    async def _build_dataset_query(
        self,
        limit: int = 10,
        offset: int = 0,
        filters: Optional[Dict[str, Any]] = None,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = "desc"
    ) -> Tuple[str, Dict[str, Any]]:
        """Build a SQL query for listing datasets with various filters"""
        # Start building the base query
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
        
        # Process filters
        params = {"limit": limit, "offset": offset}
        where_clauses = []
        
        if filters:
            for key, value in filters.items():
                if value is not None:
                    # Handle special cases
                    if key == "name" or key == "description":
                        where_clauses.append(f"d.{key} ILIKE '%' || :{key} || '%'")
                        params[key] = value
                    elif key == "created_by":
                        where_clauses.append("d.created_by = :created_by")
                        params["created_by"] = value
                    elif key == "created_at_from":
                        where_clauses.append("d.created_at >= :created_at_from")
                        params["created_at_from"] = value
                    elif key == "created_at_to":
                        where_clauses.append("d.created_at <= :created_at_to")
                        params["created_at_to"] = value
                    elif key == "updated_at_from":
                        where_clauses.append("d.updated_at >= :updated_at_from")
                        params["updated_at_from"] = value
                    elif key == "updated_at_to":
                        where_clauses.append("d.updated_at <= :updated_at_to")
                        params["updated_at_to"] = value
                    elif key == "tags" and isinstance(value, list) and len(value) > 0:
                        tag_placeholders = [f":tag_{i}" for i in range(len(value))]
                        for i, tag in enumerate(value):
                            params[f"tag_{i}"] = tag
                        where_clauses.append(f"t.name IN ({', '.join(tag_placeholders)})")
        
        # Add WHERE clause if filters are present
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
        """
        
        # Add sorting
        if sort_by in ["name", "created_at", "updated_at", "current_version"]:
            order_col = "dd." + sort_by if sort_by != "current_version" else "dv.version_number"
            sql += f"\nORDER BY {order_col} {sort_order.upper()}"
        elif sort_by == "file_size":
            sql += f"\nORDER BY f.file_size {sort_order.upper()}"
        else:  # Default sort
            sql += "\nORDER BY dd.updated_at DESC"
        
        # Add pagination
        sql += "\nLIMIT :limit OFFSET :offset;"
        
        return sql, params
    
    async def _execute_dataset_query(self, sql: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Execute a dataset query and return the results"""
        query = sa.text(sql)
        result = await self.session.execute(query, params)
        return [dict(row) for row in result.mappings()]
    
    # Dataset listing with full filtering
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
        filters = {
            "name": name,
            "description": description,
            "created_by": created_by,
            "tags": tags,
            "created_at_from": created_at_from,
            "created_at_to": created_at_to,
            "updated_at_from": updated_at_from,
            "updated_at_to": updated_at_to
        }
        
        # Filter out None values
        filters = {k: v for k, v in filters.items() if v is not None}
        
        sql, params = await self._build_dataset_query(
            limit=limit,
            offset=offset,
            filters=filters,
            sort_by=sort_by,
            sort_order=sort_order if sort_order else "desc"
        )
        
        # Add additional specialized filters
        # (These would need to be added to the query string directly)
        if file_type or file_size_min or file_size_max or version_min or version_max:
            # For now, just use the SQL file for the full complex query
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
        else:
            # Use the dynamically built query for simpler cases
            return await self._execute_dataset_query(sql, params)

    async def get_dataset(self, dataset_id: int) -> Optional[Dict[str, Any]]:
        """Get a dataset by ID including versions and tags"""
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
        """Get file information by ID"""
        query = sa.text("""
        SELECT id, storage_type, file_type, mime_type, file_path, file_size, file_data, created_at 
        FROM files 
        WHERE id = :file_id
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