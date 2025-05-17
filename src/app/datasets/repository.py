from typing import List, Optional, Dict, Any, Union, Tuple
from datetime import datetime
import json
from sqlalchemy.ext.asyncio import AsyncSession
import sqlalchemy as sa
from app.datasets.models import (
    Dataset, DatasetCreate, DatasetUpdate, DatasetVersion, DatasetVersionCreate,
    File, FileCreate, Sheet, SheetCreate, SheetMetadata, Tag, TagCreate
)

class DatasetsRepository:
    # SQL Queries
    CREATE_DATASET_SQL = """
    INSERT INTO datasets (name, description, created_by) 
    VALUES (:name, :description, :created_by)
    RETURNING id;
    """

    UPSERT_DATASET_SQL = """
    INSERT INTO datasets (name, description, created_by) 
    VALUES (:name, :description, :created_by)
    ON CONFLICT (id) DO UPDATE 
    SET name = :name,
        description = :description,
        updated_at = NOW()
    RETURNING id;
    """

    CREATE_FILE_SQL = """
    INSERT INTO files (storage_type, file_type, mime_type, file_data, file_path, file_size)
    VALUES (:storage_type, :file_type, :mime_type, :file_data, :file_path, :file_size)
    RETURNING id;
    """

    GET_NEXT_VERSION_NUMBER_SQL = """
    SELECT COALESCE(MAX(version_number), 0) + 1 as next_version
    FROM dataset_versions
    WHERE dataset_id = :dataset_id;
    """

    CREATE_DATASET_VERSION_SQL = """
    INSERT INTO dataset_versions (dataset_id, version_number, file_id, uploaded_by)
    VALUES (:dataset_id, :version_number, :file_id, :uploaded_by)
    RETURNING id;
    """

    UPDATE_DATASET_TIMESTAMP_SQL = """
    UPDATE datasets 
    SET updated_at = NOW() 
    WHERE id = :dataset_id;
    """

    UPSERT_TAG_SQL = """
    INSERT INTO tags (name, description)
    VALUES (:name, :description)
    ON CONFLICT (name) DO UPDATE 
    SET description = :description
    RETURNING id;
    """

    CREATE_DATASET_TAG_SQL = """
    INSERT INTO dataset_tags (dataset_id, tag_id)
    VALUES (:dataset_id, :tag_id)
    ON CONFLICT DO NOTHING;
    """

    CREATE_SHEET_SQL = """
    INSERT INTO sheets (dataset_version_id, name, sheet_index, description)
    VALUES (:dataset_version_id, :name, :sheet_index, :description)
    RETURNING id;
    """

    CREATE_SHEET_METADATA_SQL = """
    INSERT INTO sheet_metadata (sheet_id, metadata, profiling_report_file_id)
    VALUES (:sheet_id, :metadata, :profiling_report_file_id)
    RETURNING id;
    """

    GET_DATASET_SIMPLE_SQL = """
    -- Get a single dataset by ID with its tags and versions (simplified)
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
    WHERE 
        d.id = :dataset_id
    GROUP BY 
        d.id, d.name, d.description, d.created_by, d.created_at, d.updated_at;
    """

    GET_DATASET_VERSIONS_SQL = """
    -- Get versions for a dataset
    SELECT 
        dv.id,
        dv.dataset_id,
        dv.version_number,
        dv.file_id,
        dv.uploaded_by,
        dv.ingestion_timestamp,
        dv.last_updated_timestamp,
        f.storage_type,
        f.file_type,
        f.mime_type,
        f.file_size
    FROM 
        dataset_versions dv
    JOIN files f ON dv.file_id = f.id
    WHERE 
        dv.dataset_id = :dataset_id
    ORDER BY 
        dv.version_number DESC;
    """

    UPDATE_DATASET_SQL = """
    -- Update dataset metadata
    UPDATE datasets
    SET 
        name = COALESCE(:name, name),
        description = COALESCE(:description, description),
        updated_at = NOW()
    WHERE 
        id = :dataset_id
    RETURNING id;
    """

    DELETE_DATASET_TAGS_SQL = """
    -- Delete all tags for a dataset
    DELETE FROM dataset_tags
    WHERE dataset_id = :dataset_id;
    """

    LIST_DATASET_VERSIONS_SIMPLE_SQL = """
    -- List all versions for a specific dataset (simplified)
    SELECT 
        dv.id,
        dv.dataset_id,
        dv.version_number,
        dv.file_id,
        dv.uploaded_by,
        dv.ingestion_timestamp,
        dv.last_updated_timestamp,
        f.storage_type,
        f.file_type,
        f.mime_type,
        f.file_size
    FROM 
        dataset_versions dv
    JOIN files f ON dv.file_id = f.id
    WHERE 
        dv.dataset_id = :dataset_id
    ORDER BY 
        dv.version_number DESC;
    """

    GET_DATASET_VERSION_SIMPLE_SQL = """
    -- Get a single dataset version by ID (simplified)
    SELECT 
        dv.id,
        dv.dataset_id,
        dv.version_number,
        dv.file_id,
        dv.uploaded_by,
        dv.ingestion_timestamp,
        dv.last_updated_timestamp,
        f.storage_type,
        f.file_type,
        f.mime_type,
        f.file_size
    FROM 
        dataset_versions dv
    JOIN files f ON dv.file_id = f.id
    WHERE 
        dv.id = :version_id;
    """

    GET_VERSION_SHEETS_SQL = """
    -- Get sheets for a dataset version
    SELECT 
        s.id,
        s.name,
        s.sheet_index,
        s.description,
        sm.metadata
    FROM 
        sheets s
    LEFT JOIN sheet_metadata sm ON s.id = sm.sheet_id
    WHERE 
        s.dataset_version_id = :version_id
    ORDER BY 
        s.sheet_index;
    """

    DELETE_DATASET_VERSION_SQL = """
    -- Delete a dataset version
    -- This doesn't delete the associated file or sheets to allow for potential recovery
    -- A background job could clean up orphaned files later
    DELETE FROM dataset_versions
    WHERE id = :version_id
    RETURNING id;
    """

    LIST_TAGS_SQL = """
    -- List all tags with their usage count
    SELECT 
        t.id,
        t.name,
        t.description,
        COUNT(dt.dataset_id) AS usage_count
    FROM 
        tags t
    LEFT JOIN dataset_tags dt ON t.id = dt.tag_id
    GROUP BY 
        t.id, t.name, t.description
    ORDER BY 
        t.name;
    """

    LIST_VERSION_SHEETS_SQL = """
    -- Get all sheets for a dataset version
    SELECT 
        id,
        name, 
        sheet_index,
        description
    FROM 
        sheets
    WHERE 
        dataset_version_id = :version_id
    ORDER BY 
        sheet_index;
    """

    LIST_DATASETS_SIMPLE_SQL = """
    -- Simplified list datasets with basic filtering
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
        WHERE 
            (:name IS NULL OR d.name ILIKE '%' || :name || '%')
            AND (:created_by IS NULL OR d.created_by = :created_by)
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

    LIST_DATASETS_SQL = """
    -- List datasets with filtering, sorting, and pagination
    WITH filtered_datasets AS (
        SELECT 
            d.id,
            d.name,
            d.description,
            d.created_by,
            d.created_at,
            d.updated_at,
            array_agg(DISTINCT t.id) FILTER (WHERE t.id IS NOT NULL) AS tag_ids,
            array_agg(DISTINCT t.name) FILTER (WHERE t.name IS NOT NULL) AS tag_names,
            dv.version_number AS current_version,
            f.file_type,
            f.file_size
        FROM 
            datasets d
        LEFT JOIN dataset_tags dt ON d.id = dt.dataset_id
        LEFT JOIN tags t ON dt.tag_id = t.id
        LEFT JOIN dataset_versions dv ON d.id = dv.dataset_id
        LEFT JOIN files f ON dv.file_id = f.id
        WHERE 
            (:name IS NULL OR d.name ILIKE '%' || :name || '%')
            AND (:description IS NULL OR d.description ILIKE '%' || :description || '%')
            AND (:created_by IS NULL OR d.created_by = :created_by)
            AND (:tag IS NULL OR 
                 CASE 
                     WHEN array_length(:tag::text[], 1) > 0 THEN 
                         EXISTS (SELECT 1 FROM dataset_tags dt2 
                                 JOIN tags t2 ON dt2.tag_id = t2.id 
                                 WHERE dt2.dataset_id = d.id 
                                 AND t2.name = ANY(:tag::text[]))
                     ELSE true
                 END)
            AND (:file_type IS NULL OR f.file_type = :file_type)
            AND (:file_size_min IS NULL OR f.file_size >= :file_size_min)
            AND (:file_size_max IS NULL OR f.file_size <= :file_size_max)
            AND (:version_min IS NULL OR dv.version_number >= :version_min)
            AND (:version_max IS NULL OR dv.version_number <= :version_max)
            AND (:created_at_from IS NULL OR d.created_at >= :created_at_from)
            AND (:created_at_to IS NULL OR d.created_at <= :created_at_to)
            AND (:updated_at_from IS NULL OR d.updated_at >= :updated_at_from)
            AND (:updated_at_to IS NULL OR d.updated_at <= :updated_at_to)
        GROUP BY 
            d.id, d.name, d.description, d.created_by, d.created_at, d.updated_at, dv.version_number, f.file_type, f.file_size
    )
    SELECT 
        id,
        name,
        description,
        created_by,
        created_at,
        updated_at,
        tag_ids,
        tag_names,
        current_version,
        file_type,
        file_size
    FROM 
        filtered_datasets
    ORDER BY 
        CASE WHEN :sort_by = 'name' AND :sort_order = 'asc' THEN name END ASC,
        CASE WHEN :sort_by = 'name' AND :sort_order = 'desc' THEN name END DESC,
        CASE WHEN :sort_by = 'created_at' AND :sort_order = 'asc' THEN created_at END ASC,
        CASE WHEN :sort_by = 'created_at' AND :sort_order = 'desc' THEN created_at END DESC,
        CASE WHEN :sort_by = 'updated_at' AND :sort_order = 'asc' THEN updated_at END ASC,
        CASE WHEN :sort_by = 'updated_at' AND :sort_order = 'desc' THEN updated_at END DESC,
        CASE WHEN :sort_by = 'file_size' AND :sort_order = 'asc' THEN file_size END ASC,
        CASE WHEN :sort_by = 'file_size' AND :sort_order = 'desc' THEN file_size END DESC,
        CASE WHEN :sort_by = 'current_version' AND :sort_order = 'asc' THEN current_version END ASC,
        CASE WHEN :sort_by = 'current_version' AND :sort_order = 'desc' THEN current_version END DESC,
        -- Default sort if no valid sort_by is provided or it's null
        CASE WHEN :sort_by IS NULL OR :sort_by NOT IN ('name', 'created_at', 'updated_at', 'file_size', 'current_version') THEN updated_at END DESC
    LIMIT :limit
    OFFSET :offset;
    """

    GET_FILE_SQL = """
    SELECT id, storage_type, file_type, mime_type, file_path, file_size, file_data, created_at 
    FROM files 
    WHERE id = :file_id
    """

    def __init__(self, session: AsyncSession):
        self.session = session

    async def create_dataset(self, dataset: DatasetCreate) -> int:
        query = sa.text(self.CREATE_DATASET_SQL)
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

        query = sa.text(self.UPSERT_DATASET_SQL)
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
        query = sa.text(self.CREATE_FILE_SQL)
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
        query = sa.text(self.GET_NEXT_VERSION_NUMBER_SQL)
        values = {"dataset_id": dataset_id}
        result = await self.session.execute(query, values)
        return result.scalar_one()

    async def create_dataset_version(self, version: DatasetVersionCreate) -> int:
        query = sa.text(self.CREATE_DATASET_VERSION_SQL)
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
        query = sa.text(self.UPDATE_DATASET_TIMESTAMP_SQL)
        values = {"dataset_id": dataset_id}
        await self.session.execute(query, values)
        await self.session.commit()

    async def upsert_tag(self, tag_name: str, description: Optional[str] = None) -> int:
        query = sa.text(self.UPSERT_TAG_SQL)
        values = {
            "name": tag_name,
            "description": description
        }
        result = await self.session.execute(query, values)
        await self.session.commit()
        return result.scalar_one()

    async def create_dataset_tag(self, dataset_id: int, tag_id: int) -> None:
        query = sa.text(self.CREATE_DATASET_TAG_SQL)
        values = {
            "dataset_id": dataset_id,
            "tag_id": tag_id
        }
        await self.session.execute(query, values)
        await self.session.commit()

    async def create_sheet(self, sheet: SheetCreate) -> int:
        query = sa.text(self.CREATE_SHEET_SQL)
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
        query = sa.text(self.CREATE_SHEET_METADATA_SQL)
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
            # For now, just use the SQL directly
            query = sa.text(self.LIST_DATASETS_SQL)
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
        query = sa.text(self.GET_DATASET_SIMPLE_SQL)
        values = {"dataset_id": dataset_id}
        result = await self.session.execute(query, values)
        row = result.mappings().first()
        if not row:
            return None

        # Get versions separately
        versions_query = sa.text(self.GET_DATASET_VERSIONS_SQL)
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
        query = sa.text(self.UPDATE_DATASET_SQL)
        values = {
            "dataset_id": dataset_id,
            "name": data.name,
            "description": data.description
        }
        result = await self.session.execute(query, values)
        await self.session.commit()
        return result.scalar_one_or_none()
        
    async def delete_dataset_tags(self, dataset_id: int) -> None:
        query = sa.text(self.DELETE_DATASET_TAGS_SQL)
        values = {"dataset_id": dataset_id}
        await self.session.execute(query, values)
        await self.session.commit()
        
    async def list_dataset_versions(self, dataset_id: int) -> List[Dict[str, Any]]:
        query = sa.text(self.LIST_DATASET_VERSIONS_SIMPLE_SQL)
        values = {"dataset_id": dataset_id}
        result = await self.session.execute(query, values)
        return [dict(row) for row in result.mappings()]
        
    async def get_dataset_version(self, version_id: int) -> Optional[Dict[str, Any]]:
        query = sa.text(self.GET_DATASET_VERSION_SIMPLE_SQL)
        values = {"version_id": version_id}
        result = await self.session.execute(query, values)
        row = result.mappings().first()
        if not row:
            return None

        version = dict(row)

        # Get sheets for this version
        sheets_query = sa.text(self.GET_VERSION_SHEETS_SQL)
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
        query = sa.text(self.DELETE_DATASET_VERSION_SQL)
        values = {"version_id": version_id}
        result = await self.session.execute(query, values)
        await self.session.commit()
        return result.scalar_one_or_none()
        
    async def list_tags(self) -> List[Dict[str, Any]]:
        query = sa.text(self.LIST_TAGS_SQL)
        result = await self.session.execute(query)
        return [dict(row) for row in result.mappings()]
        
    async def get_file(self, file_id: int) -> Optional[Dict[str, Any]]:
        """Get file information by ID"""
        query = sa.text(self.GET_FILE_SQL)
        result = await self.session.execute(query, {"file_id": file_id})
        row = result.mappings().first()
        return dict(row) if row else None

    async def list_version_sheets(self, version_id: int) -> List[Dict[str, Any]]:
        """Get all sheets for a dataset version"""
        query = sa.text(self.LIST_VERSION_SHEETS_SQL)
        values = {"version_id": version_id}
        result = await self.session.execute(query, values)
        return [dict(row) for row in result.mappings()]