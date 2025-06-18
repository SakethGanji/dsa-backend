"""Unified repository for dataset operations"""
from typing import List, Optional, Dict, Any
from datetime import datetime
import json
from sqlalchemy.ext.asyncio import AsyncSession
import sqlalchemy as sa

from app.datasets.models import (
    Dataset, DatasetCreate, DatasetUpdate, DatasetVersion, DatasetVersionCreate,
    File, FileCreate, Sheet, SheetCreate, SheetMetadata, Tag, SchemaVersion, SchemaVersionCreate,
    VersionFile, VersionFileCreate, DatasetPointer, DatasetPointerCreate, DatasetPointerUpdate
)


class DatasetsRepository:
    """Repository for all dataset-related database operations"""
    
    def __init__(self, session: AsyncSession):
        self.session = session
    
    # Dataset operations
    async def create_dataset(self, dataset: DatasetCreate) -> int:
        query = sa.text("""
        INSERT INTO datasets (name, description, created_by) 
        VALUES (:name, :description, :created_by)
        RETURNING id;
        """)
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
        
        query = sa.text("""
        INSERT INTO datasets (name, description, created_by) 
        VALUES (:name, :description, :created_by)
        ON CONFLICT (id) DO UPDATE 
        SET name = :name,
            description = :description,
            updated_at = NOW()
        RETURNING id;
        """)
        values = {
            "id": dataset_id,
            "name": dataset.name,
            "description": dataset.description,
            "created_by": dataset.created_by
        }
        result = await self.session.execute(query, values)
        await self.session.commit()
        return result.scalar_one()
    
    async def get_dataset(self, dataset_id: int) -> Optional[Dataset]:
        """Get a dataset by ID including versions and tags"""
        query = sa.text("""
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
        """)
        values = {"dataset_id": dataset_id}
        result = await self.session.execute(query, values)
        dataset_row = result.mappings().first()
        
        if not dataset_row:
            return None
        
        dataset_dict = dict(dataset_row)
        
        # Get versions separately
        versions_query = sa.text("""
        SELECT 
            dv.id,
            dv.dataset_id,
            dv.version_number,
            dv.file_id,
            dv.uploaded_by,
            dv.ingestion_timestamp,
            dv.last_updated_timestamp,
            dv.parent_version_id,
            dv.message,
            dv.overlay_file_id,
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
        """)
        versions_result = await self.session.execute(versions_query, {"dataset_id": dataset_id})
        versions_list: List[DatasetVersion] = []
        for v_row_data in versions_result.mappings():
            v_dict = dict(v_row_data)
            versions_list.append(DatasetVersion(
                id=v_dict["id"],
                dataset_id=v_dict["dataset_id"],
                version_number=v_dict["version_number"],
                file_id=v_dict["file_id"],
                uploaded_by=v_dict["uploaded_by"],
                ingestion_timestamp=v_dict["ingestion_timestamp"],
                last_updated_timestamp=v_dict["last_updated_timestamp"],
                parent_version_id=v_dict.get("parent_version_id"),
                message=v_dict.get("message"),
                overlay_file_id=v_dict.get("overlay_file_id"),
                file_type=v_dict.get("file_type"),
                file_size=v_dict.get("file_size"),
                sheets=None
            ))
        
        tag_objects: List[Tag] = []
        if dataset_dict.get("tag_ids") and dataset_dict.get("tag_names"):
            for tag_id, tag_name in zip(dataset_dict["tag_ids"], dataset_dict["tag_names"]):
                tag_objects.append(Tag(id=tag_id, name=tag_name, usage_count=None))
        
        # Populate current_version, file_type, file_size from the latest version
        current_version_num: Optional[int] = None
        latest_file_type: Optional[str] = None
        latest_file_size: Optional[int] = None
        if versions_list:
            latest_version = versions_list[0]
            current_version_num = latest_version.version_number
            latest_file_type = latest_version.file_type
            latest_file_size = latest_version.file_size
        
        return Dataset(
            id=dataset_dict["id"],
            name=dataset_dict["name"],
            description=dataset_dict.get("description"),
            created_by=dataset_dict["created_by"],
            created_at=dataset_dict["created_at"],
            updated_at=dataset_dict["updated_at"],
            tags=tag_objects if tag_objects else None,
            versions=versions_list if versions_list else None,
            current_version=current_version_num,
            file_type=latest_file_type,
            file_size=latest_file_size
        )
    
    async def update_dataset(self, dataset_id: int, data: DatasetUpdate) -> Optional[int]:
        query = sa.text("""
        UPDATE datasets
        SET 
            name = COALESCE(:name, name),
            description = COALESCE(:description, description),
            updated_at = NOW()
        WHERE 
            id = :dataset_id
        RETURNING id;
        """)
        values = {
            "dataset_id": dataset_id,
            "name": data.name,
            "description": data.description
        }
        result = await self.session.execute(query, values)
        await self.session.commit()
        return result.scalar_one_or_none()
    
    async def update_dataset_timestamp(self, dataset_id: int) -> None:
        query = sa.text("UPDATE datasets SET updated_at = NOW() WHERE id = :dataset_id;")
        values = {"dataset_id": dataset_id}
        await self.session.execute(query, values)
        await self.session.commit()
    
    async def delete_dataset(self, dataset_id: int) -> Optional[int]:
        """Delete a dataset"""
        query = sa.text("""
        DELETE FROM datasets
        WHERE id = :dataset_id
        RETURNING id;
        """)
        values = {"dataset_id": dataset_id}
        result = await self.session.execute(query, values)
        await self.session.commit()
        return result.scalar_one_or_none()
    
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
        **kwargs  # Accept additional filters
    ) -> List[Dataset]:
        # Build dynamic query
        query = """
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
            WHERE TRUE
        """
        
        params = {
            "limit": limit,
            "offset": offset
        }
        
        # Add filters
        if name:
            query += " AND d.name ILIKE :name_pattern"
            params["name_pattern"] = f"%{name}%"
        if description:
            query += " AND d.description ILIKE :desc_pattern"
            params["desc_pattern"] = f"%{description}%"
        if created_by is not None:
            query += " AND d.created_by = :created_by"
            params["created_by"] = created_by
        if tags:
            query += """ AND EXISTS (
                SELECT 1 FROM dataset_tags dt2 
                JOIN tags t2 ON dt2.tag_id = t2.id 
                WHERE dt2.dataset_id = d.id 
                AND t2.name = ANY(:tags)
            )"""
            params["tags"] = tags
        
        query += """
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
        sort_mapping = {
            "name": "dd.name",
            "created_at": "dd.created_at",
            "updated_at": "dd.updated_at",
            "file_size": "f.file_size",
            "current_version": "dv.version_number"
        }
        
        sort_column = sort_mapping.get(sort_by, "dd.updated_at")
        sort_direction = "ASC" if sort_order and sort_order.lower() == "asc" else "DESC"
        query += f" ORDER BY {sort_column} {sort_direction}"
        
        # Add pagination
        query += " LIMIT :limit OFFSET :offset;"
        
        # Execute query
        result = await self.session.execute(sa.text(query), params)
        raw_results = [dict(row) for row in result.mappings()]
        
        datasets: List[Dataset] = []
        for row_dict in raw_results:
            tag_objects = []
            if row_dict.get("tag_ids") and row_dict.get("tag_names"):
                for tag_id, tag_name_val in zip(row_dict["tag_ids"], row_dict["tag_names"]):
                    tag_objects.append(Tag(id=tag_id, name=tag_name_val, usage_count=None))
            
            dataset_obj = Dataset(
                id=row_dict["id"],
                name=row_dict["name"],
                description=row_dict.get("description"),
                created_by=row_dict["created_by"],
                created_at=row_dict["created_at"],
                updated_at=row_dict["updated_at"],
                tags=tag_objects if tag_objects else None,
                current_version=row_dict.get("current_version"),
                file_type=row_dict.get("file_type"),
                file_size=row_dict.get("file_size"),
                versions=None
            )
            datasets.append(dataset_obj)
        return datasets
    
    # Version operations
    async def get_next_version_number(self, dataset_id: int) -> int:
        query = sa.text("""
        SELECT COALESCE(MAX(version_number), 0) + 1 as next_version
        FROM dataset_versions
        WHERE dataset_id = :dataset_id;
        """)
        values = {"dataset_id": dataset_id}
        result = await self.session.execute(query, values)
        return result.scalar_one()
    
    async def create_dataset_version(self, version: DatasetVersionCreate) -> int:
        query = sa.text("""
        INSERT INTO dataset_versions (
            dataset_id, version_number, file_id, uploaded_by,
            parent_version_id, message, overlay_file_id
        )
        VALUES (
            :dataset_id, :version_number, :file_id, :uploaded_by,
            :parent_version_id, :message, :overlay_file_id
        )
        RETURNING id;
        """)
        values = {
            "dataset_id": version.dataset_id,
            "version_number": version.version_number,
            "file_id": version.file_id,
            "uploaded_by": version.uploaded_by,
            "parent_version_id": version.parent_version_id,
            "message": version.message,
            "overlay_file_id": version.overlay_file_id
        }
        result = await self.session.execute(query, values)
        await self.session.commit()
        return result.scalar_one()
    
    async def list_dataset_versions(self, dataset_id: int) -> List[DatasetVersion]:
        query = sa.text("""
        SELECT 
            dv.id,
            dv.dataset_id,
            dv.version_number,
            dv.file_id,
            dv.uploaded_by,
            dv.ingestion_timestamp,
            dv.last_updated_timestamp,
            dv.parent_version_id,
            dv.message,
            dv.overlay_file_id,
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
        """)
        values = {"dataset_id": dataset_id}
        result = await self.session.execute(query, values)
        
        versions_list: List[DatasetVersion] = []
        for row_data in result.mappings():
            row_dict = dict(row_data)
            versions_list.append(DatasetVersion(
                id=row_dict["id"],
                dataset_id=row_dict["dataset_id"],
                version_number=row_dict["version_number"],
                file_id=row_dict["file_id"],
                uploaded_by=row_dict["uploaded_by"],
                ingestion_timestamp=row_dict["ingestion_timestamp"],
                last_updated_timestamp=row_dict["last_updated_timestamp"],
                parent_version_id=row_dict.get("parent_version_id"),
                message=row_dict.get("message"),
                overlay_file_id=row_dict.get("overlay_file_id"),
                file_type=row_dict.get("file_type"),
                file_size=row_dict.get("file_size"),
                sheets=None
            ))
        return versions_list
    
    async def get_dataset_version(self, version_id: int) -> Optional[DatasetVersion]:
        query = sa.text("""
        SELECT 
            dv.id,
            dv.dataset_id,
            dv.version_number,
            dv.file_id,
            dv.uploaded_by,
            dv.ingestion_timestamp,
            dv.last_updated_timestamp,
            dv.parent_version_id,
            dv.message,
            dv.overlay_file_id,
            f.storage_type,
            f.file_type,
            f.mime_type,
            f.file_size
        FROM 
            dataset_versions dv
        JOIN files f ON dv.file_id = f.id
        WHERE 
            dv.id = :version_id;
        """)
        values = {"version_id": version_id}
        result = await self.session.execute(query, values)
        version_row = result.mappings().first()
        
        if not version_row:
            return None
        
        version_dict = dict(version_row)
        
        # Get sheets for this version
        sheets_query = sa.text("""
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
        """)
        sheets_result = await self.session.execute(sheets_query, {"version_id": version_id})
        sheets_list: List[Sheet] = []
        for s_row_data in sheets_result.mappings():
            s_dict = dict(s_row_data)
            sheet_metadata_obj: Optional[SheetMetadata] = None
            if "metadata" in s_dict and s_dict["metadata"] is not None:
                try:
                    parsed_meta = json.loads(s_dict["metadata"]) if isinstance(s_dict["metadata"], str) else s_dict["metadata"]
                    sheet_metadata_obj = SheetMetadata(metadata=parsed_meta, profiling_report_file_id=None)
                except json.JSONDecodeError:
                    sheet_metadata_obj = SheetMetadata(metadata={"error": "Invalid JSON metadata in DB"}, profiling_report_file_id=None)
            
            sheets_list.append(Sheet(
                id=s_dict["id"],
                name=s_dict["name"],
                sheet_index=s_dict["sheet_index"],
                description=s_dict.get("description"),
                dataset_version_id=version_id,
                metadata=sheet_metadata_obj
            ))
        
        return DatasetVersion(
            id=version_dict["id"],
            dataset_id=version_dict["dataset_id"],
            version_number=version_dict["version_number"],
            file_id=version_dict["file_id"],
            uploaded_by=version_dict["uploaded_by"],
            ingestion_timestamp=version_dict["ingestion_timestamp"],
            last_updated_timestamp=version_dict["last_updated_timestamp"],
            parent_version_id=version_dict.get("parent_version_id"),
            message=version_dict.get("message"),
            overlay_file_id=version_dict.get("overlay_file_id"),
            file_type=version_dict.get("file_type"),
            file_size=version_dict.get("file_size"),
            sheets=sheets_list if sheets_list else None
        )
    
    async def delete_dataset_version(self, version_id: int) -> Optional[int]:
        query = sa.text("""
        DELETE FROM dataset_versions
        WHERE id = :version_id
        RETURNING id;
        """)
        values = {"version_id": version_id}
        result = await self.session.execute(query, values)
        await self.session.commit()
        return result.scalar_one_or_none()
    
    # File operations
    async def create_file(self, file: FileCreate) -> int:
        query = sa.text("""
        INSERT INTO files (storage_type, file_type, mime_type, file_data, file_path, file_size)
        VALUES (:storage_type, :file_type, :mime_type, :file_data, :file_path, :file_size)
        RETURNING id;
        """)
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
    
    async def get_file(self, file_id: int) -> Optional[File]:
        query = sa.text("""
        SELECT id, storage_type, file_type, mime_type, file_path, file_size, file_data, created_at 
        FROM files 
        WHERE id = :file_id;
        """)
        result = await self.session.execute(query, {"file_id": file_id})
        row = result.mappings().first()
        return File.model_validate(row) if row else None
    
    async def update_file_path(self, file_id: int, new_path: str) -> None:
        query = sa.text("UPDATE files SET file_path = :file_path WHERE id = :file_id")
        values = {"file_id": file_id, "file_path": new_path}
        await self.session.execute(query, values)
        await self.session.commit()
    
    # Sheet operations
    async def create_sheet(self, sheet: SheetCreate) -> int:
        query = sa.text("""
        INSERT INTO sheets (dataset_version_id, name, sheet_index, description)
        VALUES (:dataset_version_id, :name, :sheet_index, :description)
        RETURNING id;
        """)
        values = {
            "dataset_version_id": sheet.dataset_version_id,
            "name": sheet.name,
            "sheet_index": sheet.sheet_index,
            "description": sheet.description
        }
        result = await self.session.execute(query, values)
        await self.session.commit()
        return result.scalar_one()
    
    async def create_sheet_metadata(self, sheet_id: int, metadata: Dict[str, Any]) -> int:
        query = sa.text("""
        INSERT INTO sheet_metadata (sheet_id, metadata, profiling_report_file_id)
        VALUES (:sheet_id, :metadata, :profiling_report_file_id)
        RETURNING id;
        """)
        values = {
            "sheet_id": sheet_id,
            "metadata": json.dumps(metadata),
            "profiling_report_file_id": None
        }
        result = await self.session.execute(query, values)
        await self.session.commit()
        return result.scalar_one()
    
    async def list_version_sheets(self, version_id: int) -> List[Sheet]:
        query = sa.text("""
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
        """)
        values = {"version_id": version_id}
        result = await self.session.execute(query, values)
        
        sheets_list: List[Sheet] = []
        for row_data in result.mappings():
            s_dict = dict(row_data)
            sheets_list.append(Sheet(
                id=s_dict["id"],
                name=s_dict["name"],
                sheet_index=s_dict["sheet_index"],
                description=s_dict.get("description"),
                dataset_version_id=version_id,
                metadata=None
            ))
        return sheets_list
    
    # Tag operations
    async def upsert_tag(self, tag_name: str, description: Optional[str] = None) -> int:
        query = sa.text("""
        INSERT INTO tags (name, description)
        VALUES (:name, :description)
        ON CONFLICT (name) DO UPDATE 
        SET description = :description
        RETURNING id;
        """)
        values = {
            "name": tag_name,
            "description": description
        }
        result = await self.session.execute(query, values)
        await self.session.commit()
        return result.scalar_one()
    
    async def create_dataset_tag(self, dataset_id: int, tag_id: int) -> None:
        query = sa.text("""
        INSERT INTO dataset_tags (dataset_id, tag_id)
        VALUES (:dataset_id, :tag_id)
        ON CONFLICT DO NOTHING;
        """)
        values = {
            "dataset_id": dataset_id,
            "tag_id": tag_id
        }
        await self.session.execute(query, values)
        await self.session.commit()
    
    async def delete_dataset_tags(self, dataset_id: int) -> None:
        query = sa.text("DELETE FROM dataset_tags WHERE dataset_id = :dataset_id;")
        values = {"dataset_id": dataset_id}
        await self.session.execute(query, values)
        await self.session.commit()
    
    async def list_tags(self) -> List[Tag]:
        query = sa.text("""
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
        """)
        result = await self.session.execute(query)
        return [Tag.model_validate(row) for row in result.mappings()]
    
    # Schema operations
    async def create_schema_version(self, schema: SchemaVersionCreate) -> int:
        query = sa.text("""
        INSERT INTO dataset_schema_versions (dataset_version_id, schema_json)
        VALUES (:dataset_version_id, :schema_json)
        RETURNING id;
        """)
        values = {
            "dataset_version_id": schema.dataset_version_id,
            "schema_json": json.dumps(schema.schema_json)
        }
        result = await self.session.execute(query, values)
        await self.session.commit()
        return result.scalar_one()
    
    async def get_schema_version(self, version_id: int) -> Optional[SchemaVersion]:
        query = sa.text("""
        SELECT 
            id,
            dataset_version_id,
            schema_json,
            created_at
        FROM 
            dataset_schema_versions
        WHERE 
            dataset_version_id = :version_id
        ORDER BY 
            created_at DESC
        LIMIT 1;
        """)
        values = {"version_id": version_id}
        result = await self.session.execute(query, values)
        row = result.mappings().first()
        
        if not row:
            return None
        
        row_dict = dict(row)
        # Parse JSON if it's a string
        if isinstance(row_dict.get("schema_json"), str):
            row_dict["schema_json"] = json.loads(row_dict["schema_json"])
        
        return SchemaVersion(
            id=row_dict["id"],
            dataset_version_id=row_dict["dataset_version_id"],
            schema_json=row_dict["schema_json"],
            created_at=row_dict["created_at"]
        )
    
    async def compare_schemas(self, version_id1: int, version_id2: int) -> Dict[str, Any]:
        """Compare schemas between two versions"""
        schema1 = await self.get_schema_version(version_id1)
        schema2 = await self.get_schema_version(version_id2)
        
        if not schema1 or not schema2:
            return {"error": "One or both schemas not found"}
        
        # Simple comparison - can be enhanced
        comparison = {
            "version1": version_id1,
            "version2": version_id2,
            "columns_added": [],
            "columns_removed": [],
            "type_changes": []
        }
        
        schema1_cols = {col["name"]: col for col in schema1.schema_json.get("columns", [])}
        schema2_cols = {col["name"]: col for col in schema2.schema_json.get("columns", [])}
        
        # Find added columns
        for col_name in schema2_cols:
            if col_name not in schema1_cols:
                comparison["columns_added"].append(schema2_cols[col_name])
        
        # Find removed columns
        for col_name in schema1_cols:
            if col_name not in schema2_cols:
                comparison["columns_removed"].append(schema1_cols[col_name])
        
        # Find type changes
        for col_name in schema1_cols:
            if col_name in schema2_cols:
                if schema1_cols[col_name]["type"] != schema2_cols[col_name]["type"]:
                    comparison["type_changes"].append({
                        "column": col_name,
                        "old_type": schema1_cols[col_name]["type"],
                        "new_type": schema2_cols[col_name]["type"]
                    })
        
        return comparison
    
    # Version file operations
    async def create_version_file(self, version_file: VersionFileCreate) -> None:
        """Create a version-file association"""
        query = sa.text("""
        INSERT INTO dataset_version_files (
            version_id, file_id, component_type, component_name, 
            component_index, metadata
        )
        VALUES (
            :version_id, :file_id, :component_type, :component_name,
            :component_index, :metadata
        );
        """)
        values = {
            "version_id": version_file.version_id,
            "file_id": version_file.file_id,
            "component_type": version_file.component_type,
            "component_name": version_file.component_name,
            "component_index": version_file.component_index,
            "metadata": json.dumps(version_file.metadata) if version_file.metadata else None
        }
        await self.session.execute(query, values)
        await self.session.commit()
    
    async def list_version_files(self, version_id: int) -> List[VersionFile]:
        """List all files associated with a version"""
        query = sa.text("""
        SELECT 
            vf.version_id,
            vf.file_id,
            vf.component_type,
            vf.component_name,
            vf.component_index,
            vf.metadata,
            f.id as file_id,
            f.storage_type,
            f.file_type,
            f.mime_type,
            f.file_path,
            f.file_size,
            f.created_at as file_created_at
        FROM 
            dataset_version_files vf
        JOIN files f ON vf.file_id = f.id
        WHERE 
            vf.version_id = :version_id
        ORDER BY 
            vf.component_index, vf.component_name;
        """)
        values = {"version_id": version_id}
        result = await self.session.execute(query, values)
        
        version_files: List[VersionFile] = []
        for row in result.mappings():
            row_dict = dict(row)
            
            # Parse metadata if it's a string
            if row_dict.get("metadata") and isinstance(row_dict["metadata"], str):
                row_dict["metadata"] = json.loads(row_dict["metadata"])
            
            # Create File object
            file_obj = File(
                id=row_dict["file_id"],
                storage_type=row_dict["storage_type"],
                file_type=row_dict["file_type"],
                mime_type=row_dict.get("mime_type"),
                file_path=row_dict.get("file_path"),
                file_size=row_dict.get("file_size"),
                created_at=row_dict["file_created_at"]
            )
            
            version_file = VersionFile(
                version_id=row_dict["version_id"],
                file_id=row_dict["file_id"],
                component_type=row_dict["component_type"],
                component_name=row_dict.get("component_name"),
                component_index=row_dict.get("component_index"),
                metadata=row_dict.get("metadata"),
                file=file_obj
            )
            version_files.append(version_file)
        
        return version_files
    
    async def delete_version_files(self, version_id: int) -> None:
        """Delete all file associations for a version"""
        query = sa.text("""
        DELETE FROM dataset_version_files
        WHERE version_id = :version_id;
        """)
        values = {"version_id": version_id}
        await self.session.execute(query, values)
        await self.session.commit()
    
    async def get_version_file_by_component(
        self, 
        version_id: int, 
        component_type: str,
        component_name: Optional[str] = None
    ) -> Optional[VersionFile]:
        """Get a specific file by component type and name"""
        query = sa.text("""
        SELECT 
            vf.version_id,
            vf.file_id,
            vf.component_type,
            vf.component_name,
            vf.component_index,
            vf.metadata,
            f.id as file_id,
            f.storage_type,
            f.file_type,
            f.mime_type,
            f.file_path,
            f.file_size,
            f.created_at as file_created_at
        FROM 
            dataset_version_files vf
        JOIN files f ON vf.file_id = f.id
        WHERE 
            vf.version_id = :version_id
            AND vf.component_type = :component_type
        """)
        
        values = {
            "version_id": version_id,
            "component_type": component_type
        }
        
        if component_name:
            query += " AND vf.component_name = :component_name"
            values["component_name"] = component_name
        
        query += " LIMIT 1;"
        
        result = await self.session.execute(sa.text(str(query)), values)
        row = result.mappings().first()
        
        if not row:
            return None
        
        row_dict = dict(row)
        
        # Parse metadata if it's a string
        if row_dict.get("metadata") and isinstance(row_dict["metadata"], str):
            row_dict["metadata"] = json.loads(row_dict["metadata"])
        
        # Create File object
        file_obj = File(
            id=row_dict["file_id"],
            storage_type=row_dict["storage_type"],
            file_type=row_dict["file_type"],
            mime_type=row_dict.get("mime_type"),
            file_path=row_dict.get("file_path"),
            file_size=row_dict.get("file_size"),
            created_at=row_dict["file_created_at"]
        )
        
        return VersionFile(
            version_id=row_dict["version_id"],
            file_id=row_dict["file_id"],
            component_type=row_dict["component_type"],
            component_name=row_dict.get("component_name"),
            component_index=row_dict.get("component_index"),
            metadata=row_dict.get("metadata"),
            file=file_obj
        )
    
    # Pointer operations (branches and tags)
    async def create_pointer(self, pointer: DatasetPointerCreate) -> int:
        """Create a new pointer (branch or tag)"""
        query = sa.text("""
        INSERT INTO dataset_pointers (
            dataset_id, pointer_name, dataset_version_id, is_tag
        )
        VALUES (
            :dataset_id, :pointer_name, :dataset_version_id, :is_tag
        )
        ON CONFLICT (dataset_id, pointer_name) DO UPDATE
        SET dataset_version_id = :dataset_version_id,
            updated_at = NOW()
        WHERE dataset_pointers.is_tag = FALSE
        RETURNING id;
        """)
        values = {
            "dataset_id": pointer.dataset_id,
            "pointer_name": pointer.pointer_name,
            "dataset_version_id": pointer.dataset_version_id,
            "is_tag": pointer.is_tag
        }
        result = await self.session.execute(query, values)
        await self.session.commit()
        return result.scalar_one()
    
    async def update_pointer(self, dataset_id: int, pointer_name: str, version_id: int) -> bool:
        """Update a branch pointer to point to a new version"""
        query = sa.text("""
        UPDATE dataset_pointers
        SET dataset_version_id = :version_id,
            updated_at = NOW()
        WHERE dataset_id = :dataset_id 
            AND pointer_name = :pointer_name
            AND is_tag = FALSE
        RETURNING id;
        """)
        values = {
            "dataset_id": dataset_id,
            "pointer_name": pointer_name,
            "version_id": version_id
        }
        result = await self.session.execute(query, values)
        await self.session.commit()
        return result.scalar_one_or_none() is not None
    
    async def get_pointer(self, dataset_id: int, pointer_name: str) -> Optional[DatasetPointer]:
        """Get a pointer by name"""
        query = sa.text("""
        SELECT 
            id,
            dataset_id,
            pointer_name,
            dataset_version_id,
            is_tag,
            created_at,
            updated_at
        FROM 
            dataset_pointers
        WHERE 
            dataset_id = :dataset_id
            AND pointer_name = :pointer_name;
        """)
        values = {
            "dataset_id": dataset_id,
            "pointer_name": pointer_name
        }
        result = await self.session.execute(query, values)
        row = result.mappings().first()
        
        if not row:
            return None
        
        return DatasetPointer(**dict(row))
    
    async def list_dataset_pointers(self, dataset_id: int) -> List[DatasetPointer]:
        """List all pointers for a dataset"""
        query = sa.text("""
        SELECT 
            id,
            dataset_id,
            pointer_name,
            dataset_version_id,
            is_tag,
            created_at,
            updated_at
        FROM 
            dataset_pointers
        WHERE 
            dataset_id = :dataset_id
        ORDER BY 
            is_tag, pointer_name;
        """)
        values = {"dataset_id": dataset_id}
        result = await self.session.execute(query, values)
        
        return [DatasetPointer(**dict(row)) for row in result.mappings()]
    
    async def delete_pointer(self, dataset_id: int, pointer_name: str) -> bool:
        """Delete a pointer"""
        query = sa.text("""
        DELETE FROM dataset_pointers
        WHERE dataset_id = :dataset_id 
            AND pointer_name = :pointer_name
        RETURNING id;
        """)
        values = {
            "dataset_id": dataset_id,
            "pointer_name": pointer_name
        }
        result = await self.session.execute(query, values)
        await self.session.commit()
        return result.scalar_one_or_none() is not None
    
    async def resolve_pointer_to_version(self, dataset_id: int, pointer_name: str) -> Optional[int]:
        """Resolve a pointer name to a version ID"""
        pointer = await self.get_pointer(dataset_id, pointer_name)
        return pointer.dataset_version_id if pointer else None