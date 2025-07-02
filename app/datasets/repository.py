"""Unified repository for dataset operations"""
from typing import List, Optional, Dict, Any
from datetime import datetime
import json
from sqlalchemy.ext.asyncio import AsyncSession
import sqlalchemy as sa

from app.datasets.models import (
    Dataset, DatasetCreate, DatasetUpdate, DatasetVersion, DatasetVersionCreate,
    File, FileCreate, SheetInfo, Tag, SchemaVersion, SchemaVersionCreate,
    VersionFile, VersionFileCreate, VersionTag, VersionTagCreate,
    OverlayData, OverlayFileAction, VersionResolution, VersionResolutionType
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
        UPDATE datasets 
        SET name = :name,
            description = :description,
            updated_at = NOW()
        WHERE id = :id
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
            array_agg(DISTINCT t.tag_name) FILTER (WHERE t.tag_name IS NOT NULL) AS tag_names
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
            dv.overlay_file_id,
            dv.message,
            dv.status,
            dv.created_by,
            dv.created_at,
            dv.updated_at,
            COALESCE(f.file_type, of.file_type) as file_type,
            COALESCE(f.mime_type, of.mime_type) as mime_type,
            COALESCE(f.file_size, of.file_size) as file_size
        FROM 
            dataset_versions dv
        LEFT JOIN dataset_version_files vf ON dv.id = vf.version_id 
            AND vf.component_type = 'primary' 
            AND vf.component_name = 'main'
        LEFT JOIN files f ON vf.file_id = f.id
        LEFT JOIN files of ON dv.overlay_file_id = of.id
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
                overlay_file_id=v_dict["overlay_file_id"],
                message=v_dict.get("message"),
                status=v_dict.get("status", "active"),
                created_by=v_dict["created_by"],
                created_at=v_dict["created_at"],
                updated_at=v_dict["updated_at"],
                file_type=v_dict.get("file_type"),
                file_size=v_dict.get("file_size")
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
    
    async def get_dataset_by_name_and_user(self, name: str, user_id: int) -> Optional[int]:
        """Get dataset ID by name and user"""
        query = sa.text("""
        SELECT id FROM datasets 
        WHERE name = :name AND created_by = :user_id;
        """)
        result = await self.session.execute(query, {"name": name, "user_id": user_id})
        row = result.scalar_one_or_none()
        return row if row else None
    
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
                array_agg(DISTINCT t.tag_name) FILTER (WHERE t.tag_name IS NOT NULL) AS tag_names
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
                AND t2.tag_name = ANY(:tags)
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
            SELECT dv.version_number, 
                   dv.overlay_file_id,
                   vf.file_id
            FROM dataset_versions dv
            LEFT JOIN dataset_version_files vf ON dv.id = vf.version_id 
                AND vf.component_type = 'primary' 
                AND vf.component_name = 'main'
            WHERE dv.dataset_id = dd.id
            ORDER BY dv.version_number DESC
            LIMIT 1
        ) dv ON true
        LEFT JOIN files f ON COALESCE(dv.file_id, dv.overlay_file_id) = f.id
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
            dataset_id, version_number, overlay_file_id, created_by, message, status
        )
        VALUES (
            :dataset_id, :version_number, :overlay_file_id, :created_by, :message, :status
        )
        RETURNING id;
        """)
        values = {
            "dataset_id": version.dataset_id,
            "version_number": version.version_number,
            "overlay_file_id": version.overlay_file_id,
            "created_by": version.created_by,
            "message": getattr(version, 'message', f'Version {version.version_number}'),
            "status": getattr(version, 'status', 'active')
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
            dv.overlay_file_id,
            dv.message,
            dv.status,
            dv.created_by,
            dv.created_at,
            dv.updated_at,
            f.file_type,
            f.mime_type,
            f.file_size
        FROM 
            dataset_versions dv
        LEFT JOIN files f ON dv.overlay_file_id = f.id
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
                overlay_file_id=row_dict["overlay_file_id"],
                message=row_dict.get("message"),
                status=row_dict.get("status", "active"),
                created_by=row_dict["created_by"],
                created_at=row_dict["created_at"],
                updated_at=row_dict["updated_at"],
                file_type=row_dict.get("file_type"),
                file_size=row_dict.get("file_size")
            ))
        return versions_list
    
    async def get_dataset_version(self, version_id: int) -> Optional[DatasetVersion]:
        query = sa.text("""
        SELECT 
            dv.id,
            dv.dataset_id,
            dv.version_number,
            dv.overlay_file_id,
            dv.message,
            dv.status,
            dv.created_by,
            dv.created_at,
            dv.updated_at,
            f.file_type,
            f.mime_type,
            f.file_size
        FROM 
            dataset_versions dv
        LEFT JOIN files f ON dv.overlay_file_id = f.id
        WHERE 
            dv.id = :version_id;
        """)
        values = {"version_id": version_id}
        result = await self.session.execute(query, values)
        version_row = result.mappings().first()
        
        if not version_row:
            return None
        
        version_dict = dict(version_row)
        
        # Get sheets for this version from dataset_version_files
        sheets_query = sa.text("""
        SELECT 
            file_id,
            component_name as name,
            component_index as sheet_index,
            metadata
        FROM 
            dataset_version_files
        WHERE 
            version_id = :version_id
            AND component_type = 'sheet'
        ORDER BY 
            component_index;
        """)
        sheets_result = await self.session.execute(sheets_query, {"version_id": version_id})
        # Note: sheets are now handled differently through dataset_version_files
        # The DatasetVersion model no longer includes a sheets field
        
        return DatasetVersion(
            id=version_dict["id"],
            dataset_id=version_dict["dataset_id"],
            version_number=version_dict["version_number"],
            overlay_file_id=version_dict["overlay_file_id"],
            message=version_dict.get("message"),
            status=version_dict.get("status", "active"),
            created_by=version_dict["created_by"],
            created_at=version_dict["created_at"],
            updated_at=version_dict["updated_at"],
            file_type=version_dict.get("file_type"),
            file_size=version_dict.get("file_size")
            # sheets field removed - use list_version_sheets() separately
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
        INSERT INTO files (
            file_type, mime_type, file_path, file_size,
            content_hash, reference_count, compression_type, metadata
        )
        VALUES (
            :file_type, :mime_type, :file_path, :file_size,
            :content_hash, :reference_count, :compression_type, :metadata
        )
        RETURNING id;
        """)
        values = {
            "file_type": file.file_type,
            "mime_type": file.mime_type,
            "file_path": file.file_path,
            "file_size": file.file_size,
            "content_hash": file.content_hash,
            "reference_count": file.reference_count or 0,
            "compression_type": file.compression_type,
            "metadata": json.dumps(file.metadata) if file.metadata else None
        }
        result = await self.session.execute(query, values)
        await self.session.commit()
        return result.scalar_one()
    
    async def get_file(self, file_id: int) -> Optional[File]:
        query = sa.text("""
        SELECT 
            id, file_type, mime_type, file_path, file_size,
            content_hash, reference_count, compression_type, metadata, created_at 
        FROM files 
        WHERE id = :file_id;
        """)
        result = await self.session.execute(query, {"file_id": file_id})
        row = result.mappings().first()
        if row:
            row_dict = dict(row)
            # metadata is already a dict from JSONB column, no need to parse
            return File.model_validate(row_dict)
        return None
    
    async def update_file_path(self, file_id: int, new_path: str) -> None:
        query = sa.text("UPDATE files SET file_path = :file_path WHERE id = :file_id")
        values = {"file_id": file_id, "file_path": new_path}
        await self.session.execute(query, values)
        await self.session.commit()
    
    # Sheet operations - now using dataset_version_files table
    async def create_sheet_as_version_file(self, version_id: int, file_id: int, sheet_name: str, 
                                          sheet_index: int, description: Optional[str] = None,
                                          metadata: Optional[Dict[str, Any]] = None) -> None:
        """Create a sheet entry using dataset_version_files table"""
        full_metadata = metadata or {}
        if description:
            full_metadata["description"] = description
            
        query = sa.text("""
        INSERT INTO dataset_version_files 
        (version_id, file_id, component_type, component_name, component_index, metadata)
        VALUES (:version_id, :file_id, 'sheet', :component_name, :component_index, :metadata)
        """)
        values = {
            "version_id": version_id,
            "file_id": file_id,
            "component_type": "sheet",
            "component_name": sheet_name,
            "component_index": sheet_index,
            "metadata": json.dumps(full_metadata) if full_metadata else None
        }
        await self.session.execute(query, values)
        await self.session.commit()
    
    async def list_version_sheets(self, version_id: int) -> List[SheetInfo]:
        """List sheets from dataset_version_files table"""
        query = sa.text("""
        SELECT 
            file_id,
            component_name as name,
            component_index as index,
            metadata
        FROM 
            dataset_version_files
        WHERE 
            version_id = :version_id
            AND component_type = 'sheet'
        ORDER BY 
            component_index;
        """)
        values = {"version_id": version_id}
        result = await self.session.execute(query, values)
        
        sheets_list: List[SheetInfo] = []
        for row in result.mappings():
            metadata = row.get("metadata")
            description = None
            if metadata:
                try:
                    parsed_meta = json.loads(metadata) if isinstance(metadata, str) else metadata
                    description = parsed_meta.get("description")
                except:
                    pass
                    
            sheets_list.append(SheetInfo(
                name=row["name"],
                index=row["index"],
                description=description,
                file_id=row["file_id"]
            ))
        return sheets_list
    
    # Tag operations
    async def upsert_tag(self, tag_name: str, description: Optional[str] = None) -> int:
        query = sa.text("""
        INSERT INTO tags (tag_name)
        VALUES (:tag_name)
        ON CONFLICT (tag_name) DO UPDATE 
        SET tag_name = EXCLUDED.tag_name
        RETURNING id;
        """)
        values = {
            "tag_name": tag_name
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
            t.tag_name AS name,
            COUNT(dt.dataset_id) AS usage_count
        FROM 
            tags t
        LEFT JOIN dataset_tags dt ON t.id = dt.tag_id
        GROUP BY 
            t.id, t.tag_name
        ORDER BY 
            t.tag_name;
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
            "schema_json": json.dumps(schema.schema_data)
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
            schema_data=row_dict["schema_json"],
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
        
        schema1_cols = {col["name"]: col for col in schema1.schema_data.get("columns", [])}
        schema2_cols = {col["name"]: col for col in schema2.schema_data.get("columns", [])}
        
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
        query_str = """
        SELECT 
            vf.version_id,
            vf.file_id,
            vf.component_type,
            vf.component_name,
            vf.component_index,
            vf.metadata,
            f.id as file_id,
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
        """
        
        values = {
            "version_id": version_id,
            "component_type": component_type
        }
        
        if component_name:
            query_str += " AND vf.component_name = :component_name"
            values["component_name"] = component_name
        
        query_str += " LIMIT 1;"
        
        result = await self.session.execute(sa.text(query_str), values)
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
    
    # Version tag operations
    async def create_version_tag(self, tag: VersionTagCreate) -> int:
        """Create a version tag"""
        query = sa.text("""
        INSERT INTO version_tags (dataset_id, tag_name, dataset_version_id)
        VALUES (:dataset_id, :tag_name, :dataset_version_id)
        RETURNING id;
        """)
        values = {
            "dataset_id": tag.dataset_id,
            "tag_name": tag.tag_name,
            "dataset_version_id": tag.dataset_version_id
        }
        result = await self.session.execute(query, values)
        await self.session.commit()
        return result.scalar_one()
    
    async def get_version_tag(self, dataset_id: int, tag_name: str) -> Optional[VersionTag]:
        """Get a version tag by dataset and tag name"""
        query = sa.text("""
        SELECT id, dataset_id, tag_name, dataset_version_id
        FROM version_tags
        WHERE dataset_id = :dataset_id AND tag_name = :tag_name;
        """)
        values = {
            "dataset_id": dataset_id,
            "tag_name": tag_name
        }
        result = await self.session.execute(query, values)
        row = result.mappings().first()
        
        if not row:
            return None
            
        return VersionTag(
            id=row["id"],
            dataset_id=row["dataset_id"],
            tag_name=row["tag_name"],
            dataset_version_id=row["dataset_version_id"]
        )
    
    async def list_version_tags(self, dataset_id: int) -> List[VersionTag]:
        """List all version tags for a dataset"""
        query = sa.text("""
        SELECT vt.id, vt.dataset_id, vt.tag_name, vt.dataset_version_id
        FROM version_tags vt
        WHERE vt.dataset_id = :dataset_id
        ORDER BY vt.tag_name;
        """)
        values = {"dataset_id": dataset_id}
        result = await self.session.execute(query, values)
        
        tags: List[VersionTag] = []
        for row in result.mappings():
            tags.append(VersionTag(
                id=row["id"],
                dataset_id=row["dataset_id"],
                tag_name=row["tag_name"],
                dataset_version_id=row["dataset_version_id"]
            ))
        return tags
    
    async def delete_version_tag(self, dataset_id: int, tag_name: str) -> Optional[int]:
        """Delete a version tag"""
        query = sa.text("""
        DELETE FROM version_tags
        WHERE dataset_id = :dataset_id AND tag_name = :tag_name
        RETURNING id;
        """)
        values = {
            "dataset_id": dataset_id,
            "tag_name": tag_name
        }
        result = await self.session.execute(query, values)
        await self.session.commit()
        return result.scalar_one_or_none()
    
    # Overlay and versioning operations
    async def create_overlay_file(self, overlay_data: OverlayData) -> int:
        """Create an overlay file with the change operations"""
        overlay_content = overlay_data.model_dump_json()
        
        # Generate a content hash for the overlay
        import hashlib
        content_hash = hashlib.sha256(overlay_content.encode('utf-8')).hexdigest()
        
        # Create file record for the overlay with proper URI
        file_create = FileCreate(
            file_type="json",
            mime_type="application/json",
            file_path=f"file:///data/overlays/{content_hash}",
            file_size=len(overlay_content.encode('utf-8')),
            content_hash=content_hash,
            metadata={"overlay_version": overlay_data.version_number}
        )
        
        return await self.create_file(file_create)
    
    async def parse_overlay_file(self, file_id: int) -> Optional[OverlayData]:
        """Parse an overlay file and return OverlayData"""
        file_info = await self.get_file(file_id)
        if not file_info or not file_info.file_path:
            return None
        
        try:
            with open(file_info.file_path, 'r') as f:
                overlay_content = f.read()
            return OverlayData.model_validate_json(overlay_content)
        except Exception:
            return None
    
    async def resolve_version(self, dataset_id: int, resolution: VersionResolution) -> Optional[DatasetVersion]:
        """Resolve a version based on resolution type (number, tag, latest)"""
        if resolution.type == VersionResolutionType.LATEST:
            query = sa.text("""
            SELECT 
                dv.id, dv.dataset_id, dv.version_number, dv.overlay_file_id,
                dv.message, dv.status, dv.created_by,
                dv.created_at, dv.updated_at
            FROM dataset_versions dv
            WHERE dv.dataset_id = :dataset_id
            ORDER BY dv.version_number DESC
            LIMIT 1;
            """)
            values = {"dataset_id": dataset_id}
            
        elif resolution.type == VersionResolutionType.NUMBER:
            if not resolution.value or not isinstance(resolution.value, int):
                return None
            query = sa.text("""
            SELECT 
                dv.id, dv.dataset_id, dv.version_number, dv.overlay_file_id,
                dv.message, dv.status, dv.created_by,
                dv.created_at, dv.updated_at
            FROM dataset_versions dv
            WHERE dv.dataset_id = :dataset_id AND dv.version_number = :version_number;
            """)
            values = {"dataset_id": dataset_id, "version_number": resolution.value}
            
        elif resolution.type == VersionResolutionType.TAG:
            if not resolution.value or not isinstance(resolution.value, str):
                return None
            query = sa.text("""
            SELECT 
                dv.id, dv.dataset_id, dv.version_number, dv.overlay_file_id,
                dv.message, dv.status, dv.created_by,
                dv.created_at, dv.updated_at
            FROM dataset_versions dv
            JOIN version_tags vt ON dv.id = vt.dataset_version_id
            WHERE dv.dataset_id = :dataset_id AND vt.tag_name = :tag_name;
            """)
            values = {"dataset_id": dataset_id, "tag_name": resolution.value}
        else:
            return None
        
        result = await self.session.execute(query, values)
        row = result.mappings().first()
        
        if not row:
            return None
        
        return DatasetVersion(
            id=row["id"],
            dataset_id=row["dataset_id"],
            version_number=row["version_number"],
            overlay_file_id=row["overlay_file_id"],
            message=row["message"],
            status=row["status"],
            created_by=row["created_by"],
            created_at=row["created_at"],
            updated_at=row["updated_at"]
        )
    
    async def reconstruct_version_files(self, dataset_id: int, target_version: int) -> List[Dict[str, Any]]:
        """Reconstruct the file list for a version by applying overlays sequentially"""
        # Start from empty state and apply all overlays from version 1 to target_version
        file_state = {}
        
        # Get all overlays from version 1 to target_version
        overlay_query = sa.text("""
        SELECT overlay_file_id, version_number
        FROM dataset_versions
        WHERE dataset_id = :dataset_id
        AND version_number <= :target_version
        ORDER BY version_number ASC;
        """)
        
        overlay_result = await self.session.execute(overlay_query, {
            "dataset_id": dataset_id,
            "target_version": target_version
        })
        
        # Apply each overlay
        for overlay_row in overlay_result.mappings():
            overlay_data = await self.parse_overlay_file(overlay_row["overlay_file_id"])
            if overlay_data:
                file_state = self._apply_overlay(file_state, overlay_data)
        
        return list(file_state.values())
    
    def _apply_overlay(self, file_state: Dict[str, Dict], overlay_data: OverlayData) -> Dict[str, Dict]:
        """Apply overlay actions to current file state"""
        for action in overlay_data.actions:
            component_key = f"{action.component_type}:{action.component_name}"
            
            if action.operation == "add" or action.operation == "update":
                file_state[component_key] = {
                    "file_id": action.file_id,
                    "component_type": action.component_type,
                    "component_name": action.component_name,
                    "metadata": action.metadata
                }
            elif action.operation == "remove":
                file_state.pop(component_key, None)
        
        return file_state
    
    # Statistics operations
    async def upsert_dataset_statistics(
        self,
        version_id: int,
        row_count: int,
        column_count: int,
        size_bytes: int,
        statistics: Dict[str, Any]
    ) -> None:
        """Insert or update dataset statistics"""
        query = sa.text("""
            INSERT INTO dataset_statistics 
            (version_id, row_count, column_count, size_bytes, statistics, computed_at)
            VALUES (:version_id, :row_count, :column_count, :size_bytes, :statistics, NOW())
            ON CONFLICT (version_id) 
            DO UPDATE SET
                row_count = EXCLUDED.row_count,
                column_count = EXCLUDED.column_count,
                size_bytes = EXCLUDED.size_bytes,
                statistics = EXCLUDED.statistics,
                computed_at = NOW()
        """)
        
        await self.session.execute(
            query,
            {
                "version_id": version_id,
                "row_count": row_count,
                "column_count": column_count,
                "size_bytes": size_bytes,
                "statistics": json.dumps(statistics)
            }
        )
        await self.session.commit()
    
    async def get_dataset_statistics(self, version_id: int) -> Optional[Dict[str, Any]]:
        """Get statistics for a dataset version"""
        query = sa.text("""
            SELECT version_id, row_count, column_count, size_bytes, 
                   statistics, computed_at
            FROM dataset_statistics
            WHERE version_id = :version_id
        """)
        
        result = await self.session.execute(query, {"version_id": version_id})
        row = result.first()
        
        if row:
            return {
                "version_id": row.version_id,
                "row_count": row.row_count,
                "column_count": row.column_count,
                "size_bytes": row.size_bytes,
                "statistics": row.statistics,
                "computed_at": row.computed_at
            }
        return None
    
    async def create_analysis_run(
        self,
        dataset_version_id: int,
        user_id: Optional[int],
        run_type: str,
        run_parameters: Dict[str, Any],
        status: str = "pending",
        output_summary: Optional[Dict[str, Any]] = None
    ) -> int:
        """Create an analysis run record"""
        query = sa.text("""
            INSERT INTO analysis_runs 
            (dataset_version_id, user_id, run_type, run_parameters, status, 
             run_timestamp, output_summary)
            VALUES (:dataset_version_id, :user_id, :run_type::analysis_run_type, 
                    :run_parameters, :status::analysis_run_status, NOW(), :output_summary)
            RETURNING id
        """)
        
        result = await self.session.execute(
            query,
            {
                "dataset_version_id": dataset_version_id,
                "user_id": user_id,
                "run_type": run_type,
                "run_parameters": json.dumps(run_parameters),
                "status": status,
                "output_summary": json.dumps(output_summary) if output_summary else None
            }
        )
        await self.session.commit()
        return result.scalar_one()
    
    async def update_analysis_run(
        self,
        analysis_run_id: int,
        status: str,
        execution_time_ms: Optional[int] = None,
        output_summary: Optional[Dict[str, Any]] = None,
        output_file_id: Optional[int] = None
    ) -> None:
        """Update an analysis run with results"""
        query = sa.text("""
            UPDATE analysis_runs 
            SET status = :status::analysis_run_status,
                execution_time_ms = :execution_time_ms,
                output_summary = :output_summary,
                output_file_id = :output_file_id
            WHERE id = :id
        """)
        
        await self.session.execute(
            query,
            {
                "id": analysis_run_id,
                "status": status,
                "execution_time_ms": execution_time_ms,
                "output_summary": json.dumps(output_summary) if output_summary else None,
                "output_file_id": output_file_id
            }
        )
        await self.session.commit()
    
    
