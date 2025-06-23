"""Repository for dataset search operations using PostgreSQL FTS"""
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime
import time
from sqlalchemy.ext.asyncio import AsyncSession
import sqlalchemy as sa
from sqlalchemy import text, func

from app.datasets.search.models import (
    SearchRequest, SearchResult, SearchFacet, FacetValue,
    SearchSuggestion, SearchSortBy
)


class SearchRepository:
    """Repository for search-related database operations"""
    
    def __init__(self, session: AsyncSession):
        self.session = session
    
    async def search_datasets(
        self, 
        request: SearchRequest,
        user_id: int
    ) -> Tuple[List[SearchResult], int]:
        """
        Search datasets with full-text search and filtering.
        Returns (results, total_count).
        """
        start_time = time.time()
        
        # Build the main search query
        query_parts = []
        params = {
            "user_id": user_id,
            "limit": request.limit,
            "offset": request.offset
        }
        
        # Base query with permissions check
        base_query = """
        WITH user_datasets AS (
            SELECT DISTINCT 
                d.id,
                d.name,
                d.description,
                d.created_by,
                d.created_at,
                d.updated_at,
                u.soeid as created_by_name,
                COALESCE(dp.permission_type, 
                    CASE WHEN d.created_by = :user_id THEN 'admin' ELSE NULL END
                ) as user_permission,
                -- Search vectors for FTS
                to_tsvector('english', COALESCE(d.name, '')) || 
                to_tsvector('english', COALESCE(d.description, '')) as search_vector,
                -- Latest version info
                latest_v.version_number as current_version,
                latest_v.version_count,
                latest_f.file_type,
                latest_f.file_size,
                -- Tags
                array_agg(DISTINCT t.tag_name) FILTER (WHERE t.tag_name IS NOT NULL) as tags
            FROM datasets d
            LEFT JOIN users u ON d.created_by = u.id
            LEFT JOIN dataset_permissions dp ON d.id = dp.dataset_id AND dp.user_id = :user_id
            LEFT JOIN dataset_tags dt ON d.id = dt.dataset_id
            LEFT JOIN tags t ON dt.tag_id = t.id
            LEFT JOIN LATERAL (
                SELECT 
                    dv.dataset_id,
                    MAX(dv.version_number) as version_number,
                    COUNT(*) as version_count
                FROM dataset_versions dv
                WHERE dv.dataset_id = d.id
                GROUP BY dv.dataset_id
            ) latest_v ON true
            LEFT JOIN LATERAL (
                SELECT f.file_type, f.file_size
                FROM dataset_versions dv
                LEFT JOIN dataset_version_files dvf ON dv.id = dvf.version_id 
                    AND dvf.component_type = 'primary'
                LEFT JOIN files f ON COALESCE(dvf.file_id, dv.overlay_file_id) = f.id
                WHERE dv.dataset_id = d.id 
                    AND dv.version_number = latest_v.version_number
                LIMIT 1
            ) latest_f ON true
            WHERE 
                -- Only show datasets user has access to
                (d.created_by = :user_id OR dp.user_id = :user_id)
        """
        
        # Add search condition if query provided
        if request.query:
            if request.fuzzy_search:
                # Use pg_trgm for fuzzy search (requires CREATE EXTENSION pg_trgm)
                query_parts.append("(d.name % :search_query OR d.description % :search_query)")
            else:
                # Use full-text search
                query_parts.append("search_vector @@ plainto_tsquery('english', :search_query)")
            params["search_query"] = request.query
        
        # Add tag filters
        if request.tags:
            query_parts.append("""
                EXISTS (
                    SELECT 1 FROM dataset_tags dt2
                    JOIN tags t2 ON dt2.tag_id = t2.id
                    WHERE dt2.dataset_id = d.id AND t2.tag_name = ANY(:tag_filter)
                )
            """)
            params["tag_filter"] = request.tags
        
        # Add file type filters
        if request.file_types:
            query_parts.append("latest_f.file_type = ANY(:file_types)")
            params["file_types"] = request.file_types
        
        # Add creator filters
        if request.created_by:
            query_parts.append("d.created_by = ANY(:created_by)")
            params["created_by"] = request.created_by
        
        # Add date range filters
        if request.created_at:
            if request.created_at.start:
                query_parts.append("d.created_at >= :created_start")
                params["created_start"] = request.created_at.start
            if request.created_at.end:
                query_parts.append("d.created_at <= :created_end")
                params["created_end"] = request.created_at.end
        
        if request.updated_at:
            if request.updated_at.start:
                query_parts.append("d.updated_at >= :updated_start")
                params["updated_start"] = request.updated_at.start
            if request.updated_at.end:
                query_parts.append("d.updated_at <= :updated_end")
                params["updated_end"] = request.updated_at.end
        
        # Add file size filters
        if request.file_size:
            if request.file_size.min is not None:
                query_parts.append("latest_f.file_size >= :size_min")
                params["size_min"] = request.file_size.min
            if request.file_size.max is not None:
                query_parts.append("latest_f.file_size <= :size_max")
                params["size_max"] = request.file_size.max
        
        # Add version count filters
        if request.version_count:
            if request.version_count.min is not None:
                query_parts.append("latest_v.version_count >= :version_min")
                params["version_min"] = request.version_count.min
            if request.version_count.max is not None:
                query_parts.append("latest_v.version_count <= :version_max")
                params["version_max"] = request.version_count.max
        
        # Combine query parts
        where_clause = " AND ".join(query_parts) if query_parts else "TRUE"
        
        # Add GROUP BY clause
        group_by_clause = """
            GROUP BY 
                d.id, d.name, d.description, d.created_by, d.created_at, d.updated_at,
                u.soeid, dp.permission_type, latest_v.version_number, latest_v.version_count,
                latest_f.file_type, latest_f.file_size
        """
        
        # Finalize base query
        base_query += f" {where_clause} {group_by_clause} )"
        
        # Build ordering clause
        order_clause = self._build_order_clause(request.sort_by, request.sort_order, request.query)
        
        # Count query
        count_query = f"""
        SELECT COUNT(*) as total FROM user_datasets
        """
        
        # Main query with pagination
        main_query = f"""
        SELECT 
            ud.*,
            {self._build_score_expression(request.query)} as score
        FROM user_datasets ud
        {order_clause}
        LIMIT :limit OFFSET :offset
        """
        
        full_query = base_query + " " + main_query
        
        # Execute queries
        results = await self.session.execute(text(full_query), params)
        rows = results.mappings().all()
        
        # Get total count
        count_result = await self.session.execute(
            text(base_query + " " + count_query), 
            {k: v for k, v in params.items() if k not in ['limit', 'offset']}
        )
        total = count_result.scalar() or 0
        
        # Convert to SearchResult objects
        search_results = []
        for row in rows:
            search_results.append(SearchResult(
                id=row['id'],
                name=row['name'],
                description=row['description'],
                created_by=row['created_by'],
                created_by_name=row['created_by_name'],
                created_at=row['created_at'],
                updated_at=row['updated_at'],
                current_version=row['current_version'],
                version_count=row['version_count'] or 0,
                file_type=row['file_type'],
                file_size=row['file_size'],
                tags=row['tags'] or [],
                score=row['score'] or 0.0,
                user_permission=row['user_permission']
            ))
        
        return search_results, total
    
    async def get_search_facets(
        self, 
        request: SearchRequest,
        user_id: int
    ) -> Dict[str, SearchFacet]:
        """Get facet counts for search results"""
        facets = {}
        
        # Define which facets to compute
        facet_fields = request.facet_fields or ['tags', 'file_types', 'created_by']
        
        base_conditions = self._build_base_conditions(request, user_id)
        
        # Get tag facets
        if 'tags' in facet_fields:
            tag_query = """
            SELECT 
                t.tag_name as value,
                COUNT(DISTINCT d.id) as count
            FROM datasets d
            LEFT JOIN dataset_permissions dp ON d.id = dp.dataset_id AND dp.user_id = :user_id
            JOIN dataset_tags dt ON d.id = dt.dataset_id
            JOIN tags t ON dt.tag_id = t.id
            WHERE (d.created_by = :user_id OR dp.user_id = :user_id)
                AND """ + base_conditions + """
            GROUP BY t.tag_name
            ORDER BY count DESC
            LIMIT 20
            """
            
            result = await self.session.execute(
                text(tag_query), 
                {"user_id": user_id}
            )
            tag_values = [
                FacetValue(value=row['value'], count=row['count'])
                for row in result.mappings()
            ]
            
            facets['tags'] = SearchFacet(
                field='tags',
                label='Tags',
                values=tag_values,
                total_values=len(tag_values)
            )
        
        # Get file type facets
        if 'file_types' in facet_fields:
            file_type_query = """
            SELECT 
                f.file_type as value,
                COUNT(DISTINCT d.id) as count
            FROM datasets d
            LEFT JOIN dataset_permissions dp ON d.id = dp.dataset_id AND dp.user_id = :user_id
            JOIN dataset_versions dv ON d.id = dv.dataset_id
            LEFT JOIN dataset_version_files dvf ON dv.id = dvf.version_id 
                AND dvf.component_type = 'primary'
            LEFT JOIN files f ON COALESCE(dvf.file_id, dv.overlay_file_id) = f.id
            WHERE (d.created_by = :user_id OR dp.user_id = :user_id)
                AND f.file_type IS NOT NULL
                AND """ + base_conditions + """
            GROUP BY f.file_type
            ORDER BY count DESC
            LIMIT 20
            """
            
            result = await self.session.execute(
                text(file_type_query),
                {"user_id": user_id}
            )
            file_type_values = [
                FacetValue(value=row['value'], count=row['count'])
                for row in result.mappings()
            ]
            
            facets['file_types'] = SearchFacet(
                field='file_types',
                label='File Types',
                values=file_type_values,
                total_values=len(file_type_values)
            )
        
        return facets
    
    async def get_search_suggestions(
        self,
        query: str,
        limit: int,
        types: Optional[List[str]] = None
    ) -> List[SearchSuggestion]:
        """Get search suggestions based on partial query"""
        suggestions = []
        
        if not types or 'dataset_name' in types:
            # Get dataset name suggestions
            name_query = """
            SELECT DISTINCT
                name as text,
                'dataset_name' as type,
                CASE 
                    WHEN name ILIKE :pattern THEN 
                        1.0 - (LENGTH(name) - LENGTH(:query))::float / LENGTH(name)
                    ELSE 0.0
                END as score
            FROM datasets
            WHERE name ILIKE :pattern
            ORDER BY score DESC, name
            LIMIT :limit
            """
            
            result = await self.session.execute(
                text(name_query),
                {
                    "query": query,
                    "pattern": f"%{query}%",
                    "limit": limit
                }
            )
            
            for row in result.mappings():
                suggestions.append(SearchSuggestion(
                    text=row['text'],
                    type=row['type'],
                    score=row['score']
                ))
        
        if not types or 'tag' in types:
            # Get tag suggestions
            tag_query = """
            SELECT DISTINCT
                tag_name as text,
                'tag' as type,
                CASE 
                    WHEN tag_name ILIKE :pattern THEN 
                        1.0 - (LENGTH(tag_name) - LENGTH(:query))::float / LENGTH(tag_name)
                    ELSE 0.0
                END as score
            FROM tags
            WHERE tag_name ILIKE :pattern
            ORDER BY score DESC, tag_name
            LIMIT :limit
            """
            
            result = await self.session.execute(
                text(tag_query),
                {
                    "query": query,
                    "pattern": f"%{query}%",
                    "limit": limit
                }
            )
            
            for row in result.mappings():
                suggestions.append(SearchSuggestion(
                    text=row['text'],
                    type=row['type'],
                    score=row['score']
                ))
        
        # Sort all suggestions by score and limit
        suggestions.sort(key=lambda x: x.score, reverse=True)
        return suggestions[:limit]
    
    def _build_order_clause(
        self, 
        sort_by: SearchSortBy, 
        sort_order: str,
        has_query: bool
    ) -> str:
        """Build ORDER BY clause based on sort options"""
        order_map = {
            SearchSortBy.RELEVANCE: "score" if has_query else "ud.updated_at",
            SearchSortBy.NAME: "ud.name",
            SearchSortBy.CREATED_AT: "ud.created_at",
            SearchSortBy.UPDATED_AT: "ud.updated_at",
            SearchSortBy.FILE_SIZE: "ud.file_size",
            SearchSortBy.VERSION_COUNT: "ud.version_count"
        }
        
        order_field = order_map.get(sort_by, "score")
        
        # Handle NULL values
        null_handling = "NULLS LAST" if sort_order == "asc" else "NULLS LAST"
        
        return f"ORDER BY {order_field} {sort_order.upper()} {null_handling}"
    
    def _build_score_expression(self, query: Optional[str]) -> str:
        """Build relevance score expression"""
        if not query:
            return "1.0"
        
        # Combine multiple ranking factors
        return """
        CASE 
            WHEN search_vector @@ plainto_tsquery('english', :search_query) THEN
                ts_rank(search_vector, plainto_tsquery('english', :search_query))
            ELSE 0.0
        END
        """
    
    def _build_base_conditions(self, request: SearchRequest, user_id: int) -> str:
        """Build base WHERE conditions (without user permissions)"""
        # This is a simplified version - in production you'd build this dynamically
        return "TRUE"
    
    async def ensure_search_extensions(self) -> None:
        """Ensure required PostgreSQL extensions are installed"""
        # Check and create pg_trgm extension for fuzzy search
        check_query = """
        SELECT EXISTS (
            SELECT 1 FROM pg_extension WHERE extname = 'pg_trgm'
        )
        """
        result = await self.session.execute(text(check_query))
        has_trgm = result.scalar()
        
        if not has_trgm:
            try:
                await self.session.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
                await self.session.commit()
            except Exception as e:
                # Extension creation might require superuser privileges
                print(f"Warning: Could not create pg_trgm extension: {e}")
    
    async def update_search_indexes(self) -> None:
        """Create or update search indexes for better performance"""
        # Create GIN index for full-text search
        fts_index = """
        CREATE INDEX IF NOT EXISTS idx_datasets_fts ON datasets 
        USING gin(to_tsvector('english', COALESCE(name, '') || ' ' || COALESCE(description, '')))
        """
        
        # Create trigram index for fuzzy search
        trgm_index = """
        CREATE INDEX IF NOT EXISTS idx_datasets_name_trgm ON datasets 
        USING gin(name gin_trgm_ops)
        """
        
        try:
            await self.session.execute(text(fts_index))
            await self.session.execute(text(trgm_index))
            await self.session.commit()
        except Exception as e:
            print(f"Warning: Could not create search indexes: {e}")