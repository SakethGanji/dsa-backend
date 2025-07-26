"""SQL-based sampling job executor for high-performance data sampling."""

import re
import json
import logging
from typing import Dict, Any, List, AsyncGenerator, Set, Tuple, Optional
from uuid import UUID
from datetime import datetime
from abc import ABC, abstractmethod

from asyncpg import Connection
from src.infrastructure.postgres.database import DatabasePool
from src.infrastructure.postgres.event_store import PostgresEventStore
from src.core.events.publisher import JobStartedEvent, JobCompletedEvent, JobFailedEvent
from src.core.events.registry import InMemoryEventBus
from .job_worker import JobExecutor

logger = logging.getLogger(__name__)


class SamplingJobExecutor(JobExecutor):
    """Secure SQL-based sampling executor with dynamic filtering support."""
    
    # Column name validation pattern
    VALID_COLUMN_PATTERN = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')
    
    # Whitelist of allowed filter operators
    ALLOWED_OPERATORS = {
        '>': '>', '>=': '>=', '<': '<', '<=': '<=', 
        '=': '=', '!=': '!=', '<>': '!=',
        'in': 'IN', 'not_in': 'NOT IN',
        'like': 'LIKE', 'ilike': 'ILIKE',
        'is_null': 'IS NULL', 'is_not_null': 'IS NOT NULL'
    }
    
    # Configurable parameters with defaults
    DEFAULT_CONFIG = {
        'oversampling_factor': 1.5,
        'min_stratum_sample_count': 10,
        'estimation_sample_percent': 1.0,
        'cardinality_threshold': 10000,
        'default_row_estimate': 1000000
    }
    
    # SQL query templates for different sampling methods
    SAMPLING_QUERIES = {
        'random_unseeded': """
            WITH source_data AS (
                SELECT m.logical_row_id, m.row_hash, 
                       CASE 
                           WHEN r.data ? 'data' THEN r.data->'data'
                           ELSE r.data
                       END as row_data_json
                FROM dsa_core.commit_rows m
                JOIN dsa_core.rows r ON m.row_hash = r.row_hash
                WHERE m.commit_id = $1 AND m.logical_row_id LIKE ($2 || ':%')
                ORDER BY RANDOM()
            )
            SELECT * FROM source_data LIMIT $3
        """,
        
        'random_seeded_scalable': """
            -- Hash filtering approach - no sorting required
            WITH sample_params AS (
                SELECT 
                    $3::bigint as desired_samples,
                    $4::text as seed,
                    -- Get fast row count estimate
                    COALESCE(
                        (SELECT reltuples::bigint FROM pg_class WHERE oid = 'dsa_core.commit_rows'::regclass),
                        1000000
                    ) as estimated_rows
            ),
            source_data AS (
                SELECT m.logical_row_id, m.row_hash, r.data as row_data_json
                FROM dsa_core.commit_rows m
                JOIN dsa_core.rows r ON m.row_hash = r.row_hash
                CROSS JOIN sample_params sp
                WHERE m.commit_id = $1 AND m.logical_row_id LIKE ($2 || ':%')
                -- Hash filtering - scales to billions of rows
                AND ('x' || substr(md5(m.logical_row_id || sp.seed), 1, 16))::bit(64)::bigint 
                    < ((sp.desired_samples::float * $5 / NULLIF(sp.estimated_rows, 0)) * x'7fffffffffffffff'::bigint)::bigint
                AND NOT EXISTS (
                    SELECT 1 FROM temp_sampling_exclusions e 
                    WHERE e.row_id = m.logical_row_id
                )
            )
            SELECT logical_row_id, row_hash, row_data_json
            FROM source_data
            LIMIT $3
        """,
        
        'random_seeded_exact': """
            -- ORDER BY approach - use only for smaller datasets where exact counts matter
            WITH source_data AS (
                SELECT m.logical_row_id, m.row_hash, r.data as row_data_json,
                       md5(logical_row_id || $4::text) as seeded_random
                FROM dsa_core.commit_rows m
                JOIN dsa_core.rows r ON m.row_hash = r.row_hash
                WHERE m.commit_id = $1 AND m.logical_row_id LIKE ($2 || ':%')
                AND NOT EXISTS (
                    SELECT 1 FROM temp_sampling_exclusions e 
                    WHERE e.row_id = m.logical_row_id
                )
            )
            SELECT logical_row_id, row_hash, row_data_json
            FROM source_data
            ORDER BY seeded_random
            LIMIT $3
        """,
        
        'systematic': """
            WITH numbered_data AS (
                SELECT 
                    m.logical_row_id, m.row_hash, r.data as row_data_json,
                    ROW_NUMBER() OVER (ORDER BY m.logical_row_id) as rn
                FROM dsa_core.commit_rows m
                JOIN dsa_core.rows r ON m.row_hash = r.row_hash
                WHERE m.commit_id = $1 AND m.logical_row_id LIKE ($2 || ':%')
                AND NOT EXISTS (
                    SELECT 1 FROM temp_sampling_exclusions e 
                    WHERE e.row_id = m.logical_row_id
                )
            )
            SELECT logical_row_id, row_hash, row_data_json
            FROM numbered_data
            WHERE MOD(rn + $3 - 1, $4) = 0
        """,
        
        'cluster_percentage': """
            -- Option A: Sample percentage from each cluster
            WITH source_data AS (
                SELECT 
                    m.logical_row_id, m.row_hash, 
                    CASE 
                        WHEN r.data ? 'data' THEN r.data->'data'
                        ELSE r.data
                    END as row_data_json,
                    CASE 
                        WHEN r.data ? 'data' THEN r.data->'data'->>$5
                        ELSE r.data->>$5
                    END as cluster_id,
                    ('x' || substr(md5(m.logical_row_id || $6::text), 1, 16))::bit(64)::bigint as hash_value
                FROM dsa_core.commit_rows m
                JOIN dsa_core.rows r ON m.row_hash = r.row_hash
                WHERE m.commit_id = $1 AND m.logical_row_id LIKE ($2 || ':%')
            ),
            selected_clusters AS (
                SELECT DISTINCT cluster_id
                FROM source_data
                WHERE ('x' || substr(md5(cluster_id || $6::text), 1, 16))::bit(64)::bigint 
                    < (CAST($3 AS FLOAT) / GREATEST(1.0, (SELECT COUNT(DISTINCT cluster_id)::float FROM source_data)) * 9223372036854775807)::bigint
                LIMIT $3
            ),
            cluster_sample AS (
                SELECT sd.*, 
                       ROW_NUMBER() OVER (PARTITION BY sd.cluster_id ORDER BY sd.hash_value) as rn,
                       COUNT(*) OVER (PARTITION BY sd.cluster_id) as cluster_total
                FROM source_data sd
                JOIN selected_clusters sc ON sd.cluster_id = sc.cluster_id
            )
            SELECT logical_row_id, row_hash, row_data_json
            FROM cluster_sample
            WHERE rn <= GREATEST(1, FLOOR(cluster_total * $4 / 100.0))
        """,
        
        'cluster_fixed': """
            -- Option B: Sample fixed N from each cluster
            WITH source_data AS (
                SELECT 
                    m.logical_row_id, m.row_hash, 
                    CASE 
                        WHEN r.data ? 'data' THEN r.data->'data'
                        ELSE r.data
                    END as row_data_json,
                    CASE 
                        WHEN r.data ? 'data' THEN r.data->'data'->>$5
                        ELSE r.data->>$5
                    END as cluster_id,
                    ('x' || substr(md5(m.logical_row_id || $6::text), 1, 16))::bit(64)::bigint as hash_value
                FROM dsa_core.commit_rows m
                JOIN dsa_core.rows r ON m.row_hash = r.row_hash
                WHERE m.commit_id = $1 AND m.logical_row_id LIKE ($2 || ':%')
            ),
            selected_clusters AS (
                SELECT DISTINCT cluster_id
                FROM source_data
                WHERE ('x' || substr(md5(cluster_id || $6::text), 1, 16))::bit(64)::bigint 
                    < (CAST($3 AS FLOAT) / GREATEST(1.0, (SELECT COUNT(DISTINCT cluster_id)::float FROM source_data)) * 9223372036854775807)::bigint
                LIMIT $3
            ),
            cluster_sample AS (
                SELECT sd.*, 
                       ROW_NUMBER() OVER (PARTITION BY sd.cluster_id ORDER BY sd.hash_value) as rn
                FROM source_data sd
                JOIN selected_clusters sc ON sd.cluster_id = sc.cluster_id
            )
            SELECT logical_row_id, row_hash, row_data_json
            FROM cluster_sample
            WHERE rn <= $4
        """,
        
        'stratified_disproportional_fixed': """
            -- Disproportional Stratified Sampling (Fixed-N per stratum)
            WITH all_data AS (
                SELECT
                    m.logical_row_id,
                    m.row_hash,
                    r.data as row_data_json,
                    -- Use ROW_NUMBER() to rank rows randomly within each stratum
                    ROW_NUMBER() OVER (
                        PARTITION BY {strata_grouping_sql}
                        ORDER BY md5(m.logical_row_id || $4::text) -- Seeded random order
                    ) as rn
                FROM dsa_core.commit_rows m
                JOIN dsa_core.rows r ON m.row_hash = r.row_hash
                WHERE m.commit_id = $1 AND m.logical_row_id LIKE ($2 || ':%')
                AND NOT EXISTS (
                    SELECT 1 FROM temp_sampling_exclusions e
                    WHERE e.row_id = m.logical_row_id
                )
            )
            SELECT
                logical_row_id,
                row_hash,
                row_data_json
            FROM all_data
            WHERE rn <= $3 -- Select the top N rows from each stratum
        """
    }
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize with optional configuration overrides."""
        self.config = {**self.DEFAULT_CONFIG, **(config or {})}
    
    def _validate_column_name(self, column: str) -> str:
        """Validate and return safe column name."""
        if not self.VALID_COLUMN_PATTERN.match(column):
            raise ValueError(f"Invalid column name: {column}")
        return column
    
    @staticmethod
    def _get_data_extract_sql() -> str:
        """Get SQL expression to extract data handling nested structure."""
        return """CASE 
                   WHEN r.data ? 'data' THEN r.data->'data'
                   ELSE r.data
               END"""
    
    def _build_stratified_query(self, validated_columns: List[str], min_stratum_count: int = 10) -> str:
        """Build stratified sampling query with validated columns."""
        data_expr = self._get_data_extract_sql()
        col_extracts = [f"({data_expr}->>'{col}') as {col}" for col in validated_columns]
        col_names = ', '.join(validated_columns)
        
        return f"""
            -- Stratified Sampling with proportional allocation
            WITH strata_counts AS (
                SELECT 
                    {', '.join(col_extracts)},
                    COUNT(*) as stratum_size
                FROM dsa_core.commit_rows m
                JOIN dsa_core.rows r ON m.row_hash = r.row_hash
                WHERE m.commit_id = $1 AND m.logical_row_id LIKE ($2 || ':%')
                GROUP BY {col_names}
            ),
            strata_allocation AS (
                SELECT 
                    {col_names},
                    stratum_size,
                    GREATEST(
                        $3,  -- min_per_stratum
                        CEIL((stratum_size::float / SUM(stratum_size) OVER ()) * $4)
                    )::int as samples_needed
                FROM strata_counts
            ),
            all_data AS (
                SELECT 
                    m.logical_row_id, 
                    m.row_hash, 
                    {data_expr} as row_data_json,
                    {', '.join(col_extracts)},
                    ('x' || substr(md5(m.logical_row_id || $5::text), 1, 16))::bit(64)::bigint as hash_value
                FROM dsa_core.commit_rows m
                JOIN dsa_core.rows r ON m.row_hash = r.row_hash
                WHERE m.commit_id = $1 AND m.logical_row_id LIKE ($2 || ':%')
            ),
            stratified_sample AS (
                SELECT 
                    ad.logical_row_id,
                    ad.row_hash,
                    ad.row_data_json,
                    {', '.join([f'ad.{col}' for col in validated_columns])},
                    sa.samples_needed,
                    ROW_NUMBER() OVER (
                        PARTITION BY {', '.join([f'ad.{col}' for col in validated_columns])}
                        ORDER BY ad.hash_value
                    ) as rn
                FROM all_data ad
                JOIN strata_allocation sa ON 
                    {' AND '.join([f'sa.{col} = ad.{col}' for col in validated_columns])}
            )
            SELECT 
                logical_row_id, 
                row_hash, 
                row_data_json
            FROM stratified_sample
            WHERE rn <= samples_needed
        """
    
    def _build_where_clause(
        self, 
        filters: Dict[str, Any], 
        valid_columns: Set[str],
        column_types: Dict[str, str],
        param_start_index: int = 1
    ) -> Tuple[str, List[Any]]:
        """Securely builds a WHERE clause from filter specifications with type-aware casting."""
        if not filters or not filters.get('conditions'):
            return "", []

        conditions = []
        params = []
        param_idx = param_start_index

        for cond in filters['conditions']:
            column = cond.get('column')
            operator = cond.get('operator')
            value = cond.get('value')

            # SECURITY: Validate column name
            if not column or column not in valid_columns:
                raise ValueError(f"Invalid or unauthorized filter column: {column}")

            # SECURITY: Whitelist operator
            if operator not in self.ALLOWED_OPERATORS:
                raise ValueError(f"Invalid filter operator: {operator}")
            
            sql_op = self.ALLOWED_OPERATORS[operator]
            col_type = column_types.get(column, 'text')

            # Build condition based on operator type
            if operator in ['is_null', 'is_not_null']:
                conditions.append(f"r.data->>'{column}' {sql_op}")
            elif operator in ['in', 'not_in']:
                if not isinstance(value, list):
                    raise TypeError(f"Value for '{operator}' must be a list")
                placeholders = ', '.join([f'${i}' for i in range(param_idx, param_idx + len(value))])
                cast = self._get_type_cast(col_type)
                conditions.append(f"(r.data->>'{column}'){cast} {sql_op} ({placeholders})")
                params.extend(value)
                param_idx += len(value)
            else:
                # Apply appropriate type cast based on column type
                cast = self._get_type_cast(col_type)
                conditions.append(f"(r.data->>'{column}'){cast} {sql_op} ${param_idx}")
                params.append(value)
                param_idx += 1

        logic = filters.get('logic', 'AND').upper()
        if logic not in ['AND', 'OR']:
            raise ValueError(f"Invalid logic operator: {logic}")

        where_sql = f" AND ({' ' + logic + ' '.join(conditions)})" if conditions else ""
        return where_sql, params
    
    def _get_type_cast(self, col_type: str) -> str:
        """Returns appropriate PostgreSQL type cast based on column type."""
        type_map = {
            'integer': '::integer',
            'bigint': '::bigint',
            'numeric': '::numeric',
            'float': '::float',
            'double': '::double precision',
            'boolean': '::boolean',
            'date': '::date',
            'timestamp': '::timestamp',
            'time': '::time',
            'text': '',  # No cast needed for text
            'string': '',  # No cast needed
            'varchar': ''  # No cast needed
        }
        return type_map.get(col_type.lower(), '')
    
    def _build_selection_clause(
        self, 
        selection: Dict[str, Any], 
        valid_columns: Set[str]
    ) -> Tuple[str, str]:
        """Securely builds SELECT and ORDER BY clauses."""
        # Column selection
        if not selection or not selection.get('columns'):
            select_sql = "logical_row_id, row_hash, row_data_json"
        else:
            safe_cols = []
            for col in selection['columns']:
                if col in ['logical_row_id', 'row_hash', 'row_data_json']:
                    safe_cols.append(col)
                elif col in valid_columns:
                    safe_cols.append(f"row_data_json->>'{col}' as {col}")
                else:
                    raise ValueError(f"Invalid selection column: {col}")
            select_sql = ", ".join(safe_cols)

        # ORDER BY - Applied ONLY to final results, not source data!
        order_by_sql = ""
        if selection and selection.get('order_by'):
            order_col = selection['order_by']
            if order_col not in valid_columns:
                raise ValueError(f"Invalid order_by column: {order_col}")
            
            direction = "DESC" if selection.get('order_desc', False) else "ASC"
            # Use the column alias if it was selected, otherwise extract from JSON
            if order_col in selection.get('columns', []):
                order_by_sql = f'ORDER BY "{order_col}" {direction}'
            else:
                order_by_sql = f"ORDER BY row_data_json->>'{order_col}' {direction}"

        return select_sql, order_by_sql
    
    async def execute(self, job_id: str, parameters: Dict[str, Any], db_pool: DatabasePool) -> Dict[str, Any]:
        """Execute sampling job using SQL-based methods with streaming results."""
        logger.info(f"Starting sampling job {job_id} with parameters: {parameters}")
        
        # Create event bus and store for publishing events
        event_store = PostgresEventStore(db_pool)
        event_bus = InMemoryEventBus()
        event_bus.set_event_store(event_store)
        
        # Validate parameters
        if not isinstance(parameters, dict):
            raise TypeError(f"Expected parameters to be dict, got {type(parameters).__name__}")
        
        # Track execution metrics
        start_time = datetime.utcnow()
        total_sampled = 0
        round_results = []
        
        # Get job details for event publishing
        async with db_pool.acquire() as conn:
            job = await conn.fetchrow(
                "SELECT dataset_id, user_id FROM dsa_jobs.analysis_runs WHERE id = $1::uuid",
                job_id
            )
            dataset_id = job["dataset_id"] if job else parameters.get('dataset_id')
            user_id = job["user_id"] if job else parameters.get('user_id')
        
        try:
            # Publish job started event
            await event_bus.publish(JobStartedEvent(
                job_id=job_id,
                job_type='sampling',
                dataset_id=dataset_id,
                user_id=user_id
            ))
            
            async with db_pool.acquire() as conn:
                async with conn.transaction():
                    # Create temporary exclusion table with auto-cleanup
                    await conn.execute("""
                    CREATE TEMP TABLE IF NOT EXISTS temp_sampling_exclusions (
                        row_id TEXT PRIMARY KEY
                    ) ON COMMIT DROP
                    """)
                    
                    # Process each sampling round
                    rounds = parameters.get('rounds', [])
                    if not isinstance(rounds, list):
                        raise TypeError(f"Expected 'rounds' to be a list, got {type(rounds).__name__}")
                    
                    for round_idx, round_config in enumerate(rounds):
                        logger.info(f"Executing sampling round {round_idx + 1}")
                        
                        # Validate round_config
                        if not isinstance(round_config, dict):
                            raise TypeError(f"Expected round_config to be dict, got {type(round_config).__name__}")
                        
                        count, round_summary = await self._execute_sampling_round(
                            conn, 
                            parameters['source_commit_id'].strip(),  # Remove any trailing spaces
                            parameters.get('table_key', 'primary'),
                            round_config,
                            round_idx + 1
                        )
                        
                        total_sampled += count
                        round_results.append(round_summary)
                        
                        logger.info(f"Round {round_idx + 1} sampled {count} rows")
                    
                    # Export residual if requested
                    residual_count = 0
                    if parameters.get('export_residual', False):
                        residual_count = await self._export_residual_data(
                            conn,
                            parameters['source_commit_id'].strip(),  # Remove any trailing spaces
                            parameters.get('table_key', 'primary'),
                            parameters.get('residual_output_name', 'Residual Data')
                        )
                        logger.info(f"Exported {residual_count} residual rows")
                    
                    # Create output commit (always)
                    output_commit_id = await self._create_output_commit(
                        conn,
                        parameters['dataset_id'],
                        parameters['source_commit_id'].strip(),  # Remove any trailing spaces
                        parameters.get('user_id'),
                        parameters.get('commit_message', f'Sampled {total_sampled} rows'),
                        round_results
                    )
                    
                    logger.info(f"Created output commit: {output_commit_id}")
                    
                    # Create branch for the output commit
                    # Use provided branch name or default to commit ID
                    output_branch_name = parameters.get('output_branch_name') or output_commit_id
                    await self._create_output_branch(
                        conn,
                        parameters['dataset_id'],
                        output_branch_name,
                        output_commit_id
                    )
                    logger.info(f"Created output branch: {output_branch_name}")
                    
                    # Clean up temporary tables
                    for round_idx in range(len(round_results)):
                        round_table = f"temp_round_{round_idx + 1}_samples"
                        await conn.execute(f"DROP TABLE IF EXISTS {round_table}")
                
                    # Also drop residual table if it was created
                    await conn.execute("DROP TABLE IF EXISTS temp_residual_data")
            
            # Return job summary
            end_time = datetime.utcnow()
            result = {
                'status': 'completed',
                'total_sampled': total_sampled,
                'residual_count': residual_count,
                'output_commit_id': output_commit_id,
                'output_branch_name': parameters.get('output_branch_name') or output_commit_id,
                'round_results': round_results,
                'execution_time_seconds': (end_time - start_time).total_seconds(),
                'parameters_used': parameters
            }
            
            # Publish job completed event
            await event_bus.publish(JobCompletedEvent(
                job_id=job_id,
                job_type='sampling',
                dataset_id=dataset_id,
                user_id=user_id,
                result={
                    'total_sampled': total_sampled,
                    'output_commit_id': output_commit_id
                }
            ))
            
            return result
            
        except Exception as e:
            logger.error(f"Sampling job {job_id} failed: {str(e)}", exc_info=True)
            
            # Publish job failed event
            await event_bus.publish(JobFailedEvent(
                job_id=job_id,
                job_type='sampling',
                dataset_id=dataset_id,
                user_id=user_id,
                error_message=str(e)
            ))
            
            raise
    
    async def _execute_sampling_round(
        self, 
        conn: Connection,
        source_commit_id: str,
        table_key: str,
        round_config: Dict,
        round_number: int
    ) -> Tuple[int, Dict[str, Any]]:
        """Execute a single sampling round entirely in PostgreSQL."""
        method = round_config['method']
        params = round_config.get('parameters', {})
        
        # Ensure params is a dict
        if isinstance(params, str):
            try:
                params = json.loads(params)
            except json.JSONDecodeError:
                logger.error(f"Failed to parse parameters JSON in round {round_number}: {params}")
                params = {}
        elif not isinstance(params, dict):
            logger.error(f"Invalid parameters type in round {round_number}: {type(params).__name__}")
            params = {}
        
        logger.info(f"Round {round_number}: {method} sampling with params {params}")
        
        # Build query based on method
        if method == 'random':
            query, query_params = await self._build_random_query(
                conn, source_commit_id, table_key, params
            )
            
        elif method == 'stratified':
            # Check for the key parameter to decide which type of stratified sampling to run
            if 'samples_per_stratum' in params:
                # User wants a fixed number of samples from each stratum (Disproportional)
                logger.info("Using Disproportional Stratified Sampling (fixed-N per stratum)")
                query, query_params = await self._build_disproportional_stratified_query(
                    source_commit_id, table_key, params
                )
            else:
                # Default to existing Proportional sampling
                logger.info("Using Proportional Stratified Sampling")
                query, query_params = await self._build_stratified_sampling(
                    source_commit_id, table_key, params
                )
            
        elif method == 'systematic':
            query = self.SAMPLING_QUERIES['systematic']
            query_params = [
                source_commit_id, table_key,
                params.get('start', 1),
                params['interval']
            ]
            
        elif method == 'cluster':
            query, query_params = await self._build_cluster_query(
                source_commit_id, table_key, params
            )
        else:
            raise ValueError(f"Unsupported sampling method: {method}")
        
        # Create temporary table for this round's results
        round_table = f"temp_round_{round_number}_samples"
        
        # Always drop table if it exists to ensure clean state
        await conn.execute(f"DROP TABLE IF EXISTS {round_table}")
        
        # Add dynamic WHERE clause if filters are provided
        if params.get('filters'):
            # Get valid columns from schema
            valid_columns = await self._get_valid_columns(conn, source_commit_id, table_key)
            column_types = await self._get_column_types(conn, source_commit_id, table_key)
            
            where_clause, where_params = self._build_where_clause(
                params['filters'], 
                valid_columns, 
                column_types,
                len(query_params) + 1
            )
            
            # Inject WHERE clause into query
            query = query.replace(
                "WHERE m.commit_id = $1 AND m.logical_row_id LIKE ($2 || ':%')",
                f"WHERE m.commit_id = $1 AND m.logical_row_id LIKE ($2 || ':%'){where_clause}"
            )
            query_params.extend(where_params)
        
        # Handle selection and ordering if provided
        select_sql = "logical_row_id, row_hash, row_data_json"
        order_by_sql = ""
        
        if params.get('selection'):
            selection = params['selection']
            # Get valid columns
            valid_columns = await self._get_valid_columns(conn, source_commit_id, table_key)
            select_sql, order_by_sql = self._build_selection_clause(selection, valid_columns)
        
        # Wrap query with selection and ordering
        final_query = f"""
            WITH sampling_result AS (
                {query}
            )
            SELECT {select_sql}
            FROM sampling_result
            {order_by_sql}
        """
        
        # Execute sampling query
        await conn.execute(f"""
            CREATE TEMP TABLE {round_table} AS
            {final_query}
        """, *query_params)
        
        # Add to exclusions for next round
        await conn.execute(f"""
            INSERT INTO temp_sampling_exclusions (row_id)
            SELECT logical_row_id FROM {round_table}
            ON CONFLICT DO NOTHING
        """)
        
        # Get count and summary statistics
        count = await conn.fetchval(f"SELECT COUNT(*) FROM {round_table}")
        
        # Collect summary statistics
        summary = {
            'method': method,
            'round_number': round_number,
            'rows_sampled': count,
            'parameters': params,
            'output_name': params.get('output_name', f'Round {round_number} - {method}')
        }
        
        # Add method-specific statistics
        if method == 'stratified':
            strata_counts = await conn.fetch(f"""
                SELECT 
                    {', '.join([f"row_data_json->>'{col}' as {col}" for col in params['strata_columns']])},
                    COUNT(*) as count
                FROM {round_table}
                GROUP BY {', '.join([f"row_data_json->>'{col}'" for col in params['strata_columns']])}
            """)
            summary['strata_distribution'] = [dict(row) for row in strata_counts]
        
        return count, summary
    
    async def _build_random_query(
        self,
        conn: Connection,
        source_commit_id: str, 
        table_key: str, 
        params: Dict[str, Any]
    ) -> Tuple[str, List[Any]]:
        """Build appropriate random sampling query based on parameters."""
        # Validate required parameters
        if 'sample_size' not in params:
            raise ValueError("Random sampling requires 'sample_size' parameter")
        
        sample_size = params['sample_size']
        if not isinstance(sample_size, int) or sample_size <= 0:
            raise ValueError(f"Invalid sample_size: {sample_size}. Must be a positive integer")
        
        if params.get('seed'):
            # Use scalable hash filtering for large tables
            total_rows = params.get('total_rows')
            if not total_rows:
                # Get estimate from pg_class
                total_rows = await conn.fetchval("""
                    SELECT COALESCE(reltuples::bigint, 1000000) 
                    FROM pg_class 
                    WHERE oid = 'dsa_core.commit_rows'::regclass
                """)
            
            if total_rows > 100_000_000:
                query = self.SAMPLING_QUERIES['random_seeded_scalable']
                query_params = [
                    source_commit_id, table_key, 
                    params['sample_size'],
                    str(params['seed']),
                    self.config['oversampling_factor']
                ]
            else:
                query = self.SAMPLING_QUERIES['random_seeded_exact']
                query_params = [
                    source_commit_id, table_key, 
                    params['sample_size'],
                    str(params['seed'])
                ]
        else:
            query = self.SAMPLING_QUERIES['random_unseeded']
            query_params = [
                source_commit_id, table_key,
                params['sample_size']
            ]
        
        return query, query_params
    
    async def _build_stratified_sampling(
        self,
        source_commit_id: str,
        table_key: str,
        params: Dict[str, Any]
    ) -> Tuple[str, List[Any]]:
        """Build stratified sampling query."""
        # Validate column names
        validated_cols = [self._validate_column_name(col) 
                        for col in params['strata_columns']]
        
        query = self._build_stratified_query(
            validated_cols, 
            self.config['min_stratum_sample_count']
        )
        
        query_params = [
            source_commit_id, table_key,
            params.get('min_per_stratum', 1),
            params.get('sample_size', 10000),
            str(params.get('seed', 1))
        ]
        
        return query, query_params
    
    async def _build_cluster_query(
        self,
        source_commit_id: str,
        table_key: str,
        params: Dict[str, Any]
    ) -> Tuple[str, List[Any]]:
        """Build cluster sampling query."""
        # Validate cluster column
        cluster_col = self._validate_column_name(params['cluster_column'])
        
        # Determine if using percentage or fixed count
        if params.get('sample_percentage'):
            query = self.SAMPLING_QUERIES['cluster_percentage']
            within_cluster_param = params['sample_percentage']
        else:
            query = self.SAMPLING_QUERIES['cluster_fixed']
            within_cluster_param = params.get('samples_per_cluster', 100)
        
        query_params = [
            source_commit_id, table_key,
            params['num_clusters'],
            within_cluster_param,
            cluster_col,
            str(params.get('seed', 1))
        ]
        
        return query, query_params
    
    async def _build_disproportional_stratified_query(
        self,
        source_commit_id: str,
        table_key: str,
        params: Dict[str, Any]
    ) -> Tuple[str, List[Any]]:
        """Builds a disproportional stratified sampling query (fixed-N per stratum)."""
        # Validate required parameters
        if 'strata_columns' not in params or not params['strata_columns']:
            raise ValueError("Disproportional stratified sampling requires 'strata_columns'")
        if 'samples_per_stratum' not in params:
            raise ValueError("Disproportional stratified sampling requires 'samples_per_stratum'")

        validated_cols = [self._validate_column_name(col) for col in params['strata_columns']]
        
        # SQL expression to extract data from JSON for each stratum column
        # Need to handle nested data structure (data->data->column or data->column)
        data_expr = self._get_data_extract_sql()
        strata_grouping_sql = ', '.join([f"({data_expr}->>'{col}')" for col in validated_cols])
        
        # Get the template and format it with the dynamic column expressions
        query_template = self.SAMPLING_QUERIES['stratified_disproportional_fixed']
        query = query_template.format(strata_grouping_sql=strata_grouping_sql)
        
        # Prepare the query parameters for asyncpg
        query_params = [
            source_commit_id,
            table_key,
            params['samples_per_stratum'],
            str(params.get('seed', 'default_seed')) # Use a default seed if not provided
        ]
        
        return query, query_params
    
    async def _get_valid_columns(self, conn: Connection, commit_id: str, table_key: str) -> Set[str]:
        """Get valid column names from schema."""
        schema_json = await conn.fetchval("""
            SELECT schema_definition -> $2 AS table_schema
            FROM dsa_core.commit_schemas
            WHERE commit_id = $1
        """, commit_id, table_key)
        
        if not schema_json:
            # Fallback: sample data to get columns
            sample_row = await conn.fetchrow("""
                SELECT r.data
                FROM dsa_core.commit_rows m
                JOIN dsa_core.rows r ON m.row_hash = r.row_hash
                WHERE m.commit_id = $1 AND m.logical_row_id LIKE ($2 || ':%')
                LIMIT 1
            """, commit_id, table_key)
            
            if sample_row and sample_row['data']:
                data = json.loads(sample_row['data']) if isinstance(sample_row['data'], str) else sample_row['data']
                return set(data.keys())
            return set()
        
        # Extract column names from schema
        if isinstance(schema_json, str):
            schema_json = json.loads(schema_json)
        
        columns = set()
        if 'columns' in schema_json:
            for col in schema_json['columns']:
                columns.add(col['name'])
        
        return columns
    
    async def _get_column_types(self, conn: Connection, commit_id: str, table_key: str) -> Dict[str, str]:
        """Get column types from schema."""
        schema_json = await conn.fetchval("""
            SELECT schema_definition -> $2 AS table_schema
            FROM dsa_core.commit_schemas
            WHERE commit_id = $1
        """, commit_id, table_key)
        
        if not schema_json:
            return {}
        
        # Extract column types from schema
        if isinstance(schema_json, str):
            schema_json = json.loads(schema_json)
        
        column_types = {}
        if 'columns' in schema_json:
            for col in schema_json['columns']:
                column_types[col['name']] = col.get('type', 'text')
        
        return column_types
    
    async def _create_output_commit(
        self,
        conn: Connection,
        dataset_id: int,
        parent_commit_id: str,
        user_id: Optional[int],
        message: str,
        round_results: List[Dict[str, Any]]
    ) -> str:
        """Create a new commit with sampled data."""
        import hashlib
        from datetime import datetime
        
        # Generate commit ID
        commit_data = f"{dataset_id}{parent_commit_id}{message}{datetime.utcnow().isoformat()}"
        commit_id = hashlib.sha256(commit_data.encode()).hexdigest()
        
        # Create commit
        await conn.execute("""
            INSERT INTO dsa_core.commits (commit_id, dataset_id, parent_commit_id, message, author_id)
            VALUES ($1, $2, $3, $4, $5)
        """, commit_id, dataset_id, parent_commit_id, message, user_id)
        
        # Copy sampled rows to new commit
        # Use UNION to combine all rounds and remove duplicates
        union_parts = []
        for round_idx, _ in enumerate(round_results):
            round_table = f"temp_round_{round_idx + 1}_samples"
            union_parts.append(f"SELECT logical_row_id, row_hash FROM {round_table}")
        
        union_query = " UNION ".join(union_parts)
        
        await conn.execute(f"""
            INSERT INTO dsa_core.commit_rows (commit_id, logical_row_id, row_hash)
            SELECT $1, logical_row_id, row_hash
            FROM ({union_query}) AS all_samples
        """, commit_id)
        
        # Copy schema from parent
        await conn.execute("""
            INSERT INTO dsa_core.commit_schemas (commit_id, schema_definition)
            SELECT $1, schema_definition
            FROM dsa_core.commit_schemas
            WHERE commit_id = $2
        """, commit_id, parent_commit_id)
        
        # Store sampling metadata in table_analysis
        total_rows = sum(r['rows_sampled'] for r in round_results)
        table_key = 'primary'  # Default table key for sampling commits
        
        # Create analysis data with sampling metadata
        analysis_data = {
            'total_rows': total_rows,
            'columns': [],  # Sampling doesn't analyze columns
            'column_types': {},
            'null_counts': {},
            'sample_values': {},
            'statistics': {
                'sampling_metadata': {
                    'parent_commit': parent_commit_id,
                    'rounds': round_results,
                    'total_sampled': total_rows
                }
            }
        }
        
        await conn.execute("""
            INSERT INTO dsa_core.table_analysis (commit_id, table_key, analysis)
            VALUES ($1, $2, $3)
            ON CONFLICT (commit_id, table_key) 
            DO UPDATE SET analysis = EXCLUDED.analysis
        """, commit_id, table_key, json.dumps(analysis_data))
        
        return commit_id
    
    async def _export_residual_data(
        self,
        conn: Connection,
        source_commit_id: str,
        table_key: str,
        output_name: str
    ) -> int:
        """Export all unsampled rows as residual data."""
        # Create residual table using anti-join pattern
        residual_table = "temp_residual_data"
        
        await conn.execute(f"""
            CREATE TEMP TABLE {residual_table} AS
            WITH sampled_ids AS (
                SELECT row_id FROM temp_sampling_exclusions
            )
            SELECT m.logical_row_id, m.row_hash, r.data as row_data_json
            FROM dsa_core.commit_rows m
            JOIN dsa_core.rows r ON m.row_hash = r.row_hash
            LEFT JOIN sampled_ids si ON m.logical_row_id = si.row_id
            WHERE m.commit_id = $1 
            AND m.logical_row_id LIKE ($2 || ':%')
            AND si.row_id IS NULL
        """, source_commit_id, table_key)
        
        # Get count
        count = await conn.fetchval(f"SELECT COUNT(*) FROM {residual_table}")
        
        # For now, we'll just log the count
        logger.info(f"Residual data '{output_name}': {count} rows not sampled")
        
        return count


    async def _create_output_branch(self, conn: Connection, dataset_id: int, branch_name: str, commit_id: str) -> None:
        """Create a new branch pointing to the output commit."""
        await conn.execute("""
            INSERT INTO dsa_core.refs (dataset_id, name, commit_id)
            VALUES ($1, $2, $3)
            ON CONFLICT (dataset_id, name) DO UPDATE SET commit_id = EXCLUDED.commit_id
        """, dataset_id, branch_name, commit_id)


# Needed import for datetime
from datetime import datetime