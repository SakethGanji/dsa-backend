-- Fix for user: syntax causing errors when fuzzy=false
-- The issue: When all text is parsed as keywords, the main query becomes empty
-- and causes an error when trying to create a tsquery

CREATE OR REPLACE FUNCTION dsa_search.search(
    p_current_user_id INT,
    p_query TEXT DEFAULT NULL,
    p_fuzzy BOOLEAN DEFAULT TRUE,
    p_tags TEXT[] DEFAULT NULL,
    p_created_by INT[] DEFAULT NULL,
    p_created_after TIMESTAMPTZ DEFAULT NULL,
    p_created_before TIMESTAMPTZ DEFAULT NULL,
    p_updated_after TIMESTAMPTZ DEFAULT NULL,
    p_updated_before TIMESTAMPTZ DEFAULT NULL,
    p_limit INT DEFAULT 20,
    p_offset INT DEFAULT 0,
    p_sort_by TEXT DEFAULT 'relevance',
    p_sort_order TEXT DEFAULT 'desc',
    p_include_facets BOOLEAN DEFAULT TRUE,
    p_facet_fields TEXT[] DEFAULT ARRAY['tags', 'created_by']
)
RETURNS JSONB AS $$
DECLARE
    v_base_from TEXT;
    v_where_clauses TEXT[] := '{}';
    v_order_by_clause TEXT;
    v_sql TEXT;
    v_results JSONB;
    v_facets JSONB := '{}'::jsonb;
    v_total_count BIGINT;
    v_start_time TIMESTAMPTZ := clock_timestamp();
    v_main_query_text TEXT := p_query;
    v_kv_match RECORD;
    v_parsed_tags TEXT[] := '{}';
    v_parsed_users TEXT[] := '{}';
BEGIN
    -- Validate and sanitize inputs
    p_limit := LEAST(GREATEST(p_limit, 1), 100);
    p_offset := GREATEST(p_offset, 0);

    -- PERMISSION MODEL: Uses an INNER JOIN for performance
    v_base_from := format(
        'FROM dsa_search.datasets_summary s
         JOIN dsa_auth.dataset_permissions dp ON s.dataset_id = dp.dataset_id AND dp.user_id = %L',
        p_current_user_id
    );

    -- Parse special keywords from query (tag:value, user:value, by:value)
    IF v_main_query_text IS NOT NULL AND v_main_query_text <> '' THEN
        FOR v_kv_match IN
            SELECT (regexp_matches(v_main_query_text, '(tag|user|by):(\w+)', 'g')) AS m
        LOOP
            DECLARE
                v_key TEXT := v_kv_match.m[1];
                v_value TEXT := v_kv_match.m[2];
            BEGIN
                CASE v_key
                    WHEN 'tag' THEN v_parsed_tags := v_parsed_tags || v_value;
                    WHEN 'user', 'by' THEN v_parsed_users := v_parsed_users || v_value;
                END CASE;
            END;
        END LOOP;

        -- Remove parsed keywords from main query
        v_main_query_text := regexp_replace(v_main_query_text, '(tag|user|by):(\w+)', '', 'g');
        v_main_query_text := trim(regexp_replace(v_main_query_text, '\s{2,}', ' ', 'g'));
        
        -- IMPORTANT FIX: Set to NULL if empty after keyword extraction
        IF v_main_query_text = '' THEN
            v_main_query_text := NULL;
        END IF;
    END IF;

    -- Build search condition (only if there's actual search text)
    IF v_main_query_text IS NOT NULL AND v_main_query_text <> '' THEN
        IF p_fuzzy THEN
            v_where_clauses := v_where_clauses || format('similarity(s.search_text, %L) > 0.05', v_main_query_text);
        ELSE
            -- Additional safety check for tsquery
            DECLARE
                v_tsquery_text TEXT := array_to_string(regexp_split_to_array(v_main_query_text, E'\\s+'), ' & ');
            BEGIN
                -- Only add tsquery condition if the text is valid
                IF v_tsquery_text <> '' THEN
                    v_where_clauses := v_where_clauses || format(
                        's.search_tsv @@ to_tsquery(''english'', %L)',
                        v_tsquery_text
                    );
                END IF;
            END;
        END IF;
    END IF;

    -- Add filter conditions
    DECLARE
        v_all_tags TEXT[] := COALESCE(p_tags, '{}') || v_parsed_tags;
    BEGIN
        IF array_length(v_all_tags, 1) > 0 THEN
            v_where_clauses := v_where_clauses || format('s.tags @> %L', v_all_tags);
        END IF;
        IF p_created_by IS NOT NULL THEN
            v_where_clauses := v_where_clauses || format('s.created_by_id = ANY(%L)', p_created_by);
        END IF;
        IF array_length(v_parsed_users, 1) > 0 THEN
            v_where_clauses := v_where_clauses || format('s.created_by_name = ANY(%L)', v_parsed_users);
        END IF;
    END;

    -- Date filters
    IF p_created_after IS NOT NULL THEN
        v_where_clauses := v_where_clauses || format('s.created_at >= %L', p_created_after);
    END IF;
    IF p_created_before IS NOT NULL THEN
        v_where_clauses := v_where_clauses || format('s.created_at <= %L', p_created_before);
    END IF;
    IF p_updated_after IS NOT NULL THEN
        v_where_clauses := v_where_clauses || format('s.updated_at >= %L', p_updated_after);
    END IF;
    IF p_updated_before IS NOT NULL THEN
        v_where_clauses := v_where_clauses || format('s.updated_at <= %L', p_updated_before);
    END IF;

    -- Build ORDER BY clause
    v_order_by_clause := CASE
        WHEN v_main_query_text IS NOT NULL AND v_main_query_text <> '' AND p_sort_by = 'relevance' THEN
            format('similarity(fr.search_text, %L) %s', v_main_query_text, p_sort_order)
        WHEN p_sort_by = 'name' THEN 'fr.name ' || p_sort_order
        WHEN p_sort_by = 'created_at' THEN 'fr.created_at ' || p_sort_order
        WHEN p_sort_by = 'updated_at' THEN 'fr.updated_at ' || p_sort_order
        ELSE 'fr.updated_at DESC'
    END;

    -- Build and execute main query
    DECLARE
        v_where_string TEXT := CASE
            WHEN array_length(v_where_clauses, 1) > 0 THEN
                'WHERE ' || array_to_string(v_where_clauses, ' AND ')
            ELSE ''
        END;
        v_base_cte TEXT := format(
            'WITH filtered_results AS (
                SELECT s.*, dp.permission_type as user_permission %s %s
            )',
            v_base_from, v_where_string
        );
    BEGIN
        -- Get results and total count
        v_sql := v_base_cte || format('
            SELECT
                (SELECT COUNT(*) FROM filtered_results),
                (SELECT COALESCE(jsonb_agg(r), ''[]''::jsonb)
                 FROM (
                     SELECT
                         fr.dataset_id AS id,
                         fr.name,
                         fr.description,
                         fr.created_by_id as created_by,
                         fr.created_by_name,
                         fr.created_at,
                         fr.updated_at,
                         fr.tags,
                         (CASE WHEN %L IS NOT NULL THEN similarity(fr.search_text, %L) ELSE NULL END)::real AS score,
                         fr.user_permission
                     FROM filtered_results fr
                     ORDER BY %s
                     LIMIT %L
                     OFFSET %L
                 ) r)',
            v_main_query_text, v_main_query_text, v_order_by_clause, p_limit, p_offset
        );

        EXECUTE v_sql INTO v_total_count, v_results;

        -- Calculate facets if requested
        IF p_include_facets THEN
            DECLARE v_tag_facets jsonb; v_created_by_facets jsonb;
            BEGIN
                IF 'tags' = ANY(p_facet_fields) THEN
                    EXECUTE v_base_cte || '
                        SELECT COALESCE(jsonb_object_agg(value, count), ''{}''::jsonb)
                        FROM (
                            SELECT unnest(tags) as value, COUNT(*) as count
                            FROM filtered_results
                            GROUP BY value
                            ORDER BY count DESC
                            LIMIT 50
                        ) t;'
                    INTO v_tag_facets;
                    v_facets := jsonb_set(v_facets, '{tags}', v_tag_facets);
                END IF;

                IF 'created_by' = ANY(p_facet_fields) THEN
                    EXECUTE v_base_cte || '
                        SELECT COALESCE(jsonb_object_agg(value, count), ''{}''::jsonb)
                        FROM (
                            SELECT created_by_name as value, COUNT(*) as count
                            FROM filtered_results
                            WHERE created_by_name IS NOT NULL
                            GROUP BY value
                            ORDER BY count DESC
                            LIMIT 50
                        ) t;'
                    INTO v_created_by_facets;
                    v_facets := jsonb_set(v_facets, '{created_by}', v_created_by_facets);
                END IF;
            END;
        END IF;
    END;

    -- Return results
    RETURN jsonb_build_object(
        'results', v_results,
        'total', v_total_count,
        'limit', p_limit,
        'offset', p_offset,
        'has_more', (p_offset + p_limit) < v_total_count,
        'query', p_query,
        'execution_time_ms', (EXTRACT(EPOCH FROM clock_timestamp() - v_start_time) * 1000)::int,
        'facets', CASE WHEN p_include_facets THEN v_facets ELSE NULL END
    );
END;
$$ LANGUAGE plpgsql;