--
-- PostgreSQL database dump
--

-- Dumped from database version 14.1
-- Dumped by pg_dump version 14.18 (Ubuntu 14.18-0ubuntu0.22.04.1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: dsa_auth; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA dsa_auth;


ALTER SCHEMA dsa_auth OWNER TO postgres;

--
-- Name: dsa_core; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA dsa_core;


ALTER SCHEMA dsa_core OWNER TO postgres;

--
-- Name: dsa_jobs; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA dsa_jobs;


ALTER SCHEMA dsa_jobs OWNER TO postgres;

--
-- Name: dsa_search; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA dsa_search;


ALTER SCHEMA dsa_search OWNER TO postgres;

--
-- Name: btree_gin; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS btree_gin WITH SCHEMA public;


--
-- Name: EXTENSION btree_gin; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION btree_gin IS 'support for indexing common datatypes in GIN';


--
-- Name: pg_trgm; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;


--
-- Name: EXTENSION pg_trgm; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION pg_trgm IS 'text similarity measurement and index searching based on trigrams';


--
-- Name: pgcrypto; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;


--
-- Name: EXTENSION pgcrypto; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION pgcrypto IS 'cryptographic functions';


--
-- Name: uuid-ossp; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public;


--
-- Name: EXTENSION "uuid-ossp"; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION "uuid-ossp" IS 'generate universally unique identifiers (UUIDs)';


--
-- Name: dataset_permission; Type: TYPE; Schema: dsa_auth; Owner: postgres
--

CREATE TYPE dsa_auth.dataset_permission AS ENUM (
    'read',
    'write',
    'admin'
);


ALTER TYPE dsa_auth.dataset_permission OWNER TO postgres;

--
-- Name: analysis_run_status; Type: TYPE; Schema: dsa_jobs; Owner: postgres
--

CREATE TYPE dsa_jobs.analysis_run_status AS ENUM (
    'pending',
    'running',
    'completed',
    'failed'
);


ALTER TYPE dsa_jobs.analysis_run_status OWNER TO postgres;

--
-- Name: analysis_run_type; Type: TYPE; Schema: dsa_jobs; Owner: postgres
--

CREATE TYPE dsa_jobs.analysis_run_type AS ENUM (
    'import',
    'sampling',
    'exploration',
    'profiling'
);


ALTER TYPE dsa_jobs.analysis_run_type OWNER TO postgres;

--
-- Name: search_result; Type: TYPE; Schema: dsa_search; Owner: postgres
--

CREATE TYPE dsa_search.search_result AS (
	id integer,
	name character varying(255),
	description text,
	created_by integer,
	created_by_name character varying(20),
	created_at timestamp with time zone,
	updated_at timestamp with time zone,
	tags text[],
	score real,
	user_permission dsa_auth.dataset_permission
);


ALTER TYPE dsa_search.search_result OWNER TO postgres;

--
-- Name: search(integer, text, boolean, text[], integer[], timestamp with time zone, timestamp with time zone, timestamp with time zone, timestamp with time zone, integer, integer, text, text, boolean, text[]); Type: FUNCTION; Schema: dsa_search; Owner: postgres
--

CREATE FUNCTION dsa_search.search(p_current_user_id integer, p_query text DEFAULT NULL::text, p_fuzzy boolean DEFAULT true, p_tags text[] DEFAULT NULL::text[], p_created_by integer[] DEFAULT NULL::integer[], p_created_after timestamp with time zone DEFAULT NULL::timestamp with time zone, p_created_before timestamp with time zone DEFAULT NULL::timestamp with time zone, p_updated_after timestamp with time zone DEFAULT NULL::timestamp with time zone, p_updated_before timestamp with time zone DEFAULT NULL::timestamp with time zone, p_limit integer DEFAULT 20, p_offset integer DEFAULT 0, p_sort_by text DEFAULT 'relevance'::text, p_sort_order text DEFAULT 'desc'::text, p_include_facets boolean DEFAULT true, p_facet_fields text[] DEFAULT ARRAY['tags'::text, 'created_by'::text]) RETURNS jsonb
    LANGUAGE plpgsql STABLE
    AS $$
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
            SELECT (regexp_matches(v_main_query_text, '\b(tag|user|by):("([^"]+)"|[\w.-]+)\b', 'g')) AS m 
        LOOP
            DECLARE 
                v_key TEXT := v_kv_match.m[1]; 
                v_value TEXT := COALESCE(v_kv_match.m[3], v_kv_match.m[2]);
            BEGIN
                CASE v_key 
                    WHEN 'tag' THEN v_parsed_tags := v_parsed_tags || v_value; 
                    WHEN 'user', 'by' THEN v_parsed_users := v_parsed_users || v_value; 
                END CASE;
            END;
        END LOOP;
        
        -- Remove parsed keywords from main query
        v_main_query_text := regexp_replace(v_main_query_text, '\b(tag|user|by):("([^"]+)"|[\w.-]+)\b', '', 'g');
        v_main_query_text := trim(regexp_replace(v_main_query_text, '\s{2,}', ' ', 'g'));
    END IF;

    -- Build search condition
    IF v_main_query_text IS NOT NULL AND v_main_query_text <> '' THEN
        IF p_fuzzy THEN 
            v_where_clauses := v_where_clauses || format('similarity(s.search_text, %L) > 0.2', v_main_query_text);
        ELSE 
            v_where_clauses := v_where_clauses || format(
                's.search_tsv @@ to_tsquery(''english'', %L)', 
                array_to_string(regexp_split_to_array(v_main_query_text, E'\\s+'), ' & ')
            ); 
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
$$;


ALTER FUNCTION dsa_search.search(p_current_user_id integer, p_query text, p_fuzzy boolean, p_tags text[], p_created_by integer[], p_created_after timestamp with time zone, p_created_before timestamp with time zone, p_updated_after timestamp with time zone, p_updated_before timestamp with time zone, p_limit integer, p_offset integer, p_sort_by text, p_sort_order text, p_include_facets boolean, p_facet_fields text[]) OWNER TO postgres;

--
-- Name: suggest(integer, text, integer); Type: FUNCTION; Schema: dsa_search; Owner: postgres
--

CREATE FUNCTION dsa_search.suggest(p_current_user_id integer, p_query text, p_limit integer DEFAULT 10) RETURNS jsonb
    LANGUAGE plpgsql STABLE
    AS $$
DECLARE
    v_results JSONB; 
    v_start_time TIMESTAMPTZ := clock_timestamp();
BEGIN
    p_limit := LEAST(GREATEST(p_limit, 1), 50);
    
    WITH suggestions AS (
        -- Dataset name suggestions with permission check
        (SELECT 
            s.name AS text, 
            'dataset_name' AS type, 
            similarity(s.name, p_query) AS score
         FROM dsa_search.datasets_summary s
         JOIN dsa_auth.dataset_permissions dp ON s.dataset_id = dp.dataset_id
         WHERE s.name % p_query AND dp.user_id = p_current_user_id
         ORDER BY score DESC, length(s.name) ASC 
         LIMIT p_limit)
        
        UNION ALL
        
        -- Tag suggestions (no permission check needed)
        (SELECT 
            t.tag_name AS text, 
            'tag' AS type, 
            similarity(t.tag_name, p_query) AS score
         FROM dsa_core.tags t
         WHERE t.tag_name % p_query
         ORDER BY score DESC, length(t.tag_name) ASC 
         LIMIT p_limit)
    )
    SELECT COALESCE(jsonb_agg(s ORDER BY s.score DESC, s.type, s.text), '[]'::jsonb) 
    FROM (
        SELECT * FROM suggestions 
        ORDER BY score DESC, type, text 
        LIMIT p_limit
    ) s 
    INTO v_results;
    
    RETURN jsonb_build_object(
        'suggestions', v_results, 
        'query', p_query, 
        'execution_time_ms', (EXTRACT(EPOCH FROM clock_timestamp() - v_start_time) * 1000)::int
    );
END;
$$;


ALTER FUNCTION dsa_search.suggest(p_current_user_id integer, p_query text, p_limit integer) OWNER TO postgres;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: dataset_permissions; Type: TABLE; Schema: dsa_auth; Owner: postgres
--

CREATE TABLE dsa_auth.dataset_permissions (
    dataset_id integer NOT NULL,
    user_id integer NOT NULL,
    permission_type dsa_auth.dataset_permission NOT NULL
);


ALTER TABLE dsa_auth.dataset_permissions OWNER TO postgres;

--
-- Name: roles; Type: TABLE; Schema: dsa_auth; Owner: postgres
--

CREATE TABLE dsa_auth.roles (
    id integer NOT NULL,
    role_name character varying(50) NOT NULL,
    description text
);


ALTER TABLE dsa_auth.roles OWNER TO postgres;

--
-- Name: roles_id_seq; Type: SEQUENCE; Schema: dsa_auth; Owner: postgres
--

CREATE SEQUENCE dsa_auth.roles_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE dsa_auth.roles_id_seq OWNER TO postgres;

--
-- Name: roles_id_seq; Type: SEQUENCE OWNED BY; Schema: dsa_auth; Owner: postgres
--

ALTER SEQUENCE dsa_auth.roles_id_seq OWNED BY dsa_auth.roles.id;


--
-- Name: users; Type: TABLE; Schema: dsa_auth; Owner: postgres
--

CREATE TABLE dsa_auth.users (
    id integer NOT NULL,
    soeid character varying(20) NOT NULL,
    password_hash character varying(255) NOT NULL,
    role_id integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE dsa_auth.users OWNER TO postgres;

--
-- Name: users_id_seq; Type: SEQUENCE; Schema: dsa_auth; Owner: postgres
--

CREATE SEQUENCE dsa_auth.users_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE dsa_auth.users_id_seq OWNER TO postgres;

--
-- Name: users_id_seq; Type: SEQUENCE OWNED BY; Schema: dsa_auth; Owner: postgres
--

ALTER SEQUENCE dsa_auth.users_id_seq OWNED BY dsa_auth.users.id;


--
-- Name: commit_rows; Type: TABLE; Schema: dsa_core; Owner: postgres
--

CREATE TABLE dsa_core.commit_rows (
    commit_id character(64) NOT NULL,
    logical_row_id text NOT NULL,
    row_hash character(64) NOT NULL
);


ALTER TABLE dsa_core.commit_rows OWNER TO postgres;

--
-- Name: TABLE commit_rows; Type: COMMENT; Schema: dsa_core; Owner: postgres
--

COMMENT ON TABLE dsa_core.commit_rows IS 'The manifest linking a commit to its constituent rows.';


--
-- Name: commit_schemas; Type: TABLE; Schema: dsa_core; Owner: postgres
--

CREATE TABLE dsa_core.commit_schemas (
    id integer NOT NULL,
    commit_id character(64) NOT NULL,
    schema_definition jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE dsa_core.commit_schemas OWNER TO postgres;

--
-- Name: TABLE commit_schemas; Type: COMMENT; Schema: dsa_core; Owner: postgres
--

COMMENT ON TABLE dsa_core.commit_schemas IS 'Stores the schema definition(s) for a specific commit.';


--
-- Name: commit_schemas_id_seq; Type: SEQUENCE; Schema: dsa_core; Owner: postgres
--

CREATE SEQUENCE dsa_core.commit_schemas_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE dsa_core.commit_schemas_id_seq OWNER TO postgres;

--
-- Name: commit_schemas_id_seq; Type: SEQUENCE OWNED BY; Schema: dsa_core; Owner: postgres
--

ALTER SEQUENCE dsa_core.commit_schemas_id_seq OWNED BY dsa_core.commit_schemas.id;


--
-- Name: commit_statistics; Type: TABLE; Schema: dsa_core; Owner: postgres
--

CREATE TABLE dsa_core.commit_statistics (
    commit_id character(64) NOT NULL,
    row_count bigint,
    size_bytes bigint,
    statistics jsonb,
    computed_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE dsa_core.commit_statistics OWNER TO postgres;

--
-- Name: TABLE commit_statistics; Type: COMMENT; Schema: dsa_core; Owner: postgres
--

COMMENT ON TABLE dsa_core.commit_statistics IS 'Cached aggregate statistics for the data within a specific commit.';


--
-- Name: commits; Type: TABLE; Schema: dsa_core; Owner: postgres
--

CREATE TABLE dsa_core.commits (
    commit_id character(64) NOT NULL,
    dataset_id integer NOT NULL,
    parent_commit_id character(64),
    message text,
    author_id integer,
    authored_at timestamp with time zone,
    committed_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE dsa_core.commits OWNER TO postgres;

--
-- Name: TABLE commits; Type: COMMENT; Schema: dsa_core; Owner: postgres
--

COMMENT ON TABLE dsa_core.commits IS 'An immutable, point-in-time snapshot of a dataset.';


--
-- Name: dataset_tags; Type: TABLE; Schema: dsa_core; Owner: postgres
--

CREATE TABLE dsa_core.dataset_tags (
    dataset_id integer NOT NULL,
    tag_id integer NOT NULL
);


ALTER TABLE dsa_core.dataset_tags OWNER TO postgres;

--
-- Name: datasets; Type: TABLE; Schema: dsa_core; Owner: postgres
--

CREATE TABLE dsa_core.datasets (
    id integer NOT NULL,
    name character varying(255) NOT NULL,
    description text,
    created_by integer,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE dsa_core.datasets OWNER TO postgres;

--
-- Name: datasets_id_seq; Type: SEQUENCE; Schema: dsa_core; Owner: postgres
--

CREATE SEQUENCE dsa_core.datasets_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE dsa_core.datasets_id_seq OWNER TO postgres;

--
-- Name: datasets_id_seq; Type: SEQUENCE OWNED BY; Schema: dsa_core; Owner: postgres
--

ALTER SEQUENCE dsa_core.datasets_id_seq OWNED BY dsa_core.datasets.id;


--
-- Name: refs; Type: TABLE; Schema: dsa_core; Owner: postgres
--

CREATE TABLE dsa_core.refs (
    id integer NOT NULL,
    dataset_id integer NOT NULL,
    name text NOT NULL,
    commit_id character(64)
);


ALTER TABLE dsa_core.refs OWNER TO postgres;

--
-- Name: TABLE refs; Type: COMMENT; Schema: dsa_core; Owner: postgres
--

COMMENT ON TABLE dsa_core.refs IS 'Named pointers (branches, tags) to specific commits.';


--
-- Name: refs_id_seq; Type: SEQUENCE; Schema: dsa_core; Owner: postgres
--

CREATE SEQUENCE dsa_core.refs_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE dsa_core.refs_id_seq OWNER TO postgres;

--
-- Name: refs_id_seq; Type: SEQUENCE OWNED BY; Schema: dsa_core; Owner: postgres
--

ALTER SEQUENCE dsa_core.refs_id_seq OWNED BY dsa_core.refs.id;


--
-- Name: rows; Type: TABLE; Schema: dsa_core; Owner: postgres
--

CREATE TABLE dsa_core.rows (
    row_hash character(64) NOT NULL,
    data jsonb NOT NULL
);


ALTER TABLE dsa_core.rows OWNER TO postgres;

--
-- Name: TABLE rows; Type: COMMENT; Schema: dsa_core; Owner: postgres
--

COMMENT ON TABLE dsa_core.rows IS 'Content-addressable store for all unique data rows (blobs).';


--
-- Name: tags; Type: TABLE; Schema: dsa_core; Owner: postgres
--

CREATE TABLE dsa_core.tags (
    id integer NOT NULL,
    tag_name character varying(100) NOT NULL
);


ALTER TABLE dsa_core.tags OWNER TO postgres;

--
-- Name: tags_id_seq; Type: SEQUENCE; Schema: dsa_core; Owner: postgres
--

CREATE SEQUENCE dsa_core.tags_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE dsa_core.tags_id_seq OWNER TO postgres;

--
-- Name: tags_id_seq; Type: SEQUENCE OWNED BY; Schema: dsa_core; Owner: postgres
--

ALTER SEQUENCE dsa_core.tags_id_seq OWNED BY dsa_core.tags.id;


--
-- Name: analysis_configurations; Type: TABLE; Schema: dsa_jobs; Owner: postgres
--

CREATE TABLE dsa_jobs.analysis_configurations (
    id integer NOT NULL,
    name character varying(255) NOT NULL,
    description text,
    analysis_type character varying(100) NOT NULL,
    parameters jsonb NOT NULL,
    created_by integer,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE dsa_jobs.analysis_configurations OWNER TO postgres;

--
-- Name: analysis_configurations_id_seq; Type: SEQUENCE; Schema: dsa_jobs; Owner: postgres
--

CREATE SEQUENCE dsa_jobs.analysis_configurations_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE dsa_jobs.analysis_configurations_id_seq OWNER TO postgres;

--
-- Name: analysis_configurations_id_seq; Type: SEQUENCE OWNED BY; Schema: dsa_jobs; Owner: postgres
--

ALTER SEQUENCE dsa_jobs.analysis_configurations_id_seq OWNED BY dsa_jobs.analysis_configurations.id;


--
-- Name: analysis_runs; Type: TABLE; Schema: dsa_jobs; Owner: postgres
--

CREATE TABLE dsa_jobs.analysis_runs (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    run_type dsa_jobs.analysis_run_type NOT NULL,
    status dsa_jobs.analysis_run_status DEFAULT 'pending'::dsa_jobs.analysis_run_status NOT NULL,
    dataset_id integer NOT NULL,
    source_commit_id character(64),
    user_id integer,
    run_parameters jsonb,
    output_summary jsonb,
    error_message text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone
);


ALTER TABLE dsa_jobs.analysis_runs OWNER TO postgres;

--
-- Name: TABLE analysis_runs; Type: COMMENT; Schema: dsa_jobs; Owner: postgres
--

COMMENT ON TABLE dsa_jobs.analysis_runs IS 'The master job queue for all asynchronous operations.';


--
-- Name: datasets_summary; Type: MATERIALIZED VIEW; Schema: dsa_search; Owner: postgres
--

CREATE MATERIALIZED VIEW dsa_search.datasets_summary AS
 WITH dataset_tags_agg AS (
         SELECT dt.dataset_id,
            array_agg(t.tag_name ORDER BY t.tag_name) AS tags
           FROM (dsa_core.dataset_tags dt
             JOIN dsa_core.tags t ON ((dt.tag_id = t.id)))
          GROUP BY dt.dataset_id
        )
 SELECT d.id AS dataset_id,
    d.name,
    d.description,
    d.created_by AS created_by_id,
    u.soeid AS created_by_name,
    d.created_at,
    d.updated_at,
    COALESCE(dta.tags, ('{}'::text[])::character varying[]) AS tags,
    (((((d.name)::text || ' '::text) || COALESCE(d.description, ''::text)) || ' '::text) || array_to_string(COALESCE(dta.tags, '{}'::character varying[]), ' '::text)) AS search_text,
    ((to_tsvector('english'::regconfig, (d.name)::text) || to_tsvector('english'::regconfig, COALESCE(d.description, ''::text))) || to_tsvector('english'::regconfig, array_to_string(COALESCE(dta.tags, '{}'::character varying[]), ' '::text))) AS search_tsv
   FROM ((dsa_core.datasets d
     LEFT JOIN dsa_auth.users u ON ((d.created_by = u.id)))
     LEFT JOIN dataset_tags_agg dta ON ((d.id = dta.dataset_id)))
  WITH NO DATA;


ALTER TABLE dsa_search.datasets_summary OWNER TO postgres;

--
-- Name: roles id; Type: DEFAULT; Schema: dsa_auth; Owner: postgres
--

ALTER TABLE ONLY dsa_auth.roles ALTER COLUMN id SET DEFAULT nextval('dsa_auth.roles_id_seq'::regclass);


--
-- Name: users id; Type: DEFAULT; Schema: dsa_auth; Owner: postgres
--

ALTER TABLE ONLY dsa_auth.users ALTER COLUMN id SET DEFAULT nextval('dsa_auth.users_id_seq'::regclass);


--
-- Name: commit_schemas id; Type: DEFAULT; Schema: dsa_core; Owner: postgres
--

ALTER TABLE ONLY dsa_core.commit_schemas ALTER COLUMN id SET DEFAULT nextval('dsa_core.commit_schemas_id_seq'::regclass);


--
-- Name: datasets id; Type: DEFAULT; Schema: dsa_core; Owner: postgres
--

ALTER TABLE ONLY dsa_core.datasets ALTER COLUMN id SET DEFAULT nextval('dsa_core.datasets_id_seq'::regclass);


--
-- Name: refs id; Type: DEFAULT; Schema: dsa_core; Owner: postgres
--

ALTER TABLE ONLY dsa_core.refs ALTER COLUMN id SET DEFAULT nextval('dsa_core.refs_id_seq'::regclass);


--
-- Name: tags id; Type: DEFAULT; Schema: dsa_core; Owner: postgres
--

ALTER TABLE ONLY dsa_core.tags ALTER COLUMN id SET DEFAULT nextval('dsa_core.tags_id_seq'::regclass);


--
-- Name: analysis_configurations id; Type: DEFAULT; Schema: dsa_jobs; Owner: postgres
--

ALTER TABLE ONLY dsa_jobs.analysis_configurations ALTER COLUMN id SET DEFAULT nextval('dsa_jobs.analysis_configurations_id_seq'::regclass);


--
-- Name: dataset_permissions dataset_permissions_pkey; Type: CONSTRAINT; Schema: dsa_auth; Owner: postgres
--

ALTER TABLE ONLY dsa_auth.dataset_permissions
    ADD CONSTRAINT dataset_permissions_pkey PRIMARY KEY (dataset_id, user_id);


--
-- Name: roles roles_pkey; Type: CONSTRAINT; Schema: dsa_auth; Owner: postgres
--

ALTER TABLE ONLY dsa_auth.roles
    ADD CONSTRAINT roles_pkey PRIMARY KEY (id);


--
-- Name: roles roles_role_name_key; Type: CONSTRAINT; Schema: dsa_auth; Owner: postgres
--

ALTER TABLE ONLY dsa_auth.roles
    ADD CONSTRAINT roles_role_name_key UNIQUE (role_name);


--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: dsa_auth; Owner: postgres
--

ALTER TABLE ONLY dsa_auth.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);


--
-- Name: users users_soeid_key; Type: CONSTRAINT; Schema: dsa_auth; Owner: postgres
--

ALTER TABLE ONLY dsa_auth.users
    ADD CONSTRAINT users_soeid_key UNIQUE (soeid);


--
-- Name: commit_rows commit_rows_pkey; Type: CONSTRAINT; Schema: dsa_core; Owner: postgres
--

ALTER TABLE ONLY dsa_core.commit_rows
    ADD CONSTRAINT commit_rows_pkey PRIMARY KEY (commit_id, logical_row_id);


--
-- Name: commit_schemas commit_schemas_commit_id_key; Type: CONSTRAINT; Schema: dsa_core; Owner: postgres
--

ALTER TABLE ONLY dsa_core.commit_schemas
    ADD CONSTRAINT commit_schemas_commit_id_key UNIQUE (commit_id);


--
-- Name: commit_schemas commit_schemas_pkey; Type: CONSTRAINT; Schema: dsa_core; Owner: postgres
--

ALTER TABLE ONLY dsa_core.commit_schemas
    ADD CONSTRAINT commit_schemas_pkey PRIMARY KEY (id);


--
-- Name: commit_statistics commit_statistics_pkey; Type: CONSTRAINT; Schema: dsa_core; Owner: postgres
--

ALTER TABLE ONLY dsa_core.commit_statistics
    ADD CONSTRAINT commit_statistics_pkey PRIMARY KEY (commit_id);


--
-- Name: commits commits_pkey; Type: CONSTRAINT; Schema: dsa_core; Owner: postgres
--

ALTER TABLE ONLY dsa_core.commits
    ADD CONSTRAINT commits_pkey PRIMARY KEY (commit_id);


--
-- Name: dataset_tags dataset_tags_pkey; Type: CONSTRAINT; Schema: dsa_core; Owner: postgres
--

ALTER TABLE ONLY dsa_core.dataset_tags
    ADD CONSTRAINT dataset_tags_pkey PRIMARY KEY (dataset_id, tag_id);


--
-- Name: datasets datasets_name_created_by_key; Type: CONSTRAINT; Schema: dsa_core; Owner: postgres
--

ALTER TABLE ONLY dsa_core.datasets
    ADD CONSTRAINT datasets_name_created_by_key UNIQUE (name, created_by);


--
-- Name: datasets datasets_pkey; Type: CONSTRAINT; Schema: dsa_core; Owner: postgres
--

ALTER TABLE ONLY dsa_core.datasets
    ADD CONSTRAINT datasets_pkey PRIMARY KEY (id);


--
-- Name: refs refs_dataset_id_name_key; Type: CONSTRAINT; Schema: dsa_core; Owner: postgres
--

ALTER TABLE ONLY dsa_core.refs
    ADD CONSTRAINT refs_dataset_id_name_key UNIQUE (dataset_id, name);


--
-- Name: refs refs_pkey; Type: CONSTRAINT; Schema: dsa_core; Owner: postgres
--

ALTER TABLE ONLY dsa_core.refs
    ADD CONSTRAINT refs_pkey PRIMARY KEY (id);


--
-- Name: rows rows_pkey; Type: CONSTRAINT; Schema: dsa_core; Owner: postgres
--

ALTER TABLE ONLY dsa_core.rows
    ADD CONSTRAINT rows_pkey PRIMARY KEY (row_hash);


--
-- Name: tags tags_pkey; Type: CONSTRAINT; Schema: dsa_core; Owner: postgres
--

ALTER TABLE ONLY dsa_core.tags
    ADD CONSTRAINT tags_pkey PRIMARY KEY (id);


--
-- Name: tags tags_tag_name_key; Type: CONSTRAINT; Schema: dsa_core; Owner: postgres
--

ALTER TABLE ONLY dsa_core.tags
    ADD CONSTRAINT tags_tag_name_key UNIQUE (tag_name);


--
-- Name: analysis_configurations analysis_configurations_name_created_by_key; Type: CONSTRAINT; Schema: dsa_jobs; Owner: postgres
--

ALTER TABLE ONLY dsa_jobs.analysis_configurations
    ADD CONSTRAINT analysis_configurations_name_created_by_key UNIQUE (name, created_by);


--
-- Name: analysis_configurations analysis_configurations_pkey; Type: CONSTRAINT; Schema: dsa_jobs; Owner: postgres
--

ALTER TABLE ONLY dsa_jobs.analysis_configurations
    ADD CONSTRAINT analysis_configurations_pkey PRIMARY KEY (id);


--
-- Name: analysis_runs analysis_runs_pkey; Type: CONSTRAINT; Schema: dsa_jobs; Owner: postgres
--

ALTER TABLE ONLY dsa_jobs.analysis_runs
    ADD CONSTRAINT analysis_runs_pkey PRIMARY KEY (id);


--
-- Name: idx_dataset_permissions_dataset_id; Type: INDEX; Schema: dsa_auth; Owner: postgres
--

CREATE INDEX idx_dataset_permissions_dataset_id ON dsa_auth.dataset_permissions USING btree (dataset_id);


--
-- Name: idx_dataset_permissions_user_id; Type: INDEX; Schema: dsa_auth; Owner: postgres
--

CREATE INDEX idx_dataset_permissions_user_id ON dsa_auth.dataset_permissions USING btree (user_id);


--
-- Name: idx_commit_rows_row_hash; Type: INDEX; Schema: dsa_core; Owner: postgres
--

CREATE INDEX idx_commit_rows_row_hash ON dsa_core.commit_rows USING btree (row_hash);


--
-- Name: idx_commits_dataset_id; Type: INDEX; Schema: dsa_core; Owner: postgres
--

CREATE INDEX idx_commits_dataset_id ON dsa_core.commits USING btree (dataset_id);


--
-- Name: idx_analysis_runs_dataset_id; Type: INDEX; Schema: dsa_jobs; Owner: postgres
--

CREATE INDEX idx_analysis_runs_dataset_id ON dsa_jobs.analysis_runs USING btree (dataset_id);


--
-- Name: idx_analysis_runs_pending_jobs; Type: INDEX; Schema: dsa_jobs; Owner: postgres
--

CREATE INDEX idx_analysis_runs_pending_jobs ON dsa_jobs.analysis_runs USING btree (status, run_type) WHERE (status = 'pending'::dsa_jobs.analysis_run_status);


--
-- Name: idx_datasets_summary_created_at; Type: INDEX; Schema: dsa_search; Owner: postgres
--

CREATE INDEX idx_datasets_summary_created_at ON dsa_search.datasets_summary USING btree (created_at DESC);


--
-- Name: idx_datasets_summary_created_by_id; Type: INDEX; Schema: dsa_search; Owner: postgres
--

CREATE INDEX idx_datasets_summary_created_by_id ON dsa_search.datasets_summary USING btree (created_by_id);


--
-- Name: idx_datasets_summary_created_by_name; Type: INDEX; Schema: dsa_search; Owner: postgres
--

CREATE INDEX idx_datasets_summary_created_by_name ON dsa_search.datasets_summary USING btree (created_by_name);


--
-- Name: idx_datasets_summary_id; Type: INDEX; Schema: dsa_search; Owner: postgres
--

CREATE UNIQUE INDEX idx_datasets_summary_id ON dsa_search.datasets_summary USING btree (dataset_id);


--
-- Name: idx_datasets_summary_name; Type: INDEX; Schema: dsa_search; Owner: postgres
--

CREATE INDEX idx_datasets_summary_name ON dsa_search.datasets_summary USING btree (name);


--
-- Name: idx_datasets_summary_search_text_trgm; Type: INDEX; Schema: dsa_search; Owner: postgres
--

CREATE INDEX idx_datasets_summary_search_text_trgm ON dsa_search.datasets_summary USING gin (search_text public.gin_trgm_ops);


--
-- Name: idx_datasets_summary_search_tsv; Type: INDEX; Schema: dsa_search; Owner: postgres
--

CREATE INDEX idx_datasets_summary_search_tsv ON dsa_search.datasets_summary USING gin (search_tsv);


--
-- Name: idx_datasets_summary_tags; Type: INDEX; Schema: dsa_search; Owner: postgres
--

CREATE INDEX idx_datasets_summary_tags ON dsa_search.datasets_summary USING gin (tags);


--
-- Name: idx_datasets_summary_updated_at; Type: INDEX; Schema: dsa_search; Owner: postgres
--

CREATE INDEX idx_datasets_summary_updated_at ON dsa_search.datasets_summary USING btree (updated_at DESC);


--
-- Name: dataset_permissions dataset_permissions_dataset_id_fkey; Type: FK CONSTRAINT; Schema: dsa_auth; Owner: postgres
--

ALTER TABLE ONLY dsa_auth.dataset_permissions
    ADD CONSTRAINT dataset_permissions_dataset_id_fkey FOREIGN KEY (dataset_id) REFERENCES dsa_core.datasets(id) ON DELETE CASCADE;


--
-- Name: dataset_permissions dataset_permissions_user_id_fkey; Type: FK CONSTRAINT; Schema: dsa_auth; Owner: postgres
--

ALTER TABLE ONLY dsa_auth.dataset_permissions
    ADD CONSTRAINT dataset_permissions_user_id_fkey FOREIGN KEY (user_id) REFERENCES dsa_auth.users(id) ON DELETE CASCADE;


--
-- Name: users users_role_id_fkey; Type: FK CONSTRAINT; Schema: dsa_auth; Owner: postgres
--

ALTER TABLE ONLY dsa_auth.users
    ADD CONSTRAINT users_role_id_fkey FOREIGN KEY (role_id) REFERENCES dsa_auth.roles(id);


--
-- Name: commit_rows commit_rows_commit_id_fkey; Type: FK CONSTRAINT; Schema: dsa_core; Owner: postgres
--

ALTER TABLE ONLY dsa_core.commit_rows
    ADD CONSTRAINT commit_rows_commit_id_fkey FOREIGN KEY (commit_id) REFERENCES dsa_core.commits(commit_id) ON DELETE CASCADE;


--
-- Name: commit_rows commit_rows_row_hash_fkey; Type: FK CONSTRAINT; Schema: dsa_core; Owner: postgres
--

ALTER TABLE ONLY dsa_core.commit_rows
    ADD CONSTRAINT commit_rows_row_hash_fkey FOREIGN KEY (row_hash) REFERENCES dsa_core.rows(row_hash);


--
-- Name: commit_schemas commit_schemas_commit_id_fkey; Type: FK CONSTRAINT; Schema: dsa_core; Owner: postgres
--

ALTER TABLE ONLY dsa_core.commit_schemas
    ADD CONSTRAINT commit_schemas_commit_id_fkey FOREIGN KEY (commit_id) REFERENCES dsa_core.commits(commit_id) ON DELETE CASCADE;


--
-- Name: commit_statistics commit_statistics_commit_id_fkey; Type: FK CONSTRAINT; Schema: dsa_core; Owner: postgres
--

ALTER TABLE ONLY dsa_core.commit_statistics
    ADD CONSTRAINT commit_statistics_commit_id_fkey FOREIGN KEY (commit_id) REFERENCES dsa_core.commits(commit_id) ON DELETE CASCADE;


--
-- Name: commits commits_author_id_fkey; Type: FK CONSTRAINT; Schema: dsa_core; Owner: postgres
--

ALTER TABLE ONLY dsa_core.commits
    ADD CONSTRAINT commits_author_id_fkey FOREIGN KEY (author_id) REFERENCES dsa_auth.users(id) ON DELETE SET NULL;


--
-- Name: commits commits_dataset_id_fkey; Type: FK CONSTRAINT; Schema: dsa_core; Owner: postgres
--

ALTER TABLE ONLY dsa_core.commits
    ADD CONSTRAINT commits_dataset_id_fkey FOREIGN KEY (dataset_id) REFERENCES dsa_core.datasets(id) ON DELETE CASCADE;


--
-- Name: commits commits_parent_commit_id_fkey; Type: FK CONSTRAINT; Schema: dsa_core; Owner: postgres
--

ALTER TABLE ONLY dsa_core.commits
    ADD CONSTRAINT commits_parent_commit_id_fkey FOREIGN KEY (parent_commit_id) REFERENCES dsa_core.commits(commit_id);


--
-- Name: dataset_tags dataset_tags_dataset_id_fkey; Type: FK CONSTRAINT; Schema: dsa_core; Owner: postgres
--

ALTER TABLE ONLY dsa_core.dataset_tags
    ADD CONSTRAINT dataset_tags_dataset_id_fkey FOREIGN KEY (dataset_id) REFERENCES dsa_core.datasets(id) ON DELETE CASCADE;


--
-- Name: dataset_tags dataset_tags_tag_id_fkey; Type: FK CONSTRAINT; Schema: dsa_core; Owner: postgres
--

ALTER TABLE ONLY dsa_core.dataset_tags
    ADD CONSTRAINT dataset_tags_tag_id_fkey FOREIGN KEY (tag_id) REFERENCES dsa_core.tags(id) ON DELETE CASCADE;


--
-- Name: datasets datasets_created_by_fkey; Type: FK CONSTRAINT; Schema: dsa_core; Owner: postgres
--

ALTER TABLE ONLY dsa_core.datasets
    ADD CONSTRAINT datasets_created_by_fkey FOREIGN KEY (created_by) REFERENCES dsa_auth.users(id) ON DELETE SET NULL;


--
-- Name: refs refs_commit_id_fkey; Type: FK CONSTRAINT; Schema: dsa_core; Owner: postgres
--

ALTER TABLE ONLY dsa_core.refs
    ADD CONSTRAINT refs_commit_id_fkey FOREIGN KEY (commit_id) REFERENCES dsa_core.commits(commit_id) ON DELETE CASCADE;


--
-- Name: refs refs_dataset_id_fkey; Type: FK CONSTRAINT; Schema: dsa_core; Owner: postgres
--

ALTER TABLE ONLY dsa_core.refs
    ADD CONSTRAINT refs_dataset_id_fkey FOREIGN KEY (dataset_id) REFERENCES dsa_core.datasets(id) ON DELETE CASCADE;


--
-- Name: analysis_configurations analysis_configurations_created_by_fkey; Type: FK CONSTRAINT; Schema: dsa_jobs; Owner: postgres
--

ALTER TABLE ONLY dsa_jobs.analysis_configurations
    ADD CONSTRAINT analysis_configurations_created_by_fkey FOREIGN KEY (created_by) REFERENCES dsa_auth.users(id) ON DELETE SET NULL;


--
-- Name: analysis_runs analysis_runs_dataset_id_fkey; Type: FK CONSTRAINT; Schema: dsa_jobs; Owner: postgres
--

ALTER TABLE ONLY dsa_jobs.analysis_runs
    ADD CONSTRAINT analysis_runs_dataset_id_fkey FOREIGN KEY (dataset_id) REFERENCES dsa_core.datasets(id) ON DELETE CASCADE;


--
-- Name: analysis_runs analysis_runs_source_commit_id_fkey; Type: FK CONSTRAINT; Schema: dsa_jobs; Owner: postgres
--

ALTER TABLE ONLY dsa_jobs.analysis_runs
    ADD CONSTRAINT analysis_runs_source_commit_id_fkey FOREIGN KEY (source_commit_id) REFERENCES dsa_core.commits(commit_id) ON DELETE CASCADE;


--
-- Name: analysis_runs analysis_runs_user_id_fkey; Type: FK CONSTRAINT; Schema: dsa_jobs; Owner: postgres
--

ALTER TABLE ONLY dsa_jobs.analysis_runs
    ADD CONSTRAINT analysis_runs_user_id_fkey FOREIGN KEY (user_id) REFERENCES dsa_auth.users(id) ON DELETE SET NULL;


--
-- Name: SCHEMA dsa_search; Type: ACL; Schema: -; Owner: postgres
--

GRANT USAGE ON SCHEMA dsa_search TO PUBLIC;


--
-- Name: TABLE datasets_summary; Type: ACL; Schema: dsa_search; Owner: postgres
--

GRANT SELECT ON TABLE dsa_search.datasets_summary TO PUBLIC;


--
-- PostgreSQL database dump complete
--

