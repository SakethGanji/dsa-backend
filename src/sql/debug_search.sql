-- Debug version to understand what's happening
DO $$
DECLARE
    v_main_query_text TEXT := 'user:bg54677';
    v_kv_match RECORD;
    v_parsed_users TEXT[] := '{}';
    v_key TEXT;
    v_value TEXT;
BEGIN
    RAISE NOTICE 'Original query: %', v_main_query_text;
    
    -- Try the exact pattern from the function
    FOR v_kv_match IN
        SELECT (regexp_matches(v_main_query_text, '\b(tag|user|by):("([^"]+)"|[\w.-]+)\b', 'g')) AS m
    LOOP
        RAISE NOTICE 'Match found: %', v_kv_match.m;
        v_key := v_kv_match.m[1];
        v_value := COALESCE(v_kv_match.m[3], v_kv_match.m[2]);
        RAISE NOTICE 'Key: %, Value: %', v_key, v_value;
    END LOOP;
    
    -- Try simpler patterns
    RAISE NOTICE 'Testing simpler patterns...';
    
    -- Pattern 1: Basic
    FOR v_kv_match IN
        SELECT (regexp_matches(v_main_query_text, '(tag|user|by):(\w+)', 'g')) AS m
    LOOP
        RAISE NOTICE 'Simple match: %', v_kv_match.m;
    END LOOP;
    
    -- Pattern 2: With character class
    FOR v_kv_match IN
        SELECT (regexp_matches(v_main_query_text, '(tag|user|by):([a-zA-Z0-9]+)', 'g')) AS m
    LOOP
        RAISE NOTICE 'Char class match: %', v_kv_match.m;
    END LOOP;
    
    -- Test removal
    v_main_query_text := 'user:bg54677';
    v_main_query_text := regexp_replace(v_main_query_text, '(tag|user|by):(\w+)', '', 'g');
    RAISE NOTICE 'After removal: [%]', v_main_query_text;
END
$$;