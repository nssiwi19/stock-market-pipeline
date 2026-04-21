-- Create a safe read-only SQL RPC for Text-to-SQL agent
-- Usage from Python:
--   supabase.rpc("execute_readonly_sql", {"p_sql": "<SELECT ...>"})

CREATE OR REPLACE FUNCTION public.execute_readonly_sql(p_sql text)
RETURNS SETOF jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    v_sql text;
BEGIN
    v_sql := trim(coalesce(p_sql, ''));
    IF v_sql = '' THEN
        RAISE EXCEPTION 'SQL must not be empty';
    END IF;

    -- Remove trailing semicolon(s)
    v_sql := regexp_replace(v_sql, ';+\s*$', '', 'g');

    -- Block multi-statement
    IF position(';' in v_sql) > 0 THEN
        RAISE EXCEPTION 'Only one SQL statement is allowed';
    END IF;

    -- Allow only SELECT / CTE
    IF v_sql !~* '^\s*(select|with)\s+' THEN
        RAISE EXCEPTION 'Only SELECT/CTE statements are allowed';
    END IF;

    -- Block mutating and privileged keywords
    IF v_sql ~* '\b(insert|update|delete|drop|alter|create|truncate|grant|revoke|comment|copy|vacuum|analyze|refresh|call|do)\b' THEN
        RAISE EXCEPTION 'Forbidden keyword detected';
    END IF;

    -- Block system catalog access
    IF v_sql ~* '\b(pg_|information_schema)\w*' THEN
        RAISE EXCEPTION 'System catalog access is not allowed';
    END IF;

    -- Restrict to business tables
    IF v_sql !~* '\b(daily_prices|financial_reports|tickers)\b' THEN
        RAISE EXCEPTION 'Query must target allowed business tables';
    END IF;

    -- Force row cap to reduce risk
    RETURN QUERY EXECUTE format(
        'SELECT to_jsonb(t) FROM (%s) t LIMIT 500',
        v_sql
    );
END;
$$;

REVOKE ALL ON FUNCTION public.execute_readonly_sql(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.execute_readonly_sql(text) TO authenticated, service_role;
