CREATE OR REPLACE FUNCTION query_staging_tables(table_pattern TEXT)

RETURNS TABLE (
    partyid TEXT,
	partyidentifierref TEXT,
	partyidentifier TEXT,
	clientid TEXT,
	statementtype TEXT,
    statement JSON
)
LANGUAGE plpgsql
AS $$
DECLARE
    dyn_sql TEXT;
BEGIN
    -- Build dynamic SQL with all matching tables
    SELECT string_agg(
        format('SELECT * FROM %I', tablename),
        ' UNION ALL '
    )
    INTO dyn_sql
    FROM pg_tables
    WHERE tablename LIKE table_pattern;

    IF dyn_sql IS NULL THEN
        RAISE NOTICE 'No matching tables for pattern: %', table_pattern;
        RETURN;
    END IF;

    -- Return combined results
    RETURN QUERY EXECUTE dyn_sql;
END;
$$;