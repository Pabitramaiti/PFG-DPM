CREATE OR REPLACE FUNCTION drop_staging_tables(table_pattern TEXT, schema_name TEXT)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    drop_sql TEXT;
    table_list TEXT[];
    batch_size INT := 50;
    batch_start INT := 1;
    batch_end INT;
    total_tables INT;
BEGIN
    -- Step 1: Collect matching tables
    SELECT array_agg(tablename ORDER BY tablename)
    INTO table_list
    FROM pg_tables
    WHERE schemaname = schema_name
      AND tablename LIKE table_pattern;

    IF table_list IS NULL OR array_length(table_list, 1) = 0 THEN
        -- RAISE NOTICE 'No matching tables found for pattern %.', table_pattern;
        RETURN;
    END IF;

    total_tables := array_length(table_list, 1);
    -- RAISE NOTICE 'Found % tables to drop.', total_tables;

    -- Step 2: Process in batches
    WHILE batch_start <= total_tables LOOP
        batch_end := LEAST(batch_start + batch_size - 1, total_tables);

        -- Build DROP statements for current batch
        SELECT INTO drop_sql
            string_agg(format('DROP TABLE IF EXISTS %I.%I;', schema_name, t), ' ')
        FROM unnest(table_list[batch_start:batch_end]) AS t;

        -- Execute batch
        -- RAISE NOTICE 'Dropping tables % to %...', batch_start, batch_end;
        EXECUTE drop_sql;

        batch_start := batch_end + 1;
    END LOOP;

    -- RAISE NOTICE 'All tables dropped successfully in batches.';
END;

$$;