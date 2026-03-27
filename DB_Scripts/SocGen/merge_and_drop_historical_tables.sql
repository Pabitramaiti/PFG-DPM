CREATE OR REPLACE FUNCTION merge_and_drop_historical_tables(table_pattern TEXT, schema_name TEXT, run_type TEXT DEFAULT NULL) 
RETURNS void
LANGUAGE plpgsql
AS $$

DECLARE
    dyn_sql TEXT;
    drop_sql TEXT;
    missing_insert_sql TEXT;
    table_list TEXT[];
    batch_size INT := 50;
    batch_start INT := 1;
    batch_end INT;
    total_tables INT;
    historical_schema_name TEXT := 'AUX';
    -- table_pattern TEXT;
    beginning_date DATE;
    beginning_date_yyyymm TEXT;
    prev_date DATE;
    prev_table_name TEXT;
    target_table_name TEXT;
BEGIN
    -- Fetch and format BeginningDate directly as YYYYMM
	-- RAISE NOTICE 'table_pattern % ', table_pattern;
	-- RAISE NOTICE 'schema_name % ', schema_name;
 IF run_type = 'MONTHEND' OR run_type IS NULL THEN
    SELECT 
        COALESCE(beg.transactiondatevalue, '1970-01-01'::date)
    INTO beginning_date
    FROM transactionparty tp
    JOIN transactionevent tevent
        ON tp.transactionid = tevent.transactionid
        AND UPPER(tevent.transactioncategory) = 'HEADER DATE'
    LEFT JOIN transactiondate beg
        ON tp.transactionid = beg.transactionid
        AND beg.transactiondatetype = 'BeginningDate'
    LEFT JOIN transactiondate end1
        ON tp.transactionid = end1.transactionid
        AND end1.transactiondatetype = 'ClosingDate'
    LIMIT 1;

    -- Convert to YYYYMM for current (target) table
    beginning_date_yyyymm := TO_CHAR(beginning_date, 'YYYYMM');

    -- Compute previous month safely
    prev_date := (beginning_date - INTERVAL '1 month')::DATE;

    -- Construct both table names
    target_table_name := 'historicaltable_' || TO_CHAR(beginning_date, 'YYYYMM');
    prev_table_name   := 'historicaltable_' || TO_CHAR(prev_date, 'YYYYMM');
    -- table_pattern     := 'historicaltable_' || TO_CHAR(beginning_date, 'YYYY') || '_%';
    
    -- RAISE NOTICE 'beginning_date_yyyymm % ', beginning_date_yyyymm;
    -- RAISE NOTICE 'target_table_name % ', target_table_name;
    -- RAISE NOTICE 'prev_table_name % ', prev_table_name;

    -- Collect all source tables matching the pattern
    SELECT array_agg(tablename ORDER BY tablename)
    INTO table_list
    FROM pg_tables
    WHERE schemaname = schema_name
      AND tablename LIKE table_pattern;

    IF table_list IS NULL OR array_length(table_list, 1) = 0 THEN
        -- RAISE NOTICE 'No matching source tables found.';
        RETURN;
    END IF;

    total_tables := array_length(table_list, 1);
    -- RAISE NOTICE 'Found % tables to merge.', total_tables;

    -- Step 1: Process tables in batches
    WHILE batch_start <= total_tables LOOP
        batch_end := LEAST(batch_start + batch_size - 1, total_tables);

        -- RAISE NOTICE 'Processing batch % - %', batch_start, batch_end;

        SELECT INTO dyn_sql
            format(
                'INSERT INTO %I.%I
                 SELECT DISTINCT ON (partyid, clientid, sectionheader, classification, currency) *
                 FROM (%s) AS all_data
                 ON CONFLICT (partyid, clientid, sectionheader, classification, currency)
                 DO UPDATE SET
                     statementdate = EXCLUDED.statementdate,
                     recordtype = EXCLUDED.recordtype,
                     summaryforsection = EXCLUDED.summaryforsection,
                     summarydata = EXCLUDED.summarydata;',
                historical_schema_name,
                target_table_name,
                string_agg(format('SELECT * FROM %I.%I', schema_name, t), ' UNION ALL ')
            )
        FROM unnest(table_list[batch_start:batch_end]) AS t;

        EXECUTE dyn_sql;

        batch_start := batch_end + 1;
    END LOOP;

    -- Insert missing accounts from previous month

    IF EXISTS (
        SELECT 1 FROM pg_tables
        WHERE schemaname = historical_schema_name AND tablename = prev_table_name
    ) THEN

        missing_insert_sql := format($sql$
            INSERT INTO %I.%I (
                partyid,
                clientid,
                statementdate,
                currency,
                recordtype,
                sectionheader,
                summaryforsection,
                classification,
                summarydata
            )
            SELECT
                prev.partyid AS partyid,
                prev.clientid AS clientid,
                prev.statementdate AS statementdate,
                prev.currency AS currency,
                prev.recordtype AS recordtype,
                prev.sectionheader AS sectionheader,
                prev.summaryforsection AS summaryforsection,
                prev.classification AS classification,
                jsonb_set(
                    jsonb_set(
                        prev.summarydata,
                        '{previousperiod}',
                            to_jsonb((prev.summarydata->>'thisperiod')::numeric)
                        ),
                    '{thisperiod}',
                        '0'::jsonb
                ) AS summarydata
            FROM %I.%I prev
            LEFT JOIN %I.%I curr ON
                curr.partyid = prev.partyid
                AND curr.clientid = prev.clientid
                AND curr.sectionheader = prev.sectionheader
                AND curr.classification = prev.classification
                AND curr.currency = prev.currency
            WHERE curr.partyid IS NULL;
        $sql$, historical_schema_name, target_table_name, historical_schema_name, prev_table_name, historical_schema_name, target_table_name);

        EXECUTE missing_insert_sql;
    END IF;
 END IF;

    -- Step 3: Drop all source tables
    SELECT INTO drop_sql
        string_agg(format('DROP TABLE IF EXISTS %I.%I;', schema_name, tablename), ' ')
    FROM pg_tables
    WHERE schemaname = schema_name
      AND tablename LIKE table_pattern;

    IF drop_sql IS NOT NULL THEN
        EXECUTE drop_sql;
    END IF;

    -- RAISE NOTICE 'Merge completed successfully.';
END;

$$;