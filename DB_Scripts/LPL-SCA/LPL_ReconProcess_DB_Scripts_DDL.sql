-- Drop FUNCTION
-- DROP FUNCTION public.lpl_sca_truncate_schema;

--✅ Step 1: Function to Truncate All Tables from selected schema with Optional Extra Exclusions
CREATE OR REPLACE FUNCTION public.lpl_sca_truncate_schema(p_client_name varchar, p_db_name varchar, p_exclude_tables varchar DEFAULT NULL)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    trnct_recon_stmt text;
	--exclude_tables TEXT[] := ARRAY['flyway_schema_history', 'migration_metadata', 'brx_db_log_details'];
    exclude_tables TEXT[] := ARRAY['brx_details','databasechangelog','databasechangeloglock'];
	schema_exists boolean;
	db_exists boolean;
	invalid_tables text[];
BEGIN
	--p_db_name = UPPER(p_db_name)||'-SCA';

	-- ✅ Check if database exists
    SELECT EXISTS (
        SELECT 1
        FROM pg_database
        WHERE UPPER(datname) = UPPER(p_db_name)
    )INTO db_exists;

    IF NOT db_exists THEN
        RETURN format('Error: Database "%s" does not exist.', p_db_name);
    END IF;

	-- Check if schema exists
    SELECT EXISTS (
        SELECT 1
        FROM pg_namespace
        WHERE nspname = 'public'
    )
    INTO schema_exists;

    IF NOT schema_exists THEN
        RETURN format('Error: Schema "%s" does not exist.', 'public');
    END IF;

	-- ✅ Merge user-provided exclude tables if not null or empty
    IF p_exclude_tables IS NOT NULL AND length(trim(p_exclude_tables)) > 0 THEN
        -- Split comma-separated string into array and append
        exclude_tables := exclude_tables || (SELECT array_agg(trim(both ' ' FROM t)) FROM unnest(string_to_array(p_exclude_tables, ',')) t);
    END IF;

	-- ✅ Validate exclude tables: find any that do not exist
    SELECT array_agg(lower(trim(t)))
    INTO invalid_tables
	FROM unnest(exclude_tables) t
    WHERE NOT EXISTS (
        SELECT 1
        FROM pg_tables
        WHERE schemaname = 'public'
          AND tablename = lower(t)
    );

	-- If any invalid tables found, return error
    IF invalid_tables IS NOT NULL THEN
        RETURN format(
            'Error: The following tables does not exist in schema "public": %s',
            array_to_string(invalid_tables, ', ')
        );
    END IF;

	-- ✅ Proceed only if client/db match
    IF UPPER(p_client_name) = 'LPL_SCA' AND UPPER(p_db_name) = 'LPL-SCA' THEN
		-- Build TRUNCATE statement for all tables in selected schema
		SELECT string_agg(
				   format('TRUNCATE TABLE %I.%I CASCADE;', 'public', tablename),
				   ' '
			   )
		INTO trnct_recon_stmt
		FROM pg_tables 
		WHERE schemaname = 'public' 
		  AND tablename NOT IN (SELECT unnest(exclude_tables))
		  AND tablename NOT LIKE 'pg_%'
		  AND tablename NOT LIKE 'sql_%';

		-- Execute the statement
		IF trnct_recon_stmt IS NOT NULL THEN
			EXECUTE trnct_recon_stmt;
			--PERFORM NOW();
		END IF;

		RETURN format('Success: Truncation completed for ''%s'' schema of ''%s'' database!', 'public', p_db_name);
	ELSE
        RETURN 'Failed: Truncation skipped! Please pass valid input parameter.';
    END IF;
END;
$$;


--select public.lpl_sca_truncate_schema('LPL_SCA','LPL-SCA');
--select public.lpl_sca_truncate_schema('LPL_SCA','LPL-SCA','bios_lpl_sca_out_recon, bios_lpl_sca_out_recon_data_account_list');
