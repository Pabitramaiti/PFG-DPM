set search_path to public;

-- Get list of schemas in postgres database.
SELECT schema_name FROM information_schema.schemata;

-- Get list of tables from public schema.
SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name;

--list of function_name
SELECT n.nspname AS schema_name,
       p.proname AS function_name
FROM pg_proc p
JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
ORDER BY schema_name, function_name;

✅ Ways to “switch” databases
\c SocGen

--kill stacked process
SELECT pid, usename, state, wait_event, query_start, query
FROM pg_stat_activity
ORDER BY query_start;

SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid IN (9868,10607, 27225);
-----------------------------------------------------------------------------

DROP TABLE public.bios_wfs_out_recon;

CREATE TABLE public.bios_wfs_out_recon (
    filename varchar(200) NOT NULL,
    accountnumber varchar(50) NOT NULL,
    clientid varchar(50) NULL,
    status varchar(50) NULL,
    partytype varchar(50) NULL,
	partyid int8 NULL,
    partyparentid int8 NULL,
    errorjson varchar NULL,
    runtype varchar(50) NULL,
    historicaltrigger varchar(50) NULL,
    CONSTRAINT bios_wfs_out_recon_pkey PRIMARY KEY (filename)
);

CREATE INDEX index_bios_wfs_out_recon ON public.bios_wfs_out_recon USING btree (accountnumber, filename, status);

-- Permissions

ALTER TABLE public.bios_wfs_out_recon OWNER TO "DBUser";
GRANT ALL ON TABLE public.bios_wfs_out_recon TO "DBUser";

------------------------------------------------------------
-- Drop table

DROP TABLE public.bios_wfs_out_recon_data_account_list;

CREATE TABLE public.bios_wfs_out_recon_data_account_list (
	accountnumber varchar(50) NULL,
	clientid varchar(50) NULL,
	status varchar(50) NULL,
	runtype varchar(50) NULL,
	partytype varchar(50) NULL,
	partyid int8 NULL,
	partyparentid int8 NULL,
	brxstatus varchar(50) NULL,
	extractionstatus varchar(50) NULL,
	historicaltrigger varchar(50) NULL
);

CREATE INDEX index_account_list ON public.bios_wfs_out_recon_data_account_list USING btree (accountnumber, clientid, status);

-- Permissions

ALTER TABLE public.bios_wfs_out_recon_data_account_list OWNER TO "DBUser";
GRANT ALL ON TABLE public.bios_wfs_out_recon_data_account_list TO "DBUser";

CREATE INDEX idx_recon_acc_status ON public.bios_wfs_out_recon (accountNumber, status); 
CREATE INDEX idx_recon_acc ON public.bios_wfs_out_recon (accountNumber); 
CREATE INDEX idx_data_acc_brx ON public.bios_wfs_out_recon_data_account_list (accountNumber, brxStatus); 
CREATE INDEX idx_data_partytype ON public.bios_wfs_out_recon_data_account_list (partyType);
CREATE INDEX idx_recon_error ON public.bios_wfs_out_recon (accountNumber) WHERE status = 'ERROR'; 
CREATE INDEX idx_recon_processed ON public.bios_wfs_out_recon (accountNumber) WHERE status = 'PROCESSED';
---------------------------------

-- Drop FUNCTION
-- DROP FUNCTION public.wfs_truncate_schema;

--✅ Step 1: Function to Truncate All Tables from selected schema with Optional Extra Exclusions
CREATE OR REPLACE FUNCTION public.wfs_truncate_schema(p_client_name varchar, p_db_name varchar, p_exclude_tables varchar DEFAULT NULL)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    trnct_recon_stmt text;
	--exclude_tables TEXT[] := ARRAY['flyway_schema_history', 'migration_metadata', 'brx_db_log_details'];
    exclude_tables TEXT[] := ARRAY['brx_db_log_details','databasechangelog', 'databasechangeloglock'];
	schema_exists boolean;
	db_exists boolean;
	invalid_tables text[];
BEGIN

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
    IF UPPER(p_client_name) = 'WFS' AND UPPER(p_db_name) = 'WFS' THEN
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

-- select public.wfs_truncate_schema('WFS','WFS','bios_wfs_out_recon_data_account_list, bios_wfs_out_recon');
-- select public.wfs_truncate_schema('WFS','WFS',null);

-------------------------

-- select * from wfs_recon_stmt;

--✅ Step 2: Trigger Function to Call Procedure
CREATE OR REPLACE FUNCTION public.wfs_truncate_on_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    CALL public.truncate_wfs_public_schema_tables('Y');
    RETURN NEW;
END;
$$;

--✅ Step 3: Attach Trigger to Each Table

DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
    LOOP
        EXECUTE format(
            'CREATE TRIGGER public.trg_truncate_before_insert
             BEFORE INSERT ON public.%I
             FOR EACH ROW
             EXECUTE FUNCTION public.wfs_truncate_on_insert();',
            r.tablename
        );
    END LOOP;
END;
$$;


--------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION public.create_wfs_recon_tables()
RETURNS TEXT
LANGUAGE plpgsql
AS $$
BEGIN
    --------------------------------------------------------------------
    -- TABLE 1: bios_wfs_out_recon
    --------------------------------------------------------------------
    CREATE TABLE IF NOT EXISTS public.bios_wfs_out_recon (
        filename varchar(200) NOT NULL,
        accountnumber varchar(50) NOT NULL,
        clientid varchar(50),
        status varchar(50),
        partytype varchar(50),
        partyid int8,
        partyparentid int8,
        errorjson varchar,
        runtype varchar(50),
        historicaltrigger varchar(50),
        CONSTRAINT bios_wfs_out_recon_pkey PRIMARY KEY (filename)
    );

    CREATE INDEX IF NOT EXISTS index_bios_wfs_out_recon
        ON public.bios_wfs_out_recon (accountnumber, filename, status);

    ALTER TABLE public.bios_wfs_out_recon OWNER TO "DBUser";
    GRANT ALL ON TABLE public.bios_wfs_out_recon TO "DBUser";

    --------------------------------------------------------------------
    -- TABLE 2: bios_wfs_out_recon_data_account_list
    --------------------------------------------------------------------
    CREATE TABLE IF NOT EXISTS public.bios_wfs_out_recon_data_account_list (
        accountnumber varchar(50),
        clientid varchar(50),
        status varchar(50),
        runtype varchar(50),
        partytype varchar(50),
        partyid int8,
        partyparentid int8,
        brxstatus varchar(50),
        extractionstatus varchar(50),
        historicaltrigger varchar(50)
    );

    CREATE INDEX IF NOT EXISTS index_account_list
        ON public.bios_wfs_out_recon_data_account_list (accountnumber, clientid, status);

    ALTER TABLE public.bios_wfs_out_recon_data_account_list OWNER TO "DBUser";
    GRANT ALL ON TABLE public.bios_wfs_out_recon_data_account_list TO "DBUser";
RETURN 'WFS Recon tables are created!';
END;
$$;

select public.create_wfs_recon_tables();

-------------
-- Optimize below function.

CREATE OR REPLACE FUNCTION public.initialUpdateReconAccountListTable()
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    updated_count INTEGER;
BEGIN
    WITH updated AS (
        UPDATE public.bios_wfs_out_recon_data_account_list AS anl
        SET 
            partyType         = re.partyType,
            runType           = re.runType,
            historicalTrigger = re.historicalTrigger,
            partyId           = re.partyId,
            partyParentId     = re.partyParentId
        FROM public.bios_wfs_out_recon AS re
        WHERE anl.accountNumber = re.accountNumber
          AND anl.clientId     = re.clientId
          AND (re.status IS NOT NULL AND re.status <> '')
          AND (anl.brxStatus IS NULL OR anl.brxStatus = '')
          AND re.fileName LIKE '%COMBO%'
        RETURNING 1
    )
    SELECT COUNT(*) INTO updated_count FROM updated;

    RETURN updated_count;
END;
$$;

SELECT public.initialUpdateReconAccountListTable();

----------

-- Update individual ACCOUNTHOLDER
CREATE OR REPLACE FUNCTION public.updateAndGetIndividualAccountHolderAccountNumbers()
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    impacted_accounts TEXT;
BEGIN
	WITH eligible_accounts AS (
	--For PROCESSED Accounts
    SELECT DISTINCT re.accountNumber, re.status
    FROM public.bios_wfs_out_recon re
    JOIN public.bios_wfs_out_recon_data_account_list anl
      ON anl.accountNumber = re.accountNumber
    WHERE (anl.brxStatus IS NULL OR anl.brxStatus = '')
      AND UPPER(anl.partyType) = 'ACCOUNTHOLDER'
      AND re.status = 'PROCESSED'
      AND NOT EXISTS (
            SELECT 1
            FROM public.bios_wfs_out_recon err
            WHERE err.accountNumber = re.accountNumber
              AND err.status = 'ERROR'
	      )
	UNION ALL
		--For ERROR Accounts
		SELECT DISTINCT re.accountNumber, re.status
		FROM public.bios_wfs_out_recon re
		JOIN public.bios_wfs_out_recon_data_account_list anl
		  ON anl.accountNumber = re.accountNumber
		WHERE (anl.brxStatus IS NULL OR anl.brxStatus = '')
		  AND UPPER(anl.partyType) = 'ACCOUNTHOLDER'
		  AND re.status = 'ERROR'
		  AND EXISTS (
				SELECT 1
				FROM public.bios_wfs_out_recon err
				WHERE err.accountNumber = re.accountNumber
				  AND err.status = 'PROCESSED'
			  )
		)
	,
	updated_rows AS (
	    UPDATE public.bios_wfs_out_recon_data_account_list anl
	    SET brxStatus = ea.status
	    FROM eligible_accounts ea
	    WHERE anl.accountNumber = ea.accountNumber
	    RETURNING anl.accountNumber, anl.brxStatus
	)
	--SELECT string_agg(DISTINCT accountNumber, ',') INTO impacted_accounts
	--FROM updated_rows;
	SELECT COUNT(DISTINCT accountNumber)::TEXT INTO impacted_accounts FROM updated_rows;
	RETURN impacted_accounts;
END;
$$;

select public.updateAndGetIndividualAccountHolderAccountNumbers();

-- Update MasterAccounts which does not have any Sister Account

-- Update MasterAccounts which does not have any Sister Account
CREATE OR REPLACE FUNCTION public.updateAndGetIndividualMasterAccountNumbers()
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    impacted_accounts TEXT;
BEGIN
    WITH eligible_accounts AS (
	--For PROCESSED Accounts
        SELECT DISTINCT re.accountNumber, re.status
        FROM public.bios_wfs_out_recon re
        JOIN public.bios_wfs_out_recon_data_account_list anl
            ON anl.accountNumber = re.accountNumber
        WHERE (anl.brxStatus IS NULL OR anl.brxStatus = '')
          AND UPPER(anl.partyType) = 'HOUSEHOLDMASTERACCOUNT'
          AND (anl.partyparentid IS NULL OR ''||anl.partyparentid = '')
          AND re.status = 'PROCESSED'
          AND UPPER(re.partyType) <> 'HOUSEHOLDSISTERACCOUNT'
          AND NOT EXISTS (
                SELECT 1
                FROM public.bios_wfs_out_recon err
                WHERE err.accountNumber = re.accountNumber
                  AND err.status = 'ERROR'
                  AND UPPER(anl.partyType) = 'HOUSEHOLDMASTERACCOUNT'
                  AND (err.partyparentid IS NULL OR ''||err.partyparentid = '')
          )
          AND NOT EXISTS (
                SELECT 1
                FROM public.bios_wfs_out_recon sis
                WHERE sis.partyparentid = re.partyid
                  AND UPPER(sis.partyType) = 'HOUSEHOLDSISTERACCOUNT'
          )
	UNION ALL
		--For ERROR Accounts
		SELECT DISTINCT re.accountNumber, re.status
        FROM public.bios_wfs_out_recon re
        JOIN public.bios_wfs_out_recon_data_account_list anl
            ON anl.accountNumber = re.accountNumber
        WHERE (anl.brxStatus IS NULL OR anl.brxStatus = '')
          AND UPPER(anl.partyType) = 'HOUSEHOLDMASTERACCOUNT'
          AND (anl.partyparentid IS NULL OR ''||anl.partyparentid = '')
          AND re.status = 'ERROR'
          --AND UPPER(re.partyType) NOT IN ('HOUSEHOLDSISTERACCOUNT')
          AND NOT EXISTS (
                SELECT 1
                FROM public.bios_wfs_out_recon sis
                WHERE sis.partyparentid = re.partyid
                  AND UPPER(sis.partyType) = 'HOUSEHOLDSISTERACCOUNT'
          )
    ),
    updated_rows AS (
        UPDATE public.bios_wfs_out_recon_data_account_list anl
        SET brxStatus = ea.status
        FROM eligible_accounts ea
        WHERE anl.accountNumber = ea.accountNumber
        RETURNING anl.accountNumber
    )
    --SELECT string_agg(DISTINCT accountNumber, ',') INTO impacted_accounts FROM updated_rows;
	SELECT COUNT(DISTINCT accountNumber)::TEXT INTO impacted_accounts FROM updated_rows;
    RETURN impacted_accounts;
END;
$$;

select public.updateAndGetIndividualMasterAccountNumbers();


----------

CREATE OR REPLACE FUNCTION public.reconcile_all_master_accounts()
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    impacted_accounts TEXT;
BEGIN
    --------------------------------------------------------------------
    -- STEP 1: Update Master Accounts that have NO Sister Accounts
    --------------------------------------------------------------------
    --PERFORM public.updateAndGetIndividualMasterAccountNumbers();

    --------------------------------------------------------------------
    -- STEP 2: Find all pending accounts (master + sister)
    --------------------------------------------------------------------
    WITH pending AS (
        SELECT DISTINCT partyId
        FROM public.bios_wfs_out_recon_data_account_list
        WHERE brxStatus IS NULL OR brxStatus = ''
    ),

    --------------------------------------------------------------------
    -- STEP 3: Compute remaining master account statuses
    --------------------------------------------------------------------
    master_status AS (
        SELECT 
            r.accountNumber,
            r.partyId,
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE r.status='PROCESSED') AS processed,
            COUNT(*) FILTER (WHERE r.status='ERROR') AS error,
            COUNT(*) FILTER (WHERE r.status IS NULL OR r.status='') AS notprocessed,
            string_agg(DISTINCT r.status, ',') AS status_list
        FROM public.bios_wfs_out_recon r
        WHERE r.partyId IN (SELECT partyId FROM pending)
        GROUP BY r.accountNumber, r.partyId
    ),

    --------------------------------------------------------------------
    -- STEP 4: Decide final status for each master account
    --------------------------------------------------------------------
    decisions AS (
        SELECT 
            ms.accountNumber,
            CASE
                WHEN ms.error > 0 THEN 'ERROR'
                WHEN ms.processed = ms.total AND ms.notprocessed = 0 THEN 'PROCESSED'
                ELSE NULL
            END AS final_status
        FROM master_status ms
    ),

    --------------------------------------------------------------------
    -- STEP 5: Apply updates
    --------------------------------------------------------------------
    updated_rows AS (
        UPDATE public.bios_wfs_out_recon_data_account_list a
        SET brxStatus = d.final_status
        FROM decisions d
        WHERE a.accountNumber = d.accountNumber
          AND d.final_status IS NOT NULL
          AND (a.brxStatus IS NULL OR a.brxStatus = '')
        RETURNING a.accountNumber
    )

    --SELECT string_agg(DISTINCT accountNumber, ',') INTO impacted_accounts FROM updated_rows;
	SELECT COUNT(DISTINCT accountNumber)::TEXT
    INTO impacted_accounts
    FROM updated_rows;
    RETURN impacted_accounts;
END;
$$;

select public.reconcile_all_master_accounts();

------------

CREATE OR REPLACE FUNCTION public.reconcile_master_sister_accounts()
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    impacted_accounts TEXT;
BEGIN
    WITH sister AS (
        SELECT 
            anl.accountNumber AS sis_acc,
            anl.partyParentId AS parent_id
        FROM public.bios_wfs_out_recon_data_account_list anl
        WHERE UPPER(anl.partyType) = 'HOUSEHOLDSISTERACCOUNT'
          AND (anl.brxStatus IS NULL OR anl.brxStatus = '')
          AND anl.partyParentId IS NOT NULL
    ),

    sister_status AS (
        SELECT 
            s.sis_acc,
            s.parent_id,
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE r.status='PROCESSED') AS processed,
            COUNT(*) FILTER (WHERE r.status='ERROR') AS error,
            COUNT(*) FILTER (WHERE r.status IS NULL OR r.status='') AS notprocessed
        FROM sister s
        JOIN public.bios_wfs_out_recon r 
            ON r.accountNumber = s.sis_acc
        GROUP BY s.sis_acc, s.parent_id
    ),

    master AS (
        SELECT DISTINCT 
            r.accountNumber AS master_acc,
            r.partyId AS parent_id
        FROM public.bios_wfs_out_recon r
        WHERE UPPER(r.partyType) = 'HOUSEHOLDMASTERACCOUNT'
    ),

    master_status AS (
        SELECT 
            m.master_acc,
            m.parent_id,
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE r.status='PROCESSED') AS processed,
            COUNT(*) FILTER (WHERE r.status='ERROR') AS error,
            COUNT(*) FILTER (WHERE r.status IS NULL OR r.status='') AS notprocessed,
            string_agg(DISTINCT r.status, ',') AS status_list
        FROM master m
        JOIN public.bios_wfs_out_recon r 
            ON r.accountNumber = m.master_acc
        GROUP BY m.master_acc, m.parent_id
    ),

    decisions AS (
        SELECT 
            ss.sis_acc,
            ms.master_acc,
            CASE
                WHEN ss.error > 0 THEN 'ERROR'
                WHEN ss.processed = ss.total AND ss.notprocessed = 0 AND ss.error = 0 THEN
                    CASE
                        WHEN ms.error = 0 AND ms.processed = ms.total THEN 'PROCESSED'
                        ELSE 'ERROR'
                    END
                ELSE NULL
            END AS final_status
        FROM sister_status ss
        JOIN master_status ms ON ms.parent_id = ss.parent_id
    ),

    updated_rows AS (
        UPDATE public.bios_wfs_out_recon_data_account_list a
        SET brxStatus = d.final_status
        FROM decisions d
        WHERE a.accountNumber IN (d.sis_acc, d.master_acc)
          AND d.final_status IS NOT NULL
        RETURNING a.accountNumber
    )

    --SELECT string_agg(DISTINCT accountNumber, ',')
    --INTO impacted_accounts
    --FROM updated_rows;
	SELECT COUNT(DISTINCT accountNumber)::TEXT
    INTO impacted_accounts
    FROM updated_rows;
	
    RETURN impacted_accounts;
END;
$$;

select public.reconcile_master_sister_accounts();

------Pabi----------------------------------============================

CREATE OR REPLACE FUNCTION public.initialUpdateReconAccountListTable_pabi()
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    updated_count INTEGER;
BEGIN
    WITH updated AS (
        UPDATE public.bios_wfs_out_recon_data_account_list_pabi AS anl
        SET 
            partyType         = re.partyType,
            runType           = re.runType,
            historicalTrigger = re.historicalTrigger,
            partyId           = re.partyId,
            partyParentId     = re.partyParentId
        FROM public.bios_wfs_out_recon_pabi AS re
        WHERE anl.accountNumber = re.accountNumber
          AND anl.clientId     = re.clientId
          AND (re.status IS NOT NULL OR re.status <> '')
          AND (anl.brxStatus IS NULL OR anl.brxStatus = '')
          AND re.fileName LIKE '%COMBO%'
        RETURNING 1
    )
    SELECT COUNT(*) INTO updated_count FROM updated;

    RETURN updated_count;
END;
$$;

SELECT public.initialUpdateReconAccountListTable_pabi();

---------------

-- Update individual ACCOUNTHOLDER

CREATE OR REPLACE FUNCTION public.updateAndGetIndividualAccountHolderAccountNumbers_pabi()
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    impacted_accounts TEXT;
BEGIN
	WITH eligible_accounts AS (
	--For PROCESSED Accounts
    SELECT DISTINCT re.accountNumber, re.status
    FROM public.bios_wfs_out_recon_pabi re
    JOIN public.bios_wfs_out_recon_data_account_list_pabi anl
      ON anl.accountNumber = re.accountNumber
    WHERE (anl.brxStatus IS NULL OR anl.brxStatus = '')
      AND UPPER(anl.partyType) = 'ACCOUNTHOLDER'
      AND re.status = 'PROCESSED'
      AND NOT EXISTS (
            SELECT 1
            FROM public.bios_wfs_out_recon_pabi err
            WHERE err.accountNumber = re.accountNumber
              AND err.status = 'ERROR'
	      )
	UNION ALL
		--For ERROR Accounts
		SELECT DISTINCT re.accountNumber, re.status
		FROM public.bios_wfs_out_recon_pabi re
		JOIN public.bios_wfs_out_recon_data_account_list_pabi anl
		  ON anl.accountNumber = re.accountNumber
		WHERE (anl.brxStatus IS NULL OR anl.brxStatus = '')
		  AND UPPER(anl.partyType) = 'ACCOUNTHOLDER'
		  AND re.status = 'ERROR'
		  AND EXISTS (
				SELECT 1
				FROM public.bios_wfs_out_recon_pabi err
				WHERE err.accountNumber = re.accountNumber
				  AND err.status = 'PROCESSED'
			  )
		)
	,
	updated_rows AS (
	    UPDATE public.bios_wfs_out_recon_data_account_list_pabi anl
	    SET brxStatus = ea.status
	    FROM eligible_accounts ea
	    WHERE anl.accountNumber = ea.accountNumber
	    RETURNING anl.accountNumber, anl.brxStatus
	)
	--SELECT string_agg(DISTINCT accountNumber, ',') INTO impacted_accounts
	--FROM updated_rows;
	SELECT COUNT(DISTINCT accountNumber)::TEXT INTO impacted_accounts FROM updated_rows;
	RETURN impacted_accounts;
END;
$$;

select public.updateAndGetIndividualAccountHolderAccountNumbers_pabi();


-- Update MasterAccounts which does not have any Sister Account
CREATE OR REPLACE FUNCTION public.updateAndGetIndividualMasterAccountNumbers_pabi()
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    impacted_accounts TEXT;
BEGIN
    WITH eligible_accounts AS (
	--For PROCESSED Accounts
        SELECT DISTINCT re.accountNumber, re.status
        FROM public.bios_wfs_out_recon_pabi re
        JOIN public.bios_wfs_out_recon_data_account_list_pabi anl
            ON anl.accountNumber = re.accountNumber
        WHERE (anl.brxStatus IS NULL OR anl.brxStatus = '')
          AND UPPER(anl.partyType) = 'HOUSEHOLDMASTERACCOUNT'
          AND (anl.partyparentid IS NULL OR ''||anl.partyparentid = '')
          AND re.status = 'PROCESSED'
          AND UPPER(re.partyType) <> 'HOUSEHOLDSISTERACCOUNT'
          AND NOT EXISTS (
                SELECT 1
                FROM public.bios_wfs_out_recon_pabi err
                WHERE err.accountNumber = re.accountNumber
                  AND err.status = 'ERROR'
                  AND UPPER(anl.partyType) = 'HOUSEHOLDMASTERACCOUNT'
                  AND (err.partyparentid IS NULL OR ''||err.partyparentid = '')
          )
          AND NOT EXISTS (
                SELECT 1
                FROM public.bios_wfs_out_recon_pabi sis
                WHERE sis.partyparentid = re.partyid
                  AND UPPER(sis.partyType) = 'HOUSEHOLDSISTERACCOUNT'
          )
	UNION ALL
		--For ERROR Accounts
		SELECT DISTINCT re.accountNumber, re.status
        FROM public.bios_wfs_out_recon_pabi re
        JOIN public.bios_wfs_out_recon_data_account_list_pabi anl
            ON anl.accountNumber = re.accountNumber
        WHERE (anl.brxStatus IS NULL OR anl.brxStatus = '')
          AND UPPER(anl.partyType) = 'HOUSEHOLDMASTERACCOUNT'
          AND (anl.partyparentid IS NULL OR ''||anl.partyparentid = '')
          AND re.status = 'ERROR'
          --AND UPPER(re.partyType) NOT IN ('HOUSEHOLDSISTERACCOUNT')
          AND NOT EXISTS (
                SELECT 1
                FROM public.bios_wfs_out_recon_pabi sis
                WHERE sis.partyparentid = re.partyid
                  AND UPPER(sis.partyType) = 'HOUSEHOLDSISTERACCOUNT'
          )
    ),
    updated_rows AS (
        UPDATE public.bios_wfs_out_recon_data_account_list_pabi anl
        SET brxStatus = ea.status
        FROM eligible_accounts ea
        WHERE anl.accountNumber = ea.accountNumber
        RETURNING anl.accountNumber
    )
    -- SELECT string_agg(DISTINCT accountNumber, ',') INTO impacted_accounts FROM updated_rows;
	SELECT COUNT(DISTINCT accountNumber)::TEXT INTO impacted_accounts FROM updated_rows;
    RETURN impacted_accounts;
END;
$$;

select public.updateAndGetIndividualMasterAccountNumbers_pabi();

----------

CREATE OR REPLACE FUNCTION public.reconcile_all_master_accounts_pabi()
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    impacted_accounts TEXT;
BEGIN
    --------------------------------------------------------------------
    -- STEP 1: Update Master Accounts that have NO Sister Accounts
    --------------------------------------------------------------------
    --PERFORM public.updateAndGetIndividualMasterAccountNumbers_pabi();

    --------------------------------------------------------------------
    -- STEP 2: Find all pending accounts (master + sister)
    --------------------------------------------------------------------
    WITH pending AS (
        SELECT DISTINCT partyId
        FROM public.bios_wfs_out_recon_data_account_list_pabi
        WHERE brxStatus IS NULL OR brxStatus = ''
    ),

    --------------------------------------------------------------------
    -- STEP 3: Compute remaining master account statuses
    --------------------------------------------------------------------
    master_status AS (
        SELECT 
            r.accountNumber,
            r.partyId,
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE r.status='PROCESSED') AS processed,
            COUNT(*) FILTER (WHERE r.status='ERROR') AS error,
            COUNT(*) FILTER (WHERE r.status IS NULL OR r.status='') AS notprocessed,
            string_agg(DISTINCT r.status, ',') AS status_list
        FROM public.bios_wfs_out_recon_pabi r
        WHERE r.partyId IN (SELECT partyId FROM pending)
        GROUP BY r.accountNumber, r.partyId
    ),

    --------------------------------------------------------------------
    -- STEP 4: Decide final status for each master account
    --------------------------------------------------------------------
    decisions AS (
        SELECT 
            ms.accountNumber,
            CASE
                WHEN ms.error > 0 THEN 'ERROR'
                WHEN ms.processed = ms.total AND ms.notprocessed = 0 THEN 'PROCESSED'
                ELSE NULL
            END AS final_status
        FROM master_status ms
    ),

    --------------------------------------------------------------------
    -- STEP 5: Apply updates
    --------------------------------------------------------------------
    updated_rows AS (
        UPDATE public.bios_wfs_out_recon_data_account_list_pabi a
        SET brxStatus = d.final_status
        FROM decisions d
        WHERE a.accountNumber = d.accountNumber
          AND d.final_status IS NOT NULL
          AND (a.brxStatus IS NULL OR a.brxStatus = '')
        RETURNING a.accountNumber
    )

    --SELECT string_agg(DISTINCT accountNumber, ',') INTO impacted_accounts FROM updated_rows;
	SELECT COUNT(DISTINCT accountNumber)::TEXT INTO impacted_accounts FROM updated_rows;
    RETURN impacted_accounts;
END;
$$;

select public.reconcile_all_master_accounts_pabi();

------------



CREATE OR REPLACE FUNCTION public.reconcile_master_sister_accounts_pabi()
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    impacted_accounts TEXT;
BEGIN
    WITH sister AS (
        SELECT 
            anl.accountNumber AS sis_acc,
            anl.partyParentId AS parent_id
        FROM public.bios_wfs_out_recon_data_account_list_pabi anl
        WHERE UPPER(anl.partyType) = 'HOUSEHOLDSISTERACCOUNT'
          AND (anl.brxStatus IS NULL OR anl.brxStatus = '')
          AND anl.partyParentId IS NOT NULL
    ),

    sister_status AS (
        SELECT 
            s.sis_acc,
            s.parent_id,
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE r.status='PROCESSED') AS processed,
            COUNT(*) FILTER (WHERE r.status='ERROR') AS error,
            COUNT(*) FILTER (WHERE r.status IS NULL OR r.status='') AS notprocessed
        FROM sister s
        JOIN public.bios_wfs_out_recon_pabi r 
            ON r.accountNumber = s.sis_acc
        GROUP BY s.sis_acc, s.parent_id
    ),

    master AS (
        SELECT DISTINCT 
            r.accountNumber AS master_acc,
            r.partyId AS parent_id
        FROM public.bios_wfs_out_recon_pabi r
        WHERE UPPER(r.partyType) = 'HOUSEHOLDMASTERACCOUNT'
    ),

    master_status AS (
        SELECT 
            m.master_acc,
            m.parent_id,
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE r.status='PROCESSED') AS processed,
            COUNT(*) FILTER (WHERE r.status='ERROR') AS error,
            COUNT(*) FILTER (WHERE r.status IS NULL OR r.status='') AS notprocessed,
            string_agg(DISTINCT r.status, ',') AS status_list
        FROM master m
        JOIN public.bios_wfs_out_recon_pabi r 
            ON r.accountNumber = m.master_acc
        GROUP BY m.master_acc, m.parent_id
    ),

    decisions AS (
        SELECT 
            ss.sis_acc,
            ms.master_acc,
            CASE
                WHEN ss.error > 0 THEN 'ERROR'
                WHEN ss.processed = ss.total AND ss.notprocessed = 0 AND ss.error = 0 THEN
                    CASE
                        WHEN ms.error = 0 AND ms.processed = ms.total THEN 'PROCESSED'
                        ELSE 'ERROR'
                    END
                ELSE NULL
            END AS final_status
        FROM sister_status ss
        JOIN master_status ms ON ms.parent_id = ss.parent_id
    ),

    updated_rows AS (
        UPDATE public.bios_wfs_out_recon_data_account_list_pabi a
        SET brxStatus = d.final_status
        FROM decisions d
        WHERE a.accountNumber IN (d.sis_acc, d.master_acc)
          AND d.final_status IS NOT NULL
        RETURNING a.accountNumber
    )

    --SELECT string_agg(DISTINCT accountNumber, ',') INTO impacted_accounts FROM updated_rows;
	SELECT COUNT(DISTINCT accountNumber)::TEXT INTO impacted_accounts FROM updated_rows;
    RETURN impacted_accounts;
END;
$$;

select public.reconcile_master_sister_accounts_pabi();
---------------------------------------------------------------------------------------------

