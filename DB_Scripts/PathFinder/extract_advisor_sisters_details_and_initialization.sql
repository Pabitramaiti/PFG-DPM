CREATE OR REPLACE FUNCTION public.extract_advisor_sisters_details_and_initialization(p_identifier_value text, clientid text, origin text)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE

v_partyid bigint;
v_partyparentid bigint;
v_partycontacttype text;

statement_dates JSONB;
historical_table_name TEXT;
prev_historical_table_name TEXT;
curr_statementdate TEXT;
curr_beginningDate TEXT;
prev_statementdate TEXT;

BEGIN

    -- Create a temp table to hold party data
    DROP TABLE IF EXISTS temp_extraction_results_party;
    CREATE TEMP TABLE temp_extraction_results_party(
        partyid TEXT,
        clientid TEXT,
		origin	TEXT,
        sectiontype TEXT,
        sectionheader TEXT,
        data JSONB
    );--ON COMMIT DROP;

    -- Create a temp table to hold transaction data
    DROP TABLE IF EXISTS temp_extraction_results_transaction;
    CREATE TEMP TABLE temp_extraction_results_transaction (
        partyid TEXT,
        clientid TEXT,
		origin	TEXT,
		currency TEXT,
        sectiontype TEXT,
        sectionheader TEXT,
		recordtype TEXT,
        data JSONB
    );--ON COMMIT DROP;

    -- Create a temp table to hold position data
     DROP TABLE IF EXISTS temp_extraction_results_position;
     CREATE TEMP TABLE temp_extraction_results_position (
        partyid TEXT,
        clientid TEXT,
		origin	TEXT,
		currency TEXT,
        sectiontype TEXT,
        sectionheader TEXT,
        recordtype TEXT,
        data JSONB
     );--ON COMMIT DROP;

    -- Create a temp table to hold balance data
    DROP TABLE IF EXISTS temp_extraction_results_balance;
    CREATE TEMP TABLE temp_extraction_results_balance (
        partyid TEXT,
        clientid TEXT,
		origin TEXT,
        currency TEXT,
        sectiontype TEXT,
        sectionheader TEXT,
		recordtype TEXT,
        data JSONB
    );--ON COMMIT DROP;

    -- Create a temp table to hold individual summary data
    DROP TABLE IF EXISTS temp_extraction_results_summary;
	CREATE TEMP TABLE temp_extraction_results_summary (
        partyid TEXT,
        clientid TEXT,
		origin TEXT,
        currency TEXT,
        sectiontype TEXT,
        sectionheader TEXT,
        recordtype TEXT,
		amounttype TEXT,
		classification TEXT,
        data JSONB
    );--ON COMMIT DROP;

    -- Create a temp table to hold HouseHold summary data
    DROP TABLE IF EXISTS temp_extraction_results_hh_summary;
	CREATE TEMP TABLE temp_extraction_results_hh_summary (
        partyid TEXT,
        clientid TEXT,
		origin TEXT,
        currency TEXT,
        sectiontype TEXT,
        sectionheader TEXT,
        recordtype TEXT,
		amounttype TEXT,
		classification TEXT,
        data JSONB,
		PRIMARY KEY (clientid, origin, currency, sectiontype, sectionheader, recordtype, amounttype, classification)
    );--ON COMMIT DROP;

    -- Create a temp table to hold communication data
    DROP TABLE IF EXISTS temp_extraction_results_communication;
    CREATE TEMP TABLE temp_extraction_results_communication(
        partyid TEXT,
        clientid TEXT,
		origin	TEXT,
        sectiontype TEXT,
        sectionheader TEXT,
        data JSONB
    );--ON COMMIT DROP;

    -- Temp table to hold account number, contacttype and accounttype
    DROP TABLE IF EXISTS temp_account_details;
	CREATE TEMP TABLE temp_account_details (
        partyid TEXT,
        partycontacttype TEXT,
		accounttype TEXT
    );--ON COMMIT DROP;

	-- create temp table for asset allocation total amount
	CREATE TEMP TABLE temp_extraction_hh_asset_allocation_total (
        currency TEXT,
        totalassetamount numeric,
		PRIMARY KEY (currency)
    )ON COMMIT DROP;

    -- create temp table for HouseHold Summary total amount
    CREATE TEMP TABLE temp_extraction_hh_household_summary_total (
       total_beginning_balance NUMERIC DEFAULT 0,
       total_ending_balance NUMERIC DEFAULT 0,
       total_change_in_mv NUMERIC DEFAULT 0
   )ON COMMIT DROP;

    SELECT
        t1.partyid::BIGINT, t1.partyparentid::BIGINT, t5.partycontacttype
		INTO v_partyid, v_partyparentid, v_partycontacttype

    FROM partycommon t1
    JOIN partyidentifier t4
        ON t1.partyid = t4.partyid
        AND t4.partyidentifiertype = 'OtherExternal'
    JOIN partyclassification t3
        ON t1.partyid = t3.partyid
        AND t3.partyclassificationvalue IN ('Account')
    JOIN partycontact t5
        ON t1.partyid = t5.partyid
        AND t5.partycontacttype IN ('AccountHolder','HouseHoldMasterAccount')
    JOIN partycontactmailingaddress t6
        ON t1.partyid = t6.partyid
        AND t6.partycontactaddresstype = 'PrimaryAddress'
    WHERE t4.partyidentifiervalue = p_identifier_value
    AND t1.partyorigin = origin
    AND t1.partyclientidentifier = clientid;

	INSERT INTO temp_account_details(partyid, partycontacttype, accounttype )
	VALUES ( p_identifier_value,
							CASE
						    --     WHEN v_partycontacttype = 'AccountHolder'
									 -- AND EXISTS (
										--  SELECT 1 FROM partyidentifier pi
										--  WHERE pi.partyid = v_partyid
										--    AND pi.partyidentifiertype = 'AccountGroupIdentifier' AND pi.partyidentifiervalue <> 'N/A'
									 -- ) THEN NULL
								WHEN v_partycontacttype = 'AccountHolder' THEN 'SINGLE'
								WHEN v_partycontacttype = 'HouseHoldMasterAccount' THEN 'PRIMARY'
								ELSE v_partycontacttype
							END,
		   					CASE
								WHEN v_partycontacttype = 'AccountHolder'
									 AND EXISTS (
										 SELECT 1 FROM partyidentifier pi
										 WHERE pi.partyid = v_partyid
										   AND pi.partyidentifiertype = 'AccountGroupIdentifier' AND pi.partyidentifiervalue <> 'N/A'
									 ) THEN 'MULTIACCOUNT'
								WHEN v_partycontacttype = 'AccountHolder' THEN 'SINGLE'
								WHEN v_partycontacttype = 'HouseHoldMasterAccount' THEN 'HOUSEHOLD'
								ELSE v_partycontacttype
							END);

	IF v_partycontacttype = 'HouseHoldMasterAccount' THEN

		INSERT INTO temp_account_details(partyid, partycontacttype)
			SELECT t4.partyidentifiervalue,
							CASE
								WHEN t5.partycontacttype = 'HouseHoldSisterAccount' THEN 'SECONDARY'
								ELSE t5.partycontacttype
							END

			FROM partycommon t1
			JOIN partyidentifier t4
				ON t1.partyid = t4.partyid
				AND t4.partyidentifiertype = 'OtherExternal'
			JOIN partyclassification t3
				ON t1.partyid = t3.partyid
				AND t3.partyclassificationvalue IN ('Account')
			JOIN partycontact t5
				ON t1.partyid = t5.partyid
				AND t5.partycontacttype IN ('HouseHoldSisterAccount')
			JOIN partycontactmailingaddress t6
				ON t1.partyid = t6.partyid
				AND t6.partycontactaddresstype = 'PrimaryAddress'
			WHERE (t1.partyparentid = v_partyid
			OR t1.partyid = v_partyid )
			AND t1.partyorigin = origin
			AND t1.partyclientidentifier = clientid ;

	END IF;

	-- To fetch related multi-accounts in the same
	IF v_partycontacttype = 'AccountHolder' AND EXISTS (
       SELECT 1
       FROM partyidentifier pi
       WHERE pi.partyid = v_partyid
         AND pi.partyidentifiertype = 'AccountGroupIdentifier' AND pi.partyidentifiervalue <> 'N/A'
   		)
	THEN
		INSERT INTO temp_account_details(partyid, partycontacttype)
		SELECT t4.partyidentifiervalue,
			   'SINGLE'
		FROM partycommon t1
		JOIN partyidentifier t4
			ON t1.partyid = t4.partyid
			AND t4.partyidentifiertype = 'OtherExternal'
		JOIN partyclassification t3
			ON t1.partyid = t3.partyid
			AND t3.partyclassificationvalue IN ('Account')
		JOIN partycontact t5
			ON t1.partyid = t5.partyid
			AND t5.partycontacttype IN ('AccountHolder')
		JOIN partycontactmailingaddress t6
			ON t1.partyid = t6.partyid
			AND t6.partycontactaddresstype = 'PrimaryAddress'
		WHERE t1.partyorigin = origin
		  AND t1.partyid <> v_partyid
		  AND EXISTS (
			  SELECT 1
			  FROM partyidentifier pi
			  WHERE pi.partyid = t1.partyid
				AND pi.partyidentifiertype = 'AccountGroupIdentifier'
				AND pi.partyidentifiervalue IN (
					SELECT partyidentifiervalue
					FROM partyidentifier
					WHERE partyid = v_partyid
					  AND partyidentifiertype = 'AccountGroupIdentifier'
					  AND pi.partyidentifiervalue <> 'N/A'
				)
		  );
	END IF;

	-- Getting common advisor row in temporary table, it will be supplied to all related accounts
--	CREATE TEMPORARY TABLE advisor_row_temp_table ON COMMIT DROP  AS
--	SELECT *, NULL::varchar AS partyidtype, NULL::varchar AS partyidvalue FROM extract_partydata_for_advisors(v_partyparentid , clientid, origin);
--
    IF v_partyparentid IS NOT NULL THEN

        PERFORM create_json_for_party_advisor(p_identifier_value, v_partyparentid , clientid, origin);

    END IF;

    -- creating historical tables and dates
    -- Create a temp table for statement_dates for performance optimization
    CREATE TEMP TABLE temp_statement_dates_summary (
        partyid TEXT,
        BeginningDate TEXT,
        EndingDate TEXT,
        transactioncategory TEXT
    ) ON COMMIT DROP;

    -- Insert statement_dates into the temp_statement_dates_summary table
    INSERT INTO temp_statement_dates_summary(partyid, BeginningDate, EndingDate, transactioncategory)
    SELECT
        tp.transactionpartyidentifiervalue AS partyid,
        MAX(
            CASE
                WHEN tdate.transactiondatetype = 'BeginningDate'
                THEN TO_CHAR(tdate.transactiondatevalue, 'YYYY-MM-DD') || ' 00:00:00'
            END
        ) AS BeginningDate,
        MAX(
            CASE
                WHEN tdate.transactiondatetype = 'ClosingDate'
                THEN TO_CHAR(tdate.transactiondatevalue, 'YYYY-MM-DD') || ' 00:00:00'
            END
        ) AS EndingDate,
        'HEADER DATE' AS transactioncategory
    FROM
        transactionparty tp
    JOIN
        transactionevent tevent ON tp.transactionid = tevent.transactionid
        AND UPPER(tevent.transactioncategory) = 'HEADER DATE'
		AND tevent.transactionclientidentifier =  clientid::TEXT
		AND tevent.transactionorigin = origin
    -- JOIN
        -- transactionclassification tclass ON tp.transactionid = tclass.transactionid
        -- AND UPPER(tclass.transactionclassificationvalue) = 'HEADER DATE'
    LEFT JOIN
        transactiondate tdate ON tp.transactionid = tdate.transactionid
        AND tdate.transactiondatetype IN ('BeginningDate', 'ClosingDate')
    WHERE
        tp.transactionpartyidentifiervalue = p_identifier_value::TEXT
    GROUP BY
        tp.transactionpartyidentifiervalue;

    -- Construct the statement_dates JSONB array
    SELECT jsonb_agg(
        jsonb_build_object(
            'partyid', tsd.partyid,
            'BeginningDate', tsd.BeginningDate,
            'EndingDate', tsd.EndingDate,
            'transactioncategory', tsd.transactioncategory
        )
    ) INTO statement_dates
    FROM temp_statement_dates_summary tsd;

    SELECT
        TO_CHAR(TO_DATE(BeginningDate, 'YYYY-MM-DD'), 'YYYYMM'),
        BeginningDate
    INTO curr_statementdate, curr_beginningDate
    FROM temp_statement_dates_summary
    LIMIT 1;

    -- If curr_statementdate is null or empty, return null immediately
    IF curr_statementdate IS NULL OR curr_statementdate = '' THEN
	   RAISE NOTICE 'curr_statementdate is found null Header entry missing in table for this party';
--	   RETURN  NULL;
       RETURN;
    END IF;
     -- curr_statementdate := TO_CHAR(TO_DATE(in_statementdate, 'YYYY-MM'), 'YYYYMM');
    historical_table_name := 'historicaltable_' || curr_statementdate;

    RAISE NOTICE 'Creating table: %', historical_table_name;

    -- Check if the table exists, and drop it if it does
    IF EXISTS (
        SELECT FROM pg_tables
        WHERE tablename = historical_table_name
    ) THEN
        EXECUTE format('DROP TABLE IF EXISTS %I', historical_table_name);
    END IF;

    -- Create the historical table with specified columns
    EXECUTE format($f$
        CREATE TABLE %I (
            partyid TEXT NOT NULL,
            clientid TEXT NOT NULL,
			origin TEXT NOT NULL,
            statementdate TEXT NOT NULL,
            recordtype TEXT,
            sectionheader TEXT,
            summaryforsection TEXT,
            classification TEXT NOT NULL,
            summarydata JSONB,
            currency TEXT NOT NULL,
            PRIMARY KEY (partyid, clientid, origin, sectionheader, summaryforsection, classification, currency)
        )
    $f$, historical_table_name);

    IF EXTRACT(MONTH FROM curr_beginningDate::DATE) != 1 THEN
        prev_statementdate := TO_CHAR((curr_beginningDate::DATE - INTERVAL '1 month')::DATE, 'YYYYMM');
        prev_historical_table_name := 'historicaltable_' || prev_statementdate;
    END IF;

    RAISE NOTICE 'Creating prev_statementdate: %', prev_statementdate;
    RAISE NOTICE 'Creating table: %', prev_historical_table_name;

    -- Conditionally create previous historical table, if needed
    IF prev_historical_table_name IS NOT NULL THEN
        IF NOT EXISTS (
            SELECT FROM pg_tables
            WHERE tablename = prev_historical_table_name
        ) THEN
            -- Create the previous historical table if it doesn't already exist
            EXECUTE format($f$
                CREATE TABLE %I (
                    partyid TEXT NOT NULL,
                    clientid TEXT NOT NULL,
					origin TEXT NOT NULL,
                    statementdate TEXT NOT NULL,
                    recordtype TEXT,
                    sectionheader TEXT,
                    summaryforsection TEXT,
                    classification TEXT NOT NULL,
                    summarydata JSONB,
                    currency TEXT NOT NULL,
                    PRIMARY KEY (partyid, clientid, origin, sectionheader, summaryforsection, classification, currency)
                )
            $f$, prev_historical_table_name);
        ELSE
            RAISE NOTICE 'Previous table already exists: %', prev_historical_table_name;
        END IF;
    END IF;
--- historical details end here

END;
$function$
