-- FUNCTION: public.extract_advisor_sisters_details_and_initialization(text)

-- DROP FUNCTION IF EXISTS public.extract_advisor_sisters_details_and_initialization(text);

CREATE OR REPLACE FUNCTION public.extract_advisor_sisters_details_and_initialization(
	p_identifier_value text)
    RETURNS TABLE(clientid text, origin text)
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE

v_clientid TEXT;
v_origin TEXT;

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

    --Retrieve clientid and origin
    SELECT pc.partyclientidentifier,
            pc.partyorigin
    INTO   v_clientid,
           v_origin
    FROM   partycommon pc
    JOIN   partyidentifier pi
       ON pc.partyid = pi.partyid
    WHERE  pi.partyidentifiervalue = p_identifier_value;

    -- Create a temp table to hold party data
    DROP TABLE IF EXISTS temp_extraction_results_party;
    CREATE TEMP TABLE temp_extraction_results_party(
        partyid TEXT,
        clientid TEXT,
                origin  TEXT,
        sectiontype TEXT,
        sectionheader TEXT,
        data JSONB
    );--ON COMMIT DROP;

    -- Create a temp table to hold transaction data
    DROP TABLE IF EXISTS temp_extraction_results_transaction;
    CREATE TEMP TABLE temp_extraction_results_transaction (
        partyid TEXT,
        clientid TEXT,
                origin  TEXT,
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
                origin  TEXT,
                currency TEXT,
        sectiontype TEXT,
        sectionheader TEXT,
        recordtype TEXT,
        data JSONB
     );--ON COMMIT DROP;

    -- Create a temp table to hold instrument data
     DROP TABLE IF EXISTS temp_extraction_results_instrument;
     CREATE TEMP TABLE temp_extraction_results_instrument (
        partyid TEXT,
        clientid TEXT,
                origin  TEXT,
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
                origin  TEXT,
        sectiontype TEXT,
        sectionheader TEXT,
        data JSONB
    );--ON COMMIT DROP;

    -- Temp table to hold account number, contacttype and accounttype
    DROP TABLE IF EXISTS temp_account_details;
        CREATE TEMP TABLE temp_account_details (
        partyid TEXT,
        partycontacttype TEXT,
                accounttype TEXT,
                origin TEXT
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
    AND t1.partyorigin = v_origin
    AND t1.partyclientidentifier = v_clientid;

        INSERT INTO temp_account_details(partyid, partycontacttype, accounttype )
        VALUES ( p_identifier_value,
                                                        CASE
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
                        LEFT JOIN partyidentifier t7
                        ON t1.partyid = t7.partyid
                                AND t7.partyidentifiertype = 'Master'
--                      WHERE (t1.partyparentid = v_partyid
--                      OR t1.partyid = v_partyid )
                WHERE t7.partyidentifiertype='Master'
                AND t7.partyidentifiervalue= p_identifier_value
                        AND t1.partyorigin = v_origin
                        AND t1.partyclientidentifier = v_clientid ;

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
                WHERE t1.partyorigin = v_origin
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
--      CREATE TEMPORARY TABLE advisor_row_temp_table ON COMMIT DROP  AS

    IF v_partyparentid IS NOT NULL THEN

          PERFORM create_json_for_party_advisor(p_identifier_value, v_partyparentid , v_clientid, v_origin);

    END IF;

    -- Create a temp table for statement_dates for performance optimization
    CREATE TEMP TABLE temp_statement_dates_summary (
        partyid TEXT,
        BeginningDate TEXT,
        EndingDate TEXT,
        transactioncategory TEXT
    ) ON COMMIT DROP;

    -- Insert statement_dates into the temp_statement_dates_summary table
    INSERT INTO temp_statement_dates_summary (
    partyid,
    BeginningDate,
    EndingDate,
    transactioncategory
)
SELECT
    partyid,
    BeginningDate,
    EndingDate,
    transactioncategory
FROM (
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
    FROM transactionparty tp
    JOIN transactionevent tevent
        ON tp.transactionid = tevent.transactionid
       AND UPPER(tevent.transactioncategory) = 'HEADER DATE'
       AND tevent.transactionclientidentifier = v_clientid
       AND tevent.transactionorigin = v_origin
    LEFT JOIN transactiondate tdate
        ON tp.transactionid = tdate.transactionid
       AND tdate.transactiondatetype IN ('BeginningDate', 'ClosingDate')
    WHERE tp.transactionpartyidentifiervalue = p_identifier_value::TEXT
    GROUP BY tp.transactionpartyidentifiervalue
) s
LIMIT 1;

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
       RETURN;
    END IF;

    RAISE NOTICE 'currrr statement outside IF:%', curr_statementdate;

RETURN QUERY
    SELECT v_clientid, v_origin;

END;
$BODY$;

ALTER FUNCTION public.extract_advisor_sisters_details_and_initialization(text)
    OWNER TO "DBUser";
