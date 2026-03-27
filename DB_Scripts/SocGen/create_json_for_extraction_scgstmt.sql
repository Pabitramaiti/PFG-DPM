CREATE OR REPLACE FUNCTION create_json_for_extraction(accountpartyid TEXT, clientidentifier TEXT, historicaltablename TEXT)
RETURNS JSONB AS $$

DECLARE 
    section RECORD;
    transaction_data JSONB;
    combined_results JSONB;
    statement_dates JSONB;
    historical_table_name TEXT;
    prev_historical_table_name TEXT;
    curr_statementdate TEXT;
    curr_beginningDate TEXT;
    prev_statementdate TEXT;
    currency_data TEXT;
    currency_list TEXT[];
    clientid_list TEXT[];
    summary_detail_data JSONB;
    missing_summary_detail_data JSONB;
    summary_detail JSONB := '[]'::jsonb;
    summary_total JSONB;
    MMMFsummary_total JSONB;
    multicurrency_summary_detail_data JSONB;    
    multicurrency_summary_detail JSONB := '[]'::jsonb;
    multicurrency_summary_total JSONB;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    elapsed INTERVAL;
    sections_type_1 TEXT[] := ARRAY['Portfolio Value', 'Cash Activity Summary', 'Income Summary', 'Purchase and Sale', 'Pending Settlements', 'Other Activity', 'Equities - Long Positions', 'Equities - Short Positions', 'Money Market Funds', 'Mutual Funds', 'Corporate Bonds', 'Municipal Bonds', 'Government Agency REMIC', 'Convertible Bonds', 'Options', 'Foreign Fixed Income', 'Government Bonds', 'Other'];
    ----sections_type_1 TEXT[] := ARRAY['Multi-currency Account Net Equity', 'Purchase and Sale', 'Pending Settlements', 'Other Activity', 'Equities - Long Positions', 'Equities - Short Positions', 'Money Market Funds', 'Repurchase Agreement Activity', 'Dividends', 'Interest', 'Other Income', 'Withholdings', 'Funds Paid and Received', 'Mark-to-Market', 'Money Market Mutual Funds', 'Deposits', 'Options - Long Positions', 'Options - Short Positions', 'Fixed Income - Long Positions', 'Fixed Income - Short Positions', 'Other - Long Positions', 'Other - Short Positions', 'Repurchase Agreements', 'Pledge Detail Report', 'Statement of Billing Fees Collected - This is Not A Bill', 'Money Market Mutual Fund Activity', 'Deposit Activities'];
    --sections_type_1 TEXT[] := ARRAY['Multi-currency Account Net Equity', 'Purchase and Sale', 'Pending Settlements', 'Repurchase Agreement Activity', 'Dividends', 'Interest', 'Other Income', 'Withholdings', 'Funds Paid and Received', 'Other Activity', 'Mark-to-Market', 'Money Market Mutual Funds', 'Deposits', 'Equities - Long Positions', 'Equities - Short Positions', 'Options - Long Positions', 'Options - Short Positions', 'Fixed Income - Long Positions', 'Fixed Income - Short Positions', 'Other - Long Positions', 'Other - Short Positions', 'Repurchase Agreements', 'Pledge Detail Report'];
    sections_type_2 TEXT[] := ARRAY['Money Market Mutual Fund Activity', 'Deposit Activities'];
    sections_type_3 TEXT[] := ARRAY['Statement of Billing Fees Collected - This is Not A Bill']; 
	historical_schema_name TEXT := 'AUX';
    parts TEXT[];
    target_year TEXT;
    target_month TEXT;
    prev_year TEXT;
    prev_month TEXT;
    prev_month_table TEXT;
    prev_table_exists BOOLEAN;
    missing_sql TEXT;

BEGIN
    
    -- Create a temp table to hold intermediate JSONB rows
    CREATE TEMP TABLE temp_extraction_results (
        partyid TEXT,
        clientid TEXT,
        currency TEXT,
        sectiontype TEXT,
        sectionheader TEXT,
        recordtype TEXT,  
        securitygroup TEXT,  
        row_num INT,
        date DATE,      
        description TEXT,
        data JSONB
          -- );
       ) ON COMMIT DROP;
    
    -- Create a temp table for statement_dates for performance optimization
    CREATE TEMP TABLE temp_statement_dates (
        partyid TEXT,
        BeginningDate TEXT,
        EndingDate TEXT,
        transactioncategory TEXT
      --);
    ) ON COMMIT DROP;

    -- Insert statement_dates into the temp_statement_dates table

    INSERT INTO temp_statement_dates(partyid, BeginningDate, EndingDate, transactioncategory)
    SELECT 
        tp.transactionpartyidentifiervalue AS partyid,
        TO_CHAR(COALESCE(beg.transactiondatevalue, '1970-01-01'::date), 'YYYY-MM-DD') || ' 00:00:00' AS BeginningDate,
        TO_CHAR(COALESCE(end1.transactiondatevalue, '1970-01-01'::date), 'YYYY-MM-DD') || ' 00:00:00' AS EndingDate,
        'HEADER DATE' AS transactioncategory
    FROM 
        transactionparty tp
    JOIN 
        transactionevent tevent ON tp.transactionid = tevent.transactionid
        AND tevent.transactionclientidentifier = clientidentifier::TEXT
        AND UPPER(tevent.transactioncategory) = 'HEADER DATE'
    LEFT JOIN 
        transactiondate beg ON tp.transactionid = beg.transactionid AND beg.transactiondatetype = 'BeginningDate'
    LEFT JOIN 
        transactiondate end1 ON tp.transactionid = end1.transactionid AND end1.transactiondatetype = 'ClosingDate'
    WHERE 
        tp.transactionpartyidentifiervalue = accountpartyid::TEXT
    LIMIT 1;
    
    -- Construct the statement_dates JSONB array
    SELECT jsonb_agg(
        jsonb_build_object(
            'partyid', tsd.partyid,
			'sectionheader', 'Statement Date',
            'BeginningDate', tsd.BeginningDate,
            'EndingDate', tsd.EndingDate,
            'transactioncategory', tsd.transactioncategory
        )
    ) INTO statement_dates
    FROM temp_statement_dates tsd;
    
    -- RAISE NOTICE 'summary_total: %', summary_total;
    -- Get the BeginningDate from temp_statement_dates
    SELECT 
        TO_CHAR(TO_DATE(BeginningDate, 'YYYY-MM-DD'), 'YYYYMM'),
        BeginningDate
    INTO curr_statementdate, curr_beginningDate
    FROM temp_statement_dates
    LIMIT 1;
    
    -- If curr_statementdate is null or empty, return null immediately
    IF curr_statementdate IS NULL OR curr_statementdate = '' THEN
        RETURN NULL;
    END IF;
    
    INSERT INTO temp_extraction_results (
        partyid,
        clientid,
        currency,
        sectiontype,
        sectionheader,
        recordtype,
        securitygroup,
        data
    )
    VALUES (
        accountpartyid,       -- Use the variable directly
        clientidentifier,
        NULL,                 -- currency
        NULL,                 -- sectiontype
        'Statement Date',     -- sectionheader
        NULL,                 -- recordtype
        NULL,                 -- securitygroup
        statement_dates       -- Entire JSONB object
    );
    
    -- curr_statementdate := TO_CHAR(TO_DATE(in_statementdate, 'YYYY-MM'), 'YYYYMM');
    historical_table_name := 'historicaltable_' || curr_statementdate;
        
    -- Create the historical table with specified columns
    --EXECUTE format($f$
    --    CREATE TABLE IF NOT EXISTS %I.%I (
    --        partyid TEXT NOT NULL,
    --        clientid TEXT NOT NULL,
    --        statementdate TEXT NOT NULL,
    --        recordtype TEXT,
    --        sectionheader TEXT,
    --        summaryforsection TEXT,
    --        classification TEXT NOT NULL,
    --        summarydata JSONB,
    --        currency TEXT NOT NULL,
    --        PRIMARY KEY (partyid, clientid, sectionheader, classification, currency)
    --    )
    --$f$, historical_schema_name, historical_table_name);
    
    -- Check if the table exists, and delete rows matching partyid, clientid and statement date
    IF EXISTS (
      SELECT FROM pg_tables 
        WHERE schemaname = historical_schema_name AND tablename = historical_table_name 
     ) THEN
         EXECUTE format('DELETE FROM %I.%I WHERE partyid = $1 AND clientid = $2 AND statementdate = $3',
            historical_schema_name, historical_table_name
        )
        USING accountpartyid, clientidentifier, curr_statementdate;
    END IF;

    prev_statementdate := TO_CHAR((curr_beginningDate::DATE - INTERVAL '1 month')::DATE, 'YYYYMM');
    prev_historical_table_name := 'historicaltable_' || prev_statementdate;

    -- Conditionally create previous historical table, if needed
    --IF prev_historical_table_name IS NOT NULL THEN
    --    IF NOT EXISTS (
    --        SELECT FROM pg_tables
    --        WHERE schemaname = historical_schema_name AND tablename = prev_historical_table_name
    --    ) THEN
    --        -- Create the previous historical table if it doesn't already exist
    --        EXECUTE format($f$
    --            CREATE TABLE %I.%I (  
    --                partyid TEXT NOT NULL,
    --                clientid TEXT NOT NULL,
    --                statementdate TEXT NOT NULL,
    --                recordtype TEXT,
    --                sectionheader TEXT,
    --                summaryforsection TEXT,
    --                classification TEXT NOT NULL,
    --                summarydata JSONB,
    --                currency TEXT NOT NULL,
    --                PRIMARY KEY (partyid, clientid, sectionheader, classification, currency)
    --            )
    --        $f$, historical_schema_name, prev_historical_table_name);
    --    END IF;
    --END IF;
    
    start_time := clock_timestamp();
    IF array_length(sections_type_1, 1) IS NOT NULL THEN
        RAISE NOTICE 'sections_type_1: %', sections_type_1;
        RAISE NOTICE 'accountpartyid: %', accountpartyid;
        RAISE NOTICE 'clientidentifier: %', clientidentifier;
        PERFORM extract_statement_data(accountpartyid, clientidentifier, sections_type_1);
    END IF;
    
    end_time := clock_timestamp();
    elapsed := end_time - start_time;
    -- RAISE NOTICE 'Time taken for main: % ms', (EXTRACT(EPOCH FROM elapsed) * 1000)::INT;
        
    -- Combine all the extracted data
    --SELECT jsonb_agg(data) INTO transaction_data
    --FROM temp_extraction_results
    --WHERE sectionheader <> 'Multi-currency Account Net Equity'
    --AND NOT (
    --    sectionheader IN ('Money Market Mutual Fund Activity', 'Deposit Activities')
    --    AND data->>'recordtype' = 'Total'
    --);
    
    
    RETURN combined_results;

END;

$$ LANGUAGE plpgsql;