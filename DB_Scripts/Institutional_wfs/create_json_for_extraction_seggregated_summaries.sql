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
    sections_type_1 TEXT[] := ARRAY['Multi-currency Account Net Equity', 'Purchase and Sale', 'Pending Settlements', 'Repurchase Agreement Activity', 'Dividends', 'Interest', 'Other Income', 'Withholdings', 'Funds Paid and Received', 'Other Activity', 'Mark-to-Market', 'Money Market Mutual Funds', 'Deposits', 'Equities - Long Positions', 'Equities - Short Positions', 'Options - Long Positions', 'Options - Short Positions', 'Fixed Income - Long Positions', 'Fixed Income - Short Positions', 'Other - Long Positions', 'Other - Short Positions', 'Repurchase Agreements', 'Pledge Detail Report', 'Statement of Billing Fees Collected - This is Not A Bill', 'Money Market Mutual Fund Activity', 'Deposit Activities'];
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
    
    SELECT ARRAY(
            SELECT DISTINCT currency
            FROM temp_extraction_results
            WHERE recordtype = 'Total' AND clientid = clientidentifier
        ) INTO currency_list;
    
    -- RAISE NOTICE 'Currency list: %', currency_list;
    
    -- Loop through each currency and build summary JSON with conditional logic for each section
    FOREACH currency_data IN ARRAY currency_list LOOP
        WITH FilteredTotalData AS (
            SELECT 
                partyid,
                clientid,
                recordtype,
                sectionheader,
                currency AS transactionamountcurrency,
                COALESCE((data->>'TotalCredit')::numeric, 0) AS TotalCredit,
                COALESCE((data->>'TotalDebit')::numeric, 0) AS TotalDebit,
                COALESCE((data->>'AssetClassTotalValue')::numeric, 0) AS AssetClassTotalValue,
                -- COALESCE((data->>'IncomeSummaryInterest')::numeric, 0) AS IncomeSummaryInterest,
                COALESCE((data->>'IncomeSummaryStockLoanRebate')::numeric, 0) AS IncomeSummaryStockLoanRebate,
                COALESCE((data->>'IncomeSummaryMarginInterest')::numeric, 0) AS IncomeSummaryMarginInterest,
                COALESCE((data->>'IncomeSummaryDebitInterest')::numeric, 0) AS IncomeSummaryDebitInterest,
                COALESCE((data->>'IncomeSummaryUSTaxWithheldUSD')::numeric, 0) AS IncomeSummaryUSTaxWithheldUSD,
                COALESCE((data->>'IncomeSummaryUSTaxWithheldNonUSD')::numeric, 0) AS IncomeSummaryUSTaxWithheldNonUSD,
                COALESCE((data->>'IncomeSummaryOther')::numeric, 0) AS IncomeSummaryOther,
                COALESCE((data->>'EndingBalanceInterest')::numeric, 0) AS IncomeSummaryDeposits,
                COALESCE((data->>'IncomeSummaryMoneyMarketMutualFundDividends')::numeric, 0) AS IncomeSummaryMoneyMarketMutualFundDividends,
                COALESCE((data->>'NetCashBalance')::numeric, 0) AS NetCashBalance,
                COALESCE((data->>'AssetClassTotalBeginningValue')::numeric, 0) AS AssetClassTotalBeginningValue,
                COALESCE((data->>'MMMFSummaryDepandAdditions')::numeric, 0) AS MMMFSummaryDepandAdditions,
                COALESCE((data->>'MMMFSummaryDistandSubtractions')::numeric, 0) AS MMMFSummaryDistandSubtractions,
                COALESCE((data->>'MMMFSummaryDivReinvested')::numeric, 0) AS MMMFSummaryDivReinvested

            FROM temp_extraction_results
            WHERE recordtype = 'Total' AND NOT (sectionheader IN ('Money Market Mutual Fund Activity', 'Deposit Activities') AND data->>'recordtype' IN ('Head', 'Begin', 'End')) AND clientid = clientidentifier AND currency = currency_data
        ),

        CashActivitySummary AS (
            SELECT summary_object FROM (
				SELECT summary_object FROM (
					SELECT (
						build_summary_rows_from_historical(
							combined.partyid,
							combined.clientid,
							combined.BeginningDate,
							'Securities Purchased',
							'Detail',
							'Cash Activity Summary',
							'Purchase and Sale - Repurchase Agreement Activity',
							combined.transactionamountcurrency,
							combined.TotalDebit,
                            'Securities Purchased',
                            historicaltablename
						)
					).summary_object AS summary_object
					FROM (
						SELECT 
							td.partyid,
							MAX(td.clientid) AS clientid,
							MAX(tsd.BeginningDate) AS BeginningDate,
							td.transactionamountcurrency,
							SUM(td.TotalDebit) AS TotalDebit
						FROM FilteredTotalData td
						JOIN temp_statement_dates tsd ON td.partyid = tsd.partyid
						WHERE td.sectionheader IN ('Purchase and Sale', 'Repurchase Agreement Activity')
						GROUP BY td.partyid, td.transactionamountcurrency
					) AS combined
				) AS sub
				WHERE (summary_object ->> 'thisperiod')::float != 0
					OR (summary_object ->> 'previousperiod')::float != 0
					OR (summary_object ->> 'ytdperiod')::float != 0

			    UNION ALL

				SELECT summary_object FROM (
					SELECT (
						build_summary_rows_from_historical(
							combined.partyid,
							combined.clientid,
							combined.BeginningDate,
							'Securities Sold',
							'Detail',
							'Cash Activity Summary',
							'Purchase and Sale - Repurchase Agreement Activity',
							combined.transactionamountcurrency,
							combined.TotalCredit,
                            'Securities Sold',
                            historicaltablename
						)
					).summary_object AS summary_object
					FROM (
						SELECT 
							td.partyid,
							MAX(td.clientid) AS clientid,
							MAX(tsd.BeginningDate) AS BeginningDate,
							td.transactionamountcurrency,
							SUM(td.TotalCredit) AS TotalCredit
						FROM FilteredTotalData td
						JOIN temp_statement_dates tsd ON td.partyid = tsd.partyid
						WHERE td.sectionheader IN ('Purchase and Sale', 'Repurchase Agreement Activity')
						GROUP BY td.partyid, td.transactionamountcurrency
					) AS combined
				) AS sub
				WHERE (summary_object ->> 'thisperiod')::float != 0
					OR (summary_object ->> 'previousperiod')::float != 0
					OR (summary_object ->> 'ytdperiod')::float != 0

			    UNION ALL

				SELECT summary_object FROM (
					SELECT (
						build_summary_rows_from_historical(
							td.partyid, 
							td.clientid,
							tsd.BeginningDate, 
							'Funds Received', 
							'Detail', 
							'Cash Activity Summary', 
							td.sectionheader, 
							currency_data, 
							td.TotalCredit,
                            'Funds Received', 
                            historicaltablename
						)
					).summary_object AS summary_object
					FROM FilteredTotalData td
					JOIN temp_statement_dates tsd ON td.partyid = tsd.partyid
					WHERE td.sectionheader = 'Funds Paid and Received'
				) AS sub
				WHERE (summary_object ->> 'thisperiod')::float != 0
					OR (summary_object ->> 'previousperiod')::float != 0
					OR (summary_object ->> 'ytdperiod')::float != 0

			    UNION ALL

				SELECT summary_object FROM (
					SELECT (
						build_summary_rows_from_historical(
							td.partyid, 
							td.clientid,
							tsd.BeginningDate, 
							'Funds Issued', 
							'Detail', 
							'Cash Activity Summary', 
							td.sectionheader, 
							currency_data, 
							td.TotalDebit,
                            'Funds Issued', 
                            historicaltablename
						)
					).summary_object AS summary_object
					FROM FilteredTotalData td
					JOIN temp_statement_dates tsd ON td.partyid = tsd.partyid
				    WHERE td.sectionheader = 'Funds Paid and Received'
				) AS sub
				WHERE (summary_object ->> 'thisperiod')::float != 0
					OR (summary_object ->> 'previousperiod')::float != 0
					OR (summary_object ->> 'ytdperiod')::float != 0

			    UNION ALL

				SELECT summary_object FROM (
					SELECT (
						build_summary_rows_from_historical(
							td.partyid, 
							td.clientid,
							tsd.BeginningDate, 
							'Other Credit', 
							'Detail', 
							'Cash Activity Summary', 
							td.sectionheader, 
							currency_data, 
							td.TotalCredit,
                            'Other Credit', 
                            historicaltablename
						)
					).summary_object AS summary_object
					FROM FilteredTotalData td
					JOIN temp_statement_dates tsd ON td.partyid = tsd.partyid
					WHERE td.sectionheader = 'Other Activity'
				) AS sub
				WHERE (summary_object ->> 'thisperiod')::float != 0
					OR (summary_object ->> 'previousperiod')::float != 0
					OR (summary_object ->> 'ytdperiod')::float != 0

			    UNION ALL

				SELECT summary_object FROM (
					SELECT (
						build_summary_rows_from_historical(
							td.partyid, 
							td.clientid,
							tsd.BeginningDate, 
							'Other Debit', 
							'Detail', 
							'Cash Activity Summary', 
							td.sectionheader, 
							currency_data, 
							td.TotalDebit,
                            'Other Debit', 
                            historicaltablename
						)
					).summary_object AS summary_object
					FROM FilteredTotalData td
					JOIN temp_statement_dates tsd ON td.partyid = tsd.partyid
					WHERE td.sectionheader = 'Other Activity'
				) AS sub
				WHERE (summary_object ->> 'thisperiod')::float != 0
					OR (summary_object ->> 'previousperiod')::float != 0
					OR (summary_object ->> 'ytdperiod')::float != 0
            ) AS result
        ),
        
        PortfolioValueSummary AS (
            SELECT summary_object FROM (
                -- Portfolio Value - Net Cash Balance
				SELECT summary_object FROM (
					SELECT (
						build_summary_rows_from_historical(
							td.partyid, 
							td.clientid,
							tsd.BeginningDate, 
							'Net Cash Balance',
							'Detail', 
							'Portfolio Value', 
							td.sectionheader, 
							currency_data, 
							td.NetCashBalance,
                            'Net Cash Balance',
                            historicaltablename
						)
					).summary_object AS summary_object
					FROM FilteredTotalData td
					JOIN temp_statement_dates tsd ON td.partyid = tsd.partyid
					WHERE td.sectionheader = 'Multi-currency Account Net Equity'
				) AS sub
				WHERE (summary_object ->> 'thisperiod')::float != 0
					OR (summary_object ->> 'previousperiod')::float != 0
					OR (summary_object ->> 'ytdperiod')::float != 0

                UNION ALL

                -- Portfolio Value - Equities Long
				SELECT summary_object FROM (
					SELECT (
						build_summary_rows_from_historical(
							td.partyid, 
							td.clientid,
							tsd.BeginningDate, 
							CASE 
                                WHEN td.sectionheader = 'Equities - Long Positions' THEN 'Equities Long'
                                WHEN td.sectionheader = 'Equities - Securities Loan' THEN 'Equities Securities Loan'
                                ELSE td.sectionheader
                            END,
							'Detail', 
							'Portfolio Value', 
							td.sectionheader, 
							currency_data, 
							td.AssetClassTotalValue,
                            CASE 
                                WHEN td.sectionheader = 'Equities - Long Positions' THEN 'Equities Long'
                                WHEN td.sectionheader = 'Equities - Securities Loan' THEN 'Equities Securities Loan'
                                ELSE td.sectionheader
                            END,
                            historicaltablename
						)
					).summary_object AS summary_object
					FROM FilteredTotalData td
					JOIN temp_statement_dates tsd ON td.partyid = tsd.partyid
					WHERE td.sectionheader IN ('Equities - Long Positions', 'Equities - Securities Loan')
				) AS sub
				WHERE (summary_object ->> 'thisperiod')::float != 0
					OR (summary_object ->> 'previousperiod')::float != 0
					OR (summary_object ->> 'ytdperiod')::float != 0

				
                UNION ALL

				-- Portfolio Value - Equities Short
				SELECT summary_object FROM (
					SELECT (
						build_summary_rows_from_historical(
							td.partyid, 
							td.clientid,
							tsd.BeginningDate, 
							CASE 
                                WHEN td.sectionheader = 'Equities - Short Positions' THEN 'Equities Short' 
                                WHEN td.sectionheader = 'Equities - Securities Borrow' THEN 'Equities Securities Borrow' 
                                ELSE td.sectionheader 
                            END,
							'Detail', 
							'Portfolio Value', 
							td.sectionheader, 
							currency_data, 
							td.AssetClassTotalValue,
                            CASE 
                                WHEN td.sectionheader = 'Equities - Short Positions' THEN 'Equities Short' 
                                WHEN td.sectionheader = 'Equities - Securities Borrow' THEN 'Equities Securities Borrow' 
                                ELSE td.sectionheader 
                            END,
                            historicaltablename
						)
					).summary_object AS summary_object
					FROM FilteredTotalData td
				JOIN temp_statement_dates tsd ON td.partyid = tsd.partyid
				WHERE td.sectionheader IN ('Equities - Short Positions', 'Equities - Securities Borrow')
				) AS sub
				WHERE (summary_object ->> 'thisperiod')::float != 0
					OR (summary_object ->> 'previousperiod')::float != 0
					OR (summary_object ->> 'ytdperiod')::float != 0

				UNION ALL
				
                -- Portfolio Value - Fixed Income Long
				SELECT summary_object FROM (
					SELECT (
						build_summary_rows_from_historical(
							td.partyid, 
							td.clientid,
							tsd.BeginningDate, 
							CASE 
                                WHEN td.sectionheader = 'Fixed Income - Long Positions' THEN 'Fixed Income Long' 
                                WHEN td.sectionheader = 'Fixed Income - Securities Loan' THEN 'Fixed Income Securities Loan' 
                                ELSE td.sectionheader 
                            END,
							'Detail', 
							'Portfolio Value', 
							td.sectionheader, 
							currency_data, 
							td.AssetClassTotalValue,
                            CASE 
                                WHEN td.sectionheader = 'Fixed Income - Long Positions' THEN 'Fixed Income Long' 
                                WHEN td.sectionheader = 'Fixed Income - Securities Loan' THEN 'Fixed Income Securities Loan' 
                                ELSE td.sectionheader 
                            END,
                            historicaltablename
						)
					).summary_object AS summary_object
					FROM FilteredTotalData td
					JOIN temp_statement_dates tsd ON td.partyid = tsd.partyid
					WHERE td.sectionheader IN ('Fixed Income - Long Positions', 'Fixed Income - Securities Loan')
				) AS sub
				WHERE (summary_object ->> 'thisperiod')::float != 0
					OR (summary_object ->> 'previousperiod')::float != 0
					OR (summary_object ->> 'ytdperiod')::float != 0

				UNION ALL

				-- Portfolio Value - Fixed Income Short
				SELECT summary_object FROM (
					SELECT (
						build_summary_rows_from_historical(
							td.partyid, 
							td.clientid,
							tsd.BeginningDate, 
							CASE 
                                WHEN td.sectionheader = 'Fixed Income - Short Positions' THEN 'Fixed Income Short' 
                                WHEN td.sectionheader = 'Fixed Income - Securities Borrow' THEN 'Fixed Income Securities Borrow' 
                                ELSE td.sectionheader 
                            END,
							'Detail', 
							'Portfolio Value', 
							td.sectionheader, 
							currency_data, 
							td.AssetClassTotalValue,
                            CASE 
                                WHEN td.sectionheader = 'Fixed Income - Short Positions' THEN 'Fixed Income Short' 
                                WHEN td.sectionheader = 'Fixed Income - Securities Borrow' THEN 'Fixed Income Securities Borrow' 
                                ELSE td.sectionheader 
                            END,
                            historicaltablename
						)
					).summary_object AS summary_object
					FROM FilteredTotalData td
					JOIN temp_statement_dates tsd ON td.partyid = tsd.partyid
					WHERE td.sectionheader IN ('Fixed Income - Short Positions', 'Fixed Income - Securities Borrow')
				) AS sub
				WHERE (summary_object ->> 'thisperiod')::float != 0
					OR (summary_object ->> 'previousperiod')::float != 0
					OR (summary_object ->> 'ytdperiod')::float != 0

				UNION ALL

				-- Portfolio Value - Options Long
				SELECT summary_object FROM (
					SELECT (
						build_summary_rows_from_historical(
							td.partyid, 
							td.clientid,
							tsd.BeginningDate, 
							CASE 
                                WHEN td.sectionheader = 'Options - Long Positions' THEN 'Options Long' 
                                WHEN td.sectionheader = 'Options - Securities Loan' THEN 'Options Securities Loan' 
                                ELSE td.sectionheader 
                            END,
							'Detail', 
							'Portfolio Value', 
							td.sectionheader, 
							currency_data, 
							td.AssetClassTotalValue,
                            CASE 
                                WHEN td.sectionheader = 'Options - Long Positions' THEN 'Options Long' 
                                WHEN td.sectionheader = 'Options - Securities Loan' THEN 'Options Securities Loan' 
                                ELSE td.sectionheader 
                            END,
                            historicaltablename
						)
					).summary_object AS summary_object
					FROM FilteredTotalData td
					JOIN temp_statement_dates tsd ON td.partyid = tsd.partyid
					WHERE td.sectionheader IN ('Options - Long Positions', 'Options - Securities Loan')
				) AS sub
				WHERE (summary_object ->> 'thisperiod')::float != 0
					OR (summary_object ->> 'previousperiod')::float != 0
					OR (summary_object ->> 'ytdperiod')::float != 0

				UNION ALL

				-- Portfolio Value - Options Short
				SELECT summary_object FROM (
					SELECT (
						build_summary_rows_from_historical(
							td.partyid, 
							td.clientid,
							tsd.BeginningDate, 
							CASE 
                                WHEN td.sectionheader = 'Options - Short Positions' THEN 'Options Short' 
                                WHEN td.sectionheader = 'Options - Securities Borrow' THEN 'Options Securities Borrow' 
                                ELSE td.sectionheader 
                            END,
							'Detail', 
							'Portfolio Value', 
							td.sectionheader, 
							currency_data, 
							td.AssetClassTotalValue,
                            CASE 
                                WHEN td.sectionheader = 'Options - Short Positions' THEN 'Options Short' 
                                WHEN td.sectionheader = 'Options - Securities Borrow' THEN 'Options Securities Borrow' 
                                ELSE td.sectionheader 
                            END,
                            historicaltablename
						)
					).summary_object AS summary_object
					FROM FilteredTotalData td
					JOIN temp_statement_dates tsd ON td.partyid = tsd.partyid
					WHERE td.sectionheader IN ('Options - Short Positions', 'Options - Securities Borrow')
				) AS sub
				WHERE (summary_object ->> 'thisperiod')::float != 0
				  OR (summary_object ->> 'previousperiod')::float != 0
				  OR (summary_object ->> 'ytdperiod')::float != 0

				UNION ALL

				-- Portfolio Value - Repurchase Agreements
				SELECT summary_object FROM (
					SELECT (
						build_summary_rows_from_historical(
							td.partyid, 
							td.clientid,
							tsd.BeginningDate, 
							'Repurchase Agreements',
							'Detail', 
							'Portfolio Value', 
							td.sectionheader, 
							currency_data, 
							td.AssetClassTotalValue,
                            'Repurchase Agreements',
                            historicaltablename
						)
					).summary_object AS summary_object
					FROM FilteredTotalData td
					JOIN temp_statement_dates tsd ON td.partyid = tsd.partyid
					WHERE td.sectionheader = 'Repurchase Agreements'
				) AS sub
				WHERE (summary_object ->> 'thisperiod')::float != 0
				  OR (summary_object ->> 'previousperiod')::float != 0
				  OR (summary_object ->> 'ytdperiod')::float != 0

				UNION ALL

				-- Portfolio Value - Money Market Mutual Funds
				SELECT summary_object FROM (
					SELECT (
						build_summary_rows_from_historical(
							td.partyid, 
							td.clientid,
							tsd.BeginningDate, 
							'Money Market Mutual Funds',
							'Detail', 
							'Portfolio Value', 
							td.sectionheader, 
							currency_data, 
							td.AssetClassTotalValue,
                            'Money Market Mutual Funds',
                            historicaltablename
						)
					).summary_object AS summary_object
					FROM FilteredTotalData td
					JOIN temp_statement_dates tsd ON td.partyid = tsd.partyid
					WHERE td.sectionheader = 'Money Market Mutual Funds'
				) AS sub
				WHERE (summary_object ->> 'thisperiod')::float != 0
				  OR (summary_object ->> 'previousperiod')::float != 0
				  OR (summary_object ->> 'ytdperiod')::float != 0

				UNION ALL

				-- Portfolio Value - Deposits
				SELECT summary_object FROM (
					SELECT (
						build_summary_rows_from_historical(
							td.partyid, 
							td.clientid,
							tsd.BeginningDate, 
							'Deposits',
							'Detail', 
							'Portfolio Value', 
							td.sectionheader, 
							currency_data, 
							td.AssetClassTotalValue,
                            'Deposits',
                            historicaltablename
						)
					).summary_object AS summary_object
					FROM FilteredTotalData td
					JOIN temp_statement_dates tsd ON td.partyid = tsd.partyid
					WHERE td.sectionheader = 'Deposits'
				) AS sub
				WHERE (summary_object ->> 'thisperiod')::float != 0
				  OR (summary_object ->> 'previousperiod')::float != 0
				  OR (summary_object ->> 'ytdperiod')::float != 0

				UNION ALL

				-- Portfolio Value - Other Long
				SELECT summary_object FROM (
					SELECT (
						build_summary_rows_from_historical(
							td.partyid, 
							td.clientid,
							tsd.BeginningDate, 
							CASE 
                                WHEN td.sectionheader = 'Other - Long Positions' THEN 'Other Long' 
                                WHEN td.sectionheader = 'Other - Securities Loan' THEN 'Other Securities Loan' 
                                ELSE td.sectionheader 
                            END,
							'Detail', 
							'Portfolio Value', 
							td.sectionheader, 
							currency_data, 
							td.AssetClassTotalValue,
                            CASE 
                                WHEN td.sectionheader = 'Other - Long Positions' THEN 'Other Long' 
                                WHEN td.sectionheader = 'Other - Securities Loan' THEN 'Other Securities Loan' 
                                ELSE td.sectionheader 
                            END,
                            historicaltablename
						)
					).summary_object AS summary_object
					FROM FilteredTotalData td
					JOIN temp_statement_dates tsd ON td.partyid = tsd.partyid
					WHERE td.sectionheader IN ('Other - Long Positions', 'Other - Securities Loan')
				) AS sub
				WHERE (summary_object ->> 'thisperiod')::float != 0
				  OR (summary_object ->> 'previousperiod')::float != 0
				  OR (summary_object ->> 'ytdperiod')::float != 0

				UNION ALL

				-- Portfolio Value - Other Short
				SELECT summary_object FROM (
					SELECT (
						build_summary_rows_from_historical(
							td.partyid, 
							td.clientid,
							tsd.BeginningDate, 
							CASE 
                                WHEN td.sectionheader = 'Other - Short Positions' THEN 'Other Short' 
                                WHEN td.sectionheader = 'Other - Securities Borrow' THEN 'Other Securities Borrow' 
                                ELSE td.sectionheader 
                            END,
							'Detail', 
							'Portfolio Value', 
							td.sectionheader, 
							currency_data, 
							td.AssetClassTotalValue,
                            CASE 
                                WHEN td.sectionheader = 'Other - Short Positions' THEN 'Other Short' 
                                WHEN td.sectionheader = 'Other - Securities Borrow' THEN 'Other Securities Borrow' 
                                ELSE td.sectionheader 
                            END,
                            historicaltablename
						)
					).summary_object AS summary_object
					FROM FilteredTotalData td
					JOIN temp_statement_dates tsd ON td.partyid = tsd.partyid
					WHERE td.sectionheader IN ('Other - Short Positions', 'Other - Securities Borrow')
				) AS sub
				WHERE (summary_object ->> 'thisperiod')::float != 0
				  OR (summary_object ->> 'previousperiod')::float != 0
				  OR (summary_object ->> 'ytdperiod')::float != 0

            ) AS result
        ),

        IncomeSummary AS (
            SELECT summary_object FROM (

                SELECT summary_object FROM (
                    SELECT (
                        build_summary_rows_from_historical(
                            td.partyid, 
                            td.clientid,
                            tsd.BeginningDate, 
                            'Dividend/Capital Gain Income Paid', 
                            'Detail', 
                            'Income Summary', 
                            td.sectionheader, 
                            currency_data, 
                            td.TotalCredit,
                            'Dividend/Capital Gain Income Paid', 
                            historicaltablename
                        )
                    ).summary_object AS summary_object
                    FROM FilteredTotalData td
                    JOIN temp_statement_dates tsd ON td.partyid = tsd.partyid
                    WHERE td.sectionheader = 'Dividends'
                ) AS sub
                WHERE ((summary_object ->> 'thisperiod')::float != 0
                    OR (summary_object ->> 'previousperiod')::float != 0
                    OR (summary_object ->> 'ytdperiod')::float != 0)

                UNION ALL

                SELECT summary_object FROM (
                    SELECT (
                        build_summary_rows_from_historical(
                            td.partyid, 
                            td.clientid,
                            tsd.BeginningDate, 
                            'Dividend/Capital Gain Expense Paid', 
                            'Detail', 
                            'Income Summary', 
                            td.sectionheader, 
                            currency_data, 
                            td.TotalDebit,
                            'Dividend/Capital Gain Expense Paid', 
                            historicaltablename
                        )
                    ).summary_object AS summary_object
                    FROM FilteredTotalData td
                    JOIN temp_statement_dates tsd ON td.partyid = tsd.partyid
                    WHERE td.sectionheader = 'Dividends'
                ) AS sub
                WHERE ((summary_object ->> 'thisperiod')::float != 0
                    OR (summary_object ->> 'previousperiod')::float != 0
                    OR (summary_object ->> 'ytdperiod')::float != 0)

                UNION ALL
    
                SELECT summary_object FROM (
                    SELECT (
                        build_summary_rows_from_historical(
                            td.partyid, 
                            td.clientid,
                            tsd.BeginningDate,
                            'Interest', 
                            'Detail', 
                            'Income Summary', 
                            td.sectionheader, 
                            currency_data, 
                            -- td.IncomeSummaryInterest
                            td.AssetClassTotalValue - (td.IncomeSummaryStockLoanRebate + td.IncomeSummaryMarginInterest + td.IncomeSummaryDebitInterest),
                            'Interest', 
                            historicaltablename
                        )
                    ).summary_object AS summary_object
                    FROM FilteredTotalData td
                    JOIN temp_statement_dates tsd ON td.partyid = tsd.partyid
                    WHERE td.sectionheader = 'Interest'
                ) AS sub
                WHERE ((summary_object ->> 'thisperiod')::float != 0
                    OR (summary_object ->> 'previousperiod')::float != 0
                    OR (summary_object ->> 'ytdperiod')::float != 0)
              
                UNION ALL

                SELECT summary_object FROM (
                    SELECT (
                        build_summary_rows_from_historical(
                            combined.partyid,
                            combined.clientid,
                            combined.BeginningDate,
                            'Money Market Mutual Fund Dividends',
                            'Detail',
                            'Income Summary',
                            combined.sectionheader,
                            combined.transactionamountcurrency,
                            combined.IncomeSummaryMoneyMarketMutualFundDividends,
                            'Money Market Mutual Fund Dividends',
                            historicaltablename
                        )
                    ).summary_object AS summary_object
                    FROM (
                        SELECT 
                            td.partyid,
                            MAX(td.clientid) AS clientid,
                            MAX(td.sectionheader) AS sectionheader,
                            MAX(tsd.BeginningDate) AS BeginningDate,
                            td.transactionamountcurrency,
                            SUM(td.IncomeSummaryMoneyMarketMutualFundDividends) AS IncomeSummaryMoneyMarketMutualFundDividends
                        FROM FilteredTotalData td
                        JOIN temp_statement_dates tsd ON td.partyid = tsd.partyid
                        WHERE td.sectionheader IN ('Money Market Mutual Fund Activity')
                        GROUP BY td.partyid, td.transactionamountcurrency
                    ) AS combined
                ) AS sub
                WHERE ((summary_object ->> 'thisperiod')::float != 0
                    OR (summary_object ->> 'previousperiod')::float != 0
                    OR (summary_object ->> 'ytdperiod')::float != 0)

                UNION ALL

                SELECT summary_object FROM (
                    SELECT (
                        build_summary_rows_from_historical(
                            combined.partyid,
                            combined.clientid,
                            combined.BeginningDate,
                            'Deposits',
                            'Detail',
                            'Income Summary',
                            combined.sectionheader,
                            combined.transactionamountcurrency,
                            combined.IncomeSummaryDeposits,
                            'Deposits',
                            historicaltablename
                        )
                    ).summary_object AS summary_object
                    FROM (
                        SELECT 
                            td.partyid,
                            MAX(td.clientid) AS clientid,
                            MAX(td.sectionheader) AS sectionheader,
                            MAX(tsd.BeginningDate) AS BeginningDate,
                            td.transactionamountcurrency,
                            SUM(td.IncomeSummaryDeposits) AS IncomeSummaryDeposits
                        FROM FilteredTotalData td
                        JOIN temp_statement_dates tsd ON td.partyid = tsd.partyid
                        WHERE td.sectionheader IN ('Deposit Activities')
                        GROUP BY td.partyid, td.transactionamountcurrency
                    ) AS combined
                ) AS sub
                WHERE ((summary_object ->> 'thisperiod')::float != 0
                    OR (summary_object ->> 'previousperiod')::float != 0
                    OR (summary_object ->> 'ytdperiod')::float != 0)

                UNION ALL

                SELECT summary_object FROM (
                    SELECT (
                        build_summary_rows_from_historical(
                            td.partyid, 
                            td.clientid,
                            tsd.BeginningDate, 
                            'Other', 
                            'Detail', 
                            'Income Summary', 
                            td.sectionheader, 
                            currency_data, 
                            td.IncomeSummaryOther,
                            'Other', 
                            historicaltablename
                        )
                    ).summary_object AS summary_object
                    FROM FilteredTotalData td
                    JOIN temp_statement_dates tsd ON td.partyid = tsd.partyid
                    WHERE td.sectionheader = 'Other Income'
                ) AS sub
                WHERE ((summary_object ->> 'thisperiod')::float != 0
                    OR (summary_object ->> 'previousperiod')::float != 0
                    OR (summary_object ->> 'ytdperiod')::float != 0)

                UNION ALL

                SELECT summary_object FROM (
                    SELECT (
                        build_summary_rows_from_historical(
                            td.partyid, 
                            td.clientid,
                            tsd.BeginningDate, 
                            'Stock Loan Rebate', 
                            'Detail', 
                            'Income Summary', 
                            td.sectionheader, 
                            currency_data, 
                            td.IncomeSummaryStockLoanRebate,
                            'Stock Loan Rebate',
                            historicaltablename
                        )
                    ).summary_object AS summary_object
                    FROM FilteredTotalData td
                    JOIN temp_statement_dates tsd ON td.partyid = tsd.partyid
                    WHERE td.sectionheader = 'Interest'
                ) AS sub
                WHERE ((summary_object ->> 'thisperiod')::float != 0
                    OR (summary_object ->> 'previousperiod')::float != 0
                    OR (summary_object ->> 'ytdperiod')::float != 0)

                UNION ALL

                SELECT summary_object FROM (
                    SELECT (
                        build_summary_rows_from_historical(
                            td.partyid, 
                            td.clientid,
                            tsd.BeginningDate, 
                            'US Tax Withheld', 
                            'Detail', 
                            'Income Summary', 
                            td.sectionheader, 
                            currency_data, 
                            td.IncomeSummaryUSTaxWithheldUSD,
                            'US Tax Withheld', 
                            historicaltablename
                        )
                    ).summary_object AS summary_object
                    FROM FilteredTotalData td
                    JOIN temp_statement_dates tsd ON td.partyid = tsd.partyid
                    WHERE td.sectionheader = 'Withholdings'
                ) AS sub
                WHERE ((summary_object ->> 'thisperiod')::float != 0
                    OR (summary_object ->> 'previousperiod')::float != 0
                    OR (summary_object ->> 'ytdperiod')::float != 0)

                UNION ALL

                SELECT summary_object FROM (
                    SELECT (
                        build_summary_rows_from_historical(
                            td.partyid, 
                            td.clientid,
                            tsd.BeginningDate, 
                            'Non-US Tax Withheld', 
                            'Detail', 
                            'Income Summary', 
                            td.sectionheader, 
                            currency_data, 
                            td.IncomeSummaryUSTaxWithheldNonUSD,
                            'Non-US Tax Withheld', 
                            historicaltablename
                        )
                    ).summary_object AS summary_object
                    FROM FilteredTotalData td
                    JOIN temp_statement_dates tsd ON td.partyid = tsd.partyid
                    WHERE td.sectionheader = 'Withholdings'
                ) AS sub
                WHERE ((summary_object ->> 'thisperiod')::float != 0
                    OR (summary_object ->> 'previousperiod')::float != 0
                    OR (summary_object ->> 'ytdperiod')::float != 0)

                UNION ALL

                SELECT summary_object FROM (
                    SELECT (
                        build_summary_rows_from_historical(
                            td.partyid, 
                            td.clientid,
                            tsd.BeginningDate, 
                            'Margin Interest', 
                            'Detail', 
                            'Income Summary', 
                            td.sectionheader, 
                            currency_data, 
                            td.IncomeSummaryMarginInterest,
                            'Margin Interest', 
                            historicaltablename
                        )
                    ).summary_object AS summary_object
                    FROM FilteredTotalData td
                    JOIN temp_statement_dates tsd ON td.partyid = tsd.partyid
                    WHERE td.sectionheader = 'Interest'
                ) AS sub
                WHERE ((summary_object ->> 'thisperiod')::float != 0
                    OR (summary_object ->> 'previousperiod')::float != 0
                    OR (summary_object ->> 'ytdperiod')::float != 0)

                UNION ALL

                SELECT summary_object FROM (
                    SELECT (
                        build_summary_rows_from_historical(
                            td.partyid, 
                            td.clientid,
                            tsd.BeginningDate, 
                            'Debit Interest',
                            'Detail', 
                            'Income Summary', 
                            td.sectionheader, 
                            currency_data, 
                            td.IncomeSummaryDebitInterest,
                            'Debit Interest For ' || TRIM(TO_CHAR(tsd.BeginningDate::timestamp, 'FMMonth YYYY')), 
                            historicaltablename
                        )
                    ).summary_object AS summary_object
                    FROM FilteredTotalData td
                    JOIN temp_statement_dates tsd ON td.partyid = tsd.partyid
                    WHERE td.sectionheader = 'Interest'
                ) AS sub
                WHERE ((summary_object ->> 'thisperiod')::float != 0
                    OR (summary_object ->> 'previousperiod')::float != 0
                    OR (summary_object ->> 'ytdperiod')::float != 0)

            ) AS result
        ),

        MoneyMarketMutualFundsSummary AS (
            WITH agg AS (
                SELECT
                    clientid,
                    partyid,
                    currency_data AS currencydata,

                    COALESCE(MAX(CASE WHEN sectionheader = 'Money Market Mutual Funds' 
                        THEN AssetClassTotalValue END), 0) AS closing_balance,

                    COALESCE(MAX(CASE WHEN sectionheader = 'Money Market Mutual Funds' 
                        THEN AssetClassTotalBeginningValue END), 0) AS opening_balance,

                    COALESCE(SUM(CASE WHEN sectionheader = 'Money Market Mutual Fund Activity' 
                        THEN MMMFSummaryDepandAdditions END), 0) AS deposits_and_additions,

                    COALESCE(SUM(CASE WHEN sectionheader = 'Money Market Mutual Fund Activity' 
                        THEN MMMFSummaryDistandSubtractions END), 0) AS distributions_and_subtractions,

                    COALESCE(SUM(CASE WHEN sectionheader = 'Money Market Mutual Fund Activity' 
                        THEN MMMFSummaryDivReinvested END), 0) AS dividends_reinvested,

                    BOOL_OR(sectionheader = 'Money Market Mutual Funds') AS has_mmmf_holdings,
                    BOOL_OR(sectionheader = 'Money Market Mutual Fund Activity') AS has_mmmf_activity
                FROM FilteredTotalData
                GROUP BY partyid, clientid, currency_data
            )

            -- OPENING BALANCE
            SELECT jsonb_build_object(
                'partyid', partyid,
                'clientid', clientid,
                'recordtype', 'Detail',
                'sectiontype', 'Summary',
                'sectionheader', 'Money Market Mutual Funds Summary',
                'classification', 'Opening Balance',
                'thisperiod', opening_balance,
                'transactionamountcurrency', currencydata
            ) AS summary_object
            FROM agg
            WHERE has_mmmf_holdings OR has_mmmf_activity

            UNION ALL

            -- DEPOSITS & ADDITIONS
            SELECT jsonb_build_object(
                'partyid', partyid,
                'clientid', clientid,
                'recordtype', 'Detail',
                'sectiontype', 'Summary',
                'sectionheader', 'Money Market Mutual Funds Summary',
                'classification', 'Deposits and Other Additions',
                'thisperiod', deposits_and_additions,
                'transactionamountcurrency', currencydata
            ) AS summary_object
            FROM agg
            WHERE has_mmmf_activity

            UNION ALL

            -- DISTRIBUTIONS
            SELECT jsonb_build_object(
                'partyid', partyid,
                'clientid', clientid,
                'recordtype', 'Detail',
                'sectiontype', 'Summary',
                'sectionheader', 'Money Market Mutual Funds Summary',
                'classification', 'Distributions and Other Subtractions',
                'thisperiod', distributions_and_subtractions,
                'transactionamountcurrency', currencydata
            ) AS summary_object
            FROM agg
            WHERE has_mmmf_activity

            UNION ALL

            -- DIVIDENDS REINVESTED
            SELECT jsonb_build_object(
                'partyid', partyid,
                'clientid', clientid,
                'recordtype', 'Detail',
                'sectiontype', 'Summary',
                'sectionheader', 'Money Market Mutual Funds Summary',
                'classification', 'Dividends Reinvested',
                'thisperiod', dividends_reinvested,
                'transactionamountcurrency', currencydata
            ) AS summary_object
            FROM agg
            WHERE has_mmmf_activity

            UNION ALL

            -- CHANGE IN VALUE
            SELECT jsonb_build_object(
                'partyid', partyid,
                'clientid', clientid,
                'recordtype', 'Detail',
                'sectiontype', 'Summary',
                'sectionheader', 'Money Market Mutual Funds Summary',
                'classification', 'Change in Value',
                'thisperiod', closing_balance - (opening_balance + deposits_and_additions + distributions_and_subtractions + dividends_reinvested),
                'transactionamountcurrency', currencydata
            ) AS summary_object
            FROM agg
            WHERE has_mmmf_holdings OR has_mmmf_activity

            UNION ALL

            -- CLOSING BALANCE
            SELECT jsonb_build_object(
                'partyid', partyid,
                'clientid', clientid,
                'recordtype', 'Total',
                'sectiontype', 'Summary',
                'sectionheader', 'Money Market Mutual Funds Summary',
                'classification', 'Closing Balance',
                'thisperiod', closing_balance,
                'transactionamountcurrency', currencydata
            ) AS summary_object
            FROM agg
            WHERE has_mmmf_holdings OR has_mmmf_activity
        ),
        
        SummaryData AS (
            SELECT summary_object FROM CashActivitySummary
            UNION ALL
            SELECT summary_object FROM PortfolioValueSummary
            UNION ALL
            SELECT summary_object FROM IncomeSummary
            UNION ALL
            SELECT summary_object FROM MoneyMarketMutualFundsSummary
        )

        SELECT jsonb_agg(summary_object) INTO summary_detail_data FROM SummaryData;

        summary_detail := summary_detail || COALESCE(summary_detail_data, '[]'::jsonb);
        -- RAISE NOTICE 'summary_detail: %', summary_detail;

        parts := string_to_array(historical_table_name, '_');
        IF array_length(parts, 1) < 2 THEN
            RAISE EXCEPTION 'Invalid historical_table_name format: %', historical_table_name;
        END IF;

        target_year := left(parts[2], 4);
        target_month := right(parts[2], 2);

        IF target_month <> '01' THEN
            -- RAISE NOTICE 'Target month is not january';
            prev_month := lpad((target_month::int - 1)::text, 2, '0');
            prev_year := target_year;
        ELSE
            prev_month := '12';
            prev_year := target_year::int - 1;  -- Subtract 1 from the current year to get the previous year
        END IF;
    
        prev_month_table := parts[1] || '_' || prev_year || prev_month;

        IF EXISTS (
            SELECT FROM pg_tables
            WHERE schemaname = historical_schema_name AND tablename = prev_month_table
            ) THEN
                 -- Insert missing records from previous month table
                -- Step 2a: Fetch missing records into summary_detail_data
                -- Fetch missing records from previous month table
                EXECUTE format($f$
                    SELECT jsonb_agg(
                    jsonb_set(
                            jsonb_set(
                                prev.summarydata,
                                '{previousperiod}',
                                    to_jsonb((prev.summarydata->>'thisperiod')::numeric)
                                ),
                                '{thisperiod}',
                                    '0'::jsonb
                            )
                        )
                    FROM %I.%I prev
                    WHERE prev.partyid = %L
                    AND prev.currency = %L
                    AND prev.clientid = %L
                    AND prev.recordtype = 'Detail'
                    AND (
                        COALESCE((prev.summarydata->>'thisperiod')::numeric, 0) <> 0
                        OR COALESCE((prev.summarydata->>'previousperiod')::numeric, 0) <> 0
                        OR COALESCE((prev.summarydata->>'ytdperiod')::numeric, 0) <> 0
                    )
                    AND NOT EXISTS (
                        SELECT 1 
                        FROM %I curr
                        WHERE curr.partyid = prev.partyid
                            AND curr.clientid = prev.clientid
                            AND curr.sectionheader = prev.sectionheader
                            AND curr.classification = prev.classification
                            AND curr.currency = prev.currency
                )
            $f$,
            historical_schema_name, prev_month_table, accountpartyid, currency_data, clientidentifier, historicaltablename)
            INTO missing_summary_detail_data;

            -- Step 2b: Insert missing rows into historical table
                EXECUTE format($f$
                    INSERT INTO %I (
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
                        WHERE prev.partyid = %L 
                        AND prev.currency = %L
                        AND prev.clientid = %L
                        AND prev.recordtype = 'Detail'
                        AND (
                            COALESCE((prev.summarydata->>'thisperiod')::numeric, 0) <> 0
                            OR COALESCE((prev.summarydata->>'previousperiod')::numeric, 0) <> 0
                            OR COALESCE((prev.summarydata->>'ytdperiod')::numeric, 0) <> 0
                        )
                        AND NOT EXISTS (
                            SELECT 1 FROM %I curr
                                WHERE curr.partyid = prev.partyid
                                AND curr.clientid = prev.clientid
                                AND curr.sectionheader = prev.sectionheader
                                AND curr.classification = prev.classification
                                    AND curr.currency = prev.currency
                );
                $f$, historicaltablename, historical_schema_name, prev_month_table, accountpartyid, currency_data, clientidentifier, historicaltablename);
        END IF;

        -- Step 3: Add summary_detail_data to summary_detail
        -- RAISE NOTICE 'missing_summary_detail_data: %', missing_summary_detail_data;
        summary_detail := summary_detail || COALESCE(missing_summary_detail_data, '[]'::jsonb);    

    END LOOP;

    WITH EquitySummaryData AS (
        SELECT 
            MAX(partyid) AS partyid,
            MAX(clientid) AS clientid,
            MAX(currency) AS currency_for_ordering,
            MAX(data->>'GeneralNarrative') FILTER (WHERE sectionheader = 'Multi-currency Account Net Equity') AS currency,

            SUM(CASE WHEN sectionheader IN ('Multi-currency Account Net Equity')
                     THEN COALESCE((data->>'NetCashBalance')::numeric, 0)
                     ELSE 0 END) AS netcashbalance,

            SUM(CASE WHEN sectionheader IN ('Multi-currency Account Net Equity')
                     THEN COALESCE((data->>'ExchangeRate')::numeric, 0)
                     ELSE 0 END) AS exchangerate,
            
            SUM(CASE WHEN sectionheader IN ('Fixed Income - Long Positions', 'Equities - Long Positions', 'Options - Long Positions', 'Other - Long Positions', 'Equities - Securities Loan', 'Options - Securities Loan', 'Fixed Income - Securities Loan', 'Other - Securities Loan', 'Money Market Mutual Funds', 'Deposits', 'Repurchase Agreements')
                     THEN COALESCE((data->>'AssetClassTotalValue')::numeric, 0)
                     ELSE 0 END) AS longmarketvalue,

            SUM(CASE WHEN sectionheader IN ('Fixed Income - Short Positions', 'Equities - Short Positions', 'Options - Short Positions', 'Other - Short Positions', 'Equities - Securities Borrow', 'Options - Securities Borrow', 'Fixed Income - Securities Borrow', 'Other - Securities Borrow')
                     THEN COALESCE((data->>'AssetClassTotalValue')::numeric, 0)
                     ELSE 0 END) AS shortmarketvalue
        FROM temp_extraction_results
        WHERE recordtype = 'Total'
          -- AND clientid = ANY(clientid_list)
        GROUP BY currency
    )        
        
    SELECT jsonb_agg(
        jsonb_build_object(
            'partyid', partyid,
            'clientid', clientid,
            'recordtype', 'Detail',
            'sectiontype', 'Summary',
            'sectionheader', 'Multi-currency Account Net Equity',
            'currency', currency,
            'NetCashBalance', netcashbalance,
            'LongMarketValue', longmarketvalue,
            'ShortMarketValue', shortmarketvalue,
            'NetEquityinDenominationCurrency', netcashbalance + longmarketvalue + shortmarketvalue,
            'ExchangeRate', exchangerate,
            'NetEquityInUSD', (netcashbalance + longmarketvalue + shortmarketvalue) * exchangerate,
            'transactionamountcurrency', 'USD'
        )
        ORDER BY
            CASE WHEN currency_for_ordering = 'USD' THEN 0 ELSE 1 END        
        ) INTO multicurrency_summary_detail
        FROM EquitySummaryData esd
        WHERE NOT (
            COALESCE(netcashbalance, 0) = 0 AND
            COALESCE(longmarketvalue, 0) = 0 AND
            COALESCE(shortmarketvalue, 0) = 0 AND
            NOT EXISTS (
                SELECT 1 
                FROM temp_extraction_results tx
                WHERE tx.clientid = esd.clientid AND
                    tx.currency = esd.currency_for_ordering
                    AND tx.sectiontype <> 'Summary'
            )
        );
    
    WITH EquitySummaryTotal AS (
        SELECT 
            data->>'sectionheader' AS sectionheader,
            data->>'partyid' AS partyid,
            data->>'clientid' AS clientid,
            MAX(data->>'transactionamountcurrency') AS transactionamountcurrency,
            SUM((data->>'NetEquityInUSD')::numeric) AS total_usdnetequity
        FROM jsonb_array_elements(multicurrency_summary_detail) AS data
        GROUP BY 
            data->>'partyid', data->>'clientid', data->>'transactionamountcurrency', data->>'sectionheader'
        )
    
    SELECT jsonb_agg(jsonb_build_object(
        'partyid', partyid,
        'clientid', clientid,
        'recordtype', 'Total',
        'sectiontype', 'Summary',
        'sectionheader', 'Multi-currency Account Net Equity',
        'sectionheader', sectionheader,
        'classification', 'Total',
        'transactionamountcurrency', transactionamountcurrency,
        'AssetClassTotalValue', total_usdnetequity
        )
    ) INTO multicurrency_summary_total
    FROM EquitySummaryTotal;
    
    WITH SummaryTotalData AS (
        SELECT 
            data->>'sectionheader' AS sectionheader,
            data->>'partyid' AS partyid,
            data->>'clientid' AS clientid,
            SUM(
                CASE 
                    WHEN data ? 'thisperiod' THEN (data->>'thisperiod')::numeric
                    ELSE 0
                END
            ) AS thisperiodtotal,
            SUM(
                CASE 
                    WHEN data ? 'previousperiod' THEN (data->>'previousperiod')::numeric
                    ELSE 0
                END
            ) AS previousperiodtotal,
            SUM(
                CASE 
                    WHEN data ? 'ytdperiod' THEN (data->>'ytdperiod')::numeric
                    ELSE 0
                END
            ) AS ytdperiodtotal,
            MAX(data->>'transactionamountcurrency') AS transactionamountcurrency
        FROM 
            jsonb_array_elements(summary_detail) AS data
        -- WHERE data->>'sectionheader' IN ('Portfolio Value','Cash Activity Summary')
        WHERE data->>'sectiontype' = 'Summary' AND NOT (data->>'sectionheader' IN ('Money Market Mutual Funds Summary'))
        GROUP BY 
            data->>'partyid', data->>'clientid', data->>'transactionamountcurrency', data->>'sectionheader'
    )
    -- Construct the summary object for each partyid and sectionheader
    SELECT jsonb_agg(
        jsonb_build_object(
            'partyid', td.partyid,
            'clientid', td.clientid,
            'recordtype', 'Total',
            'sectiontype', 'Summary',
            'sectionheader', td.sectionheader,
            'classification', 'Total',
            'thisperiodtotal', td.thisperiodtotal,
            'previousperiodtotal', td.previousperiodtotal,
            'ytdperiodtotal', td.ytdperiodtotal,
            'transactionamountcurrency', td.transactionamountcurrency
        )
    ) INTO summary_total
    FROM SummaryTotalData td
    WHERE td.thisperiodtotal != 0
        OR td.previousperiodtotal != 0
        OR td.ytdperiodtotal != 0;
    
    -- RAISE NOTICE 'summary_total: %', summary_total;

    -- Insert rows into historical table from summary_total JSONB array
    EXECUTE format($sql$
        INSERT INTO %I (
            partyid,
            clientid,
            statementdate,
            recordtype,
            sectionheader,
            summaryforsection,
            classification,
            summarydata,
            currency
        )
        SELECT
            item->>'partyid' AS partyid,
            item->>'clientid' AS clientid,
            %L AS statementdate,
            item->>'recordtype' AS recordtype,
            item->>'sectionheader' AS sectionheader,
            NULL::text AS summaryforsection,
            item->>'classification' AS classification,
            item AS summarydata,
            item->>'transactionamountcurrency' AS currency
        FROM jsonb_array_elements(%L::jsonb) AS arr(item)
        ON CONFLICT (partyid, clientid, sectionheader, classification, currency)
        DO UPDATE SET
            statementdate = EXCLUDED.statementdate,
            recordtype = EXCLUDED.recordtype,
            summaryforsection = EXCLUDED.summaryforsection,
            summarydata = EXCLUDED.summarydata
        $sql$,
        historicaltablename,   -- %I for table name
        curr_statementdate,      -- %L for literal string
        summary_total::text      -- %L for JSONB as text
    );          

    -- Combine transaction data and summary total
    SELECT jsonb_agg(item) INTO combined_results
    FROM (
        --SELECT value AS item FROM jsonb_array_elements(statement_dates) AS value
        --UNION ALL
        --SELECT value AS item FROM jsonb_array_elements(transaction_data) AS value
        --UNION ALL
        SELECT value AS item FROM jsonb_array_elements(summary_detail) AS value
        UNION ALL
        SELECT value AS item FROM jsonb_array_elements(summary_total) AS value
        UNION ALL
        SELECT value AS item FROM jsonb_array_elements(multicurrency_summary_detail) AS value
        UNION ALL
        SELECT value AS item FROM jsonb_array_elements(multicurrency_summary_total) AS value        
    ) merged_data;

    -- Return the final result with both data and summary totals, sorted
    -- Check if combined_results has only one element with "transactioncategory" = 'HEADER DATE'
    -- IF jsonb_array_length(combined_results) = 1 AND 
    --    combined_results->0->>'transactioncategory' = 'HEADER DATE' THEN
    --    RETURN NULL;
    -- END IF;
    
    RETURN combined_results;

END;

$$ LANGUAGE plpgsql;