CREATE OR REPLACE FUNCTION public.create_json_for_summary(p_identifier_value text, clientid text, origin text, balance_non_balance_option jsonb)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
    section RECORD;
    transaction_data JSONB;
    combined_results JSONB;
    summary_total JSONB;
    statement_dates JSONB;
    historical_table_name TEXT;
    prev_historical_table_name TEXT;
    curr_statementdate TEXT;
    curr_beginningDate TEXT;
    prev_statementdate TEXT;
    currency_data TEXT;
    currency_list TEXT[];
    clientid_list TEXT[];
    client_data TEXT;
    summary_data JSONB;
    summary_detail JSONB := '[]'::jsonb;
	balance_non_balance_options JSONB := '{"Income Summary": "BalanceTable",
										"Asset Allocation": "SemiBalanceTable",
										"Margin Summary": "BalanceTable",
										"GainLoss Summary": "NonBalanceTable",
										"Retirement Summary": "BalanceTable",
										"Market Value Over Time": "BalanceTable",
										"Transaction Overview": "NonBalanceTable",
										"Change In Value": "NonBalanceTable"}'::jsonb;

BEGIN

	DROP TABLE IF EXISTS temp_extraction_results_intermediate_summary;
    CREATE TEMP TABLE temp_extraction_results_intermediate_summary (
        partyid TEXT,
        clientid TEXT,
		origin TEXT,
        currency TEXT,
        sectiontype TEXT,
        sectionheader TEXT,
        recordtype TEXT,
        data JSONB
    )ON COMMIT DROP;

	IF balance_non_balance_option IS NULL THEN
		balance_non_balance_option = balance_non_balance_options;
	END IF;

  -- Loop through each section and insert results into the temp table
    FOR section IN
        SELECT * FROM (VALUES
            -- BalanceIdentifierTypes
			('Internal','Income Summary', balance_non_balance_option ->> 'Income Summary' ),
			('Internal','Asset Allocation', balance_non_balance_option ->> 'Asset Allocation'),
			('Internal','Margin Summary', balance_non_balance_option ->> 'Margin Summary'),
			('Internal','GainLoss Summary', balance_non_balance_option ->> 'GainLoss Summary'),
			('Internal','Retirement Summary', balance_non_balance_option ->> 'Retirement Summary'),
			('Internal','Market Value Over Time', balance_non_balance_option ->> 'Market Value Over Time'),
			('Internal','Transaction Overview', balance_non_balance_option ->> 'Transaction Overview'),
			('Internal','Change In Value', balance_non_balance_option ->> 'Change In Value')

        ) AS s(sectiontype, sectionheader, balanceflag)
    LOOP
    -- Determine which function to call based on the sectionname
    IF section.sectionheader IN ('Income Summary') THEN
		IF section.balanceflag = 'BalanceTable' THEN
        	PERFORM extract_summary_details_from_balance(p_identifier_value, clientid, origin, section.sectiontype,
																				 section.sectionheader);
			RAISE NOTICE 'section.balanceflag: %', section.balanceflag ||' '|| section.sectionheader;
		END IF;
	END IF;
	IF section.sectionheader IN ('Asset Allocation') THEN
		IF section.balanceflag = 'BalanceTable' THEN
        	PERFORM extract_summary_details_from_balance(p_identifier_value, clientid, origin, section.sectiontype,
																				 section.sectionheader);
			RAISE NOTICE 'section.balanceflag: %', section.balanceflag ||' '|| section.sectionheader;
		END IF;
		IF section.balanceflag = 'SemiBalanceTable' THEN
            PERFORM extract_summary_details_from_semi_bal_for_asset_alloc(p_identifier_value, clientid, origin,
																				section.sectiontype, section.sectionheader);
			RAISE NOTICE 'section.balanceflag: %', section.balanceflag||' '|| section.sectionheader;
		END IF;
		IF section.balanceflag = 'NonBalanceTable' THEN
            PERFORM extract_summary_details_from_non_balance_for_asset_alloc(p_identifier_value, clientid, origin,
																				section.sectiontype, section.sectionheader);
			RAISE NOTICE 'section.balanceflag: %', section.balanceflag ||' '|| section.sectionheader;
		END IF;
	END IF;
	IF section.sectionheader IN ('Margin Summary') THEN
		IF section.balanceflag = 'BalanceTable' THEN
        	PERFORM extract_summary_details_from_balance(p_identifier_value, clientid, origin,
																				 section.sectiontype, section.sectionheader);
			RAISE NOTICE 'section.balanceflag: %', section.balanceflag ||' '|| section.sectionheader;

		END IF;
	END IF;
	IF section.sectionheader IN ('Retirement Summary') THEN
		IF section.balanceflag = 'BalanceTable' THEN
        	PERFORM extract_summary_details_from_balance(p_identifier_value, clientid, origin,
																				 section.sectiontype, section.sectionheader);
			RAISE NOTICE 'section.balanceflag: %', section.balanceflag ||' '|| section.sectionheader;
		END IF;
	END IF;
	IF section.sectionheader IN ('GainLoss Summary') THEN
		IF section.balanceflag = 'NonBalanceTable' THEN
            PERFORM extract_summary_details_from_non_balance_for_gainloss(p_identifier_value, clientid, origin,
																				section.sectiontype, section.sectionheader);
			RAISE NOTICE 'section.balanceflag: %', section.balanceflag ||' '|| section.sectionheader;
		END IF;
	END IF;
	IF section.sectionheader IN ('GainLoss Summary') THEN
		IF section.balanceflag = 'BalanceTable' THEN
            PERFORM extract_summary_details_from_non_balance_for_gainloss(p_identifier_value, clientid, origin,
																				section.sectiontype, section.sectionheader);
			RAISE NOTICE 'section.balanceflag: %', section.balanceflag ||' '|| section.sectionheader;
		END IF;
	END IF;
	IF section.sectionheader IN ('Market Value Over Time') THEN
		IF section.balanceflag = 'BalanceTable' THEN
        	PERFORM extract_summary_details_from_balance(p_identifier_value, clientid, origin,
																				 section.sectiontype, section.sectionheader);
			RAISE NOTICE 'section.balanceflag: %', section.balanceflag ||' '|| section.sectionheader;
		END IF;
	END IF;
	IF section.sectionheader IN ('Transaction Overview') THEN

		IF section.balanceflag = 'NonBalanceTable' THEN
            PERFORM extract_summary_details_from_non_balance_for_tran_overview(p_identifier_value, clientid, origin,
																				section.sectiontype, section.sectionheader);
			RAISE NOTICE 'section.balanceflag: %', section.balanceflag ||' '|| section.sectionheader;
		END IF;
	END IF;
	IF section.sectionheader IN ('Change In Value') THEN

		IF section.balanceflag = 'NonBalanceTable' THEN
            PERFORM extract_summary_details_from_non_balance_for_change_in_value(p_identifier_value, clientid, origin,
																				section.sectiontype, section.sectionheader);
			RAISE NOTICE 'section.balanceflag: %', section.balanceflag ||' '|| section.sectionheader;
		END IF;
	END IF;

    END LOOP;

	SELECT ARRAY(
		SELECT DISTINCT ters.clientid
		FROM temp_extraction_results_intermediate_summary ters
	) INTO clientid_list;

	RAISE NOTICE 'clientid list: %', clientid_list;

FOREACH client_data IN ARRAY clientid_list LOOP

    RAISE NOTICE 'client_data: %', client_data;

    SELECT ARRAY(
            SELECT DISTINCT currency
            FROM temp_extraction_results_intermediate_summary ters
            WHERE ters.clientid = client_data
            AND currency IS NOT NULL
        ) INTO currency_list;

    RAISE NOTICE 'Currency list: %', currency_list;

    -- Loop through each currency and build summary JSON with conditional logic for each section
    FOREACH currency_data IN ARRAY currency_list LOOP

		PERFORM create_json_for_ytd_and_update_historical(client_data, currency_data, 'temp_extraction_results_intermediate_summary');
		-- Extract details from every household account to generate summary
		PERFORM extract_summary_details_of_hh_summaries(p_identifier_value, client_data, currency_data );
    END LOOP;

    END LOOP;

END;
$function$;