CREATE OR REPLACE FUNCTION public.create_json_for_hh_summary(p_identifier_value text)
 RETURNS VOID
 LANGUAGE plpgsql
AS $function$
DECLARE
    section RECORD;
    combined_results JSONB;
    currency_data TEXT;
    currency_list TEXT[];
    clientid_list TEXT[];
    client_data TEXT;
    summary_data JSONB;
    summary_detail JSONB := '[]'::jsonb;

BEGIN

	-- Updating all summary entries with Single/Master account's partyidentifier,
	-- because earlier while summarizing for different accounts, it was not possible
	-- and now its needed for final summary, to make entries in historical table
	-- for summaries, with master account details.

	UPDATE temp_extraction_results_hh_summary
	SET partyid = p_identifier_value;

	SELECT ARRAY(
		SELECT DISTINCT terhs.clientid
		FROM temp_extraction_results_hh_summary terhs
	) INTO clientid_list;

	RAISE NOTICE 'clientid list: %', clientid_list;

	FOREACH client_data IN ARRAY clientid_list LOOP

		RAISE NOTICE 'client_data: %', client_data;

		SELECT ARRAY(
				SELECT DISTINCT currency
				FROM temp_extraction_results_hh_summary terhs
				WHERE terhs.clientid = client_data
			) INTO currency_list;

		-- Loop through each currency and build summary JSON with conditional logic for each section
		FOREACH currency_data IN ARRAY currency_list LOOP

			PERFORM create_json_for_ytd_and_update_historical(client_data, currency_data, 'temp_extraction_results_hh_summary' );

		END LOOP;

    END LOOP;

END;
$function$;