CREATE OR REPLACE FUNCTION public.create_json_for_extraction_individual_partyidentifier_details(p_identifier_value text, clientid text, origin text, summary_option jsonb)
 RETURNS VOID
 LANGUAGE plpgsql
AS $function$
DECLARE

BEGIN

    -- Call the get_party_details function and update the table temp_extraction_results_party
      PERFORM create_json_for_party_accounts(p_identifier_value, clientid, origin);

    -- Call the get_transaction_json function and update the table temp_extraction_results_transaction
       PERFORM create_json_for_transaction(p_identifier_value, clientid, origin);

	-- Call the get_position_json function and update the table temp_extraction_results_position
       PERFORM create_json_for_position(p_identifier_value, clientid, origin);

	-- Call the get_balance_json function and update the table temp_extraction_results_balance
       PERFORM create_json_for_balance(p_identifier_value, clientid, origin);

	-- Call the get_balance_json function and update the table temp_extraction_results_summary
	--for Individual summary and temp_extraction_results_hh_summary for HouseHold summary
       PERFORM create_json_for_summary(p_identifier_value, clientid, origin, summary_option);

	-- Call the create_json_for_communication function and update the table temp_extraction_results_communication
       PERFORM create_json_for_communication(p_identifier_value, origin);
END;
$function$;
