CREATE OR REPLACE FUNCTION public.create_json_for_extraction(
p_identifier_value text, clientid text, origin text, summary_option jsonb)

RETURNS VOID
 LANGUAGE plpgsql
AS $function$
DECLARE

v_master_partyid TEXT;
db_rec record;

BEGIN

	-- Call this to get 1) all accounts related to master account.   2) common advisor row in a temp table.
	-- 3) All initialization steps before processing all accounts in loop
	PERFORM extract_advisor_sisters_details_and_initialization(p_identifier_value, clientid, origin);

	FOR db_rec in (SELECT * FROM temp_account_details) LOOP

        PERFORM create_json_for_extraction_individual_partyidentifier_details(db_rec.partyid , clientid, origin, summary_option);

	END LOOP;

    SELECT partyid INTO v_master_partyid
	FROM temp_account_details
	WHERE partycontacttype = 'PRIMARY';

	IF v_master_partyid IS NOT NULL THEN

		PERFORM extract_summary_details_of_hh_summaries_post_processing();

        PERFORM create_json_for_hh_summary(p_identifier_value);

	END IF;

END;
$function$;
