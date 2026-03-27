CREATE OR REPLACE FUNCTION public.create_json_for_balance(p_identifier_value text, clientid text, origin text)
 RETURNS VOID
 LANGUAGE plpgsql
AS $function$
DECLARE
    section RECORD;
    balance_data JSONB;
    combined_results JSONB;

BEGIN
    -- Loop through each section and insert results into the temp table
    FOR section IN
        SELECT * FROM (VALUES
            -- Transactions
			-- Securities activity
				('Internal','Cash and Cash Equivalents'),
				('Internal', 'Liabilities Summary')
			       ) AS s(balanceidentifiertype, balanceidentifiervalue)

    LOOP
    -- Call function extract_transaction_details with above arguments

			PERFORM extract_balance_details(p_identifier_value, clientid, origin, section.balanceidentifiertype,
			section.balanceidentifiervalue);

    END LOOP;

END;
$function$;
