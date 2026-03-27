CREATE OR REPLACE FUNCTION public.extract_summary_details_of_hh_summaries_post_processing()
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
DECLARE

	currency_data TEXT;
    currency_list TEXT[];
    clientid_list TEXT[];
    client_data TEXT;

	dynamic_sql text;
	db_rec record;
	json_param text[];
	individual_asset_amount numeric :=0;

BEGIN

	SELECT ARRAY(
		SELECT DISTINCT tehss.clientid
		FROM temp_extraction_results_hh_summary tehss
	) INTO clientid_list;

	FOREACH client_data IN ARRAY clientid_list LOOP

		SELECT ARRAY(
				SELECT DISTINCT currency
				FROM temp_extraction_results_hh_summary tehss
				WHERE tehss.clientid = client_data
			) INTO currency_list;


		-- Loop through each currency and update the temp table with allocation percentages
		FOREACH currency_data IN ARRAY currency_list LOOP

			dynamic_sql:= 'SELECT * FROM temp_extraction_results_hh_summary tehss
						   JOIN temp_extraction_hh_asset_allocation_total tehaa ON
						   tehss.currency = tehaa.currency
						   WHERE tehss.clientid = $1 AND tehss.currency = $2
							AND tehss.sectionheader = $3';

			FOR db_rec IN EXECUTE dynamic_sql using client_data, currency_data, 'Household Asset Allocation' 	LOOP

				FOR amount_index IN 0..COALESCE(jsonb_array_length(db_rec.data -> 'BalanceAmount'),0)-1  LOOP

					individual_asset_amount = (db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountValue')::numeric ;

					FOR narrative_index IN 0..COALESCE(jsonb_array_length(db_rec.data -> 'BalanceNarrative'),0)-1  LOOP
						IF (db_rec.data -> 'BalanceNarrative' -> narrative_index ->> 'BalanceNarrativeType') = 'AssetPercentage' THEN

							json_param='{BalanceNarrative, '||narrative_index||' , BalanceNarrativeValue}';
							db_rec.data := jsonb_set(db_rec.data, json_param,
						--			to_jsonb(ROUND((individual_asset_amount / db_rec.totalassetamount) * 100, 2)));
							to_jsonb(
									CASE
										WHEN db_rec.totalassetamount != 0
										THEN ROUND((individual_asset_amount / db_rec.totalassetamount) * 100, 2)
										ELSE 0
									END  )
									);

						END IF;
					END LOOP;
				END LOOP;

				UPDATE temp_extraction_results_hh_summary
				SET data = db_rec.data
				WHERE clientid = db_rec.clientid AND origin = db_rec.origin AND currency = db_rec.currency and sectiontype = db_rec.sectiontype
						AND sectionheader =  db_rec.sectionheader AND recordtype = db_rec.recordtype AND amounttype = db_rec.amounttype AND
						classification = db_rec.classification;

			END LOOP;
		END LOOP;
	END LOOP;

    RETURN NULL;

END;
$function$;