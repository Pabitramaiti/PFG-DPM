CREATE OR REPLACE FUNCTION public.extract_summary_details_from_non_balance_for_asset_alloc(p_identifier_value text, client_data text, origin text, sectiontype text, sectionheader text)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE

        dynamic_sql_total text;
		dynamic_sql text;
		db_rec record;
		all_sections text[];
		fixed_income_sections text[];

		currency_list TEXT[];
		intermediate_amount numeric;
		individual_asset_amount numeric;

		balanceidentifier_json jsonb;
		balancenarrative_json jsonb;
		balanceamount_json jsonb;
		balanceflag_json jsonb;

		temp_data jsonb;
		json_param text[];
		currency_data TEXT;

BEGIN

	DROP TABLE IF EXISTS temp_extraction_results_summary_asset;
    CREATE TEMP TABLE temp_extraction_results_summary_asset (
        partyid TEXT,
        clientid TEXT,
		origin TEXT,
        currency TEXT,
        sectiontype TEXT,
        sectionheader TEXT,
        recordtype TEXT,
        data JSONB,
		originsectiontype TEXT,
		originsectionheader TEXT
    )ON COMMIT DROP;


	all_sections := ARRAY['Fixed Income - Government and Agency Bonds', 'Fixed Income - Corporate Bonds',
		'Fixed Income - Mortgage Bonds', 'Fixed Income - Municipal Bonds', 'Fixed Income - Structured Products', 'Equities and Options',
		'Alternative Investments', 'Certificates of Deposits', 'Unit Investment Trust', 'Mutual Funds'];

	fixed_income_sections := ARRAY['Fixed Income - Government and Agency Bonds', 'Fixed Income - Corporate Bonds',
		'Fixed Income - Mortgage Bonds', 'Fixed Income - Municipal Bonds', 'Fixed Income - Structured Products'];

	DROP TABLE IF EXISTS temp_total_asset_amount;
    CREATE TEMP TABLE temp_total_asset_amount (
        currency TEXT,
		totalassetamount numeric
    )ON COMMIT DROP;


	SELECT ARRAY(
		SELECT DISTINCT currency
		FROM temp_extraction_results_position terp
		WHERE terp.clientid = client_data
	) INTO currency_list;

	RAISE NOTICE 'Currency list: %', currency_list;

	-- Loop through each currency and build total for that currency, which will be used for finding percentage later
    FOREACH currency_data IN ARRAY currency_list LOOP

		intermediate_amount :=0;

		dynamic_sql_total:= 'SELECT * FROM temp_extraction_results_position temp_pos WHERE temp_pos.partyid= $1 AND temp_pos.clientid = $2 AND
						temp_pos.origin = $3 AND temp_pos.currency= $4 AND temp_pos.sectionheader = ANY($5) AND	temp_pos.recordtype = $6
						UNION
						SELECT * FROM temp_extraction_results_balance temp_bal WHERE temp_bal.partyid= $1 AND temp_bal.clientid = $2  AND temp_bal.origin = $3 AND 	temp_bal.currency= $4 AND temp_bal.sectionheader = $7 AND	temp_bal.recordtype = $6 ' ;

		FOR db_rec IN EXECUTE dynamic_sql_total using p_identifier_value, client_data, origin, currency_data, all_sections, 'Total',
						'Cash and Cash Equivalents'  	LOOP


			IF db_rec.sectionheader <> 'Cash and Cash Equivalents' THEN
				FOR amount_index IN 0..COALESCE(jsonb_array_length(db_rec.data -> 'PositionAmount'),0)-1  LOOP

					IF db_rec.data -> 'PositionAmount' -> amount_index ->> 'PositionAmountType' = 'TotalMarketValue' THEN

						intermediate_amount := 	intermediate_amount + (db_rec.data -> 'PositionAmount' -> amount_index ->> 'PositionAmountValue')::numeric;

					END IF;

				END LOOP;
			ELSE
				FOR amount_index IN 0..COALESCE(jsonb_array_length(db_rec.data -> 'BalanceAmount'),0)-1  LOOP

					IF db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType' = 'TotalCashAndCashEquivalents' THEN

						intermediate_amount := 	intermediate_amount + (db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountValue')::numeric;

					END IF;

				END LOOP;
			END IF;
		END LOOP;

		INSERT INTO temp_total_asset_amount
		VALUES (currency_data, intermediate_amount);

	END LOOP;



	-- Loop through each currency and build detail rows for that currency
    FOREACH currency_data IN ARRAY currency_list LOOP

		dynamic_sql := 'SELECT * FROM temp_extraction_results_position temp_pos
							 JOIN temp_total_asset_amount ttaa
							 ON	 temp_pos.currency = ttaa.currency
							 WHERE temp_pos.partyid= $1 AND temp_pos.clientid = $2 AND temp_pos.origin = $3 AND
							 temp_pos.currency= $4 AND temp_pos.sectionheader = ANY($5) AND	temp_pos.recordtype = $6
							 UNION
							 SELECT * FROM temp_extraction_results_balance temp_bal
							 JOIN temp_total_asset_amount ttaa
							 ON	 temp_bal.currency = ttaa.currency
							 WHERE temp_bal.partyid= $1 AND temp_bal.clientid = $2 AND temp_bal.origin = $3 AND
							 temp_bal.currency= $4 AND temp_bal.sectionheader = $7 AND	temp_bal.recordtype = $6 ' ;

		FOR db_rec IN EXECUTE dynamic_sql using p_identifier_value, client_data, origin, currency_data, all_sections, 'Total',
							'Cash and Cash Equivalents'  	LOOP
	  -- Preparing for the merger of all fixed income sections into one with name Fixed Income
			IF db_rec.sectionheader = ANY(fixed_income_sections) THEN
				db_rec.sectionheader := 'Fixed Income';
			END IF;

			individual_asset_amount :=0;

			IF db_rec.sectionheader <> 'Cash and Cash Equivalents' THEN
				FOR amount_index IN 0..COALESCE(jsonb_array_length(db_rec.data -> 'PositionAmount'),0)-1  LOOP
					IF db_rec.data -> 'PositionAmount' -> amount_index ->> 'PositionAmountType' = 'TotalMarketValue' THEN

						individual_asset_amount := 	(db_rec.data -> 'PositionAmount' -> amount_index ->> 'PositionAmountValue')::numeric;
					END IF;
				END LOOP;
			ELSE
				FOR amount_index IN 0..COALESCE(jsonb_array_length(db_rec.data -> 'BalanceAmount'),0)-1  LOOP
					IF db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType' = 'TotalCashAndCashEquivalents' THEN

						individual_asset_amount := (db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountValue')::numeric;

					END IF;
				END LOOP;
			END IF;

			 -- Check if this row already does not exists in temp summary table, if not insert it, else update it ( mostly fixed income amount)
			IF NOT EXISTS (
				SELECT FROM temp_extraction_results_summary_asset tersa
				WHERE tersa.partyid=db_rec.partyid AND tersa.clientid=db_rec.clientid AND tersa.origin=db_rec.origin AND tersa.currency=db_rec.currency AND tersa.originsectionheader=db_rec.sectionheader AND tersa.recordtype='Detail'
			) THEN
			RAISE NOTICE 'Record insertion: %', db_rec.sectionheader;
				balanceidentifier_json := jsonb_agg(
					jsonb_build_object(
						'BalanceIdentifierType', sectiontype,
						'BalanceIdentifierValue', sectionheader
					)
				);


				balancenarrative_json := jsonb_agg(
					jsonb_build_object(
						'BalanceNarrativeType', 'Description',
						'BalanceNarrativeValue', db_rec.sectionheader,
						'BalanceNarrativeSequence', 1
					))
					||
					jsonb_build_object(
						'BalanceNarrativeType', 'AssetPercentage',
						'BalanceNarrativeValue',
							CASE
								WHEN db_rec.totalassetamount != 0
								THEN ROUND((individual_asset_amount / db_rec.totalassetamount) * 100, 2)
							--	ELSE NULL
								ELSE 0
							END,
						'BalanceNarrativeSequence', 2);

				balanceamount_json := jsonb_agg(
					jsonb_build_object(
						'BalanceAmountType',
							CASE
								WHEN db_rec.sectionheader='Fixed Income'
								THEN 'TotalFixedIncome'
								WHEN db_rec.sectionheader='Equities and Options'
								THEN 'TotalEquityValue'
								WHEN db_rec.sectionheader='Alternative Investments'
								THEN 'TotalAlternativeInvestments'
								WHEN db_rec.sectionheader='Certificates of Deposits'
								THEN 'TotalCertificatesOfDeposits'
								WHEN db_rec.sectionheader='Unit Investment Trust'
								THEN 'TotalUnitInvestmentTrust'
								WHEN db_rec.sectionheader='Mutual Funds'
								THEN 'TotalMutualFunds'
								WHEN db_rec.sectionheader='Cash and Cash Equivalents'
								THEN 'TotalCash'
							END,
						'BalanceAmountValue', individual_asset_amount,
						'BalanceAmountCurrency', db_rec.currency
						)
					);

				balanceflag_json := jsonb_agg(
					jsonb_build_object(
						'BalanceFlagType', 'IsPTD',
						'BalanceFlagValue', '1'
						)
					);


				INSERT INTO temp_extraction_results_summary_asset (
					partyid,
					clientid,
					origin,
					currency,
					sectiontype,
					sectionheader,
					recordtype,
					data,
					originsectiontype,
					originsectionheader
					)
				VALUES(
					db_rec.partyid,
					db_rec.clientid,
					db_rec.origin,
					db_rec.currency,
					'Internal',
					'Asset Allocation',
					'Detail',
					jsonb_strip_nulls(
						jsonb_build_object(
							'BalanceIdentifier', balanceidentifier_json,
							'BalanceNarrative', balancenarrative_json,
							'BalanceAmount', balanceamount_json,
							'BalanceFlag', balanceflag_json
						)
					),
					db_rec.sectiontype,
					db_rec.sectionheader
					);

			ELSE
				RAISE NOTICE 'Record updation: %', db_rec.sectionheader;

				SELECT data INTO temp_data FROM temp_extraction_results_summary_asset tersa
				WHERE tersa.partyid=db_rec.partyid AND tersa.clientid=db_rec.clientid AND tersa.origin=db_rec.origin
				AND tersa.currency=db_rec.currency AND tersa.originsectionheader=db_rec.sectionheader AND tersa.recordtype='Detail';

				FOR amount_index IN 0..COALESCE(jsonb_array_length(temp_data -> 'BalanceAmount'),0)-1  LOOP
					IF temp_data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType' IN ( 'TotalFixedIncome','TotalEquityValue',
									'TotalAlternativeInvestments', 'TotalCertificatesOfDeposits', 'TotalUnitInvestmentTrust',
									'TotalMutualFunds', 'TotalCash') THEN

						individual_asset_amount = individual_asset_amount + (temp_data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountValue')::numeric ;
						json_param='{BalanceAmount, '||amount_index||' , BalanceAmountValue}';
						temp_data := jsonb_set(temp_data, json_param, to_jsonb(individual_asset_amount));

						FOR narrative_index IN 0..COALESCE(jsonb_array_length(temp_data -> 'BalanceNarrative'),0)-1  LOOP
							IF temp_data -> 'BalanceNarrative' -> narrative_index ->> 'BalanceNarrativeType' = 'AssetPercentage' THEN

								json_param='{BalanceNarrative, '||narrative_index||' , BalanceNarrativeValue}';
								temp_data := jsonb_set(temp_data, json_param,
											to_jsonb(ROUND((individual_asset_amount / db_rec.totalassetamount) * 100, 2)));

							END IF;
						END LOOP;

					END IF;
				END LOOP;

				UPDATE temp_extraction_results_summary_asset tersa
				SET data = temp_data
				WHERE tersa.partyid=db_rec.partyid AND tersa.clientid=db_rec.clientid AND tersa.origin=db_rec.origin AND tersa.currency=db_rec.currency AND tersa.originsectionheader=db_rec.sectionheader AND tersa.recordtype='Detail';

			END IF;

		END LOOP;
	END LOOP;

	INSERT INTO temp_extraction_results_intermediate_summary(
		partyid,
		clientid,
		origin,
		currency,
		sectiontype,
		sectionheader,
		recordtype,
		data)
	select
	tersa.partyid,
	tersa.clientid,
	tersa.origin,
	tersa.currency,
	tersa.sectiontype,
	tersa.sectionheader,
	tersa.recordtype,
	tersa.data
	FROM temp_extraction_results_summary_asset tersa;

END;
$function$;