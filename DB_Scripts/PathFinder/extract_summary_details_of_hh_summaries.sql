CREATE OR REPLACE FUNCTION public.extract_summary_details_of_hh_summaries(
p_identifier_value text, clientidentifier_arg text, currency_arg text)
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
DECLARE
        dynamic_sql text;
        table_name text := 'temp_extraction_results_summary';
		db_rec record;

		--Created below rec for debugging. should be removed
		rec record;

	    all_sections text[] := ARRAY['GainLoss Summary', 'Income Summary','Asset Allocation','Change In Value'];
		v_data JSONB;
		json_param text[];
		individual_to_hh_summary_mapping JSONB := '{"Income Summary": "Household Income Summary",
										"Asset Allocation": "Household Asset Allocation",
										"GainLoss Summary": "Household GainLoss Summary",
										"Change In Value": "HouseHold Summary"}'::jsonb;

		flag_familygroup_array JSONB :=  '[ { "BalanceFlagType": "IsFamilyGroup",
							"BalanceFlagValue": "1"
							} ]'::jsonb;

        flag_familygroup_element JSONB :=  '{ "BalanceFlagType": "IsFamilyGroup",
									"BalanceFlagValue": "1"
									}'::jsonb;

		intermediate_amount numeric;

		--HH summary variables
        v_origin TEXT;
        v_sectiontype TEXT;
        change_in_value text := 'Change In Value';
        house_hold_summary_section text := 'HouseHold Summary';
        hh_summary JSONB;
        primary_summary JSONB;
        secondary_summary JSONB;
        total_summary JSONB;
        v_partyid TEXT;
        contactytype text;
        location TEXT;
        prev_year_dec_table TEXT;
        dynamic_sql_historical text;
        beginning_value_ptd_primary NUMERIC := 0;
        beginning_value_ytd_primary NUMERIC := 0;
        ending_value_ptd_primary NUMERIC := 0;
        change_in_mv_ytd_primary NUMERIC := 0;
        beginning_value_ptd_secondary NUMERIC := 0;
        beginning_value_ytd_secondary NUMERIC := 0;
        ending_value_ptd_secondary NUMERIC := 0;
        change_in_mv_ytd_secondary NUMERIC := 0;
        previous_year TEXT;
        is_secondary BOOLEAN := false;
        current_month_historical_table_name TEXT;
        AssetsNotHeldValue text := 'AssetsNotHeldValue';
        assets_not_held_value_history_json JSONB;
        assets_not_held_value_current_json JSONB;
        balance_party_identifier_value_nick_name_primary text;
        balance_party_identifier_type_primary_and_secondary text;
        balance_party_identifier_value_nick_name_secondary text;
        --HH summary variables Ends


BEGIN
	dynamic_sql:= 'SELECT * FROM ' || table_name || ' WHERE partyid = $1 AND clientid = $2 AND currency = $3
					AND sectionheader = ANY($4)';

	FOR db_rec IN EXECUTE dynamic_sql using p_identifier_value, clientidentifier_arg, currency_arg, all_sections 	LOOP

    IF db_rec.sectionheader <> change_in_value THEN

		FOR sectionheader_index IN 0..COALESCE(jsonb_array_length(db_rec.data -> 'BalanceIdentifier'),0)-1  LOOP
			IF (db_rec.data -> 'BalanceIdentifier' -> sectionheader_index ->> 'BalanceIdentifierValue') = db_rec.sectionheader THEN

				json_param='{BalanceIdentifier, '||sectionheader_index||' , BalanceIdentifierValue}';
				db_rec.data := jsonb_set(db_rec.data, json_param,
						to_jsonb(individual_to_hh_summary_mapping ->> (db_rec.data -> 'BalanceIdentifier' -> sectionheader_index ->> 'BalanceIdentifierValue')));
			END IF;
		END LOOP;
		IF db_rec.data -> 'BalanceFlag' IS NULL THEN
			db_rec.data := jsonb_set(db_rec.data, '{BalanceFlag}', flag_familygroup_array);
		ELSE
			db_rec.data := jsonb_set(db_rec.data, '{BalanceFlag}', (db_rec.data -> 'BalanceFlag') || flag_familygroup_element);
		END IF;
		db_rec.sectionheader =  (individual_to_hh_summary_mapping ->> db_rec.sectionheader);

		intermediate_amount := 0;

	--	BEGIN
		IF NOT EXISTS ( SELECT 1 FROM temp_extraction_results_hh_summary
				WHERE clientid = db_rec.clientid AND origin = db_rec.origin AND currency = db_rec.currency and sectiontype = db_rec.sectiontype
				AND sectionheader =  db_rec.sectionheader AND recordtype = db_rec.recordtype AND amounttype = db_rec.amounttype AND
				classification = db_rec.classification ) THEN
			INSERT INTO temp_extraction_results_hh_summary VALUES ( NULL, db_rec.clientid, db_rec.origin, db_rec.currency, db_rec.sectiontype,
			db_rec.sectionheader, db_rec.recordtype, db_rec.amounttype, db_rec.classification , db_rec.data );

			-- summing up totals ( for asset allocation total)
			IF db_rec.sectionheader = 'Household Asset Allocation' THEN
				FOR amount_index IN 0..COALESCE(jsonb_array_length(db_rec.data -> 'BalanceAmount'),0)-1  LOOP
					intermediate_amount := intermediate_amount + (db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountValue')::numeric;
				END LOOP;
			END IF;

		ELSE
			SELECT data INTO v_data FROM temp_extraction_results_hh_summary
			WHERE clientid = db_rec.clientid AND origin = db_rec.origin AND currency = db_rec.currency and sectiontype = db_rec.sectiontype
			AND sectionheader =  db_rec.sectionheader AND recordtype = db_rec.recordtype AND amounttype = db_rec.amounttype AND
			classification = db_rec.classification ;


			FOR amount_index IN 0..COALESCE(jsonb_array_length(db_rec.data -> 'BalanceAmount'),0)-1  LOOP

				FOR amount_index_summaries IN 0..COALESCE(jsonb_array_length(v_data -> 'BalanceAmount'),0)-1  LOOP

					IF (db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType') = (v_data -> 'BalanceAmount' -> amount_index_summaries ->> 'BalanceAmountType') THEN

							json_param='{BalanceAmount, '||amount_index_summaries||' , BalanceAmountValue}';
							v_data := jsonb_set(v_data, json_param,
										to_jsonb((v_data -> 'BalanceAmount' -> amount_index_summaries ->> 'BalanceAmountValue')::numeric +
										(db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountValue')::numeric));

					END IF;
				END LOOP;
			END LOOP;

			UPDATE temp_extraction_results_hh_summary
			SET data = v_data
			WHERE clientid = db_rec.clientid AND origin = db_rec.origin AND currency = db_rec.currency and sectiontype = db_rec.sectiontype
			AND sectionheader = db_rec.sectionheader AND recordtype = db_rec.recordtype AND amounttype = db_rec.amounttype AND
			classification = db_rec.classification ;

			-- summing up totals ( for asset allocation total)
			IF db_rec.sectionheader = 'Household Asset Allocation' THEN
				FOR amount_index IN 0..COALESCE(jsonb_array_length(db_rec.data -> 'BalanceAmount'),0)-1  LOOP
					intermediate_amount := intermediate_amount + (db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountValue')::numeric;
				END LOOP;
			END IF;

		END IF;

		-- summing up totals ( for asset allocation total)
		IF db_rec.sectionheader = 'Household Asset Allocation' THEN

			IF EXISTS ( SELECT 1 FROM temp_extraction_hh_asset_allocation_total
					  WHERE currency= db_rec.currency) THEN
				UPDATE temp_extraction_hh_asset_allocation_total tehaat
				SET 	totalassetamount = tehaat.totalassetamount + intermediate_amount
				WHERE currency= db_rec.currency;
			ELSE
				INSERT INTO temp_extraction_hh_asset_allocation_total (totalassetamount, currency)
				VALUES (intermediate_amount, db_rec.currency);
			END IF;
		END IF;

	ELSE

    --HH summary code starts
    IF db_rec.sectionheader = change_in_value THEN
        IF db_rec.data -> 'BalanceAmount' IS NOT NULL AND db_rec.data -> 'BalanceFlag' IS NOT NULL THEN
            -- Check for BeginningValue with IsPTD
            IF (db_rec.data -> 'BalanceAmount' @> '[{"BalanceAmountType": "BeginningValue"}]') AND
               (db_rec.data -> 'BalanceFlag' @> '[{"BalanceFlagType": "IsPTD"}]') THEN
                beginning_value_ptd_primary := COALESCE((
                    jsonb_path_query_first(
                        db_rec.data,
                        '$.BalanceAmount[*] ? (@.BalanceAmountType == "BeginningValue").BalanceAmountValue'
                    ) #>> '{}'
                )::NUMERIC, 0);
            END IF;

            -- Check for BeginningValue with IsYTD
            IF (db_rec.data -> 'BalanceAmount' @> '[{"BalanceAmountType": "BeginningValue"}]') AND
               (db_rec.data -> 'BalanceFlag' @> '[{"BalanceFlagType": "IsYTD"}]') THEN
                beginning_value_ytd_primary := COALESCE((
                    jsonb_path_query_first(
                        db_rec.data,
                        '$.BalanceAmount[*] ? (@.BalanceAmountType == "BeginningValue").BalanceAmountValue'
                    ) #>> '{}'
                )::NUMERIC, 0);
            END IF;

            -- Check for EndingValue with IsPTD
            IF (db_rec.data -> 'BalanceAmount' @> '[{"BalanceAmountType": "EndingValue"}]') AND
               (db_rec.data -> 'BalanceFlag' @> '[{"BalanceFlagType": "IsPTD"}]') THEN
                ending_value_ptd_primary := COALESCE((
                    jsonb_path_query_first(
                        db_rec.data,
                        '$.BalanceAmount[*] ? (@.BalanceAmountType == "EndingValue").BalanceAmountValue'
                    ) #>> '{}'
                )::NUMERIC, 0);
            END IF;

            -- Check for ChangeInMarketValue with IsYTD
            IF (db_rec.data -> 'BalanceAmount' @> '[{"BalanceAmountType": "ChangeInMarketValue"}]') AND
               (db_rec.data -> 'BalanceFlag' @> '[{"BalanceFlagType": "IsYTD"}]') THEN
                change_in_mv_ytd_primary := COALESCE((
                    jsonb_path_query_first(
                        db_rec.data,
                        '$.BalanceAmount[*] ? (@.BalanceAmountType == "ChangeInMarketValue").BalanceAmountValue'
                    ) #>> '{}'
                )::NUMERIC, 0);
            END IF;

            -- Check for AssetsNotHeldValue with IsPTD
            IF (db_rec.data -> 'BalanceAmount' @> '[{"BalanceAmountType": "AssetsNotHeldValue"}]') AND
               (db_rec.data -> 'BalanceFlag' @> '[{"BalanceFlagType": "IsPTD"}]') THEN
                ending_value_ptd_secondary := COALESCE((
                    jsonb_path_query_first(
                        db_rec.data,
                        '$.BalanceAmount[*] ? (@.BalanceAmountType == "AssetsNotHeldValue").BalanceAmountValue'
                    ) #>> '{}'
                )::NUMERIC, 0);
                is_secondary := TRUE;
            END IF;

            -- Check for AssetsNotHeldValue with IsYTD
            IF (db_rec.data -> 'BalanceAmount' @> '[{"BalanceAmountType": "AssetsNotHeldValue"}]') AND
               (db_rec.data -> 'BalanceFlag' @> '[{"BalanceFlagType": "IsYTD"}]') THEN
                change_in_mv_ytd_secondary := COALESCE((
                    jsonb_path_query_first(
                        db_rec.data,
                        '$.BalanceAmount[*] ? (@.BalanceAmountType == "AssetsNotHeldValue").BalanceAmountValue'
                    ) #>> '{}'
                )::NUMERIC, 0);
                is_secondary := TRUE;
            END IF;
        END IF; --Ending BalanceAmount IF

        IF v_partyid IS NULL THEN
            v_partyid := db_rec.partyid;
        END IF;
        IF v_origin IS NULL THEN
            v_origin := db_rec.origin;
        END IF;
        IF v_sectiontype IS NULL THEN
            v_sectiontype := db_rec.sectiontype;
        END IF;

         --pulling beginning_value_ptd_secondary value from current month historical table
         SELECT
            TO_CHAR(TO_DATE(BeginningDate, 'YYYY-MM-DD'), 'YYYYMM')
            INTO current_month_historical_table_name
            FROM temp_statement_dates_summary
            LIMIT 1;

         dynamic_sql_historical := 'SELECT summarydata FROM ' || quote_ident(current_month_historical_table_name) || ' WHERE partyid = $1
         AND clientid = $2 AND origin = $3 AND sectionheader = $4 AND summaryforsection = $5
         AND currency = $6';
         BEGIN
            EXECUTE dynamic_sql_historical
                INTO assets_not_held_value_current_json
                USING v_partyid, v_origin, clientidentifier_arg, change_in_value, AssetsNotHeldValue, currency_arg;
         EXCEPTION
            WHEN undefined_table THEN
                -- If table doesn't exist, set assets_not_held_value_current_json to NULL
                RAISE NOTICE 'Table % does not exist. Skipping...', current_month_historical_table_name;
                assets_not_held_value_current_json := NULL;
         END;

         --Extract the value of "previousperiod"
         IF assets_not_held_value_current_json IS NOT NULL THEN
             beginning_value_ptd_secondary := COALESCE((assets_not_held_value_history_json ->> 'previousperiod')::NUMERIC, 0);
         END IF;
         --Ends pulling beginning_value_ptd_secondary value from current month historical table

         --pulling beginning_value_ytd_secondary value from previous year december historical table
         SELECT TO_CHAR(CURRENT_DATE - INTERVAL '1 year', 'YYYY') INTO previous_year;
         prev_year_dec_table := 'historicaltable_' || previous_year || '12';
         dynamic_sql_historical := 'SELECT summarydata FROM ' || quote_ident(prev_year_dec_table) || ' WHERE partyid = $1 AND clientid = $2 AND sectionheader = $3
         AND summaryforsection = $4 AND currency = $5 LIMIT 1';

         BEGIN
            EXECUTE dynamic_sql_historical
                INTO assets_not_held_value_history_json
                USING v_partyid, clientidentifier_arg, change_in_value, AssetsNotHeldValue, currency_arg;
         EXCEPTION
            WHEN undefined_table THEN
                --If table doesn't exist, set assets_not_held_value_history_json to NULL
                RAISE NOTICE 'Table % does not exist. Skipping...', prev_year_dec_table;
                assets_not_held_value_history_json := NULL;
         END;

         --Extract the value of "ytdperiod" and assign to beginning_value_ytd_secondary
         IF assets_not_held_value_history_json IS NOT NULL THEN
             beginning_value_ytd_secondary := COALESCE((assets_not_held_value_history_json ->> 'ytdperiod')::NUMERIC, 0);
         END IF;

         --Ends pulling beginning_value_ytd_secondary value from previous year december historical table

         END IF;
       END IF;
    END LOOP;--Ending dynamic query execution loop

         --Update temp_extraction_hh_household_summary_total table
         IF NOT EXISTS (SELECT 1 FROM temp_extraction_hh_household_summary_total) THEN
         INSERT INTO temp_extraction_hh_household_summary_total DEFAULT VALUES;
         END IF;

         UPDATE temp_extraction_hh_household_summary_total
         SET total_beginning_balance = total_beginning_balance + beginning_value_ptd_primary + beginning_value_ptd_secondary,
         total_ending_balance = total_ending_balance + ending_value_ptd_primary + ending_value_ptd_secondary,
         total_change_in_mv = total_change_in_mv + change_in_mv_ytd_primary + change_in_mv_ytd_secondary;
         --Ends updating temp_extraction_hh_household_summary_total

         --Checking whether MASTER OR SISTER and assigns the required parameters accordingly
         SELECT partycontacttype INTO contactytype FROM temp_account_details
         WHERE partyid = v_partyid;

         --Constructing BalancePartyIdentifierValue: Combination of PartyIdentifierValue(where PartyIdentifierType is AccountName) and PartyName.
         SELECT pi2.partyidentifiervalue || ' - ' || pc.partyname
         INTO balance_party_identifier_value_nick_name_primary
         FROM partyidentifier pi1
         JOIN partyidentifier pi2
         ON pi2.partyid = pi1.partyid
         AND pi2.partyidentifiertype = 'AccountName'
         JOIN partycommon pc
         ON pc.partyid = pi1.partyid
         WHERE pi1.partyidentifiervalue = v_partyid
         LIMIT 1;
         --Ends BalancePartyIdentifierValue

         IF UPPER(contactytype) = 'MASTER' THEN
         balance_party_identifier_type_primary_and_secondary := 'MasterAccount';

         ELSE
         balance_party_identifier_type_primary_and_secondary := 'SisterAccount';

         END IF;

         --Get primary account's location
         dynamic_sql = 'SELECT pc.partyentity FROM partycommon pc
         JOIN partyidentifier pi
		 ON pc.partyid = pi.partyid
         WHERE pi.partyidentifiervalue = $1 AND pc.partyclientidentifier = $2
         AND pc.partyorigin = $3 LIMIT 1';
         EXECUTE dynamic_sql INTO location USING v_partyid, clientidentifier_arg, v_origin;

         --Build the summary JSONB for primary with extracted values
         primary_summary := jsonb_build_object(
             'BalanceEntity', location,
             'BalanceInstrumentIdentifierType', 'External',
             'BalanceInstrumentIdentifierValue', v_partyid,
             'BalanceAccountIdentifierType', 'AccountNumber',
             'BalanceAccountIdentifierValue', v_partyid,
             'BalanceIdentifier', jsonb_build_array(
                 jsonb_build_object(
                     'BalanceIdentifierType', v_sectiontype,
                     'BalanceIdentifierValue', house_hold_summary_section
                 )
             ),
             'BalanceParty', jsonb_build_array(
                 jsonb_build_object(
                     'BalancePartyIdentifierType', 'Nickname',
                     'BalancePartyIdentifierValue', balance_party_identifier_value_nick_name_primary
                 ),
                 jsonb_build_object(
                     'BalancePartyIdentifierType', balance_party_identifier_type_primary_and_secondary,
                     'BalancePartyIdentifierValue', v_partyid
                 )
             ),
             'BalanceAmount', jsonb_build_array(
                 jsonb_build_object(
                     'BalanceAmountType', 'BeginningValue',
                     'BalanceAmountValue', beginning_value_ptd_primary,
                     'BalanceAmountCurrency', currency_arg
                 ),
                 jsonb_build_object(
                     'BalanceAmountType', 'YTDBeginningValue',
                     'BalanceAmountValue', beginning_value_ytd_primary,
                     'BalanceAmountCurrency', currency_arg
                 ),
                 jsonb_build_object(
                     'BalanceAmountType', 'EndingValue',
                     'BalanceAmountValue', ending_value_ptd_primary,
                     'BalanceAmountCurrency', currency_arg
                 ),
                 jsonb_build_object(
                     'BalanceAmountType', 'ChangeInMarketValue',
                     'BalanceAmountValue', change_in_mv_ytd_primary,
                     'BalanceAmountCurrency', currency_arg
                 )
             )
         );

        --Inserting primary summary
        IF primary_summary IS NOT NULL THEN
        INSERT INTO temp_extraction_results_hh_summary
        VALUES (
            v_partyid,
            clientidentifier_arg,
            v_origin,
            currency_arg,
            v_sectiontype,
            house_hold_summary_section,
            'Detail',
            'Multiple',
            'Primary ' || v_partyid,
            primary_summary
        );
        END IF;
        IF is_secondary THEN
             balance_party_identifier_value_nick_name_secondary := 'Assets Not Held at ' || location;
             secondary_summary := jsonb_build_object(
            'BalanceEntity', 'Other**',
            'BalanceInstrumentIdentifierType', 'External',
            'BalanceInstrumentIdentifierValue', v_partyid,
            'BalanceAccountIdentifierType', 'AccountNumber',
            'BalanceAccountIdentifierValue', v_partyid,
            'BalanceIdentifier', jsonb_build_array(
                jsonb_build_object(
                    'BalanceIdentifierType', v_sectiontype,
                    'BalanceIdentifierValue', house_hold_summary_section
                )
            ),
            'BalanceParty', jsonb_build_array(
                jsonb_build_object(
                    'BalancePartyIdentifierType', 'Nickname',
                    'BalancePartyIdentifierValue', balance_party_identifier_value_nick_name_secondary
                ),
                jsonb_build_object(
                    'BalancePartyIdentifierType', balance_party_identifier_type_primary_and_secondary,
                    'BalancePartyIdentifierValue', v_partyid
                )
            ),
            'BalanceAmount', jsonb_build_array(
                jsonb_build_object(
                    'BalanceAmountType', 'BeginningValue',
                    'BalanceAmountValue', beginning_value_ptd_secondary,
                    'BalanceAmountCurrency', currency_arg
                ),
                jsonb_build_object(
                    'BalanceAmountType', 'YTDBeginningValue',
                    'BalanceAmountValue', beginning_value_ytd_secondary,
                    'BalanceAmountCurrency', currency_arg
                ),
                jsonb_build_object(
                    'BalanceAmountType', 'EndingValue',
                    'BalanceAmountValue', ending_value_ptd_secondary,
                    'BalanceAmountCurrency', currency_arg
                ),
                jsonb_build_object(
                    'BalanceAmountType', 'ChangeInMarketValue',
                    'BalanceAmountValue', change_in_mv_ytd_secondary,
                    'BalanceAmountCurrency', currency_arg
                )
            )
        );
        END IF;

        --Inserting not held summary
        IF secondary_summary IS NOT NULL THEN
        INSERT INTO temp_extraction_results_hh_summary
        VALUES (
            v_partyid,
            clientidentifier_arg,
            v_origin,
            currency_arg,
            v_sectiontype,
            house_hold_summary_section,
            'Detail',
            'Multiple',
            'Secondary ' || v_partyid,
            secondary_summary
        );
        END IF;

        --Constructing total JSON
        SELECT jsonb_build_object(
        'BalanceIdentifier', jsonb_build_array(
            jsonb_build_object(
                'BalanceIdentifierType', v_sectiontype,
                'BalanceIdentifierValue', house_hold_summary_section
            )
        ),
        'BalanceAmount', jsonb_build_array(
            jsonb_build_object(
                'BalanceAmountType', 'TotalBeginningBalance',
                'BalanceAmountValue', COALESCE(t.total_beginning_balance, 0),
                'BalanceAmountCurrency', currency_arg
            ),
            jsonb_build_object(
                'BalanceAmountType', 'TotalEndingBalance',
                'BalanceAmountValue', COALESCE(t.total_ending_balance, 0),
                'BalanceAmountCurrency', currency_arg
            ),
            jsonb_build_object(
                'BalanceAmountType', 'TotalChangeInMarketValue',
                'BalanceAmountValue', COALESCE(t.total_change_in_mv, 0),
                'BalanceAmountCurrency', currency_arg
            )
        )
    )
    INTO total_summary
        FROM temp_extraction_hh_household_summary_total t;

    --insert total_summary into temp_extraction_results_hh_summary
    IF total_summary IS NOT NULL THEN
        INSERT INTO temp_extraction_results_hh_summary (
            partyid,
            clientid,
            origin,
            currency,
            sectiontype,
            sectionheader,
            recordtype,
            amounttype,
            classification,
            data
        )
        VALUES (
            v_partyid,
            clientidentifier_arg,
            v_origin,
            currency_arg,
            v_sectiontype,
            house_hold_summary_section,
            'Total',
            'Multiple',
            'Primary plus Secondary',
            total_summary
        )
        ON CONFLICT (
            clientid,
            origin,
            currency,
            sectiontype,
            sectionheader,
            recordtype,
            amounttype,
            classification
        )
        DO UPDATE SET
            data = EXCLUDED.data;
    END IF;
        --HH summary code ends
    -- Return the final result with both data and summary totals, sorted

    RETURN NULL;

END;
$function$;