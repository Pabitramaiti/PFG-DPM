-- FUNCTION: public.extract_summary_details_from_non_balance_for_change_in_value(text, text, text, text, text)

-- DROP FUNCTION IF EXISTS public.extract_summary_details_from_non_balance_for_change_in_value(text, text, text, text, text);

CREATE OR REPLACE FUNCTION public.extract_summary_details_from_non_balance_for_change_in_value(
	p_identifier_value text,
	client_data text,
	origin text,
	sectiontype text,
	sectionheader text)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE

                dynamic_sql text;
                db_rec record;

                currency_list TEXT[];
                currency_data TEXT;
                location TEXT := '';

                balanceidentifier_json jsonb;
                balancenarrative_json jsonb;
                balanceamount_json jsonb;
                balanceflag_json jsonb;

                hextone_sections text[];

                -- new variables
                v_portfolio_value numeric :=0 ;
                v_beginning_value numeric :=0 ;
                v_credits_value numeric  :=0 ;
                v_debits_value numeric :=0 ;
                v_other_activity_value numeric :=0 ;
                v_ending_value numeric :=0 ;
                v_change_in_market_value numeric :=0 ;
                v_net_change_amount numeric :=0 ;
                v_asset_not_held_value numeric :=0 ;
                v_accrued_interest numeric :=0 ;
                v_total_balance numeric :=0 ;

                -- new variables ytd
                v_ending_value_ytd numeric :=0 ;
                v_accrued_interest_ytd numeric :=0 ;
                v_total_balance_ytd numeric :=0 ;
                v_change_in_value_rec_count_from_balance_table numeric  :=0;
BEGIN

    DROP TABLE IF EXISTS temp_extraction_results_change_in_value;
    CREATE TEMP TABLE temp_extraction_results_change_in_value (
        amounttype TEXT,
                amountvalue TEXT,
                narrative TEXT,
        isptd TEXT,
                recordtype TEXT
    )ON COMMIT DROP;

        hextone_sections := ARRAY['External Assets - Annuities', 'External Assets - Mutual Funds',
                                                'External Assets - Alternative Investments'];

        -- Extract change in value data from balance tables in temporary table
        PERFORM extract_summary_details_from_balance(p_identifier_value, client_data, origin, 'Internal',
                                                                                                                                                                 'Change In Value');

        SELECT ARRAY(
                SELECT DISTINCT currency
                FROM temp_extraction_results_intermediate_summary teris
                WHERE teris.clientid = client_data
        ) INTO currency_list;

        RAISE NOTICE 'Currency list: %', currency_list;

         --Get primary account's location ( client name )
         dynamic_sql = 'SELECT pc.partyentity
         FROM partycommon pc
         JOIN partyidentifier pi
         ON pc.partyid = pi.partyid
         JOIN partycontact pcon
         ON pi.partyid = pcon.partyid
         WHERE
         pi.partyidentifiervalue = $1 AND pc.partyclientidentifier = $2
         AND pc.partyorigin = $3
         AND pcon.partycontacttype =  ANY($4)
         LIMIT 1';
         EXECUTE dynamic_sql INTO location USING p_identifier_value, client_data, origin, ARRAY['AccountHolder','HouseHoldMasterAccount'];

         IF location IS NULL THEN
                location := 'DEFAULT LOCATION';
         END IF;

        -- Loop through each currency and build detail rows for that currency
    FOREACH currency_data IN ARRAY currency_list LOOP

                v_portfolio_value :=0 ;
                v_beginning_value :=0 ;
                v_credits_value  :=0 ;
                v_debits_value :=0 ;
                v_other_activity_value :=0 ;
                v_ending_value :=0 ;
                v_change_in_market_value :=0 ;
                v_net_change_amount :=0 ;
                v_asset_not_held_value :=0 ;
                v_accrued_interest :=0 ;
                v_total_balance :=0 ;

                v_ending_value_ytd :=0 ;
                v_accrued_interest_ytd :=0 ;
                v_total_balance_ytd :=0 ;
                v_change_in_value_rec_count_from_balance_table :=0 ;

                delete from temp_extraction_results_change_in_value;

        -- Getting summary data of change in value from temporary table of summary data
                dynamic_sql := 'SELECT * FROM temp_extraction_results_intermediate_summary temp_summ

                                                         WHERE temp_summ.partyid= $1 AND temp_summ.clientid = $2 AND temp_summ.origin = $3 AND
                                                         temp_summ.currency= $4 AND temp_summ.sectionheader = $5 AND temp_summ.recordtype = $6 ' ;

                FOR db_rec IN EXECUTE dynamic_sql using p_identifier_value, client_data, origin, currency_data, 'Change In Value', 'Detail'
                        LOOP

                        FOR flag_index IN 0..COALESCE(jsonb_array_length(db_rec.data -> 'BalanceFlag'),0)-1  LOOP
                                v_change_in_value_rec_count_from_balance_table = v_change_in_value_rec_count_from_balance_table +1;
                                IF db_rec.data -> 'BalanceFlag' -> flag_index ->> 'BalanceFlagType' = 'IsPTD' THEN
                                        FOR amount_index IN 0..COALESCE(jsonb_array_length(db_rec.data -> 'BalanceAmount'),0)-1  LOOP
                                        /*      IF db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType' = 'PortfolioValue' THEN
                                                        v_portfolio_value := (db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountValue')::numeric;
                                                END IF;
                                                IF db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType' = 'BeginningValue' THEN
                                                        v_beginning_value := (db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountValue')::numeric;
                                                END IF;
                                                IF db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType' = 'CreditsValue' THEN
                                                        v_credits_value := (db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountValue')::numeric;
                                                END IF;
                                                IF db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType' = 'DebitsValue' THEN
                                                        v_debits_value := (db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountValue')::numeric;
                                                END IF;
                                                IF db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType' = 'OtherActivityValue' THEN
                                                        v_other_activity_value := (db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountValue')::numeric;
                                                END IF;
                                        */
                                                IF db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType' = 'EndingValue' THEN
                                                                v_ending_value := (db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountValue')::numeric;
                                                END IF;

                                                --AccruedInterest
                                                IF db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType' = 'AccruedInterest' THEN
                                                                v_accrued_interest := (db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountValue')::numeric;
                                                END IF;
                                        /*
                                                IF db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType' = 'ChangeInMarketValue' THEN
                                                        v_change_in_market_value := (db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountValue')::numeric;
                                                END IF;
                                                IF db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType' = 'NetChangeAmount' THEN
                                                        v_net_change_amount := (db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountValue')::numeric;
                                                END IF;
                                        */
                                        END LOOP;
                                END IF;
                                IF db_rec.data -> 'BalanceFlag' -> flag_index ->> 'BalanceFlagType' = 'IsYTD' THEN
                                        FOR amount_index IN 0..COALESCE(jsonb_array_length(db_rec.data -> 'BalanceAmount'),0)-1  LOOP
                                                IF db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType' = 'EndingValue' THEN
                                                                v_ending_value_ytd := (db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountValue')::numeric;
                                                END IF;

                                                --AccruedInterest
                                                IF db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType' = 'AccruedInterest' THEN
                                                                v_accrued_interest_ytd := (db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountValue')::numeric;
                                                END IF;
                                        END LOOP;
                                END IF;
                        END LOOP;
                END LOOP;

        --      v_change_in_market_value  := v_ending_value - ( v_beginning_value + v_credits_value + v_debits_value + v_other_activity_value );

        --      v_net_change_amount := v_ending_value - v_beginning_value ;

        -- Getting hextone data from temporary table of position tables
                dynamic_sql := 'SELECT * FROM temp_extraction_results_position temp_pos

                                                         WHERE temp_pos.partyid= $1 AND temp_pos.clientid = $2 AND temp_pos.origin = $3 AND
                                                         temp_pos.currency= $4  AND temp_pos.sectionheader = ANY($5) AND        temp_pos.recordtype = $6 ' ;

                FOR db_rec IN EXECUTE dynamic_sql using p_identifier_value, client_data, origin, currency_data, hextone_sections, 'Total'
                        LOOP
                        FOR amount_index IN 0..COALESCE(jsonb_array_length(db_rec.data -> 'PositionAmount'),0)-1  LOOP
                                IF db_rec.data -> 'PositionAmount' -> amount_index ->> 'PositionAmountType' = 'TotalMarketValue' THEN
                                        v_asset_not_held_value := v_asset_not_held_value + (db_rec.data -> 'PositionAmount' -> amount_index ->> 'PositionAmountValue')::numeric;
                                END IF;
                        END LOOP;
                END LOOP;

                v_total_balance := v_ending_value + v_asset_not_held_value + v_accrued_interest;

                v_total_balance_ytd := v_ending_value_ytd + v_asset_not_held_value + v_accrued_interest_ytd;

                IF v_change_in_value_rec_count_from_balance_table > 0 THEN
                -- Prepare data for calculated fields, and insert in temp table. Use this table to create data in json and
                -- insert in main summary temp table( for PTD )
                        insert into temp_extraction_results_change_in_value ( amounttype, amountvalue, narrative, isptd,  recordtype)
                        values
                --      ('ChangeInMarketValue', v_change_in_market_value, 'Change in Market Value', '1', 'Detail'),
                --      ('NetChangeAmount', v_net_change_amount, NULL, '1', 'Detail'),
                        ('AssetsNotHeldValue', v_asset_not_held_value, 'Assets Not Held at '|| location ||'*', '1', 'Detail'),
                        ('TotalBalance', v_total_balance, 'Total', '1', 'Detail');

                -- insert in main summary temp table ( for YTD )
                        insert into temp_extraction_results_change_in_value ( amounttype, amountvalue, narrative, isptd,  recordtype)
                        values
                        ('AssetsNotHeldValue', v_asset_not_held_value, 'Assets Not Held at '|| location ||'*', '0', 'Detail'),
                        ('TotalBalance', v_total_balance_ytd, 'Total', '0', 'Detail');
                END IF;

        --      RAISE NOTICE 'v_change_in_market_value: %', v_change_in_market_value;
                RAISE NOTICE 'v_asset_not_held_value: %', v_asset_not_held_value;
                RAISE NOTICE 'v_total_balance: %', v_total_balance;

                WITH json_row AS (
        SELECT DISTINCT
                                        jsonb_agg( DISTINCT
                                                jsonb_build_object(
                                                        'BalanceIdentifierType', 'Internal',
                                                        'BalanceIdentifierValue', 'Change In Value'
                        )
                    ) AS balanceidentifier_json,

                                        jsonb_agg( DISTINCT
                                                jsonb_build_object(
                                                        'BalanceNarrativeType', 'Description',
                                                        'BalanceNarrativeValue', terciv.narrative,
                                                        'BalanceNarrativeSequence', 1
                                                )
                                        ) AS balancenarrative_json,

                                        jsonb_agg( DISTINCT
                                        jsonb_build_object(
                                                'BalanceAmountType',terciv.amounttype,
                                                'BalanceAmountValue', terciv.amountvalue,
                                                'BalanceAmountCurrency', currency_data
                                                )
                                        ) AS balanceamount_json,

                                        (CASE
                                                WHEN terciv.isptd = '1'
                                                THEN jsonb_agg( DISTINCT
                                                                jsonb_build_object(
                                                                        'BalanceFlagType', 'IsPTD',
                                                                        'BalanceFlagValue', '1'
                                                                )
                                                        )
                                                ELSE jsonb_agg( DISTINCT
                                                                jsonb_build_object(
                                                                        'BalanceFlagType', 'IsYTD',
                                                                        'BalanceFlagValue', '1'
                                                                )
                                                        )
                                        END) AS balanceflag_json,

                                        terciv.recordtype

                FROM    temp_extraction_results_change_in_value terciv
            GROUP BY amounttype, isptd, recordtype
                )

                INSERT INTO temp_extraction_results_intermediate_summary (
                        partyid,
                        clientid,
                        origin,
                        currency,
                        sectiontype,
                        sectionheader,
                        recordtype,
                        data)
                SELECT
                p_identifier_value,
                client_data,
                origin,
                currency_data,
                'Internal',
                'Change In Value',
                jr.recordtype,
                         jsonb_strip_nulls(
                                jsonb_build_object(
                                'BalanceIdentifier', jr.balanceidentifier_json,
                                'BalanceNarrative', jr.balancenarrative_json,
                                'BalanceAmount', jr.balanceamount_json,
                                'BalanceFlag', jr.balanceflag_json
                )
                ) AS combined_json

                FROM json_row jr;

                -- inserting assets not held at client location record
                IF v_asset_not_held_value > 0 THEN
                        INSERT INTO temp_extraction_results_intermediate_summary (
                                partyid,
                                clientid,
                                origin,
                                currency,
                                sectiontype,
                                sectionheader,
                                recordtype,
                                data)
                        VALUES (
                        p_identifier_value,
                        client_data,
                        origin,
                        currency_data,
                        'Internal',
                        'Total Assets Not Held at '|| location,
                        'Total',
                                jsonb_build_object(
                                        'BalanceIdentifier', jsonb_build_array(
                                                jsonb_build_object(
                                                        'BalanceIdentifierType', 'Internal',
                                                        'BalanceIdentifierValue', 'Total Assets Not Held at ' || location
                                                )
                                        ),
                                        'BalanceNarrative', jsonb_build_array(
                                                jsonb_build_object(
                                                        'BalanceNarrativeType', 'Total',
                                                        'BalanceNarrativeSequence', 1,
                                                        'BalanceNarrativeValue','Total'
                                                )
                                        ),
                                        'BalanceAmount', jsonb_build_array(
                                                jsonb_build_object(
                                                        'BalanceAmountType', 'MarketValue',
                                                        'BalanceAmountValue', COALESCE(v_asset_not_held_value, 0),
                                                        'BalanceAmountCurrency', currency_data
                                                )
                                        )
                                )
                        );
                END IF;

        END LOOP;
END;
$BODY$;

ALTER FUNCTION public.extract_summary_details_from_non_balance_for_change_in_value(text, text, text, text, text)
    OWNER TO "DBUser";
