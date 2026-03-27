-- FUNCTION: public.extract_summary_details_from_non_balance_for_gainloss(text, text, text, text, text)

-- DROP FUNCTION IF EXISTS public.extract_summary_details_from_non_balance_for_gainloss(text, text, text, text, text);

CREATE OR REPLACE FUNCTION public.extract_summary_details_from_non_balance_for_gainloss(
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

                balanceidentifier_json jsonb;
                balancenarrative_json jsonb;
                balanceamount_json jsonb;
                balanceflag_json jsonb;

                is_costbasis_present boolean := false;

                currency_data TEXT;

                -- new variables
                v_realized_ptd_short_term_gain numeric :=0 ;
                v_realized_ptd_short_term_loss numeric :=0 ;
                v_realized_ptd_short_term_gain_or_loss numeric  :=0 ;
                v_realized_ptd_long_term_gain numeric :=0 ;
                v_realized_ptd_long_term_loss numeric :=0 ;
                v_realized_ptd_long_term_gain_or_loss numeric :=0 ;
                v_realized_ptd_total_gain_or_loss numeric :=0 ;

                v_realized_ytd_short_term_gain numeric :=0 ;
                v_realized_ytd_short_term_loss numeric :=0 ;
                v_realized_ytd_short_term_gain_or_loss numeric :=0 ;
                v_realized_ytd_long_term_gain numeric :=0 ;
                v_realized_ytd_long_term_loss numeric :=0 ;
                v_realized_ytd_long_term_gain_or_loss numeric :=0 ;
                v_realized_ytd_total_gain_or_loss numeric :=0 ;

                v_unrealized_ytd_short_term_gain numeric :=0 ;
                v_unrealized_ytd_short_term_loss numeric :=0 ;
                v_unrealized_ytd_short_term_gain_or_loss numeric :=0 ;
                v_unrealized_ytd_long_term_gain numeric :=0 ;
                v_unrealized_ytd_long_term_loss numeric :=0 ;
                v_unrealized_ytd_long_term_gain_or_loss numeric :=0 ;
                v_unrealized_ytd_total_gain_or_loss numeric :=0 ;

                temp_amnt numeric;

                v_amounttype text :='';
                v_amountvalue text :='';
                v_ptd_ytd_flag_type text :='';
                v_ptd_ytd_flag_value text :='';
                v_realized_flag_type text :='';
                v_realized_flag_value text :='';

BEGIN

        DROP TABLE IF EXISTS temp_extraction_results_gain_loss;
    CREATE TEMP TABLE temp_extraction_results_gain_loss (
        amounttype TEXT,
                amountvalue TEXT,
                narrative TEXT,
        isptd TEXT,
        isytd TEXT,
        isrealized TEXT,
                recordtype TEXT,
                isused TEXT
    )ON COMMIT DROP;

        SELECT ARRAY(
                SELECT DISTINCT currency
                FROM temp_extraction_results_transaction terp
                WHERE terp.clientid = client_data
        ) INTO currency_list;

        RAISE NOTICE 'Currency list: %', currency_list;

        -- Loop through each currency and build detail rows for that currency
    FOREACH currency_data IN ARRAY currency_list LOOP

                v_realized_ptd_short_term_gain  :=0 ;
                v_realized_ptd_short_term_loss  :=0 ;
                v_realized_ptd_short_term_gain_or_loss   :=0 ;
                v_realized_ptd_long_term_gain  :=0 ;
                v_realized_ptd_long_term_loss  :=0 ;
                v_realized_ptd_long_term_gain_or_loss  :=0 ;
                v_realized_ptd_total_gain_or_loss  :=0 ;

                v_realized_ytd_short_term_gain  :=0 ;
                v_realized_ytd_short_term_loss  :=0 ;
                v_realized_ytd_short_term_gain_or_loss  :=0 ;
                v_realized_ytd_long_term_gain  :=0 ;
                v_realized_ytd_long_term_loss  :=0 ;
                v_realized_ytd_long_term_gain_or_loss  :=0 ;
                v_realized_ytd_total_gain_or_loss  :=0 ;

                v_unrealized_ytd_short_term_gain  :=0 ;
                v_unrealized_ytd_short_term_loss  :=0 ;
                v_unrealized_ytd_short_term_gain_or_loss  :=0 ;
                v_unrealized_ytd_long_term_gain  :=0 ;
                v_unrealized_ytd_long_term_loss  :=0 ;
                v_unrealized_ytd_long_term_gain_or_loss  :=0 ;
                v_unrealized_ytd_total_gain_or_loss  :=0 ;

                v_amounttype  :='';
                v_amountvalue  :='';
                v_ptd_ytd_flag_type  :='';
                v_ptd_ytd_flag_value  :='';
                v_realized_flag_type  :='';
                v_realized_flag_value  :='';

                temp_amnt :=0;

                delete from temp_extraction_results_gain_loss;

                is_costbasis_present := false;

        -- Getting realized data from Transactions
                dynamic_sql := 'SELECT * FROM temp_extraction_results_transaction temp_tran

                                                         WHERE temp_tran.partyid= $1 AND temp_tran.clientid = $2 AND temp_tran.origin = $3 AND
                                                         temp_tran.currency= $4 AND temp_tran.sectionheader = $5 AND    temp_tran.recordtype = $6 ' ;

                FOR db_rec IN EXECUTE dynamic_sql using p_identifier_value, client_data, origin, currency_data, 'Realized Gain/Loss Detail', 'Detail'
                        LOOP

                        FOR amount_index IN 0..COALESCE(jsonb_array_length(db_rec.data -> 'TransactionAmount'),0)-1  LOOP
                                IF db_rec.data -> 'TransactionAmount' -> amount_index ->> 'TransactionAmountType' = 'ShortTermGainOrLoss' THEN
                                        temp_amnt := (db_rec.data -> 'TransactionAmount' -> amount_index ->> 'TransactionAmountValue')::numeric;
                                        IF      temp_amnt >= 0 THEN
                                                v_realized_ptd_short_term_gain := v_realized_ptd_short_term_gain + temp_amnt;
                                        ELSE
                                                v_realized_ptd_short_term_loss := v_realized_ptd_short_term_loss + temp_amnt;
                                        END IF;
                                        v_realized_ptd_short_term_gain_or_loss := v_realized_ptd_short_term_gain_or_loss + temp_amnt;
                                        v_realized_ptd_total_gain_or_loss       := v_realized_ptd_total_gain_or_loss + temp_amnt;
                                END IF;
                                IF db_rec.data -> 'TransactionAmount' -> amount_index ->> 'TransactionAmountType' = 'LongTermGainOrLoss' THEN
                                        temp_amnt := (db_rec.data -> 'TransactionAmount' -> amount_index ->> 'TransactionAmountValue');
                                        IF      temp_amnt >= 0 THEN
                                                v_realized_ptd_long_term_gain := v_realized_ptd_long_term_gain + temp_amnt;
                                        ELSE
                                                v_realized_ptd_long_term_loss := v_realized_ptd_long_term_loss + temp_amnt;
                                        END IF;
                                        v_realized_ptd_long_term_gain_or_loss := v_realized_ptd_long_term_gain_or_loss + temp_amnt;
                                        v_realized_ptd_total_gain_or_loss       := v_realized_ptd_total_gain_or_loss + temp_amnt;
                                END IF;
                        END LOOP;
                END LOOP;

                --   Put code here for reading from Cost Basis load and overwriting the above variables

                        dynamic_sql := 'SELECT DISTINCT
                        b.balanceid,
                        --ba.balanceamountcurrency AS balanceamountcurrency,
                        (CASE
                                WHEN ba.balanceid IS NOT NULL
                                THEN jsonb_agg(
                                                DISTINCT jsonb_build_object(
                                                        ''BalanceAmountType'', ba.balanceamounttype,
                                                        ''BalanceAmountValue'', ba.balanceamountvalue,
                                                        ''BalanceAmountCurrency'', ba.balanceamountcurrency
                                                        )
                                                )
                        END )   AS balanceamount_json,
                        (CASE
                                WHEN bf.balanceid IS NOT NULL
                                THEN jsonb_agg(
                                                DISTINCT jsonb_build_object(
                                                        ''BalanceFlagType'', bf.balanceflagtype,
                                                        ''BalanceFlagValue'', CASE
                                                                                                                WHEN bf.balanceflagvalue = TRUE THEN ''1''
                                                                                                                WHEN bf.balanceflagvalue = FALSE THEN ''0''
                                                                                                  END
                                                        )
                                                )
                        END )   AS balanceflag_json

                        FROM balance b
                        JOIN balanceidentifier bi ON b.balanceid = bi.balanceid
                        AND balanceidentifiertype = ''Internal''
                        AND balanceidentifiervalue = ''GainLoss Summary''

                        JOIN balanceparty bp ON b.balanceid = bp.balanceid
                        LEFT JOIN balanceamount ba ON  b.balanceid = ba.balanceid
                        LEFT JOIN balanceflag bf ON b.balanceid = bf.balanceid
                        WHERE bp.balancepartyidentifiervalue = $1
                        AND b.balanceclientidentifier = $2
                        AND b.balanceorigin = $3
                        AND ba.balanceamountcurrency = $4
                        GROUP BY b.balanceid,
                        bi.balanceid,
                        --ba.balanceamountcurrency,
                        ba.balanceid,
                        bf.balanceid';

                FOR db_rec IN EXECUTE dynamic_sql using p_identifier_value, client_data, origin, currency_data
                        LOOP

                        v_amounttype :='';
                        v_amountvalue :='';
                        v_ptd_ytd_flag_type :='';
                        v_ptd_ytd_flag_value :='';
                        v_realized_flag_type :='';
                        v_realized_flag_value :='';

                        FOR amount_index IN 0..COALESCE(jsonb_array_length(db_rec.balanceamount_json ),0)-1  LOOP
                                IF (db_rec.balanceamount_json -> amount_index ->> 'BalanceAmountType') IN ('ShortTermGainOrLoss','LongTermGainOrLoss',
                                                                                                                                                                                        'TotalRealizedProfitLoss') THEN

                                        v_amounttype  := db_rec.balanceamount_json -> amount_index ->> 'BalanceAmountType';
                                        v_amountvalue := db_rec.balanceamount_json -> amount_index ->> 'BalanceAmountValue';
                                END IF;
                        END LOOP;

                        FOR flag_index IN 0..COALESCE(jsonb_array_length(db_rec.balanceflag_json ),0)-1  LOOP
                                IF (db_rec.balanceflag_json -> flag_index ->> 'BalanceFlagType') IN ('IsPTD','IsYTD') THEN

                                        v_ptd_ytd_flag_type  := db_rec.balanceflag_json -> flag_index ->> 'BalanceFlagType';
                                        v_ptd_ytd_flag_value := db_rec.balanceflag_json -> flag_index ->> 'BalanceFlagValue';
                                END IF;
                                IF (db_rec.balanceflag_json -> flag_index ->> 'BalanceFlagType') IN ('IsRealized') THEN

                                        v_realized_flag_type  := db_rec.balanceflag_json -> flag_index ->> 'BalanceFlagType';
                                        v_realized_flag_value := db_rec.balanceflag_json -> flag_index ->> 'BalanceFlagValue';
                                END IF;
                        END LOOP;

                        IF ( v_amounttype = 'ShortTermGainOrLoss' AND v_ptd_ytd_flag_type = 'IsPTD' AND v_ptd_ytd_flag_value = '1'
                                AND v_realized_flag_type = 'IsRealized' AND v_realized_flag_value = '1') THEN

                                v_realized_ptd_short_term_gain_or_loss := v_amountvalue;
                        END IF;

                        IF ( v_amounttype = 'LongTermGainOrLoss' AND v_ptd_ytd_flag_type = 'IsPTD' AND v_ptd_ytd_flag_value = '1'
                                AND v_realized_flag_type = 'IsRealized' AND v_realized_flag_value = '1') THEN

                                v_realized_ptd_long_term_gain_or_loss := v_amountvalue;
                        END IF;

                        IF ( v_amounttype = 'TotalRealizedProfitLoss' AND v_ptd_ytd_flag_type = 'IsPTD' AND v_ptd_ytd_flag_value = '1'
                                AND v_realized_flag_type = 'IsRealized' AND v_realized_flag_value = '1') THEN

                                v_realized_ptd_total_gain_or_loss := v_amountvalue;
                        END IF;

                        IF ( v_amounttype = 'ShortTermGainOrLoss' AND v_ptd_ytd_flag_type = 'IsYTD' AND v_ptd_ytd_flag_value = '1'
                                AND v_realized_flag_type = 'IsRealized' AND v_realized_flag_value = '1') THEN

                                v_realized_ytd_short_term_gain_or_loss := v_amountvalue;
                        END IF;

                        IF ( v_amounttype = 'LongTermGainOrLoss' AND v_ptd_ytd_flag_type = 'IsYTD' AND v_ptd_ytd_flag_value = '1'
                                AND v_realized_flag_type = 'IsRealized' AND v_realized_flag_value = '1') THEN

                                v_realized_ytd_long_term_gain_or_loss := v_amountvalue;
                        END IF;

                        IF ( v_amounttype = 'TotalRealizedProfitLoss' AND v_ptd_ytd_flag_type = 'IsYTD' AND v_ptd_ytd_flag_value = '1'
                                AND v_realized_flag_type = 'IsRealized' AND v_realized_flag_value = '1') THEN

                                v_realized_ytd_total_gain_or_loss := v_amountvalue;
                        END IF;

                        IF is_costbasis_present = FALSE THEN
                           is_costbasis_present = TRUE;
                        END IF;

                END LOOP;

                -- Getting unrealized data from Positions
                dynamic_sql := 'SELECT * FROM temp_extraction_results_position temp_pos

                                                         WHERE temp_pos.partyid= $1 AND temp_pos.clientid = $2 AND temp_pos.origin = $3 AND
                                                         temp_pos.currency= $4  AND     temp_pos.recordtype = $5 ' ;

                FOR db_rec IN EXECUTE dynamic_sql using p_identifier_value, client_data, origin, currency_data, 'Total'
                        LOOP

                        FOR amount_index IN 0..COALESCE(jsonb_array_length(db_rec.data -> 'PositionAmount'),0)-1  LOOP
                                IF db_rec.data -> 'PositionAmount' -> amount_index ->> 'PositionAmountType' = 'TotalUnrealizedGainLoss' THEN

                                        temp_amnt := (db_rec.data -> 'PositionAmount' -> amount_index ->> 'PositionAmountValue')::numeric;
                                        v_unrealized_ytd_total_gain_or_loss := v_unrealized_ytd_total_gain_or_loss + temp_amnt;

                                END IF;
                        END LOOP;
                END LOOP;

                IF is_costbasis_present = TRUE THEN
                        insert into temp_extraction_results_gain_loss ( amounttype, amountvalue, narrative, isptd, isrealized, recordtype, isused)
                        values
                        ('ShortTermGain', v_realized_ptd_short_term_gain, 'Short Term Gain',    '1', '1', 'Detail', 'Y'),
                --      ('ShortTermGain', v_realized_ytd_short_term_gain, 'Short Term Gain',    '0', '1', 'Detail', 'N'),
                --      ('ShortTermGain', v_unrealized_ytd_short_term_gain, 'Short Term Gain','0', '0', 'Detail', 'Y'),

                        ('ShortTermLoss', v_realized_ptd_short_term_loss, 'Short Term Loss',    '1', '1', 'Detail', 'Y'),
                --      ('ShortTermLoss', v_realized_ytd_short_term_loss, 'Short Term Loss',    '0', '1', 'Detail', 'N'),
                --      ('ShortTermLoss', v_unrealized_ytd_short_term_loss, 'Short Term Loss','0', '0', 'Detail', 'Y'),

                        ('ShortTermGainOrLoss', v_realized_ptd_short_term_gain_or_loss, 'Net Short Term Gain/Loss',     '1', '1', 'Detail', 'Y'),
                        ('ShortTermGainOrLoss', v_realized_ytd_short_term_gain_or_loss, 'Net Short Term Gain/Loss',     '0', '1', 'Detail', 'Y'),
                --      ('ShortTermGainOrLoss', v_unrealized_ytd_short_term_gain_or_loss, 'Net Short Term Gain/Loss', '0', '0', 'Detail', 'Y'),

                        ('LongTermGain', v_realized_ptd_long_term_gain, 'Long Term Gain',       '1', '1', 'Detail', 'Y'),
                --      ('LongTermGain', v_realized_ytd_long_term_gain, 'Long Term Gain',       '0', '1', 'Detail', 'N'),
                --      ('LongTermGain', v_unrealized_ytd_long_term_gain, 'Long Term Gain',     '0', '0', 'Detail', 'Y'),

                        ('LongTermLoss', v_realized_ptd_long_term_loss, 'Long Term Loss',       '1', '1', 'Detail', 'Y'),
                --      ('LongTermLoss', v_realized_ytd_long_term_loss, 'Long Term Loss',       '0', '1', 'Detail', 'N'),
                --      ('LongTermLoss', v_unrealized_ytd_long_term_loss, 'Long Term Loss',     '0', '0', 'Detail', 'Y'),

                        ('LongTermGainOrLoss', v_realized_ptd_long_term_gain_or_loss, 'Net Long Term Gain/Loss',        '1', '1', 'Detail', 'Y'),
                        ('LongTermGainOrLoss', v_realized_ytd_long_term_gain_or_loss, 'Net Long Term Gain/Loss',        '0', '1', 'Detail', 'Y'),
                --      ('LongTermGainOrLoss', v_unrealized_ytd_long_term_gain_or_loss, 'Net Long Term Gain/Loss','0', '0', 'Detail', 'Y'),

                        ('TotalRealizedProfitLoss', v_realized_ptd_total_gain_or_loss, 'Total Realized PTD',            '1', '1', 'Total', 'Y'),
                        ('TotalRealizedProfitLoss', v_realized_ytd_total_gain_or_loss, 'Total Realized YTD',            '0', '1', 'Total', 'Y'),
                -- Changed the below as temp fix for WIF Core, until it is decided that it should be profit or gain at all ends
                --      ('TotalUnrealizedProfitLoss', v_unrealized_ytd_total_gain_or_loss, 'Total Unrealized YTD',      '0', '0', 'Total', 'Y');
                        ('TotalUnrealizedGainLoss', v_unrealized_ytd_total_gain_or_loss, 'Total Unrealized YTD',        '0', '0', 'Total', 'Y');

                END IF;

                WITH json_row AS (
        SELECT DISTINCT
                                        jsonb_agg( DISTINCT
                                                jsonb_build_object(
                                                        'BalanceIdentifierType', 'Internal',
                                                        'BalanceIdentifierValue', 'GainLoss Summary'
                        )
                    ) AS balanceidentifier_json,

                                        jsonb_agg( DISTINCT
                                                jsonb_build_object(
                                                        'BalanceNarrativeType', 'Description',
                                                        'BalanceNarrativeValue', tergl.narrative,
                                                        'BalanceNarrativeSequence', 1
                                                )
                                        ) AS balancenarrative_json,

                                        jsonb_agg( DISTINCT
                                        jsonb_build_object(
                                                'BalanceAmountType',tergl.amounttype,
                                                'BalanceAmountValue', tergl.amountvalue,
                                                'BalanceAmountCurrency', currency_data
                                                )
                                        ) AS balanceamount_json,

                                        (
                                        (CASE
                                                WHEN tergl.isptd = '1'
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
                                        END) ||
                                                jsonb_agg( DISTINCT
                                                        jsonb_build_object(
                                                                'BalanceFlagType', 'IsRealized',
                                                                'BalanceFlagValue', tergl.isrealized
                                                        )
                                                )

                                        )AS balanceflag_json,

                                        tergl.recordtype

                FROM    temp_extraction_results_gain_loss tergl
                WHERE   isused != 'N'
            GROUP BY amounttype, isptd, isrealized, recordtype
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
                'GainLoss Summary',
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

        END LOOP;

END;
$BODY$;

ALTER FUNCTION public.extract_summary_details_from_non_balance_for_gainloss(text, text, text, text, text)
    OWNER TO "DBUser";
