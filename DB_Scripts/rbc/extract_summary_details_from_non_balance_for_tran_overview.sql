-- FUNCTION: public.extract_summary_details_from_non_balance_for_tran_overview(text, text, text, text, text)

-- DROP FUNCTION IF EXISTS public.extract_summary_details_from_non_balance_for_tran_overview(text, text, text, text, text);

CREATE OR REPLACE FUNCTION public.extract_summary_details_from_non_balance_for_tran_overview(
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
                all_sections text[];

                currency_list TEXT[];

                balanceidentifier_json jsonb;
                balancenarrative_json jsonb;
                balanceamount_json jsonb;

                currency_data TEXT;

                temp_amnt numeric;

                v_credits_value  numeric :=0 ;
                v_debits_value numeric :=0 ;

                v_sales_and_redemptions  numeric :=0 ;
                v_income_and_distributions   numeric :=0 ;
                v_funds_received  numeric :=0 ;
                v_securities_received  numeric :=0 ;

                v_purchases   numeric :=0 ;
                v_reinvestments   numeric :=0 ;
                v_fees_and_other  numeric :=0 ;
                v_securities_delivered   numeric :=0 ;

BEGIN
        DROP TABLE IF EXISTS temp_extraction_results_transaction_overview;
    CREATE TEMP TABLE temp_extraction_results_transaction_overview (
        amounttype TEXT,
                amountvalue TEXT,
                narrative TEXT,
                recordtype TEXT
    )ON COMMIT DROP;

        all_sections := ARRAY['Securities Activity', 'Income and Reinvestments','Funds Paid and Received', 'Securities Received and Delivered'];

        SELECT ARRAY(
                SELECT DISTINCT currency
                FROM temp_extraction_results_transaction terp
                WHERE terp.clientid = client_data
        ) INTO currency_list;

        RAISE NOTICE 'Currency list: %', currency_list;

        -- Loop through each currency and build  rows for that currency
    FOREACH currency_data IN ARRAY currency_list LOOP

                temp_amnt :=0 ;

                v_credits_value   :=0 ;
                v_debits_value  :=0 ;

                v_sales_and_redemptions   :=0 ;
                v_income_and_distributions    :=0 ;
                v_funds_received   :=0 ;
                v_securities_received   :=0 ;

                v_purchases    :=0 ;
                v_reinvestments    :=0 ;
                v_fees_and_other   :=0 ;
                v_securities_delivered    :=0 ;

                delete from temp_extraction_results_transaction_overview;

        -- Getting realized data from Transactions
                dynamic_sql := 'SELECT * FROM temp_extraction_results_transaction temp_tran

                                                         WHERE temp_tran.partyid= $1 AND temp_tran.clientid = $2 AND temp_tran.origin = $3 AND
                                                         temp_tran.currency= $4 AND temp_tran.sectionheader = ANY($5) AND       temp_tran.recordtype = $6 ' ;

                FOR db_rec IN EXECUTE dynamic_sql using p_identifier_value, client_data, origin, currency_data, all_sections, 'Total'
                        LOOP

                        FOR amount_index IN 0..COALESCE(jsonb_array_length(db_rec.data -> 'TransactionAmount'),0)-1  LOOP
                                IF db_rec.data -> 'TransactionAmount' -> amount_index ->> 'TransactionAmountType' = 'TotalCredit' THEN

                                        temp_amnt := (db_rec.data -> 'TransactionAmount' -> amount_index ->> 'TransactionAmountValue')::numeric;
                                        v_credits_value := v_credits_value + temp_amnt;

                                        IF db_rec.sectionheader = 'Securities Activity' THEN
                                                v_sales_and_redemptions := temp_amnt;
                                        END IF;
                                        IF db_rec.sectionheader = 'Income and Reinvestments' THEN
                                                v_income_and_distributions := temp_amnt;
                                        END IF;

                                        IF db_rec.sectionheader = 'Funds Paid and Received' THEN
                                                v_funds_received := temp_amnt;
                                        END IF;

                                        IF db_rec.sectionheader = 'Securities Received and Delivered' THEN
                                                v_securities_received :=  temp_amnt;
                                        END IF;

                                END IF;
                                IF db_rec.data -> 'TransactionAmount' -> amount_index ->> 'TransactionAmountType' = 'TotalDebit' THEN

                                        temp_amnt := (db_rec.data -> 'TransactionAmount' -> amount_index ->> 'TransactionAmountValue')::numeric;
                                        v_debits_value := v_debits_value + temp_amnt;

                                        IF db_rec.sectionheader = 'Securities Activity' THEN
                                                v_purchases := temp_amnt;
                                        END IF;
                                        IF db_rec.sectionheader = 'Income and Reinvestments' THEN
                                                v_reinvestments := temp_amnt;
                                        END IF;

                                        IF db_rec.sectionheader = 'Funds Paid and Received' THEN
                                                v_fees_and_other := temp_amnt;
                                        END IF;

                                        IF db_rec.sectionheader = 'Securities Received and Delivered' THEN
                                                v_securities_delivered :=  temp_amnt;
                                        END IF;

                                END IF;

                        END LOOP;
                END LOOP;

                insert into temp_extraction_results_transaction_overview ( amounttype, amountvalue, narrative, recordtype )
                values
                ('CreditsValue', v_credits_value, 'Credits', 'Total'),
                ('DebitsValue' , v_debits_value , 'Debits' , 'Total'),

                ('SalesAndRedemptions', v_sales_and_redemptions, 'Sales & Redemptions', 'Detail'),
                ('IncomeAndDistributions', v_income_and_distributions, 'Income & Distributions', 'Detail'),
                ('FundsReceived', v_funds_received, 'Funds Received', 'Detail'),
                ('SecuritiesReceived', v_securities_received, 'Securities Received', 'Detail'),

                ('Purchases', v_purchases, 'Purchases', 'Detail'),
                ('Reinvestments', v_reinvestments, 'Reinvestments', 'Detail'),
                ('FeesAndOther', v_fees_and_other, 'Fees & Other', 'Detail'),
                ('SecuritiesDelivered', v_securities_delivered, 'Securities Delivered', 'Detail');

        --      RAISE NOTICE 'CreditsValue: %', v_credits_value;
        --      RAISE NOTICE 'DebitsValue: %', v_debits_value;
        --      RAISE NOTICE 'SalesAndRedemptions: %', v_sales_and_redemptions;
        --      RAISE NOTICE 'FeesAndOther: %', v_fees_and_other;

                WITH json_row AS (
        SELECT DISTINCT
                                        jsonb_agg( DISTINCT
                                                jsonb_build_object(
                                                        'BalanceIdentifierType', 'Internal',
                                                        'BalanceIdentifierValue', 'Transaction Overview'
                        )
                    ) AS balanceidentifier_json,

                                        jsonb_agg( DISTINCT
                                                jsonb_build_object(
                                                        'BalanceNarrativeType', 'Description',
                                                        'BalanceNarrativeValue', terto.narrative,
                                                        'BalanceNarrativeSequence', 1
                                                )
                                        ) AS balancenarrative_json,

                                        jsonb_agg( DISTINCT
                                        jsonb_build_object(
                                                'BalanceAmountType',terto.amounttype,
                                                'BalanceAmountValue', terto.amountvalue,
                                                'BalanceAmountCurrency', currency_data
                                                )
                                        ) AS balanceamount_json,

                                        terto.recordtype

                FROM    temp_extraction_results_transaction_overview terto
            GROUP BY amounttype, recordtype
                )

                INSERT INTO temp_extraction_results_intermediate_summary(
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
                'Transaction Overview',
                jr.recordtype,
                         jsonb_strip_nulls(
                                jsonb_build_object(
                                'BalanceIdentifier', jr.balanceidentifier_json,
                                'BalanceNarrative', jr.balancenarrative_json,
                                'BalanceAmount', jr.balanceamount_json
                )
                ) AS combined_json

                FROM json_row jr;

        END LOOP;

END;
$BODY$;

ALTER FUNCTION public.extract_summary_details_from_non_balance_for_tran_overview(text, text, text, text, text)
    OWNER TO "DBUser";
