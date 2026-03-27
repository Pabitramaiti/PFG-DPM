-- FUNCTION: public.extract_summary_details_from_semi_balance_currency_conversion(text, text, text, text, text)

-- DROP FUNCTION IF EXISTS public.extract_summary_details_from_semi_balance_currency_conversion(text, text, text, text, text);

CREATE OR REPLACE FUNCTION public.extract_summary_details_from_semi_balance_currency_conversion(
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
    v_CurrentAccountValue TEXT;
    v_CurrentPortfolioValue TEXT;
    v_Currency TEXT;
    v_currency_balanceid BIGINT;
    v_CurrencyRate NUMERIC;
    v_USDollarAccountValue NUMERIC;
    v_USDollarPortfolioValue NUMERIC;

BEGIN

FOR v_CurrentAccountValue, v_Currency IN
    SELECT ba.balanceamountvalue, ba.balanceamountcurrency
    FROM balanceamount ba
    JOIN balance b ON ba.balanceid = b.balanceid
    WHERE ba.balanceamounttype = 'PortfolioValue'
    AND b.balanceorigin = origin
    AND b.balanceclientidentifier = client_data
LOOP
    SELECT amountvalue
    INTO v_CurrentPortfolioValue
    FROM temp_extraction_results_change_in_value
    WHERE amounttype = 'TotalBalance'
    AND isptd = '1';

    SELECT bn.balanceid
    INTO v_currency_balanceid
    FROM balancenarrative bn JOIN
    balance b on bn.balanceid = b.balanceid
    JOIN balanceidentifier bi on bn.balanceid = bi.balanceid
    WHERE bn.balancenarrativevalue = v_Currency
    AND bi.balanceidentifiervalue = 'Currency Conversion Summary'
    AND b.balanceorigin = origin
    AND b.balanceclientidentifier = client_data;

    SELECT bn.balancenarrativevalue::NUMERIC
    INTO v_CurrencyRate
    FROM balancenarrative bn
    JOIN balance b on bn.balanceid = b.balanceid
    JOIN balanceidentifier bi on bn.balanceid = bi.balanceid
    WHERE bn.balanceid = v_currency_balanceid
    AND bn.balancenarrativetype = 'CurrencyRate'
    LIMIT 1;

    v_USDollarAccountValue := v_CurrentAccountValue::NUMERIC * v_CurrencyRate;
    v_USDollarPortfolioValue := v_CurrentPortfolioValue::NUMERIC * v_CurrencyRate;

--. Detail rows json
WITH detail_row AS (
    SELECT DISTINCT
        b.balanceid,

        (CASE
                WHEN bi.balanceid IS NOT NULL
                THEN jsonb_agg(
                        DISTINCT jsonb_build_object(
                                'BalanceIdentifierType', bi.balanceidentifiertype,
                                'BalanceIdentifierValue', bi.balanceidentifiervalue
                                )
                        )
        END )   AS balanceidentifier_json,

        (CASE
          WHEN bnarr.balanceid IS NOT NULL
          THEN jsonb_agg(
                                DISTINCT jsonb_build_object(
                    'BalanceNarrativeType', bnarr.balancenarrativetype,
                    'BalanceNarrativeValue', bnarr.balancenarrativevalue,
                    'BalanceNarrativeSequence', bnarr.balancenarrativesequence
                        ))
        END )   AS balancenarrative_json,

        (
    jsonb_strip_nulls(
        jsonb_build_array(
            CASE
                WHEN v_CurrentAccountValue IS NOT NULL THEN
                    jsonb_build_object(
                        'BalanceAmountType', 'CurrentAccountValue',
                        'BalanceAmountValue', v_CurrentAccountValue
                    )
            END,
            CASE
                WHEN v_CurrentPortfolioValue IS NOT NULL THEN
                    jsonb_build_object(
                        'BalanceAmountType', 'CurrentPortfolioValue',
                        'BalanceAmountValue', v_CurrentPortfolioValue
                    )
            END,
            CASE
                WHEN v_CurrentAccountValue IS NOT NULL THEN
                    jsonb_build_object(
                        'BalanceAmountType', 'USDollarAccountValue',
                        'BalanceAmountValue', v_USDollarAccountValue
                    )
            END,
            CASE
                WHEN v_CurrentAccountValue IS NOT NULL THEN
                    jsonb_build_object(
                        'BalanceAmountType', 'USDollarPortfolioValue',
                        'BalanceAmountValue', v_USDollarPortfolioValue
                    )
            END
        )
    )
    ) AS balanceamount_json,

        (CASE
                WHEN bf.balanceid IS NOT NULL
                THEN jsonb_agg(
                                DISTINCT jsonb_build_object(
                                        'BalanceFlagType', bf.balanceflagtype,
                                        'BalanceFlagValue', CASE
                                                                                        WHEN bf.balanceflagvalue = TRUE THEN '1'
                                                                                        WHEN bf.balanceflagvalue = FALSE THEN '0'
                                                                                END
                                        )
                                )
        END )   AS balanceflag_json

        FROM balance b
        JOIN balanceidentifier bi ON b.balanceid = bi.balanceid
        AND balanceidentifiertype = sectiontype
        AND balanceidentifiervalue = sectionheader

        JOIN balanceparty bp ON b.balanceid = bp.balanceid
        JOIN balancenarrative bnarr ON b.balanceid = bnarr.balanceid
        LEFT JOIN balanceflag bf ON b.balanceid = bf.balanceid

        WHERE bp.balancepartyidentifiervalue = p_identifier_value
        AND b.balanceclientidentifier = client_data
        AND b.balanceorigin = origin
        AND bnarr.balanceid = v_currency_balanceid
        GROUP BY b.balanceid,
        bi.balanceid,
        bnarr.balanceid,
        bf.balanceid,
        bi.balanceidentifiervalue
    )

-- Inserting all json segments in temp table
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
        v_Currency,
    sectiontype,
    sectionheader,
    'Detail' AS recordtype,
        jsonb_strip_nulls(
                jsonb_build_object(
                        'BalanceId', dr.balanceid,
                        'BalanceIdentifier', dr.balanceidentifier_json,
                        'BalanceNarrative', jsonb_path_query_array(dr.balancenarrative_json,'$[*] ? (@ != null)'),
                        'BalanceAmount', dr.balanceamount_json,
                        'BalanceFlag', dr.balanceflag_json
                )
        ) AS combined_json
FROM detail_row dr;

END LOOP;

END;
$BODY$;

ALTER FUNCTION public.extract_summary_details_from_semi_balance_currency_conversion(text, text, text, text, text)
    OWNER TO "DBUser";