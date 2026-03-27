-- FUNCTION: public.extract_balance_details(text, text, text, text, text)

-- DROP FUNCTION IF EXISTS public.extract_balance_details(text, text, text, text, text);

CREATE OR REPLACE FUNCTION public.extract_balance_details(
	p_identifier_value text,
	clientid text,
	origin text,
	balance_identifier_type text,
	balance_identifier_value text)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
BEGIN

--1a. Detail rows json
WITH detail_row AS (
    SELECT DISTINCT
        b.balanceid,
        ba.balanceamountcurrency AS balanceamountcurrency,

        (CASE
                WHEN bnarr.balanceid IS NOT NULL
                THEN jsonb_agg(
                                DISTINCT jsonb_build_object(
                                        'BalanceNarrativeType', bnarr.balancenarrativetype,
                                        'BalanceNarrativeValue', bnarr.balancenarrativevalue,
                                        'BalanceNarrativeSequence', bnarr.balancenarrativesequence
                                        )
                                )
        END )   AS balancenarrative_json,

        (CASE WHEN bd.balanceid IS NOT NULL THEN
                jsonb_agg(DISTINCT jsonb_build_object(
                        'BalanceDateType',  bd.balancedatetype,
                        'BalanceDateValue', bd.balancedatevalue
                ))
        END) AS balancedate_json,

        (CASE WHEN bp.balanceid IS NOT NULL THEN
                jsonb_agg(DISTINCT jsonb_build_object(
                        'BalancePartyIdentifierType',  bp.balancepartyidentifiertype,
                        'BalancePartyIdentifierValue', bp.balancepartyidentifiervalue
                ))
        END) AS balanceparty_json,

        (CASE
                WHEN ba.balanceid IS NOT NULL
                THEN jsonb_agg(
                                DISTINCT jsonb_build_object(
                                        'BalanceAmountType', ba.balanceamounttype,
                                        'BalanceAmountValue', ba.balanceamountvalue,
                                        'BalanceAmountCurrency', ba.balanceamountcurrency
                                        )
                                )
        END )   AS balanceamount_json,

        (CASE
                WHEN bf.balanceid IS NOT NULL
                THEN jsonb_agg(
                                DISTINCT jsonb_build_object(
                                        'BalanceFlagType', bf.balanceflagtype,
                                        'BalanceFlagValue',  CASE
                                                                                        WHEN bf.balanceflagvalue = TRUE  THEN '1'
                                                                                        WHEN bf.balanceflagvalue = FALSE THEN '0'
                                                                                END
                                        )
                                )
        END )   AS balanceflag_json

        FROM balance b
        JOIN balanceidentifier bi ON b.balanceid=bi.balanceid
        AND balanceidentifiertype = balance_identifier_type
        AND balanceidentifiervalue = balance_identifier_value  --'Cash and Cash Equivalents'

        JOIN balanceparty bp ON b.balanceid=bp.balanceid
        LEFT JOIN balancenarrative bnarr ON b.balanceid=bnarr.balanceid
        LEFT JOIN balancedate bd ON b.balanceid = bd.balanceid
        LEFT JOIN balanceamount ba ON  b.balanceid=ba.balanceid
        LEFT JOIN balanceflag bf ON b.balanceid=bf.balanceid

        WHERE bp.balancepartyidentifiervalue = p_identifier_value
        AND b.balanceclientidentifier = clientid
        AND b.balanceorigin = origin
        GROUP BY b.balanceid, balanceamountcurrency , bi.balanceid, bnarr.balanceid, ba.balanceid, bf.balanceid,
        bd.balanceid ,bp.balanceid
    ),

--1b) It will fetch all BalanceIdentifiers whose balanceId from above query result matches with balanceidentifier table.
--    BalanceIdentifier table can have multiple balanceidentifiertypes for one balanceid.
detail_row_identifier AS (
    SELECT DISTINCT
        dr.balanceid,
        dr.balanceamountcurrency AS balanceamountcurrency,
        (CASE
                WHEN bi.balanceid IS NOT NULL
                THEN jsonb_agg(
                        DISTINCT jsonb_build_object(
                                'BalanceIdentifierType', bi.balanceidentifiertype,
                                'BalanceIdentifierValue', bi.balanceidentifiervalue
                                )
                        )
        END )   AS balanceidentifier_json

        FROM detail_row dr
        JOIN balanceidentifier bi ON dr.balanceid=bi.balanceid
        GROUP BY dr.balanceid, dr.balanceamountcurrency, bi.balanceid
    ),

-- 2a. Aggregate of TotalBalance, if isGroup = TRUE
total_data_amount AS (
        SELECT
        ba.balanceamountcurrency AS balanceamountcurrency,
        MAX(bi.balanceidentifiertype) AS balanceidentifiertype,
        MAX(bi.balanceidentifiervalue) AS balanceidentifiervalue,

        -- Total Balance
        SUM(
                CASE
                        WHEN ba.balanceamounttype = 'TotalBalance' AND bf.balanceflagtype = 'IsGroup' AND bf.balanceflagvalue = TRUE
                        THEN ba.balanceamountvalue
                END
        ) AS TotalCashAndCashEquivalents
        ,
        -- Total Balance PTD
        SUM(
                CASE
                        WHEN ba.balanceamounttype IN( 'DividendTaxable', 'DividendNonTaxable', 'InterestTaxable', 'InterestNonTaxable')
                                AND bf.balanceflagtype = 'IsPTD' AND bf.balanceflagvalue = TRUE
                        THEN ba.balanceamountvalue
                END
        ) AS TotalBalancePTD
        ,
        -- Total Balance YTD
        SUM(
                CASE
                        WHEN ba.balanceamounttype IN( 'DividendTaxable', 'DividendNonTaxable', 'InterestTaxable', 'InterestNonTaxable')
                                AND bf.balanceflagtype = 'IsYTD' AND bf.balanceflagvalue = TRUE
                        THEN ba.balanceamountvalue
                END
        ) AS TotalBalanceYTD

    FROM balance b
        JOIN balanceidentifier bi ON b.balanceid=bi.balanceid
        AND balanceidentifiertype = balance_identifier_type
        AND balanceidentifiervalue = balance_identifier_value

        JOIN balanceparty bp ON b.balanceid=bp.balanceid
        LEFT JOIN balanceamount ba ON  b.balanceid=ba.balanceid
        LEFT JOIN balanceflag bf ON b.balanceid=bf.balanceid

        where bp.balancepartyidentifiervalue = p_identifier_value
        AND b.balanceclientidentifier = clientid
        AND b.balanceorigin = origin
        GROUP BY ba.balanceamountcurrency
  ),

-- 2b. Create the JSON for total amount calculated above
total_row_amount AS (
        SELECT
                balanceamountcurrency AS balanceamountcurrency,
                balanceidentifiertype AS balanceidentifiertype,
                balanceidentifiervalue AS balanceidentifiervalue,

                CASE
                        WHEN TotalCashAndCashEquivalents IS NOT NULL
                        THEN jsonb_build_array(
                                        jsonb_strip_nulls(
                                                jsonb_build_object(
                                                        'BalanceAmountType', 'TotalCashAndCashEquivalents',
                                                        'BalanceAmountValue', TotalCashAndCashEquivalents,
                                                        'BalanceAmountCurrency', balanceamountcurrency
                                                )
                                        )
                                )
                END
                AS total_row_amount_json,

                CASE
                        WHEN TotalCashAndCashEquivalents IS NOT NULL
                        THEN jsonb_build_array(
                                        jsonb_strip_nulls(
                                                jsonb_build_object(
                                                        'BalanceIdentifierType', balanceidentifiertype,
                                                        'BalanceIdentifierValue', balanceidentifiervalue
                                                )
                                        ),
                                        jsonb_strip_nulls(
                                                jsonb_build_object(
                                                        'BalanceIdentifierType', 'TransactionReference',
                                                        'BalanceIdentifierValue', 'Total'
                                                )
                                        )
                                )
                END
                AS total_row_balanceidentifier_json
        FROM total_data_amount

    )

-- 3. Inserting all json segments in temp table
INSERT INTO temp_extraction_results_balance (
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
    clientid,
        origin,
    dr.balanceamountcurrency AS balanceamountcurrency,
    balance_identifier_type,
    balance_identifier_value,
        'Detail' AS recordtype,
        jsonb_strip_nulls(
                jsonb_build_object(
                        'BalanceId', dr.balanceid,
                        'BalanceIdentifier', identi.balanceidentifier_json,
                        'BalanceDate',      dr.balancedate_json,
                        'BalanceParty',     dr.balanceparty_json,
                        'BalanceNarrative', dr.balancenarrative_json,
                        'BalanceAmount', dr.balanceamount_json,
                        'BalanceFlag', dr.balanceflag_json
                )
        ) AS combined_json
FROM detail_row dr
JOIN detail_row_identifier identi
ON dr.balanceid = identi.balanceid
AND dr.balanceamountcurrency = identi.balanceamountcurrency

UNION
SELECT
        p_identifier_value,
    clientid,
        origin,
    balanceamountcurrency,
    balance_identifier_type,
    balance_identifier_value,
        'Total' AS recordtype,
         jsonb_strip_nulls(
                jsonb_build_object(
                        'BalanceIdentifier', tra.total_row_balanceidentifier_json,
                        'BalanceAmount', tra.total_row_amount_json
                )
        ) AS combined_json
FROM total_row_amount tra
where tra.total_row_amount_json is not null

order by balanceamountcurrency, recordtype;

END;
$BODY$;

ALTER FUNCTION public.extract_balance_details(text, text, text, text, text)
    OWNER TO "DBUser";
