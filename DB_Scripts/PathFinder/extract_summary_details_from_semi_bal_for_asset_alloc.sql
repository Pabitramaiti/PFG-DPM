CREATE OR REPLACE FUNCTION public.extract_summary_details_from_semi_bal_for_asset_alloc(p_identifier_value text, client_data text, origin text, sectiontype text, sectionheader text)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
    dynamic_sql_total text;
    db_rec record;
    TotalCash numeric := 0;
    currency_list TEXT[];
    currency_data TEXT;
    section_head text :='Cash and Cash Equivalents';
    total_asset_plus_cash_numeric numeric := 0;
    asset_percentage numeric := 0;

BEGIN
	SELECT ARRAY(
		SELECT DISTINCT currency
		FROM temp_extraction_results_balance terb
		WHERE terb.clientid = client_data
	) INTO currency_list;

RAISE NOTICE 'currency_list from temp_extraction_results_balance: %', currency_list;

RAISE NOTICE 'Creating table temp_total_cash';
DROP TABLE IF EXISTS temp_total_cash;
CREATE TEMP TABLE temp_total_cash (
    currency TEXT PRIMARY KEY,
    total_cash NUMERIC
) ON COMMIT DROP;


FOREACH currency_data IN ARRAY currency_list LOOP
    dynamic_sql_total := '
        SELECT *
        FROM temp_extraction_results_balance temp_bal
        WHERE temp_bal.partyid = $1
        AND temp_bal.clientid = $2
        AND temp_bal.origin = $3
        AND temp_bal.currency = $4
        AND temp_bal.sectionheader = $5
        AND temp_bal.recordtype = $6
    ';

    FOR db_rec IN EXECUTE dynamic_sql_total
        USING p_identifier_value, client_data, origin, currency_data, section_head, 'Total'
    LOOP
        RAISE NOTICE 'db_record: %',db_rec;
        IF db_rec.sectionheader = 'Cash and Cash Equivalents' THEN
            FOR amount_index IN 0..COALESCE(jsonb_array_length(db_rec.data -> 'BalanceAmount'), 0) - 1 LOOP
                IF db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType' = 'TotalCashAndCashEquivalents' THEN
                    INSERT INTO temp_total_cash (currency, total_cash)
                    VALUES (currency_data, (db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountValue')::numeric)
                    ON CONFLICT (currency) DO UPDATE
                            SET total_cash = EXCLUDED.total_cash;
                END IF;
            END LOOP;
        END IF;
    END LOOP;
END LOOP;

 -- Temp table for total assets plus cash per currency
 RAISE NOTICE 'Creating table temp_total_asset_plus_cash';
 DROP TABLE IF EXISTS temp_total_asset_plus_cash;
    CREATE TEMP TABLE temp_total_asset_plus_cash (
        currency TEXT PRIMARY KEY,
        total_asset_plus_cash NUMERIC
    ) ON COMMIT DROP;

    -- Populate temp_total_asset_plus_cash

    RAISE NOTICE 'inserting to temp_total_asset_plus_cash';
    INSERT INTO temp_total_asset_plus_cash(currency, total_asset_plus_cash)
    SELECT
        COALESCE(ba.balanceamountcurrency, ttc.currency),
        SUM(
            CASE
                WHEN ba.balanceamounttype IN (
                    'TotalFixedIncome','TotalEquityValue','TotalAlternativeInvestments',
                    'TotalCertificatesOfDeposits','TotalUnitInvestmentTrust','TotalMutualFunds')
                THEN ba.balanceamountvalue
                ELSE 0
            END
        ) + COALESCE(MAX(ttc.total_cash), 0) AS total_asset_plus_cash
    FROM balance b
    JOIN balanceidentifier bi ON b.balanceid = bi.balanceid
        AND bi.balanceidentifiertype = sectiontype
        AND bi.balanceidentifiervalue = sectionheader
    JOIN balanceparty bp ON b.balanceid = bp.balanceid
--  Below conditions are moved here, so that the below FULL OUTER JOIN of temp_total_cash works
--  If these conditions are given in where clause, the RIGHT SIDE of outer join is not populated
		AND bp.balancepartyidentifiervalue = p_identifier_value
        AND b.balanceclientidentifier = client_data
        AND b.balanceorigin = origin
    LEFT JOIN balanceamount ba ON b.balanceid = ba.balanceid
    LEFT JOIN balanceflag bf ON b.balanceid=bf.balanceid
    AND bf.balanceflagtype = 'IsPTD' AND bf.balanceflagvalue = true
    FULL OUTER JOIN temp_total_cash ttc ON ba.balanceamountcurrency = ttc.currency

--    WHERE bp.balancepartyidentifiervalue = p_identifier_value
--      AND b.balanceclientidentifier = client_data
--      AND b.balanceorigin = origin
--      AND ba.balanceamountcurrency IS NOT NULL

    GROUP BY ba.balanceamountcurrency, ttc.currency;


--. Detail rows json
WITH detail_row AS (
    SELECT DISTINCT
	b.balanceid,
	ba.balanceamountcurrency AS balanceamountcurrency,
	(CASE
		  WHEN bi.balanceidentifiervalue = 'Asset Allocation' AND bnarr.balanceid IS NOT NULL
		  THEN jsonb_agg(
		        DISTINCT jsonb_build_object(
                    'BalanceNarrativeType', bnarr.balancenarrativetype,
                    'BalanceNarrativeValue', bnarr.balancenarrativevalue,
                    'BalanceNarrativeSequence', bnarr.balancenarrativesequence
                ))
                ||
                jsonb_build_object(
                    'BalanceNarrativeType', 'AssetPercentage',
                    'BalanceNarrativeValue',
                        CASE
                            WHEN MAX(tta.total_asset_plus_cash) != 0
                            THEN ROUND((MAX(ba.balanceamountvalue) / MAX(tta.total_asset_plus_cash)) * 100, 2)
                            ELSE NULL
                        END,
                    'BalanceNarrativeSequence', MAX(bnarr.balancenarrativesequence)+1
				)
		  WHEN bi.balanceidentifiervalue = 'Market Value Over Time' AND bnarr.balanceid IS NOT NULL
		  THEN jsonb_agg(
						CASE WHEN bnarr.balancenarrativesequence = '1'
								AND bi.balanceidentifiervalue = 'Market Value Over Time'
							 THEN jsonb_build_object(
									'BalanceNarrativeType', 'Description',
									'BalanceNarrativeValue',TO_CHAR(TO_TIMESTAMP(bnarr.balancenarrativevalue::text, 'MM'), 'Mon'),
								 	'BalanceNarrativeSequence',bnarr.balancenarrativesequence)
						END
					)
           WHEN bnarr.balanceid IS NOT NULL
           THEN jsonb_agg(
			      DISTINCT jsonb_build_object(
                    'BalanceNarrativeType', bnarr.balancenarrativetype,
                    'BalanceNarrativeValue', bnarr.balancenarrativevalue,
                    'BalanceNarrativeSequence', bnarr.balancenarrativesequence
			))
	END )	AS balancenarrative_json,

	(CASE
		WHEN ba.balanceid IS NOT NULL
		THEN jsonb_agg(
				DISTINCT jsonb_build_object(
					'BalanceAmountType', ba.balanceamounttype,
					'BalanceAmountValue', ba.balanceamountvalue,
					'BalanceAmountCurrency', ba.balanceamountcurrency
					)
				)
	END )	AS balanceamount_json,

    (CASE
          WHEN bdate.balanceid IS NOT NULL
          THEN jsonb_agg(
                  DISTINCT jsonb_build_object(
                     'BalanceDateType', bdate.balancedatetype,
                      'BalanceDateValue', TO_CHAR(bdate.balancedatevalue,'YYYY-MM-DD HH:MI:SS')
                      )
                  )
    END )   AS balancedate_json,

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
	END )	AS balanceflag_json

	FROM balance b
	JOIN balanceidentifier bi ON b.balanceid = bi.balanceid
	AND balanceidentifiertype = sectiontype
	AND balanceidentifiervalue = sectionheader

	JOIN balanceparty bp ON b.balanceid = bp.balanceid
	LEFT JOIN balancenarrative bnarr ON b.balanceid = bnarr.balanceid
	LEFT JOIN balancedate bdate ON b.balanceid = bdate.balanceid
	LEFT JOIN balanceamount ba ON  b.balanceid = ba.balanceid
	LEFT JOIN balanceflag bf ON b.balanceid = bf.balanceid

    LEFT JOIN temp_total_asset_plus_cash tta ON ba.balanceamountcurrency = tta.currency

	WHERE bp.balancepartyidentifiervalue = p_identifier_value
	AND b.balanceclientidentifier = client_data
	AND b.balanceorigin = origin
	AND ba.balanceamounttype IN ('TotalFixedIncome','TotalEquityValue','TotalAlternativeInvestments',
                    'TotalCertificatesOfDeposits','TotalUnitInvestmentTrust','TotalMutualFunds')
	GROUP BY b.balanceid,
	bi.balanceid,
	ba.balanceamountcurrency,
	bnarr.balanceid,
	bdate.balanceid,
	ba.balanceid,
	bf.balanceid,
	bi.balanceidentifiervalue

    ),

-- It will fetch all BalanceIdentifiers whose balanceId from above query result matches with balanceidentifier table.
--    BalanceIdentifier table can have multiple balanceidentifiertypes for one balanceid.
detail_row_identifier AS (
    SELECT DISTINCT
	dr.balanceid,

	(CASE
		WHEN bi.balanceid IS NOT NULL
		THEN jsonb_agg(
			DISTINCT jsonb_build_object(
				'BalanceIdentifierType', bi.balanceidentifiertype,
				'BalanceIdentifierValue', bi.balanceidentifiervalue
				)
			)
	END )	AS balanceidentifier_json

	FROM detail_row dr
	JOIN balanceidentifier bi ON dr.balanceid=bi.balanceid
	GROUP BY dr.balanceid, bi.balanceid
    )

-- Inserting all json segments in temp table
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
    balanceamountcurrency,
    sectiontype,
    sectionheader,
    'Detail' AS recordtype,
	jsonb_strip_nulls(
		jsonb_build_object(
			'BalanceId', dr.balanceid,
			'BalanceIdentifier', identi.balanceidentifier_json,
			'BalanceNarrative', jsonb_path_query_array(dr.balancenarrative_json,'$[*] ? (@ != null)'),
			'BalanceDate', dr.balancedate_json,
			'BalanceAmount', dr.balanceamount_json,
			'BalanceFlag', dr.balanceflag_json
		)
	) AS combined_json
FROM detail_row dr
JOIN detail_row_identifier identi ON dr.balanceid = identi.balanceid

order by balanceamountcurrency, recordtype;

-- Insert static "Cash and Cash Equivalents" record per currency

FOREACH currency_data IN ARRAY currency_list LOOP
    SELECT
        ttc.total_cash,
        ttapc.total_asset_plus_cash
    INTO
        TotalCash,
        total_asset_plus_cash_numeric
    FROM temp_total_cash ttc
    JOIN temp_total_asset_plus_cash ttapc ON ttc.currency = ttapc.currency
    WHERE ttc.currency = currency_data;

    IF total_asset_plus_cash_numeric != 0 THEN
        asset_percentage := ROUND((TotalCash / total_asset_plus_cash_numeric) * 100, 2);
    ELSE
        asset_percentage := 0;
    END IF;

    INSERT INTO temp_extraction_results_intermediate_summary(
        partyid,
        clientid,
        origin,
        currency,
        sectiontype,
        sectionheader,
        recordtype,
        data
    )
    VALUES (
        p_identifier_value,
        client_data,
        origin,
        currency_data,
        sectiontype,
        sectionheader,
        'Detail',
        jsonb_build_object(
            'BalanceIdentifier', jsonb_build_array(
                jsonb_build_object(
                    'BalanceIdentifierType', 'Internal',
                    'BalanceIdentifierValue', 'Asset Allocation'
                )
            ),
            'BalanceNarrative', jsonb_build_array(
                jsonb_build_object(
                    'BalanceNarrativeType', 'Description',
                    'BalanceNarrativeSequence', 1,
                    'BalanceNarrativeValue', 'Cash and Cash Equivalents'
                ),
                jsonb_build_object(
                    'BalanceNarrativeType', 'AssetPercentage',
                    'BalanceNarrativeSequence', 2,
                    'BalanceNarrativeValue', asset_percentage::text
                )
            ),
            'BalanceAmount', jsonb_build_array(
                jsonb_build_object(
                    'BalanceAmountType', 'TotalCash',
                    'BalanceAmountValue', COALESCE(TotalCash, 0),
                    'BalanceAmountCurrency', currency_data
                )
            )
        )
    );
END LOOP;

END;
$function$;
