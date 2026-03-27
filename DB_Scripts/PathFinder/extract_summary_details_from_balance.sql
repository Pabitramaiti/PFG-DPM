CREATE OR REPLACE FUNCTION public.extract_summary_details_from_balance(p_identifier_value text, clientid text, origin text, sectiontype text, sectionheader text)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
BEGIN

 -- Aggregate
WITH total_data_amount AS (
	SELECT
	ba.balanceamountcurrency AS balanceamountcurrency,
	bf.balanceflagtype AS balanceflagtype,
	bf.balanceflagvalue AS balanceflagvalue,
	MAX(bi.balanceidentifiertype) AS balanceidentifiertype,
	MAX(bi.balanceidentifiervalue) AS balanceidentifiervalue,

	-- Total - Income summary
	SUM(
		CASE
			WHEN ba.balanceamounttype IN (
			'DividendTaxable','DividendNonTaxable','InterestTaxable','InterestNonTaxable')
			THEN ba.balanceamountvalue
		END
	) AS TotalIncomeSummary,
	--Total - Asset Allocation
	SUM(
		CASE
			WHEN ba.balanceamounttype IN (
			'TotalCash','TotalFixedIncome','TotalEquityValue','TotalAlternativeInvestments',
				'TotalCertificatesOfDeposits','TotalUnitInvestmentTrust','TotalMutualFunds')
			THEN ba.balanceamountvalue
		END
	) AS TotalAssetAllocation

    FROM balance b
	JOIN balanceidentifier bi ON b.balanceid=bi.balanceid
	AND bi.balanceidentifiertype = sectiontype
	AND bi.balanceidentifiervalue = sectionheader

	JOIN balanceparty bp ON b.balanceid=bp.balanceid
	LEFT JOIN balanceamount ba ON  b.balanceid=ba.balanceid
	LEFT JOIN balanceflag bf ON b.balanceid=bf.balanceid
	AND (
	(bf.balanceflagtype = 'IsPTD' AND bf.balanceflagvalue = true)
	OR
	(bf.balanceflagtype = 'IsYTD' AND bf.balanceflagvalue = true)
	)

	WHERE bp.balancepartyidentifiervalue = p_identifier_value
	AND b.balanceclientidentifier = clientid
	AND b.balanceorigin = origin

	GROUP BY ba.balanceamountcurrency, bf.balanceflagtype, bf.balanceflagvalue
  ),

--. Detail rows json
detail_row AS (
    SELECT DISTINCT
	b.balanceid,
	ba.balanceamountcurrency AS balanceamountcurrency,
	(CASE
          WHEN bi.balanceidentifiervalue = 'Market Value Over Time'
          THEN row_number() OVER(ORDER BY
								 STRING_AGG(bnarr.balancenarrativevalue,''
											ORDER BY bnarr.balancenarrativesequence DESC)
								FILTER (WHERE bnarr.balancenarrativesequence IN ('1','2'))
								)
	 END) AS SortMonthYear,
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
                            WHEN tda.TotalAssetAllocation != 0
                            THEN ROUND((MAX(ba.balanceamountvalue) / tda.TotalAssetAllocation) * 100, 2)
                            ELSE 0
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
--	AND bf.balanceflagtype = 'IsPTD'
	LEFT JOIN total_data_amount tda on ba.balanceamountcurrency = tda.balanceamountcurrency
	AND bf.balanceflagtype = tda.balanceflagtype
	AND bf.balanceflagvalue = tda.balanceflagvalue

	WHERE bp.balancepartyidentifiervalue = p_identifier_value
	AND b.balanceclientidentifier = clientid
	AND b.balanceorigin = origin
	GROUP BY b.balanceid,
	bi.balanceid,
	ba.balanceamountcurrency,
	bnarr.balanceid,
	bdate.balanceid,
	ba.balanceid,
	bf.balanceid,
--	 ba.balanceamountvalue,
	tda.TotalAssetAllocation,
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
    ),
/*
    total_row_balance_flag AS (
	SELECT
	balanceflag_json AS total_row_balance_flag_json

    FROM detail_row
	Fetch first 1 row only

    ),
*/

-- . Create the JSON for total amount calculated above
total_row_amount AS (
	SELECT
		balanceamountcurrency AS balanceamountcurrency,
		balanceidentifiertype AS balanceidentifiertype,
		balanceidentifiervalue AS balanceidentifiervalue,

		CASE
			WHEN TotalIncomeSummary IS NOT NULL
			THEN jsonb_build_array(
					jsonb_strip_nulls(
						jsonb_build_object(
							'BalanceAmountType', 'TotalBalance',
							'BalanceAmountValue', TotalIncomeSummary,
							'BalanceAmountCurrency', balanceamountcurrency
						)
					)
				)
		END
		AS total_row_amount_json,

		CASE
			WHEN TotalIncomeSummary IS NOT NULL
			THEN jsonb_build_array(
					jsonb_strip_nulls(
						jsonb_build_object(
							'BalanceIdentifierType', balanceidentifiertype,
							'BalanceIdentifierValue', balanceidentifiervalue
						)
					)
				)
		END
		AS total_row_balanceidentifier_json,

		CASE
			WHEN TotalIncomeSummary IS NOT NULL
			THEN jsonb_build_array(
					jsonb_strip_nulls(
						jsonb_build_object(
							'BalanceNarrativeType', 'Description',
							'BalanceNarrativeSequence', 1,
							'BalanceNarrativeValue', 'Total'
						)
					)
				)
		END
		AS total_row_balancenarrative_json,

		CASE
			WHEN TotalIncomeSummary IS NOT NULL
			THEN jsonb_build_array(
					jsonb_strip_nulls(
						jsonb_build_object(
					'BalanceFlagType', balanceflagtype,
					'BalanceFlagValue', CASE
											WHEN balanceflagvalue = TRUE THEN '1'
											WHEN balanceflagvalue = FALSE THEN '0'
										END

					)
				)
			)
		END
		AS total_row_balance_flag_json

	FROM total_data_amount

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
    clientid,
	origin,
    balanceamountcurrency,
    sectiontype,
    sectionheader,
    'Detail' AS recordtype,
	jsonb_strip_nulls(
		jsonb_build_object(
			'SortMonthYear',sortMonthYear,
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

UNION
SELECT
    p_identifier_value,
    clientid,
	origin,
    balanceamountcurrency,
    sectiontype,
    sectionheader,
	'Total' AS recordtype,
	 jsonb_strip_nulls(
		jsonb_build_object(
			'BalanceIdentifier', tra.total_row_balanceidentifier_json,
			'BalanceNarrative', tra.total_row_balancenarrative_json,
			'BalanceAmount', tra.total_row_amount_json,
			'BalanceFlag', tra.total_row_balance_flag_json
		)
	) AS combined_json
FROM total_row_amount tra
where tra.total_row_amount_json is not null

order by balanceamountcurrency, recordtype;

END;
$function$;