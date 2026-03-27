CREATE OR REPLACE FUNCTION public.extract_transaction_details(p_identifier_value text, clientid text, origin text, sectiontype text, transactioncategory_arg text, transactiontype_arg text[], transactionsubtype_arg text[], sorting_field text)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE

sectionheader text;

BEGIN

IF transactioncategory_arg = 'PurchaseAndSaleTradeOrExecution' AND ('TradePurchase' = ANY(transactiontype_arg) OR 'TradeSell' = ANY(transactiontype_arg)) THEN
		sectionheader='Securities Activity';
ELSIF transactioncategory_arg = 'BookkeepingJournalEntry' AND ('Income' = ANY(transactiontype_arg) OR 'Reinvest' = ANY(transactiontype_arg)) THEN
		sectionheader='Income and Reinvestments';
ELSIF transactioncategory_arg = 'BookkeepingJournalEntry' AND ('Deposit' = ANY(transactiontype_arg)  OR 'Withdrawal' = ANY(transactiontype_arg)) THEN
		sectionheader='Funds Paid and Received';
ELSIF transactioncategory_arg = 'BookkeepingJournalEntry' AND ('TransferIn' = ANY(transactiontype_arg) OR 'TransferOut' = ANY(transactiontype_arg)) THEN
		sectionheader='Securities Received and Delivered';
ELSIF transactioncategory_arg = 'BookkeepingJournalEntry' AND ('Sweep' = ANY(transactiontype_arg)) THEN
		sectionheader='Cash and Bank Sweep';
ELSIF transactioncategory_arg = 'BookkeepingJournalEntry' AND ('RealizedGainOrLoss' = ANY(transactiontype_arg)) THEN
		sectionheader='Realized Gain/Loss Detail';
ELSIF transactioncategory_arg = 'BookkeepingJournalEntry' AND ('ChecksActivity' = ANY(transactiontype_arg)) THEN
		sectionheader='Checking  and Debit Activity';
ELSIF transactioncategory_arg = 'BookkeepingJournalEntry' AND ('CardActivity' = ANY(transactiontype_arg)) THEN
		sectionheader='Checking and Money Card';
ELSIF transactioncategory_arg = 'SettlementObligation' AND ('Principal' = ANY(transactiontype_arg)) THEN
		sectionheader='Pending settlements';
ELSIF transactioncategory_arg = 'PurchaseAndSaleTradeOrExecution' AND ('OpenOrder' = ANY(transactiontype_arg)) THEN
		sectionheader='Open Orders';
ELSIF transactioncategory_arg = 'Other' AND ('Other' = ANY(transactiontype_arg)) THEN
		sectionheader='Transaction Detail';
ELSIF transactioncategory_arg = 'Dividend Reinvestment Summary'
	AND ('Other' = ANY(transactiontype_arg)) THEN
	sectionheader := 'Monthly Dividend Reinvestment Summary';
END IF;


-- 1. Fetching Transaction details ( detail records )
WITH detail_row_intermediate AS (
        SELECT DISTINCT
            tp.transactionid,
			COALESCE(tamount.transactionamountcurrency, tprice.transactionpricecurrency) AS transactioncurrency,
					jsonb_agg(
                                DISTINCT jsonb_build_object(
                                    'TransactionPartyIdentifierType', tp.transactionpartyidentifiertype,
                                    'TransactionPartyIdentifierValue', tp.transactionpartyidentifiervalue
                                )
                        ) AS transactionparty_json,

					jsonb_agg(
                                DISTINCT jsonb_build_object(
                                    'TransactionCategory', tevent.transactioncategory,
                                    'TransactionType', tevent.transactiontype,
                                    'TransactionSubType', tevent.transactionsubtype,
									'TransactionDirection', tevent.transactiondirection
                                )
                            ) AS transactionevent_json,

					(CASE
						WHEN tamount.transactionid IS NOT NULL
						THEN jsonb_agg(
						DISTINCT jsonb_build_object(
							'TransactionAmountType', tamount.transactionamounttype,
							'TransactionAmountValue', tamount.transactionamountvalue,
							'TransactionAmountCurrency', tamount.transactionamountcurrency
								)
							)
					END ) AS transactionamount_json,

					(CASE
						WHEN tprice.transactionid IS NOT NULL
						THEN jsonb_agg(
						DISTINCT jsonb_build_object(
							'TransactionPriceType', tprice.transactionpricetype,
							'TransactionPriceValue', tprice.transactionpricevalue,
							'TransactionPriceCurrency', tprice.transactionpricecurrency
								)
							)
					END ) AS transactionprice_json,

					(CASE
						WHEN tdate.transactionid IS NOT NULL
						THEN jsonb_agg(
						DISTINCT jsonb_build_object(
							'TransactionDateType', transactiondatetype,
							'TransactionDateValue', TO_CHAR(transactiondatevalue,'YYYY-MM-DD HH:MI:SS')
								)
							)
					END	) AS transactiondate_json,

					(CASE
						WHEN tnarr.transactionid IS NOT NULL
						THEN jsonb_agg(
						DISTINCT jsonb_build_object(
							'TransactionNarrativeType', tnarr.transactionnarrativetype,
							'TransactionNarrativeSequence', tnarr.transactionnarrativesequence,
							'TransactionNarrativeValue', tnarr.transactionnarrativevalue
								)
							)
					END	) AS transactionnarrative_json,

					(CASE
						WHEN tquant.transactionid IS NOT NULL
						THEN jsonb_agg(
						   DISTINCT jsonb_build_object(
							'TransactionQuantityType', transactionquantitytype,
							'TransactionQuantityValue', transactionquantityvalue
								)
							)
					END	) AS transactionquantity_json,

					(CASE
						WHEN tinst.transactionid IS NOT NULL
						THEN jsonb_agg(
						DISTINCT jsonb_build_object(
							'TransactionInstrumentIdentifierType', transactioninstrumentidentifiertype,
							'TransactionInstrumentIdentifierValue', transactioninstrumentidentifiervalue
								)
							)
					END	) AS transactioninstrument_json,

					(CASE
						WHEN tidentifier.transactionid IS NOT NULL
						THEN jsonb_agg(
						DISTINCT jsonb_build_object(
							'TransactionIdentifierType', transactionidentifiertype,
							'TransactionIdentifierValue', transactionidentifiervalue
								)
							)
					END	) AS transactionidentifier_json


    FROM transactionparty tp
	JOIN transactionevent tevent ON tp.transactionid = tevent.transactionid
		AND  tevent.transactioncategory = transactioncategory_arg
		AND	 tevent.transactiontype = ANY(transactiontype_arg)
		AND	(
			(array_length(transactionsubtype_arg, 1) IS NULL )
			OR
			(array_length(transactionsubtype_arg, 1) > 0 AND TRIM(COALESCE(tevent.transactionsubtype,'')) = ANY(transactionsubtype_arg))
		)
		AND tevent.transactionorigin = origin
		AND tevent.transactionclientidentifier = clientid

	LEFT JOIN transactionamount tamount ON tp.transactionid = tamount.transactionid
		AND tamount.transactionamounttype IN (
			'NetAmount','CostBasis','Proceeds',
			'ShortTermGainOrLoss','LongTermGainOrLoss','OtherDebit','OtherCredit','AccruedInterest',
            'Gross','TotalCommission','Markup','Markdown'
		)
	LEFT JOIN transactionprice tprice ON tp.transactionid = tprice.transactionid
		AND tprice.transactionpricetype IN ('ExecutionPrice','OrderPrice','CurrentPrice')
	LEFT JOIN transactiondate tdate ON tp.transactionid = tdate.transactionid
         AND tdate.transactiondatetype IN ('TransactionDate','OrderDate','ClosingDate','AcquiredDate','SettlementDate',
         'FullyEnrichedProcessingDate')
	LEFT JOIN transactionnarrative tnarr ON tp.transactionid = tnarr.transactionid
		AND tnarr.transactionnarrativetype IN ('TransactionDescription','MultiLineDescription')
	LEFT JOIN transactionquantity tquant ON tp.transactionid = tquant.transactionid
		AND tquant.transactionquantitytype = 'TransactionQuantity'
	LEFT JOIN transactioninstrument tinst ON tp.transactionid = tinst.transactionid
		AND tinst.transactioninstrumentidentifiertype IN ('ADPNumber','CUSIP','Symbol')
	LEFT JOIN transactionidentifier tidentifier ON tp.transactionid = tidentifier.transactionid
		AND tidentifier.transactionidentifiertype IN ('CheckNumber','AccountTypeReference')
	WHERE tp.transactionpartyidentifiervalue = p_identifier_value
	GROUP BY tp.transactionid, transactioncurrency, tamount.transactionid, tprice.transactionid, tdate.transactionid, tnarr.transactionid, tquant.transactionid, tinst.transactionid, tidentifier.transactionid
    ),
    --1b.  Sorting on the field provided
detail_row AS (
        SELECT DISTINCT
            tdate.transactiondatevalue,
                row_number() OVER(ORDER BY tdate.transactiondatevalue) as sortIndex,
                dri.*

                FROM detail_row_intermediate dri
                LEFT JOIN transactiondate tdate ON dri.transactionid = tdate.transactionid
                WHERE
                tdate.transactiondatetype  = sorting_field
                --ORDER BY sortIndex
                ),

--2. Extracting TransactionEvent details for final aggregated row of current section
total_row_event AS (
	SELECT
	transactionevent_json AS total_row_event_json

    FROM detail_row
	Fetch first 1 row only

    ),


--3a Aggregate total Amounts under various amounttypes
total_data_amount AS (
	SELECT
	--MAX(tamount.transactionamountcurrency) AS transactionamountcurrency,
	tamount.transactionamountcurrency AS transactionamountcurrency,

	-- Total Debit (Negative amounts)
	SUM(
		CASE
			WHEN transactionamounttype = 'NetAmount' AND transactionamountvalue < 0
			THEN transactionamountvalue
			ELSE NULL
		END
	) AS TotalDebit,

	-- Total Credit (Positive amounts)
	SUM(
		CASE
			WHEN transactionamounttype = 'NetAmount' AND transactionamountvalue > 0
			THEN transactionamountvalue
			ELSE NULL
		END
	) AS TotalCredit,

	SUM(
		CASE
			WHEN transactionamounttype = 'NetAmount'
			THEN COALESCE(transactionamountvalue, 0)
			ELSE NULL
		END


	) AS TotalNetAmount,

	-- Total short term gain or loss for Realized Gain/Loss Detail
	SUM(
		CASE
			WHEN transactionamounttype = 'ShortTermGainOrLoss'
			THEN transactionamountvalue
			ELSE NULL
		END
	) AS TotalShortTermGainOrLoss,


	-- Total long term gain or loss for Realized Gain/Loss Detail
	SUM(
		CASE
			WHEN transactionamounttype = 'LongTermGainOrLoss'
			THEN transactionamountvalue
			ELSE NULL
		END
	) AS TotalLongTermGainOrLoss,


	-- Total OtherDebit for Checking and debit activity
	SUM(
		CASE
			WHEN transactionamounttype = 'OtherDebit'
			THEN transactionamountvalue
			ELSE NULL
		END
	) AS TotalOtherDebit,


	-- Total OtherCredit for Checking and debit activity
	SUM(
		CASE
			WHEN transactionamounttype = 'OtherCredit'
			THEN transactionamountvalue
			ELSE NULL
		END
	) AS TotalOtherCredit

	FROM transactionparty tp
	JOIN transactionevent tevent ON tp.transactionid = tevent.transactionid
		AND  tevent.transactioncategory = transactioncategory_arg
		AND	 tevent.transactiontype = ANY(transactiontype_arg)
		AND	(
			(array_length(transactionsubtype_arg, 1) IS NULL )
			OR
			(array_length(transactionsubtype_arg, 1) > 0 AND TRIM(COALESCE(tevent.transactionsubtype,'')) = ANY(transactionsubtype_arg))
		)
		AND tevent.transactionorigin = origin
		AND tevent.transactionclientidentifier = clientid
	JOIN transactionamount tamount ON tp.transactionid = tamount.transactionid
		AND tamount.transactionamounttype IN (
			'NetAmount','CostBasis','Proceeds',
			'ShortTermGainOrLoss','LongTermGainOrLoss','OtherDebit','OtherCredit','AccruedInterest',
			'Gross','TotalCommission','Markup','Markdown'
		)
	WHERE tp.transactionpartyidentifiervalue = p_identifier_value
	GROUP BY transactionamountcurrency
    ),

-- 3b. Create the JSON for total amount calculated above
total_row_amount AS (
	SELECT
		transactionamountcurrency AS transactionamountcurrency,
		jsonb_path_query_array(
				jsonb_build_array(
					CASE
						WHEN TotalDebit IS NOT NULL
						THEN jsonb_strip_nulls(
						jsonb_build_object(
							'TransactionAmountType', 'TotalDebit',
							'TransactionAmountValue', TotalDebit,
							'TransactionAmountCurrency', transactionamountcurrency
						)
					)
					END
					,
					CASE
						WHEN TotalCredit IS NOT NULL
						THEN jsonb_strip_nulls(
						jsonb_build_object(
							'TransactionAmountType', 'TotalCredit',
							'TransactionAmountValue', TotalCredit,
							'TransactionAmountCurrency', transactionamountcurrency
						)
					)
					END
					,
					CASE
						WHEN TotalNetAmount IS NOT NULL
						THEN jsonb_strip_nulls(
						jsonb_build_object(
							'TransactionAmountType', 'TotalNetAmount',
							'TransactionAmountValue', TotalNetAmount,
							'TransactionAmountCurrency', transactionamountcurrency
						)
					)
					END
					,
					CASE
						WHEN TotalShortTermGainOrLoss IS NOT NULL
						THEN jsonb_strip_nulls(
						jsonb_build_object(
							'TransactionAmountType', 'TotalShortTermGainOrLoss',
							'TransactionAmountValue', TotalShortTermGainOrLoss,
							'TransactionAmountCurrency', transactionamountcurrency
						)
					)
					END
					,
					CASE
						WHEN TotalLongTermGainOrLoss IS NOT NULL
						THEN jsonb_strip_nulls(
								jsonb_build_object(
									'TransactionAmountType', 'TotalLongTermGainOrLoss',
									'TransactionAmountValue', TotalLongTermGainOrLoss,
									'TransactionAmountCurrency', transactionamountcurrency
						)
					)
					END
					,
					CASE
						WHEN TotalOtherDebit IS NOT NULL
						THEN jsonb_strip_nulls(
								jsonb_build_object(
									'TransactionAmountType', 'TotalDebit',
									'TransactionAmountValue', TotalOtherDebit,
									'TransactionAmountCurrency', transactionamountcurrency
						)
					)
					END
					,
					CASE
						WHEN TotalOtherCredit IS NOT NULL
						THEN jsonb_strip_nulls(
								jsonb_build_object(
									'TransactionAmountType', 'TotalCredit',
									'TransactionAmountValue', TotalOtherCredit,
									'TransactionAmountCurrency', transactionamountcurrency
								)
							)
					END
				),'$[*] ? (@ != null)'
			) AS total_row_amount_json
	FROM total_data_amount
    ),

-- 4a. Aggregate total price
total_data_price AS (
	SELECT

		--MAX(tprice.transactionpricecurrency) AS transactionpricecurrency,
		tprice.transactionpricecurrency AS transactionpricecurrency,
		-- Total CurrentPrice ( or Market Price )
		SUM(
			CASE
				WHEN TransactionPriceType = 'CurrentPrice'
				THEN transactionpricevalue
				ELSE NULL
			END
		) AS TotalPrice

	FROM transactionparty tp
	JOIN transactionevent tevent ON tp.transactionid = tevent.transactionid
		AND  tevent.transactioncategory = transactioncategory_arg
		AND	 tevent.transactiontype = ANY(transactiontype_arg)
		AND	(
			(array_length(transactionsubtype_arg, 1) IS NULL )
			OR
			(array_length(transactionsubtype_arg, 1) > 0 AND TRIM(COALESCE(tevent.transactionsubtype,'')) = ANY(transactionsubtype_arg))
		)
		AND tevent.transactionorigin = origin
		AND tevent.transactionclientidentifier = clientid
	JOIN transactionprice tprice ON tp.transactionid = tprice.transactionid
		AND tprice.transactionpricetype IN ('ExecutionPrice','OrderPrice','CurrentPrice')
	WHERE tp.transactionpartyidentifiervalue = p_identifier_value
	GROUP BY transactionpricecurrency
    ),

-- 4b. Create the total price JSON separately
    total_row_price AS (
        SELECT
			transactionpricecurrency,
			CASE
				WHEN TotalPrice IS NOT NULL
				THEN jsonb_build_array(
						jsonb_strip_nulls(
							jsonb_build_object(
								'TransactionPriceType', 'TotalPrice',
								'TransactionPriceValue', TotalPrice,
								'TransactionPriceCurrency', transactionpricecurrency
					)))
				ELSE NULL
			END
		AS total_row_price_json
        FROM total_data_price
    )


-- 5. Final combination of detail rows and total rows
INSERT INTO temp_extraction_results_transaction(partyid, clientid, origin, currency, sectiontype, sectionheader, recordtype, data)
SELECT
    p_identifier_value,
    clientid,
	origin,
    transactioncurrency,
    sectiontype,
    sectionheader,
	'Detail' AS recordtype,
	jsonb_strip_nulls(jsonb_build_object(
         'sortIndex',sortIndex,
		'TransactionID', dr.transactionid,
	    'TransactionParty', dr.transactionparty_json,
		'TransactionEvent', dr.transactionevent_json,
		'TransactionAmount', dr.transactionamount_json,
		'TransactionPrice', dr.transactionprice_json,
		'TransactionDate', dr.transactionDate_json,
		'TransactionNarrative', dr.transactionnarrative_json,
		'TransactionQuantity', dr.transactionquantity_json,
		'TransactionInstrument', dr.transactioninstrument_json,
		'TransactionIdentifier', dr.transactionidentifier_json

	)) AS combined_json
FROM detail_row dr

UNION
SELECT
    p_identifier_value,
    clientid,
	origin,
    COALESCE(transactionamountcurrency, transactionpricecurrency) AS transactioncurrency,
    sectiontype,
    sectionheader,
	'Total' AS recordtype,
	jsonb_strip_nulls(jsonb_build_object(
		'TransactionEvent',te.total_row_event_json,
		'TransactionAmount', ta.total_row_amount_json,
		'TransactionPrice', tp.total_row_price_json

	)) AS combined_json
FROM  total_row_amount ta
FULL OUTER JOIN total_row_price tp
ON ta.transactionamountcurrency = tp.transactionpricecurrency
CROSS JOIN total_row_event te
order by transactioncurrency, recordtype;

END;
$function$;