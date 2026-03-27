CREATE OR REPLACE FUNCTION public.create_json_for_transaction(
p_identifier_value text, clientid text, origin text)
  RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
    section RECORD;
    statement_dates JSONB;
BEGIN

    -- Loop through each section and insert results into the temp table
    FOR section IN
        SELECT * FROM (VALUES
            -- Transactions
            -- Monthly Dividend Reinvestment Summary
                ('IsActivityRecord','Dividend Reinvestment Summary',
                  ARRAY['Other'], ARRAY[]::text[], 'TransactionDate'),
			-- Securities activity
				('IsActivityRecord','PurchaseAndSaleTradeOrExecution',ARRAY['TradePurchase','TradeSell'],
				 ARRAY[]::text[],'TransactionDate'),
			-- Income and Reinvestments
				('IsActivityRecord','BookkeepingJournalEntry',ARRAY['Income','Reinvest'],
				 ARRAY['DividendReceived','InterestPaymentReceived',''],'TransactionDate'),
			--  Securities Received and Delivered
				('IsActivityRecord','BookkeepingJournalEntry',ARRAY['TransferIn','TransferOut'],
				 ARRAY[]::text[],'TransactionDate'),
			--  Funds Paid and Received
				('IsActivityRecord','BookkeepingJournalEntry',ARRAY['Deposit','Withdrawal'],
				 ARRAY[]::text[],'TransactionDate'),
		    --  Other Open Orders
				('IsActivityRecord','PurchaseAndSaleTradeOrExecution',ARRAY['OpenOrder'],
				 ARRAY[]::text[],'OrderDate'),
			--	Transaction Detail
				('IsActivityRecord','Other',ARRAY['Other'],
				 ARRAY[]::text[],'TransactionDate'),
			--  Pending Settlements
				('IsActivityRecord','SettlementObligation',ARRAY['Principal'],
				 ARRAY[]::text[],'TransactionDate'),
			--  Checking and Money Card
				('IsActivityRecord','BookkeepingJournalEntry',ARRAY['CardActivity'],
				 ARRAY[]::text[],'TransactionDate'),
			--  Realized Gain/Loss Detail
				('IsActivityRecord','BookkeepingJournalEntry',ARRAY['RealizedGainOrLoss'],
				 ARRAY[]::text[],'ClosingDate'),
			--	Checking and Debit Activity
				('IsActivityRecord','BookkeepingJournalEntry',ARRAY['ChecksActivity'],
				 ARRAY[]::text[],'TransactionDate'),
			--  Cash and Bank Sweep
				('IsActivityRecord','BookkeepingJournalEntry',ARRAY['Sweep'],
				 ARRAY[]::text[],'TransactionDate')
			       ) AS s(sectionrecord, transactioncategory, transactiontype, transactionsubtype, sortingfield)

    LOOP
    -- Call function extract_transaction_details with above arguments

			PERFORM extract_transaction_details(p_identifier_value, clientid, origin, section.sectionrecord, section.transactioncategory, section.transactiontype, section.transactionsubtype, section.sortingfield);

    END LOOP;

        -- Construct the statement_dates JSONB
    SELECT
    jsonb_build_object(
		'TransactionId',max(tp.transactionid),
        'TransactionParty', jsonb_agg(
            DISTINCT jsonb_build_object(
                'TransactionPartyIdentifierType', tp.transactionpartyidentifiertype,
                'TransactionPartyIdentifierValue', tp.transactionpartyidentifiervalue
            )
        ),
        'TransactionEvent', jsonb_agg(
            DISTINCT jsonb_build_object(
                'TransactionCategory', 'Header Date'
            )
        ),
        'TransactionDate', jsonb_agg(
            DISTINCT jsonb_build_object(
                'TransactionDateType', tdate.transactiondatetype,
                'TransactionDateValue', TO_CHAR(tdate.transactiondatevalue,'YYYY-MM-DD HH:MI:SS')
            )
        )
    ) INTO statement_dates
    FROM
        transactionparty tp
    JOIN
        transactionevent tevent ON tp.transactionid = tevent.transactionid
        AND UPPER(tevent.transactioncategory) = 'HEADER DATE'
    JOIN
        transactionclassification tclass ON tp.transactionid = tclass.transactionid
        AND UPPER(tclass.transactionclassificationvalue) = 'HEADER DATE'
    LEFT JOIN
        transactiondate tdate ON tp.transactionid = tdate.transactionid
        AND tdate.transactiondatetype IN ('BeginningDate', 'EndingDate')
    WHERE
        tp.transactionpartyidentifiervalue = p_identifier_value::TEXT
        AND tevent.transactionorigin = origin
        AND tevent.transactionclientidentifier = clientid
    GROUP BY
        tp.transactionpartyidentifiervalue LIMIT 1;

    IF statement_dates IS NOT NULL THEN
        INSERT INTO temp_extraction_results_transaction(partyid, clientid, origin, currency, sectiontype, sectionheader, recordtype, data)
        VALUES(p_identifier_value, clientid, origin, null, 'statement_date', 'BeginningDate and EndingDate', 'Detail', statement_dates);
    END IF;
END;
$function$;