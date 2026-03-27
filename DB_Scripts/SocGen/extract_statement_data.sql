CREATE OR REPLACE FUNCTION extract_statement_data(accountpartyid TEXT, clientidentifier TEXT, sectionnames TEXT[])
RETURNS JSONB AS $$

DECLARE
    result JSONB;
	
BEGIN

DROP TABLE IF EXISTS temp_base_data_1;
DROP TABLE IF EXISTS temp_total_data_1;
DROP TABLE IF EXISTS temp_total_data_2;
DROP TABLE IF EXISTS temp_transaction_with_quantity_2;
DROP TABLE IF EXISTS temp_total_data_3;
--DROP TABLE IF EXISTS temp_extraction_results;

-- STEP 1: Create filtered base data with all required fields and aggregations
--CREATE TEMP TABLE temp_extraction_results (
  --      partyid TEXT,
    --    clientid TEXT,
      --  currency TEXT,
        --sectiontype TEXT,
--        sectionheader TEXT,
  --      recordtype TEXT,  
    --    data JSONB
      --     );
        -- ) ON COMMIT DROP;
        
CREATE TEMP TABLE temp_base_data_1 ON COMMIT DROP AS
WITH filtered_transactions AS (
    SELECT 
        tp.transactionpartyidentifiervalue AS partyid, 
        tp.transactionid
    FROM 
        transactionparty tp
    JOIN transactionevent tevent ON tp.transactionid = tevent.transactionid
        AND tevent.transactionclientidentifier = clientidentifier::TEXT
    JOIN transactionclassification tclass ON tp.transactionid = tclass.transactionid
        AND tclass.transactionclassificationvalue = ANY(sectionnames)
    WHERE tp.transactionpartyidentifiervalue = accountpartyid::TEXT
)

SELECT 
    ft.partyid,
    ft.transactionid,
    MAX(UPPER(te.transactioncategory)) AS transactioncategory,
    MAX(te.transactiondirection) AS transactiondirection,
    MAX(te.transactionclientidentifier) AS clientid,
    -- MAX(tc.transactionclassificationvalue) AS sectionheader,
    MAX(
        CASE 
            WHEN te.transactionclientidentifier = '381' THEN
            --WHEN te.transactionclientidentifier = '000' THEN
                CASE tc.transactionclassificationvalue
                    WHEN 'Equities - Long Positions' THEN 'Equities - Securities Loan'
                    WHEN 'Equities - Short Positions' THEN 'Equities - Securities Borrow'
                    WHEN 'Options - Long Positions' THEN 'Options - Securities Loan'
                    WHEN 'Options - Short Positions' THEN 'Options - Securities Borrow'
                    WHEN 'Fixed Income - Long Positions' THEN 'Fixed Income - Securities Loan'
                    WHEN 'Fixed Income - Short Positions' THEN 'Fixed Income - Securities Borrow'
                    WHEN 'Other - Long Positions' THEN 'Other - Securities Loan'
                    WHEN 'Other - Short Positions' THEN 'Other - Securities Borrow'
                    ELSE tc.transactionclassificationvalue
                END
            ELSE tc.transactionclassificationvalue
        END
    ) AS sectionheader,

    'Detail' AS recordtype,

    -- Section type flags
    MAX(CASE 
        WHEN tf.transactionflagtype = 'IsActivityRecord' AND tf.transactionflagvalue = 'Yes' THEN 'Activity'
        WHEN tf.transactionflagtype = 'IsHoldingRecord' AND tf.transactionflagvalue = 'Yes' THEN 'Holdings'
        WHEN tf.transactionflagtype = 'IsSummaryRecord' AND tf.transactionflagvalue = 'Yes' THEN 'Summary'
        ELSE NULL
    END) AS sectiontype,

    COALESCE(
        MAX(ta.transactionamountcurrency),
        CASE WHEN MAX(tc.transactionclassificationvalue) = 'Other Activity' THEN 'USD' ELSE NULL END
    ) AS transactionamountcurrency,

    -- Amounts
    MAX(CASE WHEN ta.transactionamounttype = 'NetAmount' THEN ta.transactionamountvalue END) AS NetAmount,
    MAX(CASE WHEN ta.transactionamounttype = 'NetAmount' AND te.transactiondirection = 'Credit' THEN ta.transactionamountvalue END) AS NetAmountCredit,
    MAX(CASE WHEN ta.transactionamounttype = 'NetAmount' AND te.transactiondirection = 'Debit' THEN ta.transactionamountvalue END) AS NetAmountDebit,
    MAX(CASE WHEN ta.transactionamounttype = 'MarketValue' THEN ta.transactionamountvalue END) AS MarketValue,
    MAX(CASE WHEN ta.transactionamounttype = 'EstimatedAnnualIncome' THEN ta.transactionamountvalue END) AS EstimatedAnnualIncome,
    MAX(CASE WHEN ta.transactionamounttype = 'CouponInterestAmount' THEN ta.transactionamountvalue END) AS CouponInterestAmount,
    MAX(CASE WHEN ta.transactionamounttype = 'BeginMarketValue' THEN ta.transactionamountvalue END) AS BeginMarketValue,
    MAX(CASE WHEN ta.transactionamounttype = 'ClosingBalance' THEN ta.transactionamountvalue END) AS ClosingBalance,
    MAX(CASE WHEN ta.transactionamounttype = 'NetCashBalance' THEN ta.transactionamountvalue END) AS NetCashBalance,

    -- Quantities
    MAX(CASE WHEN tq.transactionquantitytype = 'PrimaryQuantity' THEN tq.transactionquantityvalue END) AS PrimaryQuantity,
    MAX(CASE WHEN tq.transactionquantitytype = 'CurrentFaceQuantity' THEN tq.transactionquantityvalue END) AS CurrentFaceQuantity,
    MAX(CASE WHEN tq.transactionquantitytype = 'OutstandingStockQuantity' THEN tq.transactionquantityvalue END) AS OutstandingStockQuantity,
    MAX(CASE WHEN tq.transactionquantitytype = 'UnderlyingQuantity' THEN tq.transactionquantityvalue END) AS UnderlyingQuantity,
    MAX(CASE WHEN tq.transactionquantitytype = 'TotalOrderQuantity' THEN tq.transactionquantityvalue END) AS TotalOrderQuantity,
    MAX(CASE WHEN tq.transactionquantitytype = 'CurrentQuantity' THEN tq.transactionquantityvalue END) AS CurrentQuantity,
    MAX(CASE WHEN tq.transactionquantitytype = 'BeginQuantity' THEN tq.transactionquantityvalue END) AS BeginQuantity,

    -- Dates
    MAX(CASE WHEN td.transactiondatetype = 'SettlementDate' THEN td.transactiondatevalue::text END) AS SettlementDate,
    MAX(CASE WHEN td.transactiondatetype = 'TransactionDate' THEN td.transactiondatevalue::text END) AS TransactionDate,
    MAX(CASE WHEN td.transactiondatetype = 'MaturityDate' THEN td.transactiondatevalue::text END) AS MaturityDate,
    MAX(CASE WHEN td.transactiondatetype = 'BeginningDate' THEN td.transactiondatevalue::text END) AS BeginningDate,

    -- Rates
    MAX(CASE WHEN tr.transactionratetype = 'ExchangeRate' THEN tr.transactionratevalue::numeric END) AS ExchangeRate,
    MAX(CASE WHEN tr.transactionratetype = 'InterestRate' THEN tr.transactionratevalue::numeric END) AS InterestRate,
    MAX(CASE WHEN tr.transactionratetype = 'RateType' THEN tr.transactionratevalue::numeric END) AS RateType,
    MAX(CASE WHEN tr.transactionratetype = 'ReferenceRate' THEN tr.transactionratevalue::numeric END) AS ReferenceRate,

    -- Prices
    --MAX(CASE WHEN tpz.transactionpricetype = 'OrderPrice' AND tpz.transactionpricesubtype = 'Interest' THEN tpz.transactionpricevalue::numeric END) AS Interest,
    --MAX(CASE WHEN tpz.transactionpricetype = 'Interest' THEN tpz.transactionpricevalue END) AS Interest,
    MAX(
        CASE 
            WHEN tc.transactionclassificationvalue IN ('Money Market Mutual Fund Activity', 'Deposit Activities') 
                THEN CASE 
                    WHEN tpz.transactionpricetype = 'OrderPrice' AND tpz.transactionpricesubtype = 'Interest' 
                    THEN tpz.transactionpricevalue::numeric 
                END
            ELSE CASE 
                WHEN tpz.transactionpricetype = 'Interest' 
                THEN tpz.transactionpricevalue::numeric 
            END
    END
    ) AS Interest,
    MAX(CASE WHEN tpz.transactionpricetype = 'OrderPrice' THEN tpz.transactionpricevalue END) AS OrderPrice,
    MAX(CASE WHEN tpz.transactionpricetype = 'Principal' THEN tpz.transactionpricevalue END) AS Principal,
    MAX(CASE WHEN tpz.transactionpricetype = 'MarketPrice' THEN tpz.transactionpricevalue END) AS MarketPrice,
    MAX(CASE WHEN tpz.transactionpricetype = 'BeginPrice' THEN tpz.transactionpricevalue END) AS BeginPrice,

    -- Fees
    MAX(CASE WHEN tfee.transactionfeetype = 'WithholdingTax' THEN tfee.transactionfeevalue::numeric END) AS WithholdingTax,

    -- Instruments
    TRIM(COALESCE(
        MAX(ti.TransactionInstrumentIdentifierValue) FILTER (
            WHERE ti.TransactionInstrumentIdentifierType IN ('Symbol', 'Sedol', 'CUSIP', 'ISIN', 'ADPNumber')
        ), ''
    )) AS ADPNumber,
    MAX(CASE WHEN ti.TransactionInstrumentIdentifierType = 'FundName' THEN ti.TransactionInstrumentIdentifierValue ELSE '' END) AS FundName,
    MAX(CASE WHEN ti.TransactionInstrumentIdentifierType = 'ShortName' THEN ti.TransactionInstrumentIdentifierValue ELSE '' END) AS ShortName,

    -- Narratives
    --STRING_AGG(DISTINCT CASE WHEN tn.transactionnarrativetype = 'MultiLineDescription' THEN TRIM(REGEXP_REPLACE(tn.transactionnarrativevalue, '\s+', ' ', 'g')) ELSE NULL END, '|') AS Description,
    --STRING_AGG(DISTINCT CASE WHEN tn.transactionnarrativetype = 'TransactionDescription' THEN TRIM(REGEXP_REPLACE(tn.transactionnarrativevalue, '\s+', ' ', 'g')) ELSE NULL END, '|') AS TransactionDescription,
    -- Description
    (
        SELECT STRING_AGG(val, '|') 
            FROM (
                SELECT DISTINCT
                    tn.transactionnarrativesequence,
                    TRIM(REGEXP_REPLACE(tn.transactionnarrativevalue, '\s+', ' ', 'g')) AS val
                FROM transactionnarrative tn
                WHERE tn.transactionid = ft.transactionid
                    AND tn.transactionnarrativetype = 'MultiLineDescription'
                ORDER BY tn.transactionnarrativesequence
            ) sub_desc
    ) AS Description,

    -- TransactionDescription
    (
        SELECT STRING_AGG(val, '|')
        FROM (
            SELECT DISTINCT
                tn.transactionnarrativesequence,
                CASE 
                    WHEN tn.transactionnarrativevalue = 'B' THEN 'Purchase'
                    WHEN tn.transactionnarrativevalue = 'S' THEN 'Sale'
                    ELSE TRIM(REGEXP_REPLACE(tn.transactionnarrativevalue, '\s+', ' ', 'g'))
                END AS val
            FROM transactionnarrative tn
            WHERE tn.transactionid = ft.transactionid
              AND tn.transactionnarrativetype = 'TransactionDescription'
            ORDER BY tn.transactionnarrativesequence
        ) sub_txn_desc
    ) AS TransactionDescription,
    
    MAX(CASE WHEN tn.transactionnarrativetype IN ('AdditionalNarrative') THEN tn.transactionnarrativevalue ELSE NULL END) AS AdditionalNarrative,
    MAX(CASE WHEN tn.transactionnarrativetype IN ('GeneralNarrative') THEN tn.transactionnarrativevalue ELSE NULL END) AS GeneralNarrative,

    -- Identifiers
    MAX(CASE WHEN tid.transactionidentifiertype IN ('BatchCode') THEN tid.transactionidentifiervalue ELSE NULL END) AS BatchCode,
    MAX(CASE WHEN tid.transactionidentifiertype IN ('EntryCode') THEN tid.transactionidentifiervalue ELSE NULL END) AS EntryCode
            
FROM 
    filtered_transactions ft
JOIN transactionevent te ON ft.transactionid = te.transactionid
JOIN transactionclassification tc ON ft.transactionid = tc.transactionid
LEFT JOIN transactionflag tf ON ft.transactionid = tf.transactionid
    AND (tf.transactionflagtype IN ('IsActivityRecord','IsHoldingRecord','IsSummaryRecord') AND tf.transactionflagvalue IN ('Yes'))
LEFT JOIN transactionamount ta ON ft.transactionid = ta.transactionid
    AND ta.transactionamounttype IN ('NetAmount', 'BeginMarketValue', 'ClosingBalance', 'MarketValue', 'CouponInterestAmount', 'NetCashBalance', 'EstimatedAnnualIncome')
LEFT JOIN transactionquantity tq ON ft.transactionid = tq.transactionid
    AND tq.transactionquantitytype IN ('PrimaryQuantity', 'CurrentFaceQuantity', 'OutstandingStockQuantity', 'UnderlyingQuantity', 'TotalOrderQuantity', 'CurrentQuantity', 'BeginQuantity')
LEFT JOIN transactiondate td ON ft.transactionid = td.transactionid
    AND td.transactiondatetype IN ('SettlementDate', 'TransactionDate', 'MaturityDate', 'BeginningDate')
LEFT JOIN transactionprice tpz ON ft.transactionid = tpz.transactionid
    AND tpz.transactionpricetype IN ('OrderPrice', 'Principal', 'Interest', 'MarketPrice', 'BeginPrice')
LEFT JOIN transactioninstrument ti ON ft.transactionid = ti.transactionid
    AND ti.TransactionInstrumentIdentifierType IN ('ADPNumber', 'ShortName', 'Symbol', 'CUSIP', 'Sedol', 'ISIN', 'FundName')
LEFT JOIN transactionrate tr ON ft.transactionid = tr.transactionid
    AND tr.transactionratetype IN ('InterestRate', 'ExchangeRate', 'RateType', 'ReferenceRate')
LEFT JOIN transactionnarrative tn ON ft.transactionid = tn.transactionid
    AND tn.transactionnarrativetype IN ('TransactionDescription', 'MultiLineDescription', 'AdditionalNarrative', 'GeneralNarrative')
LEFT JOIN transactionidentifier tid ON ft.transactionid = tid.transactionid
    AND tid.transactionidentifiertype IN ('BatchCode', 'EntryCode')   
LEFT JOIN transactionfee tfee ON ft.transactionid = tfee.transactionid 
        AND tfee.transactionfeetype IN ('WithholdingTax')       
GROUP BY ft.partyid, ft.transactionid;

-- STEP 2: Aggregate total per section
CREATE TEMP TABLE temp_total_data_1 ON COMMIT DROP AS
SELECT
    MAX(partyid) AS partyid,
    MAX(transactioncategory) AS transactioncategory,
    clientid,
    MAX(ADPNumber) AS ADPNumber,
    MAX(CASE WHEN sectionheader = 'Multi-currency Account Net Equity' THEN GeneralNarrative ELSE NULL END) AS GeneralNarrative,    
    transactionamountcurrency,    
    sectiontype,
    sectionheader,
    
    SUM(CASE WHEN MarketValue IS NOT NULL AND MarketValue != 0 THEN MarketValue ELSE NetAmount END) AS AssetClassTotalValue,
    SUM(CASE WHEN EstimatedAnnualIncome IS NOT NULL AND EstimatedAnnualIncome != 0 THEN EstimatedAnnualIncome ELSE 0 END) AS TotalEstimatedAnnualIncome,
    SUM(CASE WHEN CouponInterestAmount IS NOT NULL THEN CouponInterestAmount ELSE 0 END) AS AccruedCouponInterest,
    SUM(CASE WHEN BeginMarketValue IS NOT NULL THEN BeginMarketValue ELSE 0 END) AS AssetClassTotalBeginningValue,
    MAX(CASE WHEN ClosingBalance IS NOT NULL THEN ClosingBalance ELSE 0 END) AS ClosingBalance,
    SUM(CASE WHEN NetAmountCredit IS NOT NULL THEN NetAmountCredit ELSE 0 END) AS TotalCredit,
    SUM(CASE WHEN NetAmountDebit IS NOT NULL THEN NetAmountDebit ELSE 0 END) AS TotalDebit,
    SUM(CASE WHEN Interest IS NOT NULL THEN Interest ELSE 0 END) AS AccruedInterest,
    
    MAX(CASE WHEN PrimaryQuantity IS NOT NULL THEN PrimaryQuantity ELSE 0 END) AS PrincipalBalanceTotal,
    MAX(CASE WHEN TotalOrderQuantity IS NOT NULL THEN TotalOrderQuantity ELSE 0 END) AS TotalOrderQuantity,
    SUM(CASE WHEN PrimaryQuantity IS NOT NULL THEN PrimaryQuantity ELSE 0 END) AS TotalNumberofSecurities,
    SUM(CASE WHEN CurrentFaceQuantity IS NOT NULL THEN CurrentFaceQuantity ELSE 0 END) AS TotalCurrentFaceQuantity,
    SUM(CASE WHEN OutstandingStockQuantity IS NOT NULL THEN OutstandingStockQuantity ELSE 0 END) AS TotalOutstandingStockQuantity,

    SUM(CASE WHEN sectionheader = 'Interest' AND BatchCode IN ('XC','XN','VC','VN') AND EntryCode = 'INT' THEN NetAmount ELSE 0 END) AS IncomeSummaryStockLoanRebate,
    SUM(CASE WHEN sectionheader = 'Interest' AND BatchCode IN ('PB','QN') AND EntryCode = 'INT' THEN NetAmount ELSE 0 END) AS IncomeSummaryMarginInterest,
    SUM(CASE WHEN sectionheader = 'Interest' AND BatchCode = 'QQ' AND (EntryCode = 'INT' OR EntryCode IS NULL OR TRIM(EntryCode) = '') THEN NetAmount ELSE 0 END) AS IncomeSummaryDebitInterest,
    SUM(CASE WHEN sectionheader = 'Withholdings' AND transactionamountcurrency = 'USD' THEN NetAmount ELSE 0 END) AS IncomeSummaryUSTaxWithheldUSD,
    SUM(CASE WHEN sectionheader = 'Withholdings' AND transactionamountcurrency <> 'USD' THEN NetAmount ELSE 0 END) AS IncomeSummaryUSTaxWithheldNonUSD,
    SUM(CASE WHEN sectionheader = 'Other Income' THEN NetAmount ELSE 0 END) AS IncomeSummaryOther,
    
    SUM(CASE WHEN sectionheader = 'Multi-currency Account Net Equity' THEN NetCashBalance ELSE 0 END) AS NetCashBalance,
    MAX(CASE WHEN sectionheader = 'Multi-currency Account Net Equity' THEN ExchangeRate ELSE 0 END) AS ExchangeRate

FROM temp_base_data_1
WHERE transactionamountcurrency IS NOT NULL AND sectionheader NOT IN ('Statement of Billing Fees Collected - This is Not A Bill', 'Money Market Mutual Fund Activity', 'Deposit Activities')
GROUP BY clientid, sectiontype, sectionheader, transactionamountcurrency;

-- Total for Money Market Mutual Fund Activity and Deposit Activities sections
CREATE TEMP TABLE temp_total_data_2 ON COMMIT DROP AS
SELECT
    MAX(partyid) AS partyid,
    MAX(transactioncategory) AS transactioncategory,
    MAX(clientid) AS clientid,
    MAX(ADPNumber) AS ADPNumber,
    MAX(ShortName) AS ShortName,
    MAX(InterestRate) AS InterestRate,
    transactionamountcurrency,
    sectiontype,
    sectionheader,
    MAX(CASE WHEN BeginMarketValue IS NULL OR BeginMarketValue = 0 THEN 0 ELSE BeginMarketValue END) AS BeginningBalanceMarketValue,
    MAX(CASE WHEN ClosingBalance IS NULL OR ClosingBalance = 0 THEN 0 ELSE ClosingBalance END) AS EndingBalanceMarketValue,
    SUM(CASE WHEN NetAmount IS NULL OR NetAmount = 0 THEN 0 ELSE NetAmount END) AS AssetClassTotalValue,
    SUM(CASE WHEN Interest IS NULL OR Interest = 0 THEN 0 ELSE Interest END) AS EndingBalanceInterest,
    MAX(CASE WHEN OrderPrice IS NULL OR OrderPrice = 0 THEN 0 ELSE OrderPrice END) AS OrderPrice,
    MAX(CASE WHEN BeginPrice IS NULL OR BeginPrice = 0 THEN 0 ELSE BeginPrice END) AS BeginPrice,
    MAX(CASE WHEN PrimaryQuantity IS NULL OR PrimaryQuantity = 0 THEN 0 ELSE PrimaryQuantity END) AS BeginningBalancePrincipalBalance,
    MAX(CASE WHEN TotalOrderQuantity IS NULL OR TotalOrderQuantity = 0 THEN 0 ELSE TotalOrderQuantity END) AS EndingBalancePrincipalBalance,
    SUM(
        CASE 
            WHEN sectionheader = 'Money Market Mutual Fund Activity' 
                AND MarketValue > 0
                AND NOT (BatchCode IN ('OT') AND EntryCode IN ('MMR', 'MRE', 'MRC'))
            THEN MarketValue ELSE 0 
        END
    ) AS MMMFSummaryDepandAdditions,
    SUM(
        CASE 
            WHEN sectionheader = 'Money Market Mutual Fund Activity' 
                AND MarketValue < 0
                AND NOT (BatchCode IN ('OT') AND EntryCode IN ('MMR', 'MRE', 'MRC'))
            THEN MarketValue ELSE 0 
        END
    ) AS MMMFSummaryDistandSubtractions,
    SUM(
        CASE 
            WHEN sectionheader = 'Money Market Mutual Fund Activity' 
                AND ((BatchCode IN ('OT')) AND (EntryCode IN ('MMR', 'MRE', 'MRC')))
            THEN MarketValue ELSE 0 
        END
    ) AS MMMFSummaryDivReinvested      
FROM 
    temp_base_data_1 bd
WHERE 
    transactionamountcurrency IS NOT NULL AND sectionheader IN ('Money Market Mutual Fund Activity', 'Deposit Activities')
GROUP BY 
    sectiontype, sectionheader, transactionamountcurrency, ADPNumber;

-- Create temp table for TransactionDataWithCurrentQuantity
CREATE TEMP TABLE temp_transaction_with_quantity_2 ON COMMIT DROP AS
SELECT 
    td.*, 
    ROW_NUMBER() OVER (PARTITION BY td.partyid, td.transactionamountcurrency, td.ADPNumber ORDER BY td.SettlementDate) AS row_num,
    CASE 
        WHEN ROW_NUMBER() OVER (PARTITION BY td.partyid, td.transactionamountcurrency, td.ADPNumber ORDER BY td.SettlementDate) = 1
        THEN td.PrimaryQuantity + td.UnderlyingQuantity
        ELSE 
            SUM(td.UnderlyingQuantity) OVER (PARTITION BY td.partyid, td.transactionamountcurrency, td.ADPNumber ORDER BY td.SettlementDate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) + td.PrimaryQuantity
    END AS CurrentQuantity1
FROM temp_base_data_1 td
WHERE transactionamountcurrency IS NOT NULL AND sectionheader IN ('Money Market Mutual Fund Activity', 'Deposit Activities');

-- Total for Statement of Billing Fees Collected - This is Not A Bill section
CREATE TEMP TABLE temp_total_data_3 ON COMMIT DROP AS
SELECT
    MAX(partyid) AS partyid,
    MAX(clientid) AS clientid,
    MAX(transactioncategory) AS transactioncategory,
    MAX(GeneralNarrative) AS GeneralNarrative,
    transactionamountcurrency,
    sectiontype,
    sectionheader,
    SUM(CASE WHEN MarketValue IS NOT NULL THEN MarketValue ELSE 0 END) AS AssetClassTotalValue
FROM temp_base_data_1
WHERE transactionamountcurrency IS NOT NULL AND sectionheader = 'Statement of Billing Fees Collected - This is Not A Bill'
GROUP BY sectiontype, sectionheader, transactionamountcurrency, GeneralNarrative;

-- STEP 3: Insert detail and total records into temp_extraction_results
INSERT INTO temp_extraction_results (
    partyid, clientid, currency, sectiontype, sectionheader, recordtype, securitygroup, row_num, date, description, data
)
SELECT 
    partyid,
    clientid,
    transactionamountcurrency,
    sectiontype,
    sectionheader,
    'Detail',
    ADPNumber,
    NULL::BIGINT AS row_num,
    --CASE 
      --  WHEN sectionheader IN (
        --    'Repurchase Agreements',
        --    'Pledge Detail Report'
        --) THEN ADPNumber
        --ELSE NULL
    --END AS securitygroup,
    CASE 
        WHEN sectionheader IN (
            'Fixed Income - Long Positions',
            'Fixed Income - Short Positions',
            'Other - Long Positions',
            'Other - Short Positions',
			'Fixed Income - Securities Loan',
			'Fixed Income - Securities Borrow',
			'Other - Securities Loan',
			'Other - Securities Borrow',
			'Pledge Detail Report'
        ) THEN MaturityDate::DATE
        
        WHEN sectionheader IN (
                'Purchase and Sale',
                'Pending Settlements',
                'Repurchase Agreement Activity',
                'Dividends',
                'Interest',
                'Other Income',
                'Withholdings',
                'Funds Paid and Received',
                'Mark-to-Market'
            ) THEN SettlementDate::DATE

        WHEN sectionheader = 'Other Activity' THEN TransactionDate::DATE    
        ELSE NULL
    END AS date,
    Description,
    jsonb_strip_nulls(jsonb_build_object(
                        'partyid', partyid,
                        'clientid', clientid,
                        'transactionid', transactionid, 
                        'transactioncategory', transactioncategory,
                        'transactionamountcurrency', transactionamountcurrency,
                        'sectiontype', sectiontype,
                        'sectionheader', sectionheader,
                        'recordtype', 'Detail',

                        'NetAmount', COALESCE(NetAmount, 0),
                        'MarketValue', MarketValue,
                        'EstimatedAnnualIncome', EstimatedAnnualIncome,
                        'BeginMarketValue',BeginMarketValue,
                        'CouponInterestAmount', CouponInterestAmount,
                        'ClosingBalance', ClosingBalance, 
                        'NetAmountCredit', NetAmountCredit,
                        'NetAmountDebit', NetAmountDebit,
                        'NetCashBalance', NetCashBalance,

                        'PrimaryQuantity', PrimaryQuantity,
                        'CurrentFaceQuantity', CurrentFaceQuantity,
                        'UnderlyingQuantity', UnderlyingQuantity,
                        'OutstandingStockQuantity', OutstandingStockQuantity,
                        'TotalOrderQuantity', TotalOrderQuantity,
                        'CurrentQuantity', CurrentQuantity,
                        'BeginQuantity', BeginQuantity,
      
                        'OrderPrice', OrderPrice,
                        'Principal', Principal,
                        'MarketPrice', MarketPrice,      
                        'BeginPrice', BeginPrice,    
						'Interest', Interest,
                        
                        'SettlementDate', SettlementDate,
                        'MaturityDate', MaturityDate,
                        'TransactionDate', TransactionDate,
                        'BeginningDate', BeginningDate,
        
                        'FundName', FundName,
                        'ADPNumber', ADPNumber,
      
                        'ExchangeRate', ExchangeRate,
                        'RateType', RateType, 
      
                        'Description', Description,
                        'TransactionDescription', TransactionDescription,
                        'AdditionalNarrative', AdditionalNarrative,
                        'GeneralNarrative', GeneralNarrative,
        
                        'BatchCode', BatchCode,
                        'EntryCode', EntryCode
        -- Add more fields as needed...
    ))
FROM temp_base_data_1
WHERE sectionheader NOT IN ('Statement of Billing Fees Collected - This is Not A Bill', 'Money Market Mutual Fund Activity', 'Deposit Activities')

UNION ALL

SELECT 
    partyid,
    clientid,
    transactionamountcurrency,
    sectiontype,
    sectionheader,
    'Total',
    NULL,
    NULL::BIGINT AS row_num,
    NULL,
    NULL,
    jsonb_strip_nulls(jsonb_build_object(
                        'partyid', partyid,
                        'clientid', clientid,
                        'transactioncategory', transactioncategory,
                        'transactionamountcurrency', transactionamountcurrency,
                        'sectiontype', sectiontype,
                        'sectionheader', sectionheader,
                        'recordtype', 'Total',      
                        
                        'AssetClassTotalValue', AssetClassTotalValue,
                        'TotalEstimatedAnnualIncome', TotalEstimatedAnnualIncome,
                        'AssetClassTotalBeginningValue', AssetClassTotalBeginningValue,
                        'TotalCredit', TotalCredit,
                        'TotalDebit', TotalDebit,
                        'AccruedCouponInterest', AccruedCouponInterest,
                        'IncomeSummaryStockLoanRebate', IncomeSummaryStockLoanRebate,
                        'IncomeSummaryMarginInterest', IncomeSummaryMarginInterest,
                        'IncomeSummaryDebitInterest', IncomeSummaryDebitInterest,
                        'IncomeSummaryUSTaxWithheldUSD', IncomeSummaryUSTaxWithheldUSD,
                        'IncomeSummaryUSTaxWithheldNonUSD', IncomeSummaryUSTaxWithheldNonUSD,
                        'IncomeSummaryOther', IncomeSummaryOther,
                        'NetCashBalance', NetCashBalance,
        
                        'TotalNumberofSecurities', TotalNumberofSecurities,
                        'TotalCurrentFaceQuantity', TotalCurrentFaceQuantity,
                        'TotalOutstandingStockQuantity', TotalOutstandingStockQuantity,

                        'ExchangeRate', ExchangeRate,
                        'GeneralNarrative', GeneralNarrative
        -- Add more as needed...
    ))
FROM temp_total_data_1

UNION ALL

SELECT 
    partyid,
	clientid,
	transactionamountcurrency,
	sectiontype,
	sectionheader,
	'Detail',
    ADPNumber,
    row_num,
    SettlementDate::DATE,
    NULL,
    jsonb_strip_nulls(jsonb_build_object(
        'partyid', partyid,
        'clientid', clientid,
        'transactionid', transactionid, 
        'transactioncategory', transactioncategory,
        'transactionamountcurrency', transactionamountcurrency,
        'sectiontype', sectiontype,
        'sectionheader', sectionheader,
        'recordtype', recordtype,
        'NetAmount', COALESCE(NetAmount, 0),
        'MarketValue', MarketValue,
        'EstimatedAnnualIncome', EstimatedAnnualIncome,
        'UnderlyingQuantity', UnderlyingQuantity,
        'CurrentQuantity', CurrentQuantity1,
        'Interest', Interest,
        'SettlementDate', SettlementDate,
        'ADPNumber', ADPNumber,
        'SecurityGroup', ADPNumber,
        'ShortName', ShortName,
        'InterestRate', InterestRate,                 
        'TransactionDescription', TransactionDescription,
        'WithholdingTax', WithholdingTax,
        'BatchCode', BatchCode,
        'EntryCode', EntryCode,
        'row_num', row_num
    ))
FROM temp_transaction_with_quantity_2
WHERE transactionamountcurrency IS NOT NULL

UNION ALL

SELECT 
    partyid,
	clientid,
	transactionamountcurrency,
	sectiontype,
	sectionheader,
	'Detail',
    ADPNumber,
    row_num,
    SettlementDate::DATE,
    NULL,
    jsonb_strip_nulls(jsonb_build_object(
        'partyid', partyid,
        'clientid', clientid,
        'transactionid', transactionid, 
        'transactioncategory', transactioncategory,
        'transactionamountcurrency', transactionamountcurrency,
        'sectiontype', sectiontype,
        'sectionheader', sectionheader,
        'recordtype', recordtype,
        'NetAmount', 0,
        'MarketValue', WithholdingTax,
        'UnderlyingQuantity', 0,
        'CurrentQuantity', CurrentQuantity1,
        'Interest', 0,
        'SettlementDate', SettlementDate,
        'ADPNumber', ADPNumber,
        'SecurityGroup', ADPNumber,
        'ShortName', ShortName,
        'InterestRate', InterestRate,                 
        'TransactionDescription', 'Withholding',
        'WithholdingTax', WithholdingTax,
        'BatchCode', BatchCode,
        'EntryCode', EntryCode,
        'row_num', row_num
    ))
FROM temp_transaction_with_quantity_2
WHERE transactionamountcurrency IS NOT NULL AND WithholdingTax IS NOT NULL AND WithholdingTax != 0

UNION ALL
	
SELECT 
    partyid,
	clientid,
	transactionamountcurrency,
	sectiontype,
	sectionheader,
	'Total',
    ADPNumber,
    NULL::BIGINT AS row_num,
    NULL,
    NULL,
    jsonb_strip_nulls(jsonb_build_object(
        'partyid', partyid,
        'clientid', clientid,
        'transactioncategory', transactioncategory,
        'transactionamountcurrency', transactionamountcurrency,
        'sectiontype', sectiontype,
        'sectionheader', sectionheader,
        'recordtype', 'Head',  -- Indicating this row is for total
        'ADPNumber', ADPNumber,
        'SecurityGroup', ADPNumber,
        'ShortName', ShortName,
        'InterestRate', InterestRate,
        'BeginPrice', BeginPrice,
        'OrderPrice', OrderPrice
    ))
FROM temp_total_data_2

UNION ALL

SELECT 
    partyid,
	clientid,
	transactionamountcurrency,
	sectiontype,
	sectionheader,
	'Total',
    ADPNumber,
    NULL::BIGINT AS row_num,
    NULL,
    NULL,
    jsonb_strip_nulls(jsonb_build_object(
        'partyid', partyid,
        'clientid', clientid,
        'transactioncategory', transactioncategory,
        'transactionamountcurrency', transactionamountcurrency,
        'sectiontype', sectiontype,
        'sectionheader', sectionheader,
        'recordtype', 'Begin',
        'ADPNumber', ADPNumber,
        'SecurityGroup', ADPNumber,
        'BeginningBalanceMarketValue', BeginningBalanceMarketValue,
        'BeginningBalancePrincipalBalance', BeginningBalancePrincipalBalance
    ))
FROM temp_total_data_2

UNION ALL

SELECT 
    partyid,
	clientid,
	transactionamountcurrency,
	sectiontype,
	sectionheader,
	'Total',
    ADPNumber,
    NULL::BIGINT AS row_num,
    NULL,
    NULL,
    jsonb_strip_nulls(jsonb_build_object(
        'partyid', partyid,
        'clientid', clientid,
        'transactioncategory', transactioncategory,
        'transactionamountcurrency', transactionamountcurrency,
        'sectiontype', sectiontype,
        'sectionheader', sectionheader,
        'recordtype', 'End',
        'ADPNumber', ADPNumber,
        'SecurityGroup', ADPNumber,
        'EndingBalanceMarketValue', EndingBalanceMarketValue,
		'AssetClassTotalValue', AssetClassTotalValue,
        'EndingBalancePrincipalBalance', EndingBalancePrincipalBalance,
        'EndingBalanceInterest', EndingBalanceInterest
    ))
FROM temp_total_data_2

UNION ALL

SELECT 
    partyid,
	clientid,
	transactionamountcurrency,
	sectiontype,
	sectionheader,
	'Total',
    ADPNumber,
    NULL::BIGINT AS row_num,
    NULL,
    NULL,
    jsonb_strip_nulls(jsonb_build_object(
        'partyid', partyid,
        'clientid', clientid,
        'transactioncategory', transactioncategory,
        'transactionamountcurrency', transactionamountcurrency,
        'sectiontype', sectiontype,
        'sectionheader', sectionheader,
        'recordtype', 'Total',
        'ADPNumber', ADPNumber,
        'SecurityGroup', ADPNumber,
        'IncomeSummaryMoneyMarketMutualFundDividends', AssetClassTotalValue,
        'MMMFSummaryDepandAdditions', MMMFSummaryDepandAdditions,
        'MMMFSummaryDistandSubtractions', MMMFSummaryDistandSubtractions,
        'MMMFSummaryDivReinvested', MMMFSummaryDivReinvested,
        'EndingBalanceInterest', EndingBalanceInterest
    ))
FROM temp_total_data_2

UNION ALL

SELECT 
    partyid,
    clientid,
    transactionamountcurrency,
    sectiontype,
    sectionheader,
    'Detail',
    GeneralNarrative,
    NULL::BIGINT AS row_num,
    TransactionDate::DATE,
    Description,
    jsonb_strip_nulls(jsonb_build_object(
        'partyid', partyid,
        'clientid', clientid,
        'transactionid', transactionid, 
        'transactioncategory', transactioncategory,
        'transactionamountcurrency', transactionamountcurrency,
        'sectiontype', sectiontype,
        'sectionheader', sectionheader,
        'recordtype', 'Detail',
        'MarketValue', MarketValue,
        'PrimaryQuantity', PrimaryQuantity,
        'TransactionDate', TransactionDate,
        'ReferenceRate', ReferenceRate,
        'Description', Description,
        'GeneralNarrative', GeneralNarrative,
        'SecurityGroup', GeneralNarrative,
        'BatchCode', BatchCode,
        'EntryCode', EntryCode
    ))
FROM temp_base_data_1
where sectionheader = 'Statement of Billing Fees Collected - This is Not A Bill'

UNION ALL

-- Head Rows
SELECT 
    partyid,
    clientid,
    transactionamountcurrency,
    sectiontype,
    sectionheader,
    'Total',
    GeneralNarrative,
    NULL::BIGINT AS row_num,
    NULL,
    NULL,
    jsonb_strip_nulls(jsonb_build_object(
        'partyid', partyid,
        'clientid', clientid,
        'transactioncategory', transactioncategory,
        'transactionamountcurrency', transactionamountcurrency,
        'sectiontype', sectiontype,
        'sectionheader', sectionheader,
        'recordtype', 'Head',
        'GeneralNarrative', GeneralNarrative,
        'SecurityGroup', GeneralNarrative
    ))
FROM temp_total_data_3

UNION ALL

-- Total Rows
SELECT 
    partyid,
    clientid,
    transactionamountcurrency,
    sectiontype,
    sectionheader,
    'Total',
    GeneralNarrative,
    NULL::BIGINT AS row_num,
    NULL,
    NULL,
    jsonb_strip_nulls(jsonb_build_object(
        'partyid', partyid,
        'clientid', clientid,
        'transactioncategory', transactioncategory,
        'transactionamountcurrency', transactionamountcurrency,
        'sectiontype', sectiontype,
        'sectionheader', sectionheader,
        'recordtype', 'Total',
        'GeneralNarrative', GeneralNarrative,
        'SecurityGroup', GeneralNarrative,
        'AssetClassTotalValue', AssetClassTotalValue
    ))
FROM temp_total_data_3;

-- Return the result
RETURN null;
END;

$$ LANGUAGE plpgsql;