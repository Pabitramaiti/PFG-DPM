-- FUNCTION: public.extract_position_details(text, text, text, text, text)

-- DROP FUNCTION IF EXISTS public.extract_position_details(text, text, text, text, text);

CREATE OR REPLACE FUNCTION public.extract_position_details(
	p_identifier_value text,
	clientid text,
	origin text,
	sectiontype text,
	sectionheader text)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
BEGIN
    WITH detail_row AS (
        SELECT DISTINCT
            pp.positionid,
            pamount.positionamountcurrency,
                        --PositionCommon
                        (CASE
                            WHEN pcommon.positionid IS NOT NULL
                            THEN jsonb_agg(
                                DISTINCT jsonb_build_object(
                                    'PositionInstrumentIdentifierType', positioninstrumentidentifiertype,
                                    'PositionInstrumentIdentifierValue', positioninstrumentidentifiervalue,
                                    'PositionQuantityLong', positionquantitylong,
                                                                        'PositionQuantityShort',positionquantityshort,
                                                                        'PositionFromDate', TO_CHAR(positionfromdate,'YYYY-MM-DD HH:MI:SS')
                                )
                            )
                        END) AS positioncommon_json,

                        -- InstrumentAssetClass
                        (CASE
                            WHEN instassetc.instrumentid IS NOT NULL
                            THEN jsonb_agg(
                                DISTINCT jsonb_build_object(
                                    'InstrumentAssetClassType', instrumentassetclasstype,
                                                        'InstrumentAssetClassValue', instrumentassetclassvalue,
                                                                        'InstrumentAssetClassGroup', instrumentassetclassgroup
                                )
                            )
                        END) AS instrumentassetclass_json,

                    -- InstrumentIdentifier
                        (CASE
                            WHEN instiden.instrumentid IS NOT NULL
                                THEN jsonb_agg(
                                            DISTINCT jsonb_build_object(
                                                        'PositionInstrumentIdentifierType',instiden.instrumentidentifiertype,
                                                        'PositionInstrumentIdentifierValue',instiden.instrumentidentifiervalue
                                                )
                                            )
                                END) AS instrumentidentifier_json,

                    -- PositionNarrative
                        (CASE
                            WHEN pnarr.positionid IS NOT NULL
                            THEN jsonb_agg(
                                DISTINCT jsonb_build_object(
                                    'PositionNarrativeType', pnarr.positionnarrativetype,
                                    'PositionNarrativeSequence', pnarr.positionnarrativesequence,
                                    'PositionNarrativeValue', pnarr.positionnarrativevalue
                                )
                            )
                        END) AS positionnarrative_json,

                        -- PositionAmount
                        (CASE
                                            WHEN pamount.positionid IS NOT NULL
                                                THEN jsonb_agg(
                                                    DISTINCT jsonb_build_object(
                                                            'PositionAmountType', pamount.positionamounttype,
                                                            'PositionAmountValue', pamount.positionamountvalue,
                                                            'PositionAmountCurrency', pamount.positionamountcurrency
                                                            )
                                                    )
                                            END) AS positionamount_json,

                        -- PositionFlag
                        (CASE
                                            WHEN pflag.positionid IS NOT NULL
                                                THEN jsonb_agg(
                                                    DISTINCT jsonb_build_object(
                                                            'PositionFlagType', pflag.positionflagtype,
                                                            'PositionFlagValue', pflag.positionflagvalue
                                                            )
                                                    )
                                            END) AS positionflag_json

                FROM positioncommon pcommon
                JOIN positionparty pp ON pcommon.positionid = pp.positionid
                JOIN instrument instru ON
                instru.instrumentidentifiertype = pcommon.positioninstrumentidentifiertype
                AND instru.instrumentidentifiervalue = pcommon.positioninstrumentidentifiervalue
                JOIN instrumentassetclass instassetc ON
                instassetc.instrumentid = instru.instrumentid
                AND instassetc.instrumentassetclasstype = sectiontype
                AND instassetc.instrumentassetclassvalue = sectionheader
                LEFT JOIN positionamount pamount ON pamount.positionid = pcommon.positionid
                LEFT JOIN positionnarrative pnarr ON pnarr.positionid = pcommon.positionid
                LEFT JOIN instrumentidentifier instiden ON instru.instrumentid = instiden.instrumentid
                AND instiden.instrumentidentifiertype IN ('CUSIP','Symbol','ADPNumber')
                AND instiden.instrumentidentifiervalue <> 'N/A'
                LEFT JOIN positionflag pflag ON pflag.positionid = pcommon.positionid

                WHERE pp.positionpartyidentifiervalue = p_identifier_value
                AND pcommon.positionclientidentifier = clientid
                AND pcommon.positionorigin = origin

                GROUP BY pp.positionid,
                    pamount.positionamountcurrency,
                    pcommon.positionid,
            instassetc.instrumentid,
            pnarr.positionid,
            pamount.positionid,
            instiden.instrumentid,
            instru.instrumentid,
            pflag.positionid

    ),

    total_row_asset_class AS (
        SELECT
        instrumentassetclass_json AS total_row_asset_class_json

    FROM detail_row
        Fetch first 1 row only

    ),

    -- 2. Aggregate total NetAmount, grouped by transactionclassificationvalue and transactionamountcurrency
    total_data_amount AS (
        SELECT

            instrumentassetclasstype,
            instrumentassetclassvalue,
            pamount.positionamountcurrency,

            -- TotalSharePrice only for Mutual funds and alternative investments
            SUM(
                CASE
                    WHEN positionamounttype = 'SharePrice'
                                                 AND instrumentassetclassvalue IN ('Mutual Funds','Alternative Investments')
                    THEN positionamountvalue
                    ELSE NULL
                END
            ) AS TotalSharePrice,

            -- TotalMarketValue
            SUM(
                CASE
                    WHEN positionamounttype = 'MarketValue'
                    THEN positionamountvalue
                    ELSE NULL
                END
            ) AS TotalMarketValue,

                        -- TotalCostBasis is not for Mutual funds and alternative investments
            SUM(
                CASE
                    WHEN positionamounttype = 'CostBasis'
                                                 AND pflag.positionflagvalue <> 'N'
                    THEN positionamountvalue
                    ELSE NULL
                END
                        ) AS TotalCostBasis,

            -- TotalInterestAccrued
            SUM(
                CASE
                    WHEN positionamounttype = 'AccruedInterest'
                    THEN positionamountvalue
                    ELSE NULL
                END
            ) AS TotalInterestAccrued,

                        -- TotalEstAnnualIncome
            SUM(
                CASE
                    WHEN positionamounttype = 'EstAnnualIncome'
                    THEN positionamountvalue
                    ELSE NULL
                END
            ) AS TotalEstAnnualIncome,

            -- TotalEstYield
            SUM(
                CASE
                    WHEN positionamounttype = 'EstYield'
                    THEN positionamountvalue
                    ELSE NULL
                END
            ) AS TotalEstYield,

            -- TotalUnrealizedGainLoss
            SUM(
                CASE
                    WHEN positionamounttype = 'UnrealizedGainLoss'
                                             AND pflag.positionflagvalue <> 'N'
                    THEN positionamountvalue
                    ELSE NULL
                END
            ) AS TotalUnrealizedGainLoss

        FROM positioncommon pcommon
                JOIN positionparty pp ON pcommon.positionid = pp.positionid
                JOIN instrument instru ON
                instru.instrumentidentifiertype = pcommon.positioninstrumentidentifiertype
                AND instru.instrumentidentifiervalue = pcommon.positioninstrumentidentifiervalue
                JOIN instrumentassetclass instassetc ON
                instassetc.instrumentid = instru.instrumentid
                AND instassetc.instrumentassetclasstype = sectiontype
                AND instassetc.instrumentassetclassvalue = sectionheader

                LEFT JOIN positionamount pamount ON pamount.positionid = pcommon.positionid
                LEFT JOIN positionflag pflag ON pflag.positionid = pcommon.positionid
                AND pflag.positionflagtype = 'IsCostBasis'

                WHERE pp.positionpartyidentifiervalue = p_identifier_value
                AND pcommon.positionclientidentifier = clientid
                AND pcommon.positionorigin = origin
                GROUP BY pamount.positionamountcurrency,
                instassetc.instrumentassetclasstype,
                instassetc.instrumentassetclassvalue
    ),

    -- 4. Create the total JSON separately
    total_row_amount AS (
        SELECT
            positionamountcurrency,
            jsonb_path_query_array(jsonb_build_array(
                            CASE
                                                                WHEN TotalSharePrice IS NOT NULL
                                                                THEN jsonb_strip_nulls(
                                jsonb_build_object(
                                    'PositionAmountType', 'TotalSharePrice',
                                    'PositionAmountValue', TotalSharePrice,
                                    'PositionAmountCurrency', positionamountcurrency
                                )
                            )
                                                        END
                                                        ,
                            CASE
                                                                WHEN TotalMarketValue IS NOT NULL
                                                                THEN jsonb_strip_nulls(
                                jsonb_build_object(
                                    'PositionAmountType', 'TotalMarketValue',
                                    'PositionAmountValue', TotalMarketValue,
                                    'PositionAmountCurrency', positionamountcurrency
                                )
                            )
                                                        END
                                                        ,
                            CASE
                                                                WHEN TotalCostBasis IS NOT NULL
                                                                THEN jsonb_strip_nulls(
                                jsonb_build_object(
                                    'PositionAmountType', 'TotalCostBasis',
                                    'PositionAmountValue', TotalCostBasis,
                                    'PositionAmountCurrency', positionamountcurrency
                                )
                            )
                                                        END
                                                        ,
                            CASE
                                                                WHEN TotalInterestAccrued IS NOT NULL
                                                                THEN jsonb_strip_nulls(
                                jsonb_build_object(
                                    'PositionAmountType', 'TotalInterestAccrued',
                                    'PositionAmountValue', TotalInterestAccrued,
                                    'PositionAmountCurrency', positionamountcurrency
                                )
                            )
                                                        END
                                                        ,
                            CASE
                                                                WHEN TotalEstAnnualIncome IS NOT NULL
                                                                THEN jsonb_strip_nulls(
                                jsonb_build_object(
                                    'PositionAmountType', 'TotalEstAnnualIncome',
                                    'PositionAmountValue', TotalEstAnnualIncome,
                                    'PositionAmountCurrency', positionamountcurrency
                                )
                            )
                                                        END
                                                        ,
                            CASE
                                                                WHEN TotalEstYield IS NOT NULL
                                                                THEN jsonb_strip_nulls(
                                jsonb_build_object(
                                    'PositionAmountType', 'TotalEstYield',
                                    'PositionAmountValue', TotalEstYield,
                                    'PositionAmountCurrency', positionamountcurrency
                                )
                            )
                                                        END
                                                        ,
                            CASE
                                                                WHEN TotalUnrealizedGainLoss IS NOT NULL
                                                                THEN jsonb_strip_nulls(
                                jsonb_build_object(
                                    'PositionAmountType', 'TotalUnrealizedGainLoss',
                                    'PositionAmountValue', TotalUnrealizedGainLoss,
                                    'PositionAmountCurrency', positionamountcurrency
                                )
                            )
                                                        END
                                                ),'$[*] ? (@ != null)'
                ) AS total_row_amount_json
        FROM total_data_amount
    )

-- 5. Final combination of detail rows and total rows
INSERT INTO temp_extraction_results_position(
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
    positionamountcurrency,
    sectiontype,
    sectionheader,
        'Detail' AS recordtype,
        jsonb_strip_nulls(jsonb_build_object(
                'PositionId', dr.positionid,
            'PositionCommon', dr.positioncommon_json,
                'InstrumentAssetClass', dr.instrumentassetclass_json,
                'InstrumentIdentifier', dr.instrumentidentifier_json,
                'PositionNarrative', dr.positionnarrative_json,
                'PositionAmount', dr.positionamount_json,
                'PositionFlag', dr.positionflag_json

        )) AS combined_json
FROM detail_row dr

UNION
SELECT
    p_identifier_value,
    clientid,
        origin,
    positionamountcurrency,
    sectiontype,
    sectionheader,
        'Total' AS recordtype,
        jsonb_strip_nulls(jsonb_build_object(
                'InstrumentAssetClass', iac.total_row_asset_class_json,
                'PositionAmount', pa.total_row_amount_json

        )) AS combined_json
FROM total_row_amount pa, total_row_asset_class iac
order by positionamountcurrency, recordtype;

END;
$BODY$;

ALTER FUNCTION public.extract_position_details(text, text, text, text, text)
    OWNER TO "DBUser";
