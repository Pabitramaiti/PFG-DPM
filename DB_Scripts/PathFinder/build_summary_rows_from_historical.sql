CREATE OR REPLACE FUNCTION public.build_summary_rows_from_historical(
in_partyid text, in_clientid text, in_origin text, in_statementdate text, in_classification text, in_recordtype text, in_sectionheader text, in_sectionname text, in_currency text, in_thisperiod_amount numeric)
 RETURNS TABLE(summary_object jsonb, partyid text, clientid text, origin text, statementdate text, currency text, recordtype text, sectionheader text, summaryforsection text, classification text)
 LANGUAGE plpgsql
AS $function$
DECLARE
    updated_summary JSONB;
    query TEXT;
    insert_sql TEXT;
    prev_statementdate TEXT;
    curr_statementdate TEXT;
BEGIN
    -- Format dates as YYYYMM
    curr_statementdate := TO_CHAR(TO_DATE(in_statementdate, 'YYYY-MM'), 'YYYYMM');

    -- Check if the month of in_statementdate is not January (1)
    IF EXTRACT(MONTH FROM TO_DATE(in_statementdate, 'YYYY-MM')) <> 1 THEN
        prev_statementdate := TO_CHAR(TO_DATE(in_statementdate, 'YYYY-MM') - INTERVAL '1 month', 'YYYYMM');
        -- Construct dynamic SQL for SELECT query (when it's not January)
        query := format('SELECT jsonb_strip_nulls(
                    jsonb_set(
                        jsonb_set(
                            jsonb_set(
                                ht.summarydata,
                                ''{previousperiod}'',
                                to_jsonb((ht.summarydata->>''thisperiod'')::numeric)
                            ),
                            ''{thisperiod}'',
                            to_jsonb($1)
                        ),
                        ''{ytdperiod}'',
                        to_jsonb(COALESCE((ht.summarydata->>''ytdperiod'')::numeric, 0) + COALESCE($1, 0))
                    )
                )
                FROM %I ht
                WHERE ht.partyid = $2
				  AND ht.clientid = $3
				  AND ht.origin = $4
                  AND ht.statementdate = TO_CHAR(($5 || ''-01'')::date - INTERVAL ''1 month'', ''YYYY-MM'')
                  AND ht.classification = $6
                  AND ht.recordtype = $7
                  AND ht.sectionheader = $8
                  AND ht.summaryforsection = $9
                  AND ht.currency = $10
                LIMIT 1;', 'historicaltable_' || prev_statementdate);

        -- Execute dynamic SELECT query and assign the result to updated_summary
        EXECUTE query INTO updated_summary USING in_thisperiod_amount, in_partyid, in_clientid, in_origin, in_statementdate, in_classification, in_recordtype, in_sectionheader, in_sectionname, in_currency;
    ELSE
        -- Handle case when it's January (set previousperiod to 0)
        updated_summary := jsonb_strip_nulls(
            jsonb_build_object(
                'partyid', in_partyid,
				'clientid', in_clientid,
                'previousperiod', 0,
                -- 'thisperiod', in_thisperiod_amount,
                'thisperiod', COALESCE(in_thisperiod_amount, 0),  -- If NULL, set to 0
                'ytdperiod', COALESCE(in_thisperiod_amount, 0),
                'classification', in_classification,
                'recordtype', in_recordtype,
                'sectiontype', 'Summary',
                'sectionheader', in_sectionheader,
                'transactionamountcurrency', COALESCE(in_currency, 'USD')
            )
        );
    END IF;

    -- If no row was found (updated_summary is NULL), create a new row
    IF updated_summary IS NULL THEN
        updated_summary := jsonb_strip_nulls(
            jsonb_build_object(
                'partyid', in_partyid,
				'clientid', in_clientid,
                'previousperiod', 0,
                -- 'thisperiod', in_thisperiod_amount,
                'thisperiod', COALESCE(in_thisperiod_amount, 0),
                'ytdperiod', COALESCE(in_thisperiod_amount, 0),
                'classification', in_classification,
                'recordtype', in_recordtype,
                'sectiontype', 'Summary',
                'sectionheader', in_sectionheader,
                'transactionamountcurrency', COALESCE(in_currency, 'USD')
            )
        );
     END IF;

    -- Insert into the current month table (dynamic table name)
    -- Construct dynamic INSERT SQL
    RAISE NOTICE 'in_clientid: %', in_clientid;
    RAISE NOTICE 'in_currency: %', in_currency;

    insert_sql := format(
        'INSERT INTO %I (
            partyid, clientid, origin, statementdate, currency, recordtype,
            sectionheader, summaryforsection, classification, summarydata
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)',
        'historicaltable_' || curr_statementdate
    );

    -- Execute dynamic INSERT
    EXECUTE insert_sql
    USING in_partyid, in_clientid, in_origin, curr_statementdate, in_currency, in_recordtype,
          in_sectionheader, in_sectionname, in_classification, updated_summary;

    -- Return the inserted result
    RETURN QUERY
    SELECT
        updated_summary,
        in_partyid,
		in_clientid,
		in_origin,
        in_statementdate,
        in_currency,
        in_recordtype,
        in_sectionheader,
        in_sectionname,
        in_classification;
END;
$function$;
