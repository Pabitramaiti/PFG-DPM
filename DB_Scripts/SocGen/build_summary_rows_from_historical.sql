CREATE OR REPLACE FUNCTION build_summary_rows_from_historical(
    in_partyid TEXT,
	in_clientid TEXT,
    in_statementdate TEXT,
    in_classification TEXT,
    in_recordtype TEXT,
    in_sectionheader TEXT,
    in_sectionname TEXT,
    in_currency TEXT,
    in_thisperiod_amount NUMERIC,
	in_classification_val TEXT,
	in_historical_tablename TEXT
)
RETURNS TABLE (
    summary_object JSONB,
    partyid TEXT,
	clientid TEXT,
    statementdate TEXT,
    currency TEXT,
    recordtype TEXT,
    sectionheader TEXT,
    summaryforsection TEXT,
    classification TEXT
) AS $$

DECLARE
    updated_summary JSONB;
    query TEXT;
    insert_sql TEXT;
    prev_statementdate TEXT;
    curr_statementdate TEXT;
    prev_table_name TEXT;
    historical_schema_name TEXT := 'AUX';
    is_january BOOLEAN;
BEGIN
    -- Compute previous statement date from in_statementdate
    -- RAISE NOTICE 'in_statementdate: %', in_statementdate;
    curr_statementdate := TO_CHAR(TO_DATE(in_statementdate, 'YYYY-MM'), 'YYYYMM');
    prev_statementdate := TO_CHAR((TO_DATE(in_statementdate, 'YYYY-MM') - INTERVAL '1 month')::DATE, 'YYYYMM');
    
    -- Check if the current month is January
    is_january := TO_CHAR(TO_DATE(in_statementdate, 'YYYY-MM'), 'MM') = '01';

    -- Construct previous table name dynamically
    prev_table_name := 'historicaltable_' || prev_statementdate;

    -- Build dynamic SQL to fetch previous month's record
    query := format($f$
		SELECT jsonb_strip_nulls(
			jsonb_set(
				jsonb_set(
					jsonb_set(
						jsonb_set(
							ht.summarydata,
							'{previousperiod}',
							to_jsonb((ht.summarydata->>'thisperiod')::numeric)
						),
						'{thisperiod}',
						to_jsonb($1)
					),
					'{ytdperiod}',
					to_jsonb(
						CASE
							WHEN $9::boolean THEN COALESCE($1, 0)
							ELSE COALESCE((ht.summarydata->>'ytdperiod')::numeric, 0) + COALESCE($1, 0)
						END
					)
				),
				'{classification}', to_jsonb($10)
			)
		)
        FROM %I.%I ht
        WHERE ht.partyid = $2
          AND ht.clientid = $3
          AND ht.classification = $4
          AND ht.recordtype = $5
          AND ht.sectionheader = $6
          AND ht.summaryforsection = $7
          AND ht.currency = $8
        LIMIT 1;
    $f$, historical_schema_name, prev_table_name);

    -- Try to get the previous record
    EXECUTE query
        INTO updated_summary
        USING in_thisperiod_amount, in_partyid, in_clientid,
              in_classification, in_recordtype, in_sectionheader, in_sectionname, in_currency, is_january, in_classification_val;

    -- If no previous record exists → initialize fresh summary
    -- IF updated_summary IS NULL THEN
	IF updated_summary IS NULL AND COALESCE(in_thisperiod_amount, 0) <> 0 THEN
        updated_summary := jsonb_strip_nulls(
            jsonb_build_object(
                'partyid', in_partyid,
				'clientid', in_clientid,
                'previousperiod', 0,
                'thisperiod', COALESCE(in_thisperiod_amount, 0),
                'ytdperiod', COALESCE(in_thisperiod_amount, 0),
                'classification', in_classification_val,
                'recordtype', in_recordtype,
                'sectiontype', 'Summary',
                'sectionheader', in_sectionheader,
                'transactionamountcurrency', COALESCE(in_currency, 'USD')
            )
        );
    END IF;

    IF updated_summary IS NOT NULL THEN
        -- Insert or update record in current historical table (passed as argument)
        insert_sql := format($f$
            INSERT INTO %I (
                partyid, clientid, statementdate, currency, recordtype,
                sectionheader, summaryforsection, classification, summarydata
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (partyid, clientid, sectionheader, classification, currency)
            DO UPDATE SET
                statementdate = EXCLUDED.statementdate,
                recordtype = EXCLUDED.recordtype,
                summaryforsection = EXCLUDED.summaryforsection,
                summarydata = EXCLUDED.summarydata;
        $f$, in_historical_tablename);

        EXECUTE insert_sql
            USING in_partyid, in_clientid, curr_statementdate, in_currency,
                in_recordtype, in_sectionheader, in_sectionname,
                in_classification, updated_summary;
    END IF;

    -- Return inserted/updated record
    RETURN QUERY
    SELECT 
        updated_summary,
        in_partyid,
		in_clientid,
        in_statementdate,
        in_currency,
        in_recordtype,
        in_sectionheader,
        in_sectionname,
        in_classification;
END;

$$ LANGUAGE plpgsql;