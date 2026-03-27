CREATE OR REPLACE FUNCTION create_base_historical_tables(schema_name TEXT DEFAULT 'AUX')
RETURNS VOID
LANGUAGE plpgsql
AS
$$

DECLARE 
	beginning_date DATE;
    beginning_date_yyyymm TEXT;
    prev_date DATE;
    curr_hist_table_name TEXT;
    prev_hist_table_name TEXT;
BEGIN
    SELECT 
        COALESCE(beg.transactiondatevalue, '1970-01-01'::date)
    INTO beginning_date
    FROM transactionparty tp
    JOIN transactionevent tevent
        ON tp.transactionid = tevent.transactionid
        AND UPPER(tevent.transactioncategory) = 'HEADER DATE'
    LEFT JOIN transactiondate beg
        ON tp.transactionid = beg.transactionid
        AND beg.transactiondatetype = 'BeginningDate'
    LEFT JOIN transactiondate end1
        ON tp.transactionid = end1.transactionid
        AND end1.transactiondatetype = 'ClosingDate'
    LIMIT 1;

    -- Convert to YYYYMM for current (target) table
    beginning_date_yyyymm := TO_CHAR(beginning_date, 'YYYYMM');

    -- Compute previous month safely
    prev_date := (beginning_date - INTERVAL '1 month')::DATE;

    -- Construct both table names
    curr_hist_table_name := 'historicaltable_' || TO_CHAR(beginning_date, 'YYYYMM');
    prev_hist_table_name   := 'historicaltable_' || TO_CHAR(prev_date, 'YYYYMM');

    -- Drop table if exists, then create latest historical table
    IF NOT EXISTS (
		SELECT FROM pg_tables WHERE schemaname = schema_name and tablename = curr_hist_table_name
    ) THEN
		EXECUTE format($f$
			CREATE TABLE IF NOT EXISTS %I.%I (
				partyid TEXT NOT NULL,
				clientid TEXT NOT NULL,
				statementdate TEXT NOT NULL,
				recordtype TEXT,
				sectionheader TEXT,
				summaryforsection TEXT,
				classification TEXT NOT NULL,
				summarydata JSONB,
				currency TEXT NOT NULL,
				PRIMARY KEY (partyid, clientid, sectionheader, classification, currency)
			)
		$f$, schema_name, curr_hist_table_name);
    END IF;
	
    IF prev_hist_table_name IS NOT NULL THEN
        IF NOT EXISTS (
            SELECT FROM pg_tables WHERE schemaname = schema_name and tablename = prev_hist_table_name
        ) THEN
            EXECUTE format($f$
                CREATE TABLE %I.%I (
                    partyid TEXT NOT NULL,
                    clientid TEXT NOT NULL,
                    statementdate TEXT NOT NULL,
                    recordtype TEXT,
                    sectionheader TEXT,
                    summaryforsection TEXT,
                    classification TEXT NOT NULL,
                    summarydata JSONB,
                    currency TEXT NOT NULL,
                    PRIMARY KEY (partyid, clientid, sectionheader, classification, currency)
                )
            $f$, schema_name, prev_hist_table_name);
        END IF;
    END IF;
END;
$$;