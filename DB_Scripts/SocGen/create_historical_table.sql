CREATE OR REPLACE FUNCTION create_historical_table(historical_table_name TEXT, schema_name TEXT DEFAULT 'AUX')
RETURNS void AS
$$

BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_tables 
        WHERE schemaname = schema_name and tablename = historical_table_name
    ) THEN
        EXECUTE format($f$
            CREATE TABLE %I (
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
        $f$, historical_table_name);
    -- ELSE
        -- RAISE NOTICE 'Current month historical table exists: %', historical_table_name;
    END IF;
END;

$$
LANGUAGE plpgsql;