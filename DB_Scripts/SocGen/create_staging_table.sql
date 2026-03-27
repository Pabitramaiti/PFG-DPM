CREATE OR REPLACE FUNCTION create_staging_table(staging_table_name TEXT, schema_name TEXT)
RETURNS void AS
$$
BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_tables 
        WHERE schemaname = schema_name and tablename = staging_table_name
    ) THEN
        EXECUTE format($f$
            CREATE TABLE %I (
                category TEXT NOT NULL,
                s3path TEXT NOT NULL,
                PRIMARY KEY (category, s3path)
            )
        $f$, staging_table_name);
    -- ELSE
        -- RAISE NOTICE 'Staging table already exists: %', staging_table_name;
    END IF;
END;
$$
LANGUAGE plpgsql;
