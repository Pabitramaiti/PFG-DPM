-- FUNCTION: public.recon_prerun_queries(text, text, text)

-- DROP FUNCTION IF EXISTS public.recon_prerun_queries(text, text, text);

CREATE OR REPLACE FUNCTION public.recon_prerun_queries(
	p_schema_name text,
	p_account_table text,
	p_trid_table text)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    v_sql TEXT;
    v_rows_updated INTEGER := 0;
BEGIN

    -- Add column if it does not exist

    v_sql := format(
        'ALTER TABLE %I.%I
         ADD COLUMN IF NOT EXISTS total_trids_extracted INTEGER DEFAULT 0',
        p_schema_name,
        p_account_table
    );

    EXECUTE v_sql;


    -- Populate total_trids_extracted from recon table counts

    v_sql := format(
        'UPDATE %I.%I al
         SET total_trids_extracted = COALESCE(sub.file_count, 0)
         FROM (
             SELECT accountnumber, COUNT(filename) AS file_count
             FROM %I.%I
             GROUP BY accountnumber
         ) sub
         WHERE al.accountnumber = sub.accountnumber',
        p_schema_name,
        p_account_table,
        p_schema_name,
        p_trid_table
    );

    EXECUTE v_sql;
    GET DIAGNOSTICS v_rows_updated = ROW_COUNT;

    RETURN jsonb_build_object(
        'state', 'Pre-run queries completed',
        'rows_updated', v_rows_updated
    );
END;
$BODY$;

ALTER FUNCTION public.recon_prerun_queries(text, text, text)
    OWNER TO "DBUser";
