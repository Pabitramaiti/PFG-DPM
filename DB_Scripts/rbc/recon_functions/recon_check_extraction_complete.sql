-- FUNCTION: public.recon_check_extraction_complete(text, text)

-- DROP FUNCTION IF EXISTS public.recon_check_extraction_complete(text, text);

CREATE OR REPLACE FUNCTION public.recon_check_extraction_complete(
	p_schema_name text,
	p_account_table text)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    v_has_incomplete BOOLEAN;
    v_sql TEXT;
    v_accounts JSONB;
BEGIN

    -- Check if any extraction still pending

    v_sql := format(
        'SELECT EXISTS (
            SELECT 1
            FROM %I.%I
            WHERE extractionstatus IS NULL
               OR extractionstatus NOT IN (%L, %L)
        )',
        p_schema_name,
        p_account_table,
        'COMPLETED',
        'ERROR'
    );

    EXECUTE v_sql INTO v_has_incomplete;


    -- If still processing

    IF v_has_incomplete THEN
        RETURN jsonb_build_object(
            'extraction_complete', false,
            'extraction_success_accounts', ARRAY[]::TEXT[],
            'extraction_failed_accounts',  ARRAY[]::TEXT[],
            'processed_individual_count', 0,
            'processed_master_count',     0,
            'processed_sister_count',     0
        );
    END IF;


    -- If extraction complete → fetch accounts + counts

    v_accounts :=
        public.recon_get_extraction_accounts(
            p_schema_name,
            p_account_table
        );

    RETURN jsonb_build_object(
        'extraction_complete', true
    ) || v_accounts;
END;
$BODY$;

ALTER FUNCTION public.recon_check_extraction_complete(text, text)
    OWNER TO "DBUser";
