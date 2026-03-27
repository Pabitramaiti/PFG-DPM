-- FUNCTION: public.recon_check_brx_complete(text, text)

-- DROP FUNCTION IF EXISTS public.recon_check_brx_complete(text, text);

CREATE OR REPLACE FUNCTION public.recon_check_brx_complete(
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
    -- Check if any BRX still pending
    v_sql := format(
        'SELECT EXISTS (
            SELECT 1
            FROM %I.%I
            WHERE brxstatus IS NULL
               OR brxstatus NOT IN (%L, %L)
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
            'brx_complete', false,
            'brx_success_accounts', ARRAY[]::TEXT[],
            'brx_failed_accounts', ARRAY[]::TEXT[]
        );
    END IF;

    -- If BRX complete → fetch account lists
    v_accounts :=
        public.recon_get_brx_accounts(
            p_schema_name,
            p_account_table
        );

    RETURN jsonb_build_object(
        'brx_complete', true,
        'brx_success_accounts',
            v_accounts->'brx_success_accounts',
        'brx_failed_accounts',
            v_accounts->'brx_failed_accounts'
    );
END;
$BODY$;

ALTER FUNCTION public.recon_check_brx_complete(text, text)
    OWNER TO "DBUser";
