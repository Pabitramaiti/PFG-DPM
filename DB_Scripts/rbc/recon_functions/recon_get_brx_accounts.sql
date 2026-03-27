-- FUNCTION: public.recon_get_brx_accounts(text, text)

-- DROP FUNCTION IF EXISTS public.recon_get_brx_accounts(text, text);

CREATE OR REPLACE FUNCTION public.recon_get_brx_accounts(
	p_schema_name text,
	p_account_table text)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    v_success_accounts TEXT[];
    v_failed_accounts  TEXT[];
    v_sql TEXT;
BEGIN

    -- Successful BRX accounts

    v_sql := format(
        'SELECT array_agg(accountnumber)
         FROM %I.%I
         WHERE brxstatus = %L',
        p_schema_name,
        p_account_table,
        'COMPLETED'
    );

    EXECUTE v_sql INTO v_success_accounts;


    -- Failed BRX accounts

    v_sql := format(
        'SELECT array_agg(accountnumber)
         FROM %I.%I
         WHERE brxstatus = %L',
        p_schema_name,
        p_account_table,
        'ERROR'
    );

    EXECUTE v_sql INTO v_failed_accounts;

    RETURN jsonb_build_object(
        'brx_success_accounts', COALESCE(v_success_accounts, ARRAY[]::TEXT[]),
        'brx_failed_accounts',  COALESCE(v_failed_accounts, ARRAY[]::TEXT[])
    );
END;
$BODY$;

ALTER FUNCTION public.recon_get_brx_accounts(text, text)
    OWNER TO "DBUser";
