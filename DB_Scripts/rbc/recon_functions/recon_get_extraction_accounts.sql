-- FUNCTION: public.recon_get_extraction_accounts(text, text)

-- DROP FUNCTION IF EXISTS public.recon_get_extraction_accounts(text, text);

CREATE OR REPLACE FUNCTION public.recon_get_extraction_accounts(
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
    v_individual_count INTEGER := 0;
    v_master_count     INTEGER := 0;
    v_sister_count     INTEGER := 0;
    v_sql TEXT;
BEGIN

    -- Successful accounts

    v_sql := format(
        'SELECT array_agg(accountnumber)
         FROM %I.%I
         WHERE extractionstatus = %L',
        p_schema_name,
        p_account_table,
        'COMPLETED'
    );

    EXECUTE v_sql INTO v_success_accounts;


    -- Failed accounts

    v_sql := format(
        'SELECT array_agg(accountnumber)
         FROM %I.%I
         WHERE extractionstatus = %L',
        p_schema_name,
        p_account_table,
        'ERROR'
    );

    EXECUTE v_sql INTO v_failed_accounts;


    -- Count processed accounts by partytype (COMPLETED only)

    v_sql := format(
        'SELECT
            COUNT(*) FILTER (WHERE partytype = %L AND extractionstatus = %L),
            COUNT(*) FILTER (WHERE partytype = %L AND extractionstatus = %L),
            COUNT(*) FILTER (WHERE partytype = %L AND extractionstatus = %L)
         FROM %I.%I',
        'Individual Account', 'COMPLETED',
        'Household Master',     'COMPLETED',
        'Household Sister',     'COMPLETED',
        p_schema_name,
        p_account_table
    );

    EXECUTE v_sql
    INTO v_individual_count, v_master_count, v_sister_count;

    RETURN jsonb_build_object(
        'extraction_success_accounts', COALESCE(v_success_accounts, ARRAY[]::TEXT[]),
        'extraction_failed_accounts',  COALESCE(v_failed_accounts, ARRAY[]::TEXT[]),
        'processed_individual_count',  v_individual_count,
        'processed_master_count',      v_master_count,
        'processed_sister_count',      v_sister_count
    );
END;
$BODY$;

ALTER FUNCTION public.recon_get_extraction_accounts(text, text)
    OWNER TO "DBUser";
