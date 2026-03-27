-- FUNCTION: public.recon_should_continue(text, text)

-- DROP FUNCTION IF EXISTS public.recon_should_continue(text, text);

CREATE OR REPLACE FUNCTION public.recon_should_continue(
	p_schema_name text,
	p_account_table text)
    RETURNS boolean
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    v_sql    TEXT;
    v_exists BOOLEAN;
BEGIN
    v_sql := format(
        'SELECT EXISTS (
            SELECT 1
            FROM %I.%I
            WHERE brxstatus IS NULL
        )',
        p_schema_name,
        p_account_table
    );

    EXECUTE v_sql INTO v_exists;

    RETURN v_exists;
END;
$BODY$;

ALTER FUNCTION public.recon_should_continue(text, text)
    OWNER TO "DBUser";
