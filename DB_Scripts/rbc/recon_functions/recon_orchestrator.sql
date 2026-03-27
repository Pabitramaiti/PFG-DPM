-- FUNCTION: public.recon_orchestrator(text, text, text, integer, integer)

-- DROP FUNCTION IF EXISTS public.recon_orchestrator(text, text, text, integer, integer);

CREATE OR REPLACE FUNCTION public.recon_orchestrator(
	p_schema_name text,
	p_account_table text,
	p_trid_table text,
	p_individual_batch_size integer,
	p_all_accounts_batch_size integer)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    v_has_null   BOOLEAN;
    v_sql        TEXT;
    v_result     JSONB;
BEGIN
    ------------------------------------------------------------------
    -- Global TRID check
    ------------------------------------------------------------------
    v_sql := format(
        'SELECT EXISTS (
            SELECT 1
            FROM %I.%I
            WHERE status IS NULL
        )',
        p_schema_name,
        p_trid_table
    );

    EXECUTE v_sql INTO v_has_null;

    IF v_has_null THEN

        -- Stage‑2 INDIVIDUAL

        v_result :=
            public.recon_process_individual_accounts(
                p_schema_name,
                p_account_table,
                p_trid_table,
                p_individual_batch_size
            );

        RETURN jsonb_build_object(
            'state', 'Individual accounts being processed',
            'result', v_result
        );
    ELSE

        -- Stage‑3 ALL ACCOUNTS

        v_result :=
            public.recon_process_household_accounts(
                p_schema_name,
                p_account_table,
                p_trid_table,
                p_all_accounts_batch_size
            );

        RETURN jsonb_build_object(
            'state', 'All accounts being processed',
            'result', v_result
        );
    END IF;
END;
$BODY$;

ALTER FUNCTION public.recon_orchestrator(text, text, text, integer, integer)
    OWNER TO "DBUser";
