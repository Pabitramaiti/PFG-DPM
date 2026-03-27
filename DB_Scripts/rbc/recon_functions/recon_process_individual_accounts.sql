-- FUNCTION: public.recon_process_individual_accounts(text, text, text, integer)

-- DROP FUNCTION IF EXISTS public.recon_process_individual_accounts(text, text, text, integer);

CREATE OR REPLACE FUNCTION public.recon_process_individual_accounts(
	p_schema_name text,
	p_account_table text,
	p_trid_table text,
	p_batch_size integer)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    r_account             RECORD;
    v_has_error           BOOLEAN;
    v_has_null            BOOLEAN;
    v_processed_count     INTEGER;

    v_completed_accounts  TEXT[] := ARRAY[]::TEXT[];
    v_error_accounts      TEXT[] := ARRAY[]::TEXT[];

    v_tmp_accounts        TEXT[];
    v_batch_size          INTEGER := COALESCE(p_batch_size, 50000);
BEGIN
    FOR r_account IN
        EXECUTE format(
            'SELECT accountnumber, total_trids_extracted
             FROM %I.%I
             WHERE partytype = ''Individual Account''
               AND brxstatus IS NULL
               AND total_trids_extracted IS NOT NULL
             FOR UPDATE SKIP LOCKED
             LIMIT %s',
            p_schema_name,
            p_account_table,
            v_batch_size
        )
    LOOP
        -- hard reset per iteration
        v_tmp_accounts    := NULL;
        v_has_error       := FALSE;
        v_has_null        := FALSE;
        v_processed_count := 0;


        -- ERROR check

        EXECUTE format(
            'SELECT EXISTS (
                SELECT 1 FROM %I.%I
                WHERE accountnumber = $1
                  AND status = ''ERROR''
            )',
            p_schema_name,
            p_trid_table
        )
        INTO v_has_error
        USING r_account.accountnumber;

        IF v_has_error THEN
            EXECUTE format(
                'WITH u AS (
                    UPDATE %I.%I
                    SET brxstatus = ''ERROR''
                    WHERE accountnumber = $1
                      AND brxstatus IS NULL
                    RETURNING accountnumber
                )
                SELECT array_agg(accountnumber) FROM u',
                p_schema_name,
                p_account_table
            )
            INTO v_tmp_accounts
            USING r_account.accountnumber;

            IF v_tmp_accounts IS NOT NULL THEN
                v_error_accounts := v_error_accounts || v_tmp_accounts;
            END IF;

            CONTINUE;
        END IF;


        --  NULL check (WAIT)

        EXECUTE format(
            'SELECT EXISTS (
                SELECT 1 FROM %I.%I
                WHERE accountnumber = $1
                  AND status IS NULL
            )',
            p_schema_name,
            p_trid_table
        )
        INTO v_has_null
        USING r_account.accountnumber;

        IF v_has_null THEN
            CONTINUE;
        END IF;


        -- PROCESSED count

        EXECUTE format(
            'SELECT COUNT(*)
             FROM %I.%I
             WHERE accountnumber = $1
               AND status = ''PROCESSED''',
            p_schema_name,
            p_trid_table
        )
        INTO v_processed_count
        USING r_account.accountnumber;


        -- COMPLETED

        IF v_processed_count = r_account.total_trids_extracted THEN
            EXECUTE format(
                'WITH u AS (
                    UPDATE %I.%I
                    SET brxstatus = ''COMPLETED''
                    WHERE accountnumber = $1
                      AND brxstatus IS NULL
                    RETURNING accountnumber
                )
                SELECT array_agg(accountnumber) FROM u',
                p_schema_name,
                p_account_table
            )
            INTO v_tmp_accounts
            USING r_account.accountnumber;

            IF v_tmp_accounts IS NOT NULL THEN
                v_completed_accounts := v_completed_accounts || v_tmp_accounts;
            END IF;
        END IF;

    END LOOP;

    RETURN jsonb_build_object(
        'batch_size', v_batch_size,
        'completed_accounts', v_completed_accounts,
        'error_accounts',     v_error_accounts
    );
END;
$BODY$;

ALTER FUNCTION public.recon_process_individual_accounts(text, text, text, integer)
    OWNER TO "DBUser";
