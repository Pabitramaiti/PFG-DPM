-- FUNCTION: public.recon_process_household_accounts(text, text, text, integer)

-- DROP FUNCTION IF EXISTS public.recon_process_household_accounts(text, text, text, integer);

CREATE OR REPLACE FUNCTION public.recon_process_household_accounts(
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
    r_account            RECORD;
    v_has_error          BOOLEAN;
    v_has_null           BOOLEAN;
    v_master_status      TEXT;

    v_completed_accounts TEXT[] := ARRAY[]::TEXT[];
    v_error_accounts     TEXT[] := ARRAY[]::TEXT[];

    v_tmp_accounts       TEXT[];
    v_batch_size         INTEGER := COALESCE(p_batch_size, 5000);
BEGIN

    -- STEP 1: Row-by-row evaluation (ALL account types)

    FOR r_account IN
        EXECUTE format(
            'SELECT accountnumber, partytype, partyid, partyparentid
             FROM %I.%I
             WHERE brxstatus IS NULL
             FOR UPDATE SKIP LOCKED
             LIMIT %s',
            p_schema_name,
            p_account_table,
            v_batch_size
        )
    LOOP
        -- reset per iteration
        v_tmp_accounts := NULL;
        v_has_error    := FALSE;
        v_has_null     := FALSE;
        v_master_status := NULL;


        -- Account-level TRID ERROR check

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


        -- Account-level NULL check

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


        -- Party-specific logic

        IF r_account.partytype = 'Individual Account' THEN
            -- terminal
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

        ELSIF r_account.partytype = 'Household Sister' THEN
            EXECUTE format(
                'SELECT brxstatus
                 FROM %I.%I
                 WHERE partytype = ''Household Master''
                   AND partyid = $1
                 LIMIT 1',
                p_schema_name,
                p_account_table
            )
            INTO v_master_status
            USING r_account.partyparentid;

            IF v_master_status = 'ERROR' THEN
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

            -- IF v_tmp_accounts IS NOT NULL THEN
            --     v_completed_accounts := v_completed_accounts || v_tmp_accounts;
            -- END IF;

        ELSIF r_account.partytype = 'Household Master' THEN
            CONTINUE;
        END IF;

    END LOOP;


    -- STEP 2: Any SISTER ERROR → MASTER ERROR

    EXECUTE format(
        'WITH u AS (
            UPDATE %I.%I m
            SET brxstatus = ''ERROR''
            FROM %I.%I s
            WHERE s.partytype = ''Household Sister''
              AND s.partyparentid = m.partyid
              AND s.brxstatus = ''ERROR''
              AND m.partytype = ''Household Master''
              AND m.brxstatus IS NULL
            RETURNING m.accountnumber
        )
        SELECT array_agg(accountnumber) FROM u',
        p_schema_name, p_account_table,
        p_schema_name, p_account_table
    )
    INTO v_tmp_accounts;

    IF v_tmp_accounts IS NOT NULL THEN
        v_error_accounts := v_error_accounts || v_tmp_accounts;
    END IF;


    -- STEP 3: Finalize MASTERs when all SISTERS are COMPLETED

    EXECUTE format(
        'WITH u AS (
            UPDATE %I.%I m
            SET brxstatus = ''COMPLETED''
            WHERE m.partytype = ''Household Master''
              AND m.brxstatus IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM %I.%I s
                  WHERE s.partytype = ''Household Sister''
                    AND s.partyparentid = m.partyid
                    AND s.brxstatus IS DISTINCT FROM ''COMPLETED''
              )
            RETURNING m.accountnumber
        )
        SELECT array_agg(accountnumber) FROM u',
        p_schema_name, p_account_table,
        p_schema_name, p_account_table
    )
    INTO v_tmp_accounts;

    IF v_tmp_accounts IS NOT NULL THEN
        v_completed_accounts := v_completed_accounts || v_tmp_accounts;
    END IF;

    RETURN jsonb_build_object(
        'batch_size', v_batch_size,
        'completed_accounts', v_completed_accounts,
        'error_accounts',     v_error_accounts
    );
END;
$BODY$;

ALTER FUNCTION public.recon_process_household_accounts(text, text, text, integer)
    OWNER TO "DBUser";
