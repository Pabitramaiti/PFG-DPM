CREATE OR REPLACE FUNCTION insert_into_staging_and_update_recon(
    p_input JSON,
	p_staging_table TEXT,
	p_aux_schema_name TEXT DEFAULT 'AUX'
)
RETURNS VOID
LANGUAGE plpgsql
AS
$$

DECLARE
    rec JSON;
    v_partyid TEXT;
    v_partyidentifierref TEXT;
    v_partyidentifier TEXT;
    v_clientid TEXT;
    v_statementtype TEXT;
    v_statement JSONB;
	v_sql TEXT;
BEGIN
    -- Loop through each JSON object in the array
    FOR rec IN SELECT * FROM json_array_elements(p_input)
    LOOP
        -- Extract fields from each JSON object
        v_partyid := rec->>'partyid';
        v_partyidentifierref := rec->>'partyidentifierref';
        v_partyidentifier := rec->>'partyidentifier';
        v_clientid := rec->>'clientid';
        v_statementtype := rec->>'statementtype';
        v_statement := rec->'statement';

        -- Insert into staging table
        v_sql := format(
            'INSERT INTO %I (partyid, partyidentifierref, partyidentifier, clientid, statementtype, statement) 
             VALUES ($1, $2, $3, $4, $5, $6)',
            p_staging_table
        );
        -- EXECUTE v_sql USING v_partyid, v_partyidentifierref, v_partyidentifier, v_clientid, v_statementtype, v_statement;

        -- Update the target table’s status to 'succeed'
        --v_sql := format(
        --    'UPDATE %I.bios_wfs_out_recon_data_account_list
        --     SET extraction_status = ''SUCCEED''
        --     WHERE accountnumber = $1
        --       AND partytype = $2
        --       AND clientid = $3',
        --    p_aux_schema_name
        --);
        --EXECUTE v_sql USING v_partyidentifier, v_statementtype, v_clientid;
        v_sql := format(
            'UPDATE %I.bios_wfs_out_recon_data_account_list
             SET extraction_status = ''SUCCEED''
             WHERE accountnumber = $1
               AND clientid = $2',
            p_aux_schema_name
        );
        EXECUTE v_sql USING v_partyidentifier, v_clientid;
    END LOOP;

    RAISE NOTICE 'Records inserted into staging_table and statuses updated.';
END;

$$;
