CREATE OR REPLACE FUNCTION update_recon_table(
    p_accountinfo JSON
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    rec JSON;
    v_accountnumber TEXT;
    v_clientid TEXT;
    v_partytype TEXT;
BEGIN
    -- Loop through each object in the input JSON array
    FOR rec IN SELECT * FROM json_array_elements(p_accountinfo)
    LOOP
        v_accountnumber := rec->>'account_number';
        v_clientid      := rec->>'client_id';
        v_partytype     := rec->>'party_type';

        -- Update recon table
        UPDATE bios_wfs_out_recon_data_account_list
        SET status = 'EXT_SUCCEED'
        WHERE accountnumber = v_accountnumber
          AND clientid = v_clientid;
		--SET extraction_status = 'SUCCEED'		  
		--WHERE accountnumber = v_accountnumber
         --AND clientid = v_clientid
          --AND partytype = v_partytype;
    END LOOP;

    RAISE NOTICE 'Recon table updated for all accounts.';
END;
$$;