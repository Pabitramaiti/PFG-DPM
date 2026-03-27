CREATE OR REPLACE FUNCTION extract_communication_data(
    p_accountpartyid TEXT,
    p_clientidentifier TEXT
) 
RETURNS JSONB 
LANGUAGE plpgsql 
AS
$$
DECLARE
    v_comm_id INTEGER;
    result JSONB;
BEGIN
    SELECT ci.communicationid
    INTO v_comm_id
    FROM CommunicationIdentifier ci
    JOIN CommunicationCommon cc 
        ON ci.communicationid = cc.communicationid
    WHERE ci.communicationidentifiervalue = p_accountpartyid
      AND cc.communicationentity = p_clientidentifier
    LIMIT 1;

    IF v_comm_id IS NULL THEN
        RETURN '{}'::jsonb; -- or NULL if you prefer
    END IF;

    SELECT jsonb_object_agg(
                ccu.communicationcustomkey,
                TRIM(
                        CASE
                                WHEN ccu.communicationcustomvalue IS NULL THEN ' '
                                WHEN UPPER(TRIM(ccu.communicationcustomvalue)) = 'N/A' THEN ' '
                                ELSE ccu.communicationcustomvalue
                        END
                )
        )
    INTO result
    FROM CommunicationCustom ccu
    WHERE ccu.communicationid = v_comm_id;

    RETURN COALESCE(result, '{}'::jsonb);
END;

$$;