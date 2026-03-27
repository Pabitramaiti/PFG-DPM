CREATE OR REPLACE FUNCTION get_account_metadata(account_numbers TEXT[] DEFAULT NULL)
RETURNS TABLE (
    partyid_list BIGINT,
    partyidentifiervalue_list VARCHAR,
	clientid_list VARCHAR
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        t1.partyid,
        t3.partyidentifiervalue,
        t1.partyoriginuser
    FROM 
        partycommon t1
    JOIN 
        partyclassification t2 
        ON t1.partyid = t2.partyid 
        AND t2.partyclassificationvalue = 'Account'
    JOIN 
        partyidentifier t3 
        ON t1.partyid = t3.partyid 
        AND t3.partyidentifiertype = 'OtherExternal'
    JOIN 
        partycontact t4 
        ON t1.partyid = t4.partyid 
        AND t4.partycontacttype IN ('AccountHolder', 'HouseHoldMasterAccount')
    JOIN 
        partycontactmailingaddress t5 
        ON t1.partyid = t5.partyid 
        AND t5.PartyContactAddressType = 'PrimaryAddress'
    WHERE
        (
            account_numbers IS NULL
            OR cardinality(account_numbers) = 0 
            OR t3.partyidentifierreference = ANY(account_numbers)
        );
END;
$$;
