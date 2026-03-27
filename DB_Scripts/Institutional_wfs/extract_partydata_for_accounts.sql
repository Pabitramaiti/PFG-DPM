CREATE OR REPLACE FUNCTION extract_partydata_for_accounts(
    party_ids INTEGER[],
    classification_filter TEXT[],
    contacttype_filter TEXT[],
    identifiertype_filter TEXT[],
    addresstype_filter TEXT[]
)
RETURNS TABLE (
    ma_partyid BIGINT,
	ma_partyname VARCHAR,
	ma_partyentity VARCHAR,
    ma_parentid BIGINT,
    ma_clientid VARCHAR,
    ma_partyclassification_value VARCHAR,
    ma_partyidentifier_value VARCHAR,
	ma_partyidentifier_reference VARCHAR,
    ma_partycontact_firstname VARCHAR,
    ma_partycontact_lastname VARCHAR,
    ma_partycontact_fullname VARCHAR,
    ma_partycontact_type VARCHAR,
    ma_partycontact_addressline1 VARCHAR,
    ma_partycontact_addressline2 VARCHAR,
    ma_partycontact_addressline3 VARCHAR,
    ma_partycontact_addressline4 VARCHAR,
    ma_partycontact_addressline5 VARCHAR,
    ma_partycontact_addressline6 VARCHAR
)
LANGUAGE plpgsql
AS $$

BEGIN
    RETURN QUERY
    SELECT 
        t1.partyid AS ma_partyid,
		cleanup_whitespace(t1.partyname)::VARCHAR AS ma_partyname,
		cleanup_whitespace(t1.partyentity)::VARCHAR AS ma_partyentity,
        t1.partyparentid AS ma_parentid,
        t1.partyoriginuser AS ma_clientid,
        t2.partyclassificationvalue AS ma_partyclassification_value,
		t3.partyidentifiervalue AS ma_partyidentifier_value,
		t3.partyidentifierreference AS ma_partyidentifier_reference,
        -- t4.partycontactfirstname AS ma_partycontact_firstname,
        cleanup_whitespace(t4.partycontactfirstname)::VARCHAR AS ma_partycontact_firstname,
        -- t4.partycontactlastname AS ma_partycontact_lastname,
        cleanup_whitespace(t4.partycontactlastname)::VARCHAR AS ma_partycontact_lastname,
        -- t4.partycontactfullname AS ma_partycontact_fullname,
        cleanup_whitespace(t4.partycontactfullname)::VARCHAR AS ma_partycontact_fullname,
        t4.partycontacttype AS ma_partycontact_type,
        -- t5.partycontactaddressline1 AS ma_partycontact_addressline1,
        cleanup_whitespace(t5.partycontactaddressline1)::VARCHAR AS ma_partycontact_addressline1,
        -- t5.partycontactaddressline2 AS ma_partycontact_addressline2,
        cleanup_whitespace(t5.partycontactaddressline2)::VARCHAR AS ma_partycontact_addressline2,
        -- t5.partycontactaddressline3 AS ma_partycontact_addressline3,
        cleanup_whitespace(t5.partycontactaddressline3)::VARCHAR AS ma_partycontact_addressline3,
        -- t5.partycontactaddressline4 AS ma_partycontact_addressline4,
        cleanup_whitespace(t5.partycontactaddressline4)::VARCHAR AS ma_partycontact_addressline4,
        -- t5.partycontactaddressline5 AS ma_partycontact_addressline5,
        cleanup_whitespace(t5.partycontactaddressline5)::VARCHAR AS ma_partycontact_addressline5,
        -- t5.partycontactaddressline6 AS ma_partycontact_addressline6
        cleanup_whitespace(t5.partycontactaddressline6)::VARCHAR AS ma_partycontact_addressline6
    FROM partycommon t1
    JOIN partyclassification t2 
        ON t1.partyid = t2.partyid AND t2.partyclassificationvalue = ANY(classification_filter)
    JOIN partyidentifier t3 
        ON t1.partyid = t3.partyid AND t3.partyidentifiertype = ANY(identifiertype_filter)
    JOIN partycontact t4 
        ON t1.partyid = t4.partyid AND t4.partycontacttype = ANY(contacttype_filter)
    JOIN partycontactmailingaddress t5 
        ON t1.partyid = t5.partyid AND t5.PartyContactAddressType = ANY(addresstype_filter)
    WHERE t1.partyid = ANY(party_ids) or t1.partyparentid = ANY(party_ids);
END;

$$;