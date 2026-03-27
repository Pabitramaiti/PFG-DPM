CREATE OR REPLACE FUNCTION extract_partydata_for_interested_parties(
    party_ids INTEGER[],
    classification_filter TEXT[],
    contacttype_filter TEXT[],
    identifiertype_filter TEXT[],
    addresstype_filter TEXT[]
)

RETURNS TABLE (
    ip_partyid BIGINT,
    ip_partyname VARCHAR,
	ip_partyentity VARCHAR,
    ip_parentid BIGINT,
    ip_clientid VARCHAR,
    ip_partyorigin VARCHAR,
    ip_partyclassification_value VARCHAR,
    ip_partyidentifier_value VARCHAR,
	ip_partyidentifier_reference VARCHAR,
    ip_partycontact_firstname VARCHAR,
    ip_partycontact_lastname VARCHAR,
    ip_partycontact_fullname VARCHAR,
    ip_partycontact_type VARCHAR,
    ip_partycontact_addressline1 VARCHAR,
    ip_partycontact_addressline2 VARCHAR,
    ip_partycontact_addressline3 VARCHAR,
    ip_partycontact_addressline4 VARCHAR,
    ip_partycontact_addressline5 VARCHAR,
    ip_partycontact_addressline6 VARCHAR
)
LANGUAGE plpgsql
AS $$

BEGIN
    RETURN QUERY
    SELECT 
        t1.partyid AS ip_partyid,
		cleanup_whitespace(t1.partyname)::VARCHAR AS ip_partyname,
		cleanup_whitespace(t1.partyentity)::VARCHAR AS ip_partyentity,
        t1.partyparentid AS ip_parentid,
        t1.partyoriginuser AS ip_clientid,
        (regexp_replace(t1.partyorigin, '^[^_]+_([^_]+)_[^_]+_[^_]+\.json$', '\1') || '.json')::VARCHAR AS ip_partyorigin,
        t2.partyclassificationvalue AS ip_partyclassification_value,
        t3.partyidentifiervalue AS ip_partyidentifier_value,
		t3.partyidentifierreference AS ip_partyidentifier_reference,
        -- t4.partycontactfirstname AS ip_partycontact_firstname,
        cleanup_whitespace(t4.partycontactfirstname)::VARCHAR AS ip_partycontact_firstname,
        -- t4.partycontactlastname AS ip_partycontact_lastname,
        cleanup_whitespace(t4.partycontactlastname)::VARCHAR AS ip_partycontact_lastname,
        -- t4.partycontactfullname AS ip_partycontact_fullname,
        cleanup_whitespace(t4.partycontactfullname)::VARCHAR AS ip_partycontact_fullname,
        t4.partycontacttype AS ip_partycontact_type,
        -- t5.partycontactaddressline1 AS ip_partycontact_addressline1,
        cleanup_whitespace(t5.partycontactaddressline1)::VARCHAR AS ip_partycontact_addressline1,
        -- t5.partycontactaddressline2 AS ip_partycontact_addressline2,
        cleanup_whitespace(t5.partycontactaddressline2)::VARCHAR AS ip_partycontact_addressline2,
        -- t5.partycontactaddressline3 AS ip_partycontact_addressline3,
        cleanup_whitespace(t5.partycontactaddressline3)::VARCHAR AS ip_partycontact_addressline3,
        -- t5.partycontactaddressline4 AS ip_partycontact_addressline4,
        cleanup_whitespace(t5.partycontactaddressline4)::VARCHAR AS ip_partycontact_addressline4,
        -- t5.partycontactaddressline5 AS ip_partycontact_addressline5,
        cleanup_whitespace(t5.partycontactaddressline5)::VARCHAR AS ip_partycontact_addressline5,
        -- t5.partycontactaddressline6 AS ip_partycontact_addressline6
        cleanup_whitespace(t5.partycontactaddressline6)::VARCHAR AS ip_partycontact_addressline6
		
    FROM partycommon t1
    JOIN partyclassification t2 
        ON t1.partyid = t2.partyid AND t2.partyclassificationvalue = ANY(classification_filter)
    JOIN partyidentifier t3 
        ON t1.partyid = t3.partyid AND t3.partyidentifiertype = ANY(identifiertype_filter)
    JOIN partycontact t4 
        ON t1.partyid = t4.partyid AND t4.partycontacttype = ANY(contacttype_filter)
    JOIN partycontactmailingaddress t5 
        ON t1.partyid = t5.partyid AND t5.PartyContactAddressType = ANY(addresstype_filter)
    WHERE t1.partyparentid = ANY(party_ids) OR t1.partyparentid = ANY(SELECT partyid FROM partycommon WHERE partyparentid = ANY(party_ids));
END;

$$;
