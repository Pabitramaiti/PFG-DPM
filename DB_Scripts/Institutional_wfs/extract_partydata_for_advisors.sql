CREATE OR REPLACE FUNCTION extract_partydata_for_advisors(
    party_ids INTEGER[],
    classification_filter TEXT[],
    contacttype_filter TEXT[],
    identifiertype_filter TEXT[],
    addresstype_filter TEXT[]
)
RETURNS TABLE (
    fa_partyid BIGINT,
    fa_name_1 VARCHAR,
	fa_name_2 VARCHAR,
    fa_combinedaddress TEXT,
    fa_primaryemailaddress VARCHAR,
    fa_officetelephone VARCHAR
)
LANGUAGE plpgsql
AS $$

BEGIN
    RETURN QUERY
    SELECT 
        t1.partyid AS fa_partyid,
		CASE 
			WHEN t1.partyoriginuser = '117' THEN cleanup_whitespace(t1.partyname)::VARCHAR
			WHEN t1.partyoriginuser IN ('127', '381') THEN t4.partycontactfirstname
			ELSE NULL
		END AS fa_name_1,
		CASE 
			WHEN t1.partyoriginuser IN ('127', '381') THEN t4.partycontactlastname
			ELSE NULL
		END AS fa_name_2,
        CONCAT_WS(', ', t5.partycontactaddressline1, t5.partycontactaddressline2, t5.partycontactaddressline3, t5.partycontactaddressline4, t5.partycontactaddressline5) AS fa_combinedaddress,
        t4.PartyContactPrimaryEmailAddress AS fa_primaryemailaddress,
		CASE 
			WHEN TRIM(t4.PartyContactOfficeTelephone) IS NULL OR TRIM(t4.PartyContactOfficeTelephone) = '' THEN NULL

			WHEN t1.partyoriginuser IN ('127', '381') THEN 
				'Phone: ' || 
				CASE 
					WHEN LENGTH(TRIM(t4.PartyContactOfficeTelephone)) = 10 THEN
						SUBSTRING(t4.PartyContactOfficeTelephone FROM 1 FOR 3) || '-' ||
						SUBSTRING(t4.PartyContactOfficeTelephone FROM 4 FOR 3) || '-' ||
						SUBSTRING(t4.PartyContactOfficeTelephone FROM 7 FOR 4)

					WHEN LENGTH(TRIM(t4.PartyContactOfficeTelephone)) = 11 THEN
						SUBSTRING(t4.PartyContactOfficeTelephone FROM 1 FOR 1) || '-' ||
						SUBSTRING(t4.PartyContactOfficeTelephone FROM 2 FOR 3) || '-' ||
						SUBSTRING(t4.PartyContactOfficeTelephone FROM 5 FOR 3) || '-' ||
						SUBSTRING(t4.PartyContactOfficeTelephone FROM 8 FOR 4)

					ELSE TRIM(t4.PartyContactOfficeTelephone)
				END

			ELSE 
				CASE 
					WHEN LENGTH(TRIM(t4.PartyContactOfficeTelephone)) = 10 THEN
						SUBSTRING(t4.PartyContactOfficeTelephone FROM 1 FOR 3) || '-' ||
						SUBSTRING(t4.PartyContactOfficeTelephone FROM 4 FOR 3) || '-' ||
						SUBSTRING(t4.PartyContactOfficeTelephone FROM 7 FOR 4)

					WHEN LENGTH(TRIM(t4.PartyContactOfficeTelephone)) = 11 THEN
						SUBSTRING(t4.PartyContactOfficeTelephone FROM 1 FOR 1) || '-' ||
						SUBSTRING(t4.PartyContactOfficeTelephone FROM 2 FOR 3) || '-' ||
						SUBSTRING(t4.PartyContactOfficeTelephone FROM 5 FOR 3) || '-' ||
						SUBSTRING(t4.PartyContactOfficeTelephone FROM 8 FOR 4)

					ELSE TRIM(t4.PartyContactOfficeTelephone)
				END
		END::VARCHAR AS fa_officetelephone

    FROM partycommon t1
    JOIN partyclassification t2 
        ON t1.partyid = t2.partyid AND t2.partyclassificationvalue = ANY(classification_filter)
    JOIN partyidentifier t3 
        ON t1.partyid = t3.partyid AND t3.partyidentifiertype = ANY(identifiertype_filter)
    JOIN partycontact t4 
        ON t1.partyid = t4.partyid AND t4.partycontacttype = ANY(contacttype_filter)
    JOIN partycontactmailingaddress t5 
        ON t1.partyid = t5.partyid AND t5.PartyContactAddressType = ANY(addresstype_filter)
    WHERE t1.partyid = ANY(
        SELECT partyparentid
        FROM partycommon
        WHERE partyid = ANY(party_ids)
    );
END;

$$;