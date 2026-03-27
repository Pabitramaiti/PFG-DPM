CREATE OR REPLACE FUNCTION get_party_details(party_ids INTEGER[])

RETURNS TABLE (
    party_id BIGINT,
    party_name TEXT, 
    party_mailing_name TEXT, 
    party_entity TEXT, 
    party_parentid BIGINT, 
    party_clientid TEXT, 
    partyorigin TEXT, 
    party_classificationvalue VARCHAR, 
    party_identifiervalue VARCHAR, 
    party_identifierreference VARCHAR, 
    party_firstname TEXT, 
    party_lastname TEXT, 
    party_fullname TEXT, 
    party_type TEXT, 
    party_addressline1 TEXT, 
    party_addressline2 TEXT, 
    party_addressline3 TEXT, 
    party_addressline4 TEXT, 
    party_addressline5 TEXT, 
    party_addressline6 TEXT, 
    rep_num TEXT, 
    ----financialadvisor_name_1 TEXT, 
    ----financialadvisor_name_2 TEXT, 
    ----financialadvisor_combinedaddress TEXT, 
    ----financialadvisor_primaryemailaddress TEXT, 
    ----financialadvisor_officetelephone TEXT, 
    uc_entity TEXT, 
    primary_logo TEXT, 
    return_address TEXT, 
    return_address_second TEXT, 
    graphic_message_page1 TEXT, 
    statement_backer TEXT, 
    duplicate_copies JSON
)
LANGUAGE plpgsql
AS $$


BEGIN
    RETURN QUERY

    WITH MainAccountsCTE AS (
        SELECT * FROM extract_partydata_for_accounts(
            party_ids, 
            ARRAY['Account'],
            ARRAY['AccountHolder', 'HouseHoldMasterAccount', 'HouseHoldSisterAccount'],
            ARRAY['OtherExternal'],
            ARRAY['PrimaryAddress']
        )
    ),

    AdvisorsCTE AS (
        SELECT * FROM extract_partydata_for_advisors(
            party_ids, 
            ARRAY['Advisor'],
            ARRAY['FinancialAdvisor'],
            ARRAY['OtherExternal'],
            ARRAY['Office']
        )
    ),

    InterestedPartiesCTE AS (
        SELECT * FROM extract_partydata_for_interested_parties(
            party_ids, 
            ARRAY['InterestedParty'],
            ARRAY['InterestedParty'],
            ARRAY['OtherExternal'],
            ARRAY['PrimaryAddress']
        )
    ),

    LogosMessagesCTE AS (
        SELECT * FROM extract_logomsgdata_for_accounts(
            party_ids
        )
    ),

    MastersAndHoldersCTE AS (
        SELECT *
        FROM MainAccountsCTE
        WHERE ma_partycontact_type IN ('AccountHolder', 'HouseHoldMasterAccount')
    ),

    SistersWithAdvisorIdCTE AS (
        SELECT 
            s.*, 
            m.ma_parentid AS advisor_partyid
        FROM MainAccountsCTE s
        JOIN MastersAndHoldersCTE m
            ON s.ma_parentid = m.ma_partyid
        WHERE s.ma_partycontact_type = 'HouseHoldSisterAccount'
    ),

    CombinedAccountsCTE AS (
        SELECT 
            ma.*, 
            ma.ma_parentid AS advisor_partyid
        FROM MastersAndHoldersCTE ma

        UNION ALL

        SELECT * FROM SistersWithAdvisorIdCTE
    )

    -- Final SELECT: All accounts
    SELECT 
        ca.ma_partyid,
        MAX(ca.ma_partyname),
		MAX(ca.ma_partyname),
		MAX(ca.ma_partyentity),
        ca.ma_parentid,
        MAX(ca.ma_clientid),
        MAX(ca.ma_partyorigin),
        ca.ma_partyclassification_value,
        ca.ma_partyidentifier_value,
        ca.ma_partyidentifier_reference,
        MAX(ca.ma_partycontact_firstname),
        MAX(ca.ma_partycontact_lastname),
        -- MAX(ca.ma_partycontact_fullname),
		MAX(ca.ma_partyname),
        MAX(ca.ma_partycontact_type),
        MAX(ca.ma_partycontact_addressline1),
        MAX(ca.ma_partycontact_addressline2),
        MAX(ca.ma_partycontact_addressline3),
        MAX(ca.ma_partycontact_addressline4),
        MAX(ca.ma_partycontact_addressline5),
        MAX(ca.ma_partycontact_addressline6),
        ---- MAX(fa.fa_name_1),
		---- MAX(fa.fa_name_2),
        ---- MAX(fa.fa_combinedaddress),
        ---- MAX(fa.fa_primaryemailaddress),
        ---- MAX(fa.fa_officetelephone),
        MAX(fa.fa_rep_num),
        MAX(lm.lm_entity),
        MAX(lm.lm_primary_logo),
        MAX(lm.lm_return_address),
        MAX(lm.lm_return_address_second),
        MAX(lm.lm_graphic_message_page1),
        MAX(lm.lm_statement_backer),
        JSON_AGG(
            CASE 
                WHEN ipa.ip_partyname IS NOT NULL THEN 
                    JSON_BUILD_OBJECT(
                        'Name', ipa.ip_partyname,
                        'Address', CONCAT_WS('|', 
                            NULLIF(ipa.ip_partycontact_addressline1, ''), 
                            NULLIF(ipa.ip_partycontact_addressline2, ''), 
                            NULLIF(ipa.ip_partycontact_addressline3, ''), 
                            NULLIF(ipa.ip_partycontact_addressline4, ''), 
                            NULLIF(ipa.ip_partycontact_addressline5, ''), 
                            NULLIF(ipa.ip_partycontact_addressline6, '')
                        )
                    )
            END
        ) AS duplicate_copies
    FROM CombinedAccountsCTE ca
    LEFT JOIN AdvisorsCTE fa
        ON ca.advisor_partyid = fa.fa_partyid
    LEFT JOIN InterestedPartiesCTE ipa
        ON ca.ma_partyid = ipa.ip_parentid
    LEFT JOIN LogosMessagesCTE lm
        ON ca.ma_partyid = lm.lm_partyid        
    GROUP BY 
        ca.ma_partyid, 
        ca.ma_parentid, 
        ca.ma_partyclassification_value, 
        ca.ma_partyidentifier_value,
        ca.ma_partyidentifier_reference

    UNION ALL

    -- IPs as standalone (only if they aren't already shown above)
    SELECT 
        ipa.ip_partyid,
		ca.ma_partyname,		
        ipa.ip_partyname,
		ipa.ip_partyentity,
        ipa.ip_parentid,
        ipa.ip_clientid,
        ipa.ip_partyorigin,
        ipa.ip_partyclassification_value,
        ipa.ip_partyidentifier_value,
        ipa.ip_partyidentifier_reference,
        ipa.ip_partycontact_firstname,
        ipa.ip_partycontact_lastname,
        -- ipa.ip_partycontact_fullname,
		' ' AS party_fullname,
        ipa.ip_partycontact_type,
        ipa.ip_partycontact_addressline1,
        ipa.ip_partycontact_addressline2,
        ipa.ip_partycontact_addressline3,
        ipa.ip_partycontact_addressline4,
        ipa.ip_partycontact_addressline5,
        ipa.ip_partycontact_addressline6,
        ---- fa.fa_name_1,
		---- fa.fa_name_2,
        ---- fa.fa_combinedaddress,
        ---- fa.fa_primaryemailaddress,
        ---- fa.fa_officetelephone,
        fa.fa_rep_num,
        lm.lm_entity,
        lm.lm_primary_logo,
        lm.lm_return_address,
        lm.lm_return_address_second,
        lm.lm_graphic_message_page1,
        lm.lm_statement_backer,
        NULL::JSON AS duplicate_copies
    FROM CombinedAccountsCTE ca
    JOIN InterestedPartiesCTE ipa
        ON ca.ma_partyid = ipa.ip_parentid
    LEFT JOIN LogosMessagesCTE lm
        ON ca.ma_partyid = lm.lm_partyid         
    LEFT JOIN AdvisorsCTE fa
        ON ca.advisor_partyid = fa.fa_partyid;

END;

$$;