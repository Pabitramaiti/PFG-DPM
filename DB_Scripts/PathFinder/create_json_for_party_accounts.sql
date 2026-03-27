CREATE OR REPLACE FUNCTION public.create_json_for_party_accounts(
    p_identifier_value text,
    clientid text,
    origin text
)
RETURNS VOID
LANGUAGE plpgsql
AS $function$
DECLARE
    v_partyid BIGINT;
    v_partyparentid BIGINT;
    v_partycontacttype TEXT;
    v_result jsonb;
BEGIN
    -- Build JSON structure for account/household data
    SELECT   jsonb_strip_nulls(
                   jsonb_build_object(
                       'PartyId', br.partyid,
                       'PartyName', br.partyname,
                       'PartyClassification', jsonb_build_array(
                           jsonb_strip_nulls(
                               jsonb_build_object(
                                   'PartyClassificationType', br.partyclassificationtype,
                                   'PartyClassificationValue', br.partyclassificationvalue
                               )
                           )
                       ),
                       'PartyCommon', jsonb_strip_nulls(
                           jsonb_build_object(
                               'PartyStatus', br.partystatus,
                               'PartyName', br.partyname,
                               'PartyParentid', br.partyparentid,
                               'PartyOrigin', br.partyorigin,
                               'PartyClientIdentifier', br.partyclientidentifier,
                               'PartyEntity', br.partyentity
                           )
                       ),
                       'PartyContact', jsonb_build_array(
                           jsonb_strip_nulls(
                               jsonb_build_object(
                                   'PartyContactIdentifierType', br.partycontactidentifiertype,
                                   'PartyContactIdentifierValue', br.partycontactidentifiervalue,
                                   'PartyContactType', br.partycontacttype,
                                   'PartyContactFullName', br.partycontactfullname,
                                   'PartyContactFirstName', br.partycontactfirstname,
                                   'PartyContactMiddleName', br.partycontactmiddlename,
                                   'PartyContactLastName', br.partycontactlastname,
                                   'PartyContactPrimaryEmailAddress', br.partycontactprimaryemailaddress,
                                   'PartyContactMobileTelephone', br.partycontactmobiletelephone
                               )
                           )
                       ),
                       'PartyPreference',jsonb_build_array(
                            jsonb_strip_nulls(
                                jsonb_build_object(
                                    'PartyPreferenceIdentifierType', br.partypreferenceidentifiertype,
                                    'PartyPreferenceIdentifierValue', br.partypreferenceidentifiervalue,
                                    'PartyPreferenceMethod', br.partypreferencemethod
                                )
                            )
                        ),
                       'PartyContactMailingAddress', jsonb_build_array(
                           jsonb_strip_nulls(
                               jsonb_build_object(
                                   'PartyContactAddressType', br.partycontactaddresstype,
                                   'PartyContactAddressLine1', br.partycontactaddressline1,
                                   'PartyContactAddressLine2', br.partycontactaddressline2,
                                   'PartyContactAddressLine3', br.partycontactaddressline3,
                                   'PartyContactAddressLine4', br.partycontactaddressline4,
                                   'PartyContactPostCodeZipCode', br.partycontactpostcodezipcode,
                                   'PartyContactCity', br.partycontactcity,
                                   'PartyStateProvinceCounty', br.partystateprovincecounty
                               )
                           )
                       ),
                       'PartyIdentifier', jsonb_build_array(
                           jsonb_strip_nulls(
                               jsonb_build_object(
                                   'PartyIdentifierType', br.partyidentifiertype,
                                   'PartyIdentifierValue', br.partyidentifiervalue
                               )
                           ),
                           jsonb_strip_nulls(
                               jsonb_build_object(
                                   'PartyIdentifierType', ani.partyidentifiertype,
                                   'PartyIdentifierValue', ani.partyidentifiervalue
                               )
                           ),
                           jsonb_strip_nulls(
                               jsonb_build_object(
                                   'PartyIdentifierType', agi.partyidentifiertype,
                                   'PartyIdentifierValue', agi.partyidentifiervalue
                               )
                           )
                       )
                   )
           )
    INTO v_result
    FROM (
        SELECT
            t1.partyid,
            t1.partyname,
            t1.partystatus,
            t1.partyparentid,
            t1.partyultimateparentid,
            t1.partyorigin,
            t1.partyclientidentifier,
            t1.partyentity,
            t3.partyclassificationtype,
            t3.partyclassificationvalue,
            t4.partyidentifiertype,
            t4.partyidentifiervalue,
            t5.partycontactidentifiertype,
            t5.partycontactidentifiervalue,
            t5.partycontacttype,
            t5.partycontactfullname,
            t5.partycontactfirstname,
            t5.partycontactmiddlename,
            t5.partycontactlastname,
            t5.partycontactprimaryemailaddress,
            t5.partycontactmobiletelephone,
            t6.partycontactaddresstype,
            t6.partycontactaddressline1,
            t6.partycontactaddressline2,
            t6.partycontactaddressline3,
            t6.partycontactaddressline4,
            t6.partycontactpostcodezipcode,
            t6.partycontactcity,
            t6.partystateprovincecounty,
            t7.PartyPreferenceIdentifierType,
            t7.PartyPreferenceIdentifierValue,
            t7.partypreferencemethod

        FROM partycommon t1
        JOIN partyclassification t3
            ON t1.partyid = t3.partyid
            AND t3.partyclassificationvalue IN ('Account')
        JOIN partyidentifier t4
            ON t1.partyid = t4.partyid
            AND t4.partyidentifiertype = 'OtherExternal'
        JOIN partycontact t5
            ON t1.partyid = t5.partyid
            AND t5.partycontacttype IN ('AccountHolder', 'HouseHoldMasterAccount', 'HouseHoldSisterAccount')
        JOIN partycontactmailingaddress t6
            ON t1.partyid = t6.partyid
            AND t6.partycontactaddresstype = 'PrimaryAddress'
        LEFT JOIN partypreference t7
        ON t1.partyid = t7.partyid
        WHERE t4.partyidentifiervalue = p_identifier_value
          AND t1.partyorigin = origin
          AND t1.partyclientidentifier = clientid
    ) br
    LEFT JOIN (
        SELECT
            pi.partyid,
            pi.partyidentifiertype,
            pi.partyidentifiervalue
        FROM partyidentifier pi
        WHERE pi.partyidentifiertype = 'AccountName'
    ) ani ON br.partyid = ani.partyid

    LEFT JOIN(
            SELECT
            pi.partyid,
            pi.partyidentifiertype,
            pi.partyidentifiervalue
        FROM partyidentifier pi
        WHERE pi.partyidentifiertype = 'AccountGroupIdentifier'
        AND pi.partyidentifiervalue <> 'N/A'
    ) agi ON br.partyid = agi.partyid;

    -- Default empty JSON array if no rows found
    v_result := COALESCE(v_result, '[]'::jsonb);

    SELECT pid.partyid, pcm.partyparentid, pc.partycontacttype
	INTO v_partyid, v_partyparentid, v_partycontacttype
	FROM partyidentifier pid
	JOIN partycommon pcm ON pid.partyid = pcm.partyid
	JOIN partycontact pc ON pid.partyid = pc.partyid
	WHERE pid.partyidentifiervalue = p_identifier_value
	AND pcm.partyorigin = origin
	AND pcm.partyclientidentifier = clientid LIMIT 1;

	-- Insert JSON result into temp_extraction_results
	IF v_result IS NOT NULL THEN
        INSERT INTO temp_extraction_results_party(
        partyid, clientid, origin, sectiontype, sectionheader, data
        )
        VALUES(p_identifier_value, clientid, origin, 'Account', v_partycontacttype, v_result);
    END IF;

	 -- Pulling Interested party
    PERFORM create_json_for_party_interested_parties(p_identifier_value, v_partyid, clientid, origin);
END;
$function$;
