CREATE OR REPLACE FUNCTION public.create_json_for_party_advisor
(p_identifier_value text, p_partyparentid bigint, clientid text, origin text)
RETURNS VOID
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_result jsonb;
BEGIN
    SELECT   jsonb_strip_nulls(
                    jsonb_build_object(
                        'PartyId', t1.partyid,
                        'PartyName', t1.partyname,
                        'PartyClassification', jsonb_build_array(
                            jsonb_strip_nulls(
                                jsonb_build_object(
                                    'PartyClassificationType', t3.partyclassificationtype,
                                    'PartyClassificationValue', t3.partyclassificationvalue
                                )
                            )
                        ),
                        'PartyCommon', jsonb_strip_nulls(
                            jsonb_build_object(
                                'PartyStatus', t1.partystatus,
                                'PartyName', t1.partyname,
                                'PartyParentid', t1.partyparentid,
                                'PartyOrigin', t1.partyorigin,
                                'PartyClientIdentifier', t1.partyclientidentifier,
                                'PartyEntity', t1.partyentity
                            )
                        ),
                        'PartyContact', jsonb_build_array(
                            jsonb_strip_nulls(
                                jsonb_build_object(
                                    'PartyContactIdentifierType', t5.partycontactidentifiertype,
                                    'PartyContactIdentifierValue', t5.partycontactidentifiervalue,
                                    'PartyContactType', t5.partycontacttype,
                                    'PartyContactFullName', t5.partycontactfullname,
                                    'PartyContactFirstName', t5.partycontactfirstname,
                                    'PartyContactMiddleName', t5.partycontactmiddlename,
                                    'PartyContactLastName', t5.partycontactlastname,
                                    'PartyContactPrimaryEmailAddress', t5.partycontactprimaryemailaddress,
                                    'PartyContactMobileTelephone', t5.partycontactmobiletelephone,
                                    'PartyContactURL', t5.partycontactsecondaryemailaddress
                                )
                            )
                        ),
                        'PartyPreference',jsonb_build_array(
                            jsonb_strip_nulls(
                                jsonb_build_object(
                                    'PartyPreferenceIdentifierType', t7.partypreferenceidentifiertype,
                                    'PartyPreferenceIdentifierValue', t7.partypreferenceidentifiervalue,
                                    'PartyPreferenceMethod', t7.partypreferencemethod
                                )
                            )
                        ),
                        'PartyContactMailingAddress', jsonb_build_array(
                            jsonb_strip_nulls(
                                jsonb_build_object(
                                    'PartyContactAddressType', t6.partycontactaddresstype,
                                    'PartyContactAddressLine1', t6.partycontactaddressline1,
                                    'PartyContactAddressLine2', t6.partycontactaddressline2,
                                    'PartyContactAddressLine3', t6.partycontactaddressline3,
                                    'PartyContactAddressLine4', t6.partycontactaddressline4,
                                    'PartyContactPostCodeZipCode', t6.partycontactpostcodezipcode,
                                    'PartyContactCity', t6.partycontactcity,
                                    'PartyStateProvinceCounty', t6.partystateprovincecounty
                                )
                            )
                        ),
                        'PartyIdentifier', jsonb_build_array(
                            jsonb_strip_nulls(
                                jsonb_build_object(
                                    'PartyIdentifierType', t4.partyidentifiertype,
                                    'PartyIdentifierValue', t4.partyidentifiervalue
                                )
                            )

                        )
                    )
            ) INTO v_result

   FROM partycommon t1
    JOIN partyclassification t3
		ON t1.partyid = t3.partyid
		AND t3.partyclassificationvalue='Advisor'
    JOIN partyidentifier t4
        ON t1.partyid = t4.partyid
		AND t4.partyidentifiertype='OtherExternal'
    JOIN partycontact t5
        ON t1.partyid = t5.partyid
		AND t5.partycontacttype = 'FinancialAdvisor'
    JOIN partycontactmailingaddress t6
		ON t1.partyid = t6.partyid
        AND t6.partycontactaddresstype = 'Office'
    LEFT JOIN partypreference t7
        ON t1.partyid = t7.partyid
    WHERE t1.partyid = p_partyparentid
        AND t1.partyorigin = origin
        AND t1.partyclientidentifier = clientid;

    IF v_result IS NOT NULL THEN
        INSERT INTO temp_extraction_results_party(partyid, clientid, origin, sectiontype, sectionheader, data)
        VALUES(p_identifier_value, clientid, origin, 'Advisor', 'FinancialAdvisor', v_result);
    END IF;
END;
$function$;
