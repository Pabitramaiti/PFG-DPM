CREATE OR REPLACE FUNCTION public.create_json_for_communication(
    p_identifier_value text,
    origin text
)
RETURNS VOID
LANGUAGE plpgsql
AS $function$
DECLARE
    v_result jsonb;
BEGIN
    WITH comm_data AS (
        SELECT
            t5.communicationid,
            t5.communicationcustomtype,
            t5.communicationcustomkey,
            TRIM(t5.communicationcustomvalue) AS communicationcustomvalue
        FROM communicationcustom t5
        JOIN communicationcommon t2
            ON t5.communicationid = t2.communicationid
        JOIN communicationidentifier t3
            ON t5.communicationid = t3.communicationid
        WHERE t2.communicationorigin = origin
          AND t3.communicationidentifiervalue = p_identifier_value
    ),
    grouped AS (
        SELECT
            communicationid,
            jsonb_agg(DISTINCT jsonb_build_object(
                'CommunicationCustomType', communicationcustomtype,
                'CommunicationCustomKey', communicationcustomkey,
                'CommunicationCustomValue', communicationcustomvalue
            )) FILTER (WHERE communicationcustomtype IS NOT NULL) AS custom_data
        FROM comm_data
        GROUP BY communicationid
    )
    SELECT jsonb_agg(
        jsonb_build_object(
            'CommunicationID', communicationid,
            'CommunicationCustom', COALESCE(custom_data, '[]'::jsonb)
        )
    )
    INTO v_result
    FROM grouped;

    IF v_result IS NOT NULL THEN
    INSERT INTO temp_extraction_results_communication(
        partyid, clientid, origin, sectiontype, sectionheader, data
    )
    SELECT
        p_identifier_value,
        NULL,
        origin,
        'Communication',
        'Communication',
        item
    FROM jsonb_array_elements(v_result) AS item;
    END IF;

END;
$function$;