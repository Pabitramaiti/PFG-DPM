CREATE OR REPLACE FUNCTION extract_logomsgdata_for_accounts(
    party_ids INTEGER[]
)
RETURNS TABLE (
    lm_partyid INTEGER,
    lm_entity VARCHAR,
    lm_primary_logo VARCHAR,
    lm_return_address VARCHAR,
    lm_return_address_second VARCHAR,
    lm_graphic_message_page1 VARCHAR,
    lm_statement_backer VARCHAR
)
LANGUAGE plpgsql
AS $$

BEGIN
    RETURN QUERY
    SELECT
        pid AS lm_partyid,
        lm.entity AS lm_entity,
        lm.primary_logo AS lm_primary_logo,
        lm.return_address AS lm_return_address,
        lm.return_address_secondary AS lm_return_address_second,
        lm.graphic_message_page1 AS lm_graphic_message_page1,
        lm.statement_backer AS lm_statement_backer
    FROM unnest(party_ids) AS pid
    JOIN LATERAL (
        SELECT *
        FROM "AUX".logomsgs lm
        WHERE lm.entityid = (
            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM partyidentifier p
                    WHERE p.partyid = pid
                      AND p.partyidentifierreference::INTEGER BETWEEN 70100000 AND 70999999
                    LIMIT 1
                ) THEN '1'
                WHEN EXISTS (
                    SELECT 1
                    FROM partyidentifier p
                    WHERE p.partyid = pid
                      AND p.partyidentifierreference::INTEGER BETWEEN 71100000 AND 71199999
                    LIMIT 1
                ) THEN '3'
                ELSE '2'
            END
        )
        LIMIT 1
    ) AS lm ON TRUE;
END;

$$;
