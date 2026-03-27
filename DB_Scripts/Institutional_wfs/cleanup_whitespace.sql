CREATE OR REPLACE FUNCTION cleanup_whitespace(
    input_text TEXT,
    mode TEXT DEFAULT 'normalize'
)
RETURNS TEXT AS $$
DECLARE
    cleaned_input TEXT := COALESCE(input_text, '');
BEGIN
    CASE lower(mode)
        WHEN 'normalize' THEN
            RETURN regexp_replace(trim(cleaned_input), '\s+', ' ', 'g');
        WHEN 'strip' THEN
            RETURN regexp_replace(cleaned_input, '\s+', '', 'g');
        ELSE
            RAISE EXCEPTION 'Unknown mode: %, expected "normalize" or "strip"', mode;
    END CASE;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
