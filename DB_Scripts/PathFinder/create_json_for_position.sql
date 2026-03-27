CREATE OR REPLACE FUNCTION public.create_json_for_position(
p_identifier_value text, clientid text, origin text)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
     holdingtype RECORD;
 BEGIN

     -- Loop through each holding and insert results into the temp table
     FOR holdingtype IN
         SELECT * FROM (VALUES
         -- Holdings
             ('WealthStatementAssetClass','Certificates of Deposits'),
             ('WealthStatementAssetClass','Fixed Income - Government and Agency Bonds'),
                     ('WealthStatementAssetClass','Fixed Income - Corporate Bonds'),
                     ('WealthStatementAssetClass','Fixed Income - Mortgage Bonds'),
                     ('WealthStatementAssetClass','Fixed Income - Municipal Bonds'),
                     ('WealthStatementAssetClass','Fixed Income - Structured Products'),
                     ('WealthStatementAssetClass','Equities and Options'),
                     ('WealthStatementAssetClass','ETFs'),
					 ('WealthStatementAssetClass','Preferred'),
					 ('WealthStatementAssetClass','Other'),
                     ('WealthStatementAssetClass','Mutual Funds'),
                     ('WealthStatementAssetClass','Alternative Investments'),
                     ('WealthStatementAssetClass','Unit Investment Trust'),
                     ('WealthStatementAssetClass','External Assets - Mutual Funds'),
                     ('WealthStatementAssetClass','External Assets - Alternative Investments'),
                     ('WealthStatementAssetClass','External Assets - Annuities')

                     ) AS s(holdingtypename, holdingtypevalue)
     LOOP
     -- Determine which function to call based on the holdingtypename
         IF holdingtype.holdingtypevalue IN (
             			 'Certificates of Deposits',
                         'Fixed Income - Government and Agency Bonds',
                         'Fixed Income - Corporate Bonds',
                         'Fixed Income - Mortgage Bonds',
                         'Fixed Income - Municipal Bonds',
                         'Fixed Income - Structured Products',
                         'Equities and Options',
                         'ETFs',
			 			 'Preferred',
						 'Other',
                         'Mutual Funds',
                         'Alternative Investments',
                         'Unit Investment Trust',
                         'External Assets - Mutual Funds',
                         'External Assets - Alternative Investments',
                         'External Assets - Annuities'

                 ) THEN
             PERFORM extract_position_details(p_identifier_value, clientid, origin, holdingtype.holdingtypename, holdingtype.holdingtypevalue);
         END IF;
     END LOOP;
 END;
$function$;
