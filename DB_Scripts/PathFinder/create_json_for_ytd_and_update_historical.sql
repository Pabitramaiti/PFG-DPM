CREATE OR REPLACE FUNCTION public.create_json_for_ytd_and_update_historical(clientidentifier_arg text, currency_arg text, temp_table_name text)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
        dynamic_sql text;
        table_name text := temp_table_name;
		db_rec record;
		db_rec_ytd record;
		array_len numeric;
		summary_object_hist JSONB;
		json_param text[];
		classification_key text :='';
		classification_part1 text :='';
		classification_part2 text :='';
		history_ytd_generation boolean := FALSE;

BEGIN

	dynamic_sql:= 'SELECT * FROM ' || table_name || ' WHERE clientid = $1 AND currency = $2' ;

	FOR db_rec IN EXECUTE dynamic_sql using clientidentifier_arg, currency_arg 	LOOP

		classification_key :='' ;

		IF db_rec.sectionheader = 'Income Summary' THEN

			FOR amount_index IN 0..COALESCE(jsonb_array_length(db_rec.data -> 'BalanceAmount'),0)-1  LOOP

				IF (db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType') IN (
								'DividendTaxable','DividendNonTaxable','InterestTaxable','InterestNonTaxable','TotalBalance' ) THEN

					FOR flag_index IN 0..COALESCE(jsonb_array_length(db_rec.data -> 'BalanceFlag'),0)-1  LOOP
						IF db_rec.data -> 'BalanceFlag' -> flag_index ->> 'BalanceFlagType' = 'IsPTD' THEN

							classification_key :='' ;
							classification_part1 := ' PTD ';
							classification_key := classification_key || classification_part1;

							INSERT INTO temp_extraction_results_summary
							VALUES (db_rec.partyid, db_rec.clientid, db_rec.origin, db_rec.currency,db_rec.sectiontype,db_rec.sectionheader,
							db_rec.recordtype, db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType', classification_key,   db_rec.data);

							WITH SummaryData AS (
								SELECT summary_object FROM (
									SELECT summary_object FROM (
										SELECT (
											build_summary_rows_from_historical(
												db_rec.partyid,
												db_rec.clientid,
												db_rec.origin,
												tsd.BeginningDate,
												classification_key,
												db_rec.recordtype,
												db_rec.sectionheader,
												db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType',
												currency_arg,
												(db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountValue')::numeric
											)
										).summary_object AS summary_object
										FROM temp_statement_dates_summary tsd
									) AS sub

								) result
							)

							SELECT summary_object INTO summary_object_hist FROM SummaryData;

							IF history_ytd_generation = TRUE THEN

								db_rec_ytd := db_rec;
								json_param='{BalanceAmount, '||amount_index||' , BalanceAmountValue}';
								db_rec_ytd.data := jsonb_set(db_rec_ytd.data, json_param,
											to_jsonb((summary_object_hist ->> 'ytdperiod')::numeric));

								json_param='{BalanceFlag, '||flag_index||' , BalanceFlagType}';
								db_rec_ytd.data := jsonb_set(db_rec_ytd.data, json_param, '"IsYTD"');

								classification_key :='' ;
								classification_part1 := ' YTD ';
								classification_key := classification_key || classification_part1;

								INSERT INTO temp_extraction_results_summary
								VALUES (db_rec_ytd.partyid, db_rec_ytd.clientid, db_rec_ytd.origin, db_rec_ytd.currency, db_rec_ytd.sectiontype, db_rec_ytd.sectionheader, db_rec_ytd.recordtype, db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType', classification_key,  db_rec_ytd.data) ;
							END IF;
						END IF;
						IF db_rec.data -> 'BalanceFlag' -> flag_index ->> 'BalanceFlagType' = 'IsYTD' THEN
							IF history_ytd_generation = FALSE THEN

								classification_key :='' ;
								classification_part1 := ' YTD ';
								classification_key := classification_key || classification_part1;

								INSERT INTO temp_extraction_results_summary
								VALUES (db_rec.partyid, db_rec.clientid, db_rec.origin, db_rec.currency,db_rec.sectiontype,db_rec.sectionheader,
								db_rec.recordtype, db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType', classification_key,  db_rec.data);
							END IF;
						END IF;
					END LOOP;
				END IF;
			END LOOP;
		END IF;

		--Next section
		IF db_rec.sectionheader = 'Asset Allocation' THEN

			FOR amount_index IN 0..COALESCE(jsonb_array_length(db_rec.data -> 'BalanceAmount'),0)-1  LOOP

				IF (db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType') IN (
								'TotalCash','TotalFixedIncome','TotalEquityValue','TotalAlternativeInvestments','TotalCertificatesOfDeposits',
								'TotalMutualFunds','TotalUnitInvestmentTrust') THEN

					classification_key :='' ;

					WITH SummaryData AS (
						SELECT summary_object FROM (
							SELECT summary_object FROM (
								SELECT (
									build_summary_rows_from_historical(
										db_rec.partyid,
										db_rec.clientid,
										db_rec.origin,
										tsd.BeginningDate,
										classification_key,
										db_rec.recordtype,
										db_rec.sectionheader,
										db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType',
										currency_arg,
										(db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountValue')::numeric
									)
								).summary_object AS summary_object
								FROM temp_statement_dates_summary tsd
							) AS sub

						) result
					)

					SELECT summary_object INTO summary_object_hist FROM SummaryData;

					INSERT INTO temp_extraction_results_summary
					VALUES (db_rec.partyid, db_rec.clientid, db_rec.origin, db_rec.currency,db_rec.sectiontype,db_rec.sectionheader,
					db_rec.recordtype, db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType', classification_key,  db_rec.data);

				END IF;
			END LOOP;
		END IF;

		--Next section
		IF db_rec.sectionheader = 'Margin Summary' THEN

			FOR amount_index IN 0..COALESCE(jsonb_array_length(db_rec.data -> 'BalanceAmount'),0)-1  LOOP

				IF (db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType') IN (
								'TotalMarginBalance','TotalCashAvailableForWithdraw','TotalCashAvailableForTrade' ) THEN

					classification_key := '';

					WITH SummaryData AS (
						SELECT summary_object FROM (
							SELECT summary_object FROM (
								SELECT (
									build_summary_rows_from_historical(
										db_rec.partyid,
										db_rec.clientid,
										db_rec.origin,
										tsd.BeginningDate,
										classification_key,
										db_rec.recordtype,
										db_rec.sectionheader,
										db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType',
										currency_arg,
										(db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountValue')::numeric
									)
								).summary_object AS summary_object
								FROM temp_statement_dates_summary tsd
							) AS sub

						) result
					)

					SELECT summary_object INTO summary_object_hist FROM SummaryData;

					INSERT INTO temp_extraction_results_summary
					VALUES (db_rec.partyid, db_rec.clientid, db_rec.origin, db_rec.currency,db_rec.sectiontype,db_rec.sectionheader,
					db_rec.recordtype, db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType', classification_key, db_rec.data);
				END IF;
			END LOOP;
		END IF;

	 --Next section
	 	IF db_rec.sectionheader = 'Retirement Summary' THEN

			FOR amount_index IN 0..COALESCE(jsonb_array_length(db_rec.data -> 'BalanceAmount'),0)-1  LOOP

				IF (db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType') IN (
								'RetirementContributions','RetirementDistributions','TaxWithheld','FairMarketValue','RequiredMinimumDistributionAmount','RequiredDistributionRemaining','AnnualMaintenancefee') THEN

					FOR flag_index IN 0..jsonb_array_length(db_rec.data -> 'BalanceFlag')-1  LOOP
						IF db_rec.data -> 'BalanceFlag' -> flag_index ->> 'BalanceFlagType' = 'IsPTD' THEN

							classification_key :='' ;
							classification_part1 := ' PTD ';
							classification_key := classification_key || classification_part1;

							INSERT INTO temp_extraction_results_summary
							VALUES (db_rec.partyid, db_rec.clientid, db_rec.origin, db_rec.currency,db_rec.sectiontype,db_rec.sectionheader,
							db_rec.recordtype, db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType', classification_key,
  db_rec.data);

							WITH SummaryData AS (
								SELECT summary_object FROM (
									SELECT summary_object FROM (
										SELECT (
											build_summary_rows_from_historical(
												db_rec.partyid,
												db_rec.clientid,
												db_rec.origin,
												tsd.BeginningDate,
												'',
												db_rec.recordtype,
												db_rec.sectionheader,
												db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType',
												currency_arg,
												(db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountValue')::numeric
											)
										).summary_object AS summary_object
										FROM temp_statement_dates_summary tsd
									) AS sub
							--		WHERE (summary_object ->> 'thisperiod')::float != 0
							--		  OR (summary_object ->> 'previousperiod')::float != 0
							--		  OR (summary_object ->> 'ytdperiod')::float != 0

								) result
							)

							SELECT summary_object INTO summary_object_hist FROM SummaryData;

							IF history_ytd_generation = TRUE THEN

								db_rec_ytd := db_rec;

								json_param='{BalanceAmount, '||amount_index||' , BalanceAmountValue}';
								db_rec_ytd.data := jsonb_set(db_rec_ytd.data, json_param,
											to_jsonb((summary_object_hist ->> 'ytdperiod')::numeric));

								json_param='{BalanceFlag, '||flag_index||' , BalanceFlagType}';
								db_rec_ytd.data := jsonb_set(db_rec_ytd.data, json_param, '"IsYTD"');

								classification_key :='' ;
								classification_part1 := ' YTD ';
								classification_key := classification_key || classification_part1;

								INSERT INTO temp_extraction_results_summary
								VALUES (db_rec_ytd.partyid, db_rec_ytd.clientid, db_rec_ytd.origin, db_rec_ytd.currency, db_rec_ytd.sectiontype, db_rec_ytd.sectionheader, db_rec_ytd.recordtype, db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType', classification_key,  db_rec_ytd.data) ;
							END IF;
						END IF;
						IF db_rec.data -> 'BalanceFlag' -> flag_index ->> 'BalanceFlagType' = 'IsYTD' THEN
							IF history_ytd_generation = FALSE THEN

								classification_key :='' ;
								classification_part1 := ' YTD ';
								classification_key := classification_key || classification_part1;

								INSERT INTO temp_extraction_results_summary
								VALUES (db_rec.partyid, db_rec.clientid, db_rec.origin, db_rec.currency,db_rec.sectiontype,db_rec.sectionheader,
								db_rec.recordtype, db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType', classification_key, db_rec.data);
							END IF;
						END IF;
					END LOOP;
				END IF;
			END LOOP;
		END IF;

		--Next section
		IF db_rec.sectionheader = 'GainLoss Summary' THEN
	--	RAISE NOTICE 'db_rec %', db_rec;

			FOR amount_index IN 0..COALESCE(jsonb_array_length(db_rec.data -> 'BalanceAmount'),0)-1  LOOP

				IF (db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType') IN (
								'ShortTermGain','ShortTermLoss','ShortTermGainOrLoss','LongTermGain','LongTermLoss',
								'LongTermGainOrLoss','TotalRealizedProfitLoss','TotalUnrealizedGainLoss') THEN

							classification_key := '';
							classification_part1 :='';
							classification_part2 :='';

					FOR flag_index IN 0..COALESCE(jsonb_array_length(db_rec.data -> 'BalanceFlag'),0)-1  LOOP
						IF (db_rec.data -> 'BalanceFlag' -> flag_index ->> 'BalanceFlagType' = 'IsPTD') THEN
						    IF (db_rec.data -> 'BalanceFlag' -> flag_index ->> 'BalanceFlagValue' = '1') THEN
								classification_part1 :=' PTD ';
							END IF;
						END IF;
						IF (db_rec.data -> 'BalanceFlag' -> flag_index ->> 'BalanceFlagType' = 'IsYTD') THEN
						    IF (db_rec.data -> 'BalanceFlag' -> flag_index ->> 'BalanceFlagValue' = '1') THEN
								classification_part1 := ' YTD ';
							END IF;
						END IF;
						IF (db_rec.data -> 'BalanceFlag' -> flag_index ->> 'BalanceFlagType' = 'IsRealized') THEN
						    IF (db_rec.data -> 'BalanceFlag' -> flag_index ->> 'BalanceFlagValue' = '1') THEN
								classification_part2 := ' Realized ';
							ELSE
								classification_part2 := ' unRealized ';
							END IF;
						END IF;
					END LOOP;

					classification_key := classification_part1 || classification_part2;

	--- Check with Balamurugan, on the primary keys of historical table. Especially gainloss summary. Should we increase number of keys
	--  Or merge important field values in one and put in the remaining field
	-- Also check how PTD and YTD will go in historical table for realized YTD.

					WITH SummaryData AS (
						SELECT summary_object FROM (
							SELECT summary_object FROM (
								SELECT (
									build_summary_rows_from_historical(
										db_rec.partyid,
										db_rec.clientid,
										db_rec.origin,
										tsd.BeginningDate,
										classification_key,
										db_rec.recordtype,
										db_rec.sectionheader,
										db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType',
										currency_arg,
										(db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountValue')::numeric
									)
								).summary_object AS summary_object
								FROM temp_statement_dates_summary tsd
							) AS sub

						) result
					)

					SELECT summary_object INTO summary_object_hist FROM SummaryData;

					INSERT INTO temp_extraction_results_summary
					VALUES (db_rec.partyid, db_rec.clientid, db_rec.origin, db_rec.currency,db_rec.sectiontype,db_rec.sectionheader,
					db_rec.recordtype,  db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType', classification_key,  db_rec.data);

				END IF;
			END LOOP;
		END IF;

			--Next section
		IF db_rec.sectionheader = 'Market Value Over Time' THEN

			FOR amount_index IN 0..COALESCE(jsonb_array_length(db_rec.data -> 'BalanceAmount'),0)-1  LOOP

				IF (db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType') IN (
								'PortfolioValue') THEN

				--- find narrative type and put it in classification...
					FOR narrative_index IN 0..COALESCE(jsonb_array_length(db_rec.data -> 'BalanceNarrative'),0)-1  LOOP
						IF (db_rec.data -> 'BalanceNarrative' -> narrative_index ->> 'BalanceNarrativeType' = 'Description') THEN

							classification_key :='' ;
							classification_part1 := db_rec.data -> 'BalanceNarrative' -> narrative_index ->> 'BalanceNarrativeValue';
							classification_key := classification_key || classification_part1;

						END IF;
					END LOOP;

					WITH SummaryData AS (
						SELECT summary_object FROM (
							SELECT summary_object FROM (
								SELECT (
									build_summary_rows_from_historical(
										db_rec.partyid,
										db_rec.clientid,
										db_rec.origin,
										tsd.BeginningDate,
										classification_key,
										db_rec.recordtype,
										db_rec.sectionheader,
										db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType',
										currency_arg,
										(db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountValue')::numeric
									)
								).summary_object AS summary_object
								FROM temp_statement_dates_summary tsd
							) AS sub
						) result
					)

					SELECT summary_object INTO summary_object_hist FROM SummaryData;

					INSERT INTO temp_extraction_results_summary
					VALUES (db_rec.partyid, db_rec.clientid, db_rec.origin, db_rec.currency,db_rec.sectiontype,db_rec.sectionheader,
					db_rec.recordtype, db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType', classification_key,  db_rec.data);

				END IF;
			END LOOP;
		END IF;

	--Next section
	-- Tranansaction overview can be merged with Asset allocation. We need to remove the if condition checking all amount types. Only risk is,
	-- if the balanceamount will have multiple balanceamounttypes, then it will not work. Other way could be, keep the if condition, in the
	-- comparison array, give all balanceamounttypes which you are expecting from asset allocation and transaction overview, together.
		IF db_rec.sectionheader = 'Transaction Overview' THEN

			FOR amount_index IN 0..COALESCE(jsonb_array_length(db_rec.data -> 'BalanceAmount'),0)-1  LOOP

				IF (db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType') IN (
								'CreditsValue','DebitsValue','SalesAndRedemptions',
								'IncomeAndDistributions','FundsReceived','SecuritiesReceived',
								'Purchases','Reinvestments','FeesAndOther','SecuritiesDelivered') THEN

					classification_key :='' ;

					WITH SummaryData AS (
						SELECT summary_object FROM (
							SELECT summary_object FROM (
								SELECT (
									build_summary_rows_from_historical(
										db_rec.partyid,
										db_rec.clientid,
										db_rec.origin,
										tsd.BeginningDate,
										classification_key,
										db_rec.recordtype,
										db_rec.sectionheader,
										db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType',
										currency_arg,
										(db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountValue')::numeric
									)
								).summary_object AS summary_object
								FROM temp_statement_dates_summary tsd
							) AS sub
						) result
					)

					SELECT summary_object INTO summary_object_hist FROM SummaryData;

					INSERT INTO temp_extraction_results_summary
					VALUES (db_rec.partyid, db_rec.clientid, db_rec.origin, db_rec.currency,db_rec.sectiontype,db_rec.sectionheader,
					db_rec.recordtype, db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType', classification_key,  db_rec.data);

				END IF;
			END LOOP;
		END IF;

	-- Next section
		IF db_rec.sectionheader = 'Change In Value' THEN

			FOR amount_index IN 0..COALESCE(jsonb_array_length(db_rec.data -> 'BalanceAmount'),0)-1  LOOP

				IF (db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType') IN (
							'PortfolioValue','BeginningValue','CreditsValue',
							'DebitsValue','OtherActivityValue','EndingValue',
							'ChangeInMarketValue','NetChangeAmount','AssetsNotHeldValue','AccruedInterest','TotalBalance') THEN

					FOR flag_index IN 0..COALESCE(jsonb_array_length(db_rec.data -> 'BalanceFlag'),0)-1  LOOP
						IF db_rec.data -> 'BalanceFlag' -> flag_index ->> 'BalanceFlagType' = 'IsPTD' THEN

							classification_key :='' ;
							classification_part1 := ' PTD ';
							classification_key := classification_key || classification_part1;

							INSERT INTO temp_extraction_results_summary
							VALUES (db_rec.partyid, db_rec.clientid, db_rec.origin, db_rec.currency,db_rec.sectiontype,db_rec.sectionheader,
							db_rec.recordtype,  db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType', classification_key, db_rec.data);

							WITH SummaryData AS (
								SELECT summary_object FROM (
									SELECT summary_object FROM (
										SELECT (
											build_summary_rows_from_historical(
												db_rec.partyid,
												db_rec.clientid,
												db_rec.origin,
												tsd.BeginningDate,
												'',
												db_rec.recordtype,
												db_rec.sectionheader,
												db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType',
												currency_arg,
												(db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountValue')::numeric
											)
										).summary_object AS summary_object
										FROM temp_statement_dates_summary tsd
									) AS sub
								) result
							)

							SELECT summary_object INTO summary_object_hist FROM SummaryData;

							IF history_ytd_generation = TRUE
							--	OR (db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType') IN ('AssetsNotHeldValue','TotalBalance') 
								THEN

								db_rec_ytd := db_rec;
								json_param='{BalanceAmount, '||amount_index||' , BalanceAmountValue}';
								db_rec_ytd.data := jsonb_set(db_rec_ytd.data, json_param,
											to_jsonb((summary_object_hist ->> 'ytdperiod')::numeric));

								json_param='{BalanceFlag, '||flag_index||' , BalanceFlagType}';
								db_rec_ytd.data := jsonb_set(db_rec_ytd.data, json_param, '"IsYTD"');

								classification_key :='' ;
								classification_part1 := ' YTD ';
								classification_key := classification_key || classification_part1;

								INSERT INTO temp_extraction_results_summary
								VALUES (db_rec_ytd.partyid, db_rec_ytd.clientid, db_rec_ytd.origin, db_rec_ytd.currency, db_rec_ytd.sectiontype, db_rec_ytd.sectionheader, db_rec_ytd.recordtype, db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType', classification_key,  db_rec_ytd.data) ;
							END IF;
						END IF;
						IF db_rec.data -> 'BalanceFlag' -> flag_index ->> 'BalanceFlagType' = 'IsYTD' THEN
							IF history_ytd_generation = FALSE THEN

								classification_key :='' ;
								classification_part1 := ' YTD ';
								classification_key := classification_key || classification_part1;

								INSERT INTO temp_extraction_results_summary
								VALUES (db_rec.partyid, db_rec.clientid, db_rec.origin, db_rec.currency,db_rec.sectiontype,db_rec.sectionheader,
								db_rec.recordtype,  db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType', classification_key,  db_rec.data);
							END IF;
						END IF;
					END LOOP;
				END IF;
			END LOOP;
		END IF;



	 -- Making entries for Household gainloss summary
		IF db_rec.sectionheader = 'Household GainLoss Summary' THEN

			FOR amount_index IN 0..COALESCE(jsonb_array_length(db_rec.data -> 'BalanceAmount'),0)-1  LOOP

				IF (db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType') IN (
								'ShortTermGain','ShortTermLoss','ShortTermGainOrLoss','LongTermGain','LongTermLoss',
								'LongTermGainOrLoss','TotalRealizedProfitLoss' ) THEN

							WITH SummaryData AS (
								SELECT summary_object FROM (
								SELECT summary_object FROM (
									SELECT (
									build_summary_rows_from_historical(
										db_rec.partyid,
										db_rec.clientid,
										db_rec.origin,
										tsd.BeginningDate,
								--		'',        --this is date field, given empty string currently
										db_rec.classification,
										db_rec.recordtype,
										db_rec.sectionheader,
										db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType',
										currency_arg,
										(db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountValue')::numeric
									)
								).summary_object AS summary_object
										FROM temp_statement_dates_summary tsd
								) AS sub
							) result
							)

							SELECT summary_object INTO summary_object_hist FROM SummaryData;

				END IF;
			END LOOP;
		END IF;

	 -- Making entries for Household Income summary
		IF db_rec.sectionheader = 'Household Income Summary' THEN

			FOR amount_index IN 0..COALESCE(jsonb_array_length(db_rec.data -> 'BalanceAmount'),0)-1  LOOP

				IF (db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType') IN (
								'DividendTaxable','DividendNonTaxable','InterestTaxable','InterestNonTaxable','TotalBalance') THEN

							WITH SummaryData AS (
								SELECT summary_object FROM (
								SELECT summary_object FROM (
									SELECT (
									build_summary_rows_from_historical(
										db_rec.partyid,
										db_rec.clientid,
										db_rec.origin,
										tsd.BeginningDate,
								--		'',        --this is date field, given empty string currently
										db_rec.classification,
										db_rec.recordtype,
										db_rec.sectionheader,
										db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType',
										currency_arg,
										(db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountValue')::numeric
									)
								).summary_object AS summary_object
										FROM temp_statement_dates_summary tsd
								) AS sub
							) result
							)

							SELECT summary_object INTO summary_object_hist FROM SummaryData;

				END IF;
			END LOOP;
		END IF;

	 -- Making entries for Household Asset Allocation
		IF db_rec.sectionheader = 'Household Asset Allocation' THEN

			FOR amount_index IN 0..COALESCE(jsonb_array_length(db_rec.data -> 'BalanceAmount'),0)-1  LOOP

			--	IF (db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType') IN (
			--					'DividendTaxable','DividendNonTaxable','InterestTaxable','InterestNonTaxable','TotalBalance') THEN

							WITH SummaryData AS (
								SELECT summary_object FROM (
								SELECT summary_object FROM (
									SELECT (
									build_summary_rows_from_historical(
										db_rec.partyid,
										db_rec.clientid,
										db_rec.origin,
										tsd.BeginningDate,
										db_rec.classification,
										db_rec.recordtype,
										db_rec.sectionheader,
										db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType',
										currency_arg,
										(db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountValue')::numeric
									)
								).summary_object AS summary_object
										FROM temp_statement_dates_summary tsd
								) AS sub
							) result
							)

							SELECT summary_object INTO summary_object_hist FROM SummaryData;

			--	END IF;
			END LOOP;
		END IF;
	-- Next section will come here

        -- Making entries for Household summary
		IF db_rec.sectionheader = 'HouseHold Summary' THEN
			FOR amount_index IN 0..COALESCE(jsonb_array_length(db_rec.data -> 'BalanceAmount'),0)-1  LOOP
				IF (db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType') IN (
								'BeginningValue','YTDBeginningValue','EndingValue','ChangeInMarketValue',
								'TotalBeginningBalance','TotalEndingBalance','TotalChangeInMarketValue') THEN
							WITH SummaryData AS (
								SELECT summary_object FROM (
								SELECT summary_object FROM (
									SELECT (
									build_summary_rows_from_historical(
										db_rec.partyid,
										db_rec.clientid,
										db_rec.origin,
										tsd.BeginningDate,
								--		'',        --this is date field, given empty string currently
										db_rec.classification,
										db_rec.recordtype,
										db_rec.sectionheader,
										db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountType',
										currency_arg,
										(db_rec.data -> 'BalanceAmount' -> amount_index ->> 'BalanceAmountValue')::numeric
									)
								).summary_object AS summary_object
										FROM temp_statement_dates_summary tsd
								) AS sub
							) result
							)

							SELECT summary_object INTO summary_object_hist FROM SummaryData;
				END IF;
			END LOOP;
		END IF;
	END LOOP;

END;
$function$
