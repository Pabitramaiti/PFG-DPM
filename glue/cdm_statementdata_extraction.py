import boto3
import psycopg2
from awsglue.utils import getResolvedOptions
from itertools import groupby
import uuid
from collections import defaultdict
from sqlalchemy import Table, Column, create_engine, text, String, MetaData, Numeric, Integer, Float, BigInteger, JSON, insert, select, func, inspect, bindparam, ARRAY, and_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from decimal import Decimal
import splunk  # import the module from the s3 bucket
#import jsonformatter
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
import json
import time
import os
#import xmltodict
import zipfile
from io import BytesIO
import re
import copy
import traceback
import sys
import glob
import shutil
from collections import defaultdict

# simplefilestorage to get data in s3 object in bytes
glue_client = boto3.client('glue')
s3 = boto3.client('s3')
ssm_client = boto3.client("secretsmanager")

def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:store_auxrecords_postgres:" + account_id

def get_secret_values(secret_name):
    response = ssm_client.get_secret_value(SecretId = secret_name)
    secret_value = response['SecretString']
    json_secret_value = json.loads(secret_value)

    return json_secret_value

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)

class DataPostgres:
    def __init__(self):
        self.conn = None
        self.engine = None
        self.trigger_file_name = os.getenv("trigger_file_name")
        self.run_type = (self.trigger_file_name.split('_')[1] if len(self.trigger_file_name.split('_')) > 2 else '')
        self.client_name = os.getenv("client_name")
        self.sql_queries = json.loads(os.getenv("sql_queries"))
        self.s3_wip = os.getenv("s3_wip")
        #self.update_recon_table_query = self.sql_queries.get("update_recon_table")
        self.get_statement_date = self.sql_queries.get("get_statement_date")
        self.get_section_list = self.sql_queries.get("get_section_list")
        self.get_section_data = self.sql_queries.get("get_section_data")
        self.s3_bucket = os.getenv("bucketName")
        self.combinations = self.sql_queries.get("combinations")
        self.driver_layout = json.loads(os.getenv("driver_layout"))
        self.formats = json.loads(os.getenv("formats"))
        self.partyid_list_values = json.loads(os.getenv("partyids_list"))
        self.historical_table_name = self.partyid_list_values.get('historical_table_name')
        self.stagingtable = self.partyid_list_values.get('staging_table_name')
        self.tableinfo=json.loads(os.getenv("tableInfo"))
        self.region = self.tableinfo.get("region")
        self.dbkey=self.tableinfo.get("dbkey")
        self.dbname=self.tableinfo.get("dbname")
        self.schema=self.tableinfo.get("schema")
        self.message = ""
        self.index = 1
        self.records = {}
        self.currency_output = {}
        self.first_row = True
        self.section_file_handles = {}  # section_key -> open file
        self.section_row_flags = {}     # section_key -> True/False for comma control
        self.currency_sections = [] 
        self.is_household = 'Y'
        #self.individual_statements_for = ["AccountHolder", "InterestedParty", "HouseHoldMasterAccount", "HouseHoldSisterAccount"]
        self.individual_statements_for = ["AccountHolder", "InterestedParty"]
        self.summary_section_order = list(self.driver_layout[0].get("template", {}).get("detail_summary_table", {}).keys())
        self.transactions_section_order = list(self.driver_layout[0].get("template", {}).get("detail_transactions_table", {}).keys())
        self.holdings_section_order = list(self.driver_layout[0].get("template", {}).get("detail_holdings_table", {}).keys())
        now = datetime.now()
        self.mmddyyyy = now.strftime('%m%d%Y')
        self.timestamp = now.strftime('%H%M%S')
        self.unique_id = uuid.uuid4().hex
        self.create_connection()
    
    def create_connection(self):
        
        try:
            #creates engine connection
            ssm = boto3.client('ssm', region_name=self.region)
            ssmdbkey = self.dbkey
            secret_name = ssm.get_parameter(Name=ssmdbkey, WithDecryption=True)
            secret_name=secret_name['Parameter']['Value']
            host = get_secret_values(secret_name)
            #self.engine = create_engine(f"postgresql://{host['username']}:{host['password']}@{host['host']}:{host['port']}/{host['dbname']}", future=True)
            self.engine = create_engine(f"postgresql://{host['username']}:{host['password']}@{host['host']}:{host['port']}/{self.dbname}", future=True)
            
            self.conn = self.engine.connect()

            # Schema should be obtained from config file
            #self.meta = MetaData(schema="bala_cetera")
            self.meta = MetaData(schema=self.schema)
            #self.meta = MetaData()

            result = splunk.log_message({'Status': 'success', 'InputFileName': 'test file','Message': 'Connection with Aurora Postgres Cluster is established'},  get_run_id())
            
        except Exception as e:
            self.message = "Connection failed due to error "+ str(e)

            result = splunk.log_message({
                'InputFileName': 'test file',
                'Status':'failed',
                'Message': self.message},  get_run_id())

            raise Exception(self.message)

    def extract_partyids(self):
        try:
            # Logic to extract list of advisor partyids from the config data
            partyids_list = self.partyid_list_values.get('partyid_list', [])
        
            return partyids_list

        except Exception as e:
            message = f"Advisor partyid extraction failed. {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': self.trigger_file_name, 'Message': message}, get_run_id())
            raise Exception(message)

    def extract_account_partyids(self, extracted_account_data):
        try:
            #if "wfs" in self.trigger_file_name.lower():
            #if self.client_name.lower() in ['wfs']:
                #account_partyids = [account_party_list['party_identifiervalue'] for account_party_list in extracted_account_data]
            #else:
            #    account_partyids = [account_party_list['party_id'] for account_party_list in extracted_account_data]          
            account_partyids = [account_party_list['party_identifiervalue'] for account_party_list in extracted_account_data]
            return account_partyids

        except Exception as e:
            message = f"Account partyid extraction failed. {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': self.trigger_file_name, 'Message': message}, get_run_id())
            raise Exception(message)

    def generate_account_sql_query(self, partyid_list):
        try:
            sql_account_query = self.sql_queries.get("party", "")
            sql_account_query = sql_account_query.format(party_ids=partyid_list)

            return sql_account_query

        except Exception as e:
            message = f"Account sql query generation failed. {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': self.trigger_file_name, 'Message': message}, get_run_id())
            raise Exception(message)

    def execute_account_sql_query(self, sql_query):
        try:
            with self.engine.connect() as connection:
                # Execute SQL query
                connection.execute(text(f"SET search_path TO {self.schema}"))
                extracted_account_data_rows = connection.execute(text(sql_query))

            return extracted_account_data_rows

        except Exception as e:
            message = f"Account sql query execution failed. {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': self.trigger_file_name, 'Message': message}, get_run_id())
            raise Exception(message)

    def generate_transaction_sql_query(self, partyid_str, partyclientid_str):
        try:
            sql_transaction_query = self.sql_queries.get("transaction", "")

            sql_transaction_query = sql_transaction_query.format(partyid=partyid_str,clientidentifier=partyclientid_str,historicaltablename=self.historical_table_name)

            return sql_transaction_query

        except Exception as e:
            message = f"Transaction sql query generation failed. {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': self.trigger_file_name, 'Message': message}, get_run_id())
            raise Exception(message)

    def execute_transaction_sql_query(self, sql_transaction_query, partydata, batch_size=10000, batch_processor=None):
        try:
            # Use raw psycopg2 connection for full control
            raw_conn = self.engine.raw_connection()
            try:
                #self.currency_output = {}
                raw_conn.autocommit = False  # Required to keep temp tables alive
                summary_sections = defaultdict(list)
                db_section_combinations = set()
                currencies_with_data = set()  # Currencies present in temp_extraction_results

                with raw_conn.cursor() as cur:
                    # Set schema
                    cur.execute(f"SET search_path TO {self.schema}")

                    # Execute your SQL query or function (e.g., create_json_for_extraction)
                    query_start_time = time.time()
                    cur.execute(sql_transaction_query)  # Pass string like SELECT create_json_for_extraction(...)
                    query_end_time = time.time()

                    # Calculate time difference in minutes
                    query_time_taken = query_end_time - query_start_time
                    print(f"Execution time for SQL query: {query_time_taken:.4f} seconds")

                    # transaction_records = cur.fetchone()[0]  # Already a list of dicts from jsonb
                    result = cur.fetchone()
                    transaction_records = result[0] if result and result[0] else []

                    for record in transaction_records:
                        currency = record.get("transactionamountcurrency")
                        sectiontype = record.get("sectiontype")
                        sectionheader = record.get("sectionheader")

                        if sectiontype == "Summary" and currency and sectionheader:
                            summary_sections[(currency, sectiontype, sectionheader)].append(record)

                    # Step 0: Special case — get 'Statement Date' data where currency is NULL
                    cur.execute(self.get_statement_date)

                    statementdate_transactions = cur.fetchall()
                    #print("statementdate_transactions : ", statementdate_transactions)

                    if statementdate_transactions:
                        statement_data = statementdate_transactions[0][0]

                        # Write to a party-specific temp file
                        party_ref = partydata["party_identifierreference"]
                        temp_file_path = f"/tmp/statement_date_{party_ref}.json"

                        with open(temp_file_path, "w", encoding="utf-8") as f:
                            json.dump(statement_data, f, ensure_ascii=False, separators=(",", ":"))

                    # Step 1: Get all combinations
                    cur.execute(self.get_section_list)
                    db_section_combinations = set(cur.fetchall())

                    # Record currencies present in DB (holdings/transactions)
                    currencies_with_data = {c[0] for c in db_section_combinations}

                all_combinations = set(db_section_combinations)
                all_combinations.update(summary_sections.keys())
                
                all_combinations = sorted(
                    all_combinations,
                    key=lambda x: (
                        0 if x[0] == "USD" else 1,
                        x[0],
                        x[1],
                        x[2]
                    )
                )

                # ------------------------------------------------------
                # Determine currencies to skip based on your rule
                # ------------------------------------------------------
                currencies_to_skip = set()

                for currency in {c[0] for c in all_combinations}:
                    # Only consider currencies with NO holdings/transactions
                    if currency in currencies_with_data:
                        continue

                    pv_records = summary_sections.get(
                        (currency, "Summary", "Portfolio Value"), []
                    )

                    # Extract Net Cash Balance records
                    net_cash_records = [
                        rec for rec in pv_records
                        if rec.get("classification") == "Net Cash Balance"
                    ]

                    # Case 1: Net Cash Balance exists and is 0
                    if net_cash_records and all(rec.get("thisperiod") == 0 for rec in net_cash_records):
                        currencies_to_skip.add(currency)

                    # Case 2: Net Cash Balance does NOT exist at all
                    elif not net_cash_records:
                        currencies_to_skip.add(currency)

                #print("summary_sections : ", summary_sections)
                #print("db_section_combinations : ", db_section_combinations)
                #print("all_combinations : ", all_combinations)
                #print("currencies_with_data : ", currencies_with_data)
                #print("currencies_to_skip : ", currencies_to_skip)

                # Step 2: For each distinct group, stream data in batches
                for currency, sectiontype, sectionheader in all_combinations:

                    # Skip entire currency if needed
                    if currency in currencies_to_skip:
                        # For USD with the skip flag, skip all sections except "Multi-currency Account Net Equity"
                        if currency == "USD":
                            if not (sectiontype == "Summary" and sectionheader == "Multi-currency Account Net Equity"):
                                print(f"Skipping section {sectionheader} for currency {currency} due to Net Cash Balance condition")
                                continue
                        else:
                            # For other currencies, skip entire currency as before
                            print(f"Skipping entire currency {currency} due to Net Cash Balance condition")
                            continue

                    if sectiontype == "Summary":
                        records = summary_sections.get((currency, sectiontype, sectionheader), [])

                        if records:
                            # Special sorting (if needed)
                            if sectionheader == "Multi-currency Account Net Equity":
                                def currency_sort_key(r):
                                    curr = r.get("currency")
                                    if curr is None:
                                        return (2, "")
                                    if curr.startswith("USD"):
                                        return (0, "")
                                    return (1, curr)

                                records = sorted(records, key=currency_sort_key)

                            if batch_processor:
                                batch_processor(
                                    records,
                                    currency,
                                    sectiontype,
                                    sectionheader
                                )
                        continue

                    # Build query dynamically
                    if sectionheader in self.batching_not_required_sections:
                        order_by = self.order_by_for_sections.get(sectionheader)

                        # Build the full SQL query
                        sql_section_query = self.get_section_data.format(
                            currency=currency,
                            sectiontype=sectiontype,   
                            sectionheader=sectionheader,
                            order_by=order_by
                        )

                        with raw_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                            cursor.execute(sql_section_query)
                            rows = cursor.fetchall()  # no batching

                            if rows and batch_processor:
                                plain_rows = [row['data'] for row in rows]
                                batch_processor(plain_rows, currency, sectiontype, sectionheader)

                    elif sectionheader in self.batching_required_sections:
                        order_by = self.order_by_for_sections.get(sectionheader)
                        
                        # Build the full SQL query
                        sql_section_query = self.get_section_data.format(
                            currency=currency,
                            sectiontype=sectiontype,   
                            sectionheader=sectionheader,
                            order_by=order_by
                        )

                        with raw_conn.cursor(name="batch_cursor", cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                            cursor.execute(sql_section_query)

                            while True:
                                rows = cursor.fetchmany(batch_size)
                                if not rows:
                                    break

                                if batch_processor:
                                    plain_rows = [row['data'] for row in rows]
                                    batch_processor(plain_rows, currency, sectiontype, sectionheader)

                raw_conn.commit()

            except Exception:
                raw_conn.rollback() # rollback on any failure inside the connection scope
                raise
            finally:
                raw_conn.close()  # Closes temp table as well

        except Exception as e:
            message = f"Transaction sql query execution failed. {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': self.trigger_file_name, 'Message': message}, get_run_id())
            raise Exception(message)

    def generate_communication_sql_query(self, partyid_str, partyclientid_str):
        try:
            sql_communication_query = self.sql_queries.get("communication", "")

            sql_communication_query = sql_communication_query.format(partyid=partyid_str,clientidentifier=partyclientid_str)

            return sql_communication_query

        except Exception as e:
            message = f"Communication sql query generation failed. {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': self.trigger_file_name, 'Message': message}, get_run_id())
            raise Exception(message)

    def execute_communication_sql_query(self, sql_communication_query):
        try:
            # with self.engine.connect() as connection:
            with self.engine.begin() as connection:
                # Set the schema dynamically
                connection.execute(text(f"SET search_path TO {self.schema}"))

                # Execute SQL query
                extracted_communication_data_rows = connection.execute(text(sql_communication_query))

            return extracted_communication_data_rows

        except Exception as e:
            message = f"Communication sql query execution failed. {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': self.trigger_file_name, 'Message': message}, get_run_id())
            raise Exception(message)

    def format_value(self, value, format_type):
        # Check if format_type is an empty string
        if not format_type:
            if isinstance(value, str):
                return value.strip()
            else:
                #return value.strip()
                return value
        
        # Split format_type to get the main type and subtype
        main_type, sub_type = format_type.split('.')
    
        # Check if the main_type (e.g., "date", "currency", "decimal") exists in the formats
        if main_type in self.formats:
            # Check if the sub_type exists in the corresponding list
            format_config = next((item for item in self.formats[main_type] if sub_type in item), None)
            if format_config:
                # Get the format string for the current type
                format_str = format_config[sub_type].get("format")
            
                # Handle the specific formatting logic based on format_type
                if main_type == "date":
                    # Date formatting logic
                    if isinstance(value, str):
                        try:
                            # Check if the value has the 'T' separator between date and time
                            if 'T' in value:
                                # Parse datetime and extract date only (ignore time)
                                date_value = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S").date()  # Only extract the date part
                            else:
                                # Parse without 'T' (for the case of date-only strings)
                                date_value = datetime.strptime(value, "%Y-%m-%d %H:%M:%S").date()  # Extract date
                        except ValueError:
                            return ""  # Return empty string if the date is not in the correct format

                    elif isinstance(value, datetime):
                        date_value = value.date()  # Extract the date part from a datetime object
                    elif isinstance(value, date):
                        date_value = value  # It's already a date object, no need for conversion
                    else:
                        date_value = ""

                    if isinstance(date_value, date):  # Check if value is a datetime object for date formatting
                        return date_value.strftime(format_str) if value else ''
                    else:
                        print(f"Invalid date value: {value}")  # Log invalid date type
                        return ""  # If value is not a datetime, return empty string (or handle as needed)

                elif main_type == "currency" or main_type == "decimal":
                    # Handle "currency" and "decimal" formatting
                    negative_sign_format = format_config[sub_type].get("negativeSignFormat", None)
                    default_value = format_config[sub_type].get("default", None)

                    if isinstance(value, (int, float, Decimal)):  # Check if value is numeric for formatting
                        if value == 0:
                            return default_value
                        if value < 0:
                            # formatted_value = format_str % abs(value)  # Get the absolute value
                            formatted_value = format_str.format(abs(value))
                            if negative_sign_format == "signEnclosed":
                                return f"({formatted_value})"
                            elif negative_sign_format == "signRight":
                                return f"{formatted_value}-"
                            elif negative_sign_format == "signLeft":
                                return f"-{formatted_value}"
                        else:
                            #return format_str.format(value) if value is not None else ''
                            return format_str.format(value)
                    else:
                        return default_value

                elif isinstance(value, str):
                    return value.strip()

                else:
                    return ""  # Return as is if not a date or numeric value
        else:
            return ""  # Return empty if the main_type does not exist in the formats dictionary

    def get_nested_value(self, d, keys):
        """Helper function to get a value from a nested dictionary."""
        keys = keys.split('.')
        for key in keys:
            if isinstance(d, dict):
                d = d.get(key, '')
            else:
                return None
        return d

    def create_record(self, category_name, detail_items, transaction, record_type):
        try:
            record_list = []
            for detail_group in detail_items:
                for category, items in detail_group.items():
                    record_entry = {}
                    for detail_item in items:
                        label, column, template, fields, condition = detail_item
                        clear_template = "N"
                        # Check conditions if specified
                        if condition:
                            check_type = condition.get("check")
                            check_value = self.get_nested_value(transaction, condition["field"])

                            if isinstance(check_value, str):
                                clear_template = "Y"
                            else:
                                # Evaluate based on the specified condition
                                if check_type == "null" and check_value is None:
                                    clear_template = "Y"  # Clear template if null
                                elif check_type == "greater" and check_value <= 0:
                                    clear_template = "Y"  # Clear template if not greater
                                elif check_type == "greaterthanequal" and check_value < 0:
                                    clear_template = "Y"  # Clear template if not greater
                                elif check_type == "lesser" and check_value >= 0:
                                    clear_template = "Y"  # Clear template if not lesser
                                elif check_type == "lesserthanequal" and check_value > 0:
                                    clear_template = "Y"  # Clear template if not lesser
                                elif check_type == "notequalzero" and check_value == 0:
                                    clear_template = "Y"  # Clear template if not lesser

                        if template:
                            if clear_template == "N":
                                # Get values for each field
                                field_values = {}
                                for field in fields:
                                    field_name = field["field"]
                        
                                    # Extract the name from the field
                                    name_parts = field_name.split('.')
                                    name = name_parts[-1] if len(name_parts) > 1 else field_name
                        
                                    field_value = self.get_nested_value(transaction, field_name)
                                    formatted_value = self.format_value(field_value, field.get("format", ""))
                                    field_values[name] = formatted_value  # Use name for template substitution

                                # Format the final output using the template
                                formatted_output = template.format(**field_values)

                                # Add the formatted output to the entry
                                record_entry[column] = formatted_output
                            else:
                                record_entry[column] = ""
                        else:
                            record_entry[column] = label

                    if record_entry:
                        record_list.append(record_entry)

            return record_list

        except Exception as e:
            message = f"{record_type.capitalize()} - {category_name.lower()} record creation failed. {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': self.trigger_file_name, 'Message': message}, get_run_id())
            raise Exception(message)

    def create_holdings_record(self, category_name, category_details, transaction):
        
        if transaction.get("recordtype") == "Detail":
            detail_items = category_details.get("Detail", [])
        elif transaction.get("recordtype") == "Total":
            detail_items = category_details.get("Total", [])

        return self.create_record(category_name, detail_items, transaction, "holdings")

    def create_summary_record(self, category_name, category_details, transaction):

        if transaction.get("recordtype") == "Detail":
            detail_items = category_details.get("Detail", [])
        elif transaction.get("recordtype") == "Total":
            detail_items = category_details.get("Total", [])
            # This else part has to be removed once Abhishek is done with detail/total differentiation
        else:
            detail_items = category_details.get("Detail", [])
        
        return self.create_record(category_name, detail_items, transaction, "summary")

    def create_transactions_record(self, category_name, category_details, transaction):

        # Determine which detail items to use based on the presence of OpeningBalance or ClosingBalance
        if transaction.get("recordtype") == "Detail":
            if transaction.get("transactioncategory") == "OpeningBalance":
                detail_items = category_details.get("OpeningBalance", [])
            elif transaction.get("transactioncategory") == "ClosingBalance":
                detail_items = category_details.get("ClosingBalance", [])
            else:  
                detail_items = category_details.get("Detail", [])
        elif transaction.get("recordtype") == "Total":
            detail_items = category_details.get("Total", [])
        elif transaction.get("recordtype") == "Head":
            detail_items = category_details.get("Head", [])
        elif transaction.get("recordtype") == "Begin":
            detail_items = category_details.get("Begin", [])
        elif transaction.get("recordtype") == "End":
            detail_items = category_details.get("End", [])

        return self.create_record(category_name, detail_items, transaction, "transactions")

    def create_statementdates_record(self, category_name, category_details, transaction):
        
        try:
            detail_items = category_details.get("StatementDate", [])
            # print"detail_items : ", detail_items)
            statementdates_entry = self.create_record(category_name, detail_items, transaction, "statementdates")

            # Return the result for use by the calling function
            return statementdates_entry
        except Exception as e:
            message = f"Statementdate record creation failed. {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': self.trigger_file_name, 'Message': message}, get_run_id())
            raise Exception(message)

    def extract_records(self, partyids_list):
        try:
            sql_account_query = self.generate_account_sql_query(partyids_list)

            extracted_account_data_rows = self.execute_account_sql_query(sql_account_query)
                
            # Copy extracted account data to a list. This list can be used multiple times for processing.
            extracted_account_data_rows_list = list(extracted_account_data_rows)

            consolidated_partyids = self.extract_account_partyids(extracted_account_data_rows_list)

            # Check if consolidated_partyids_str is empty
            if not consolidated_partyids:
                message = f"No party data found for the partyid(s) {partyids_list}. Exiting the program."
                splunk.log_message(
                    {
                        'Status': 'failed',
                        'InputFileName': self.trigger_file_name,
                        'Message': message
                    },
                    get_run_id()
                )
                sys.exit(0)  # Exit the program if consolidated_partyids is empty

            account_column_names = extracted_account_data_rows.keys()
                
            # Create a dict using the column names and account data.
            accounts_list = [dict(zip(account_column_names, account_list)) for account_list in extracted_account_data_rows_list]

            groups = []

            for account in accounts_list:
                party_type = account['party_type']
                party_id = account['party_id']

                # Group: HouseHoldMasterAccount
                if party_type == 'HouseHoldMasterAccount':
                    group = [account]

                    # 1. Add direct IPs of the master
                    for ip in accounts_list:
                        if ip['party_type'] == 'InterestedParty' and ip.get('party_parentid') == party_id:
                            group.append(ip)

                    # 2. Find and add sisters
                    for sister in accounts_list:
                        if sister['party_type'] == 'HouseHoldSisterAccount' and sister.get('party_parentid') == party_id:
                            group.append(sister)
                            sister_id = sister['party_id']

                            # 3. Add IPs of the sister
                            for ip in accounts_list:
                                if ip['party_type'] == 'InterestedParty' and ip.get('party_parentid') == sister_id:
                                    group.append(ip)

                    groups.append(group)

                # Group: AccountHolder
                elif party_type == 'AccountHolder':
                    group = [account]
                    acc_id = account['party_id']

                    # Add all InterestedParties linked to the AccountHolder
                    for ip in accounts_list:
                        if ip['party_type'] == 'InterestedParty' and ip.get('party_parentid') == acc_id:
                            group.append(ip)

                    groups.append(group)

            # print"groups : ", groups)

            # Custom sorting function to prioritize 'USD' first
            def custom_sort_with_currency(record):
                currency = record.get('transactionamountcurrency', '')
                if currency == "USD":
                    return (0, currency)  # 'USD' will have priority
                return (1, currency)  # Other currencies will follow

            def process_communication(party_id,party_clientid):
                # Generate and execute SQL query for transactions
                sql_communication_query = self.generate_communication_sql_query(party_id,party_clientid)

                extracted_communications_data_rows = self.execute_communication_sql_query(sql_communication_query)
                extracted_communications_rows_list = list(extracted_communications_data_rows)
                communications_column_names = extracted_communications_data_rows.keys()
                communications_consolidated_list = [dict(zip(communications_column_names, extracted_communications_row_list)) for extracted_communications_row_list in extracted_communications_rows_list]
    
                return communications_consolidated_list

            zip_file_groups = {combo['category']: [] for combo in self.combinations}
            account_info_list = []

            # 3. Iterate through the groups
            # for identifier_ref, group in accounts_grouped_by_reference:
            for group in groups:
                group_list = list(group)  # materialize the group into a list
                #print("group_list : ", group_list)
                # print(f"\nGroup - Client ID: {clientid}, Reference: {identifier_ref}")
                self.currency_sections = []
                #file_paths = []

                for partydata in group_list:
                    start_time = time.time()

                    # print"partydata : ", partydata)

                    partyid = partydata['party_identifierreference']
                    partyidvalue = partydata['party_identifiervalue']
                    partyclientid = partydata['party_clientid']
                    partytype = partydata['party_type']

                    print("partyid : ", partyid)
                    print("partyidvalue : ", partyidvalue)
            
                    customer_dict = copy.deepcopy(self.driver_layout[0]["customer"])
                    # Empty the detail_table list
                    customer_dict["documents"][0]["document"]["details"] = []

                    if partytype in ('AccountHolder', 'HouseHoldMasterAccount', 'HouseHoldSisterAccount'):

                        unique_party_id = str(partydata['party_id'])  # comes from outer loop
                        # print"unique_party_id : ", unique_party_id)

                        template = self.driver_layout[0].get("template", {}).get("Template", {})
                        self.batching_required_sections = []
                        self.batching_not_required_sections = []
                        self.holdings_batching_required_sections = []
                        self.holdings_batching_not_required_sections = []
                        self.transactions_batching_required_sections = []
                        self.transactions_batching_not_required_sections = []
                        self.summary_batching_required_sections = []
                        self.summary_batching_not_required_sections = []
                        self.holdings_single_section_headers = []
                        self.holdings_multi_section_headers = []
                        self.transactions_single_section_headers = []
                        self.transactions_multi_section_headers = []
                        self.summary_single_section_headers = []
                        self.summary_multi_section_headers = []
                        self.order_by_for_sections = []

                        party_currency_sections = {
                           "party_id": partyid,
                           "party_clientid": partyclientid,
                            "currency_sections": {},
                            "section_headers": {}
                        }
                        
                        def build_layout_category_lists(table):
                            layout_S = []
                            layout_M = []

                            if not isinstance(table, dict):
                                return layout_S, layout_M

                            for key, details in table.items():
                                if not isinstance(details, dict):
                                    continue

                                heading = details.get("Heading")
                                layout = details.get("layout_category")

                                if not heading or not layout:
                                    continue

                                if layout == "S":
                                    layout_S.append(heading)
                                elif layout == "M":
                                    layout_M.append(heading)

                            return layout_S, layout_M
                        
                        def build_batching_lists(table):
                            batching_Y = []
                            batching_N = []

                            if not isinstance(table, dict):
                                return batching_Y, batching_N

                            # Process rows
                            for key, details in table.items():
                                if not isinstance(details, dict):
                                    continue

                                heading = details.get("Heading")
                                batching_flag = details.get("batching_required", "N")

                                if heading:
                                    if batching_flag == "Y":
                                        batching_Y.append(heading)
                                    else:
                                        batching_N.append(heading)
                            return batching_Y, batching_N
                        
                        def build_order_by_for_sections(*tables):
                            order_by_map = {}

                            for table in tables:
                                if not isinstance(table, dict):
                                    continue

                                for key, details in table.items():
                                    if not isinstance(details, dict):
                                        continue

                                    heading = details.get("Heading")
                                    order_by = details.get("order_by")

                                    if heading and order_by:
                                        order_by_map[heading] = order_by

                            return order_by_map

                        self.detail_summary_table = self.driver_layout[0].get("template", {}).get("detail_summary_table", {})
                        self.detail_holdings_table = self.driver_layout[0].get("template", {}).get("detail_holdings_table", {})
                        self.detail_transactions_table = self.driver_layout[0].get("template", {}).get("detail_transactions_table", {})
                        self.summary_batching_required_sections, self.summary_batching_not_required_sections = build_batching_lists(self.detail_summary_table)
                        self.holdings_batching_required_sections, self.holdings_batching_not_required_sections = build_batching_lists(self.detail_holdings_table)
                        self.transactions_batching_required_sections, self.transactions_batching_not_required_sections = build_batching_lists(self.detail_transactions_table)
                        self.batching_required_sections = self.summary_batching_required_sections + self.holdings_batching_required_sections + self.transactions_batching_required_sections
                        self.batching_not_required_sections = self.summary_batching_not_required_sections + self.holdings_batching_not_required_sections + self.transactions_batching_not_required_sections
                        self.holdings_single_section_headers, self.holdings_multi_section_headers = build_layout_category_lists(self.detail_holdings_table)
                        self.transactions_single_section_headers, self.transactions_multi_section_headers = build_layout_category_lists(self.detail_transactions_table)
                        self.summary_single_section_headers, self.summary_multi_section_headers = build_layout_category_lists(self.detail_summary_table)
                        self.order_by_for_sections = build_order_by_for_sections(self.detail_summary_table, self.detail_holdings_table, self.detail_transactions_table)
                        # print("self.order_by_for_sections : ", self.order_by_for_sections)
                        self.statement_dates_type_1 = self.driver_layout[0].get("template", {}).get("statement_dates_type_1", {})
                        self.duplicate_copy_sent_to_type_1 = self.driver_layout[0].get("template", {}).get("duplicate_copy_sent_to_type_1", {})

                        # Initialize and fill heading dicts
                        def initialize_table_data(table):
                            headings_dict = {}
                            data_dict = {}

                            for category, details in table.items():
                                data_dict[category] = {}

                                for key, value in details.items():
                                    template_key = details.get(key, None)
                                    template_data = self.get_nested_value(template, template_key) if template_key else None
                                    data_dict[category][key] = copy.deepcopy(template_data) if template_data else value
                                    #headings_dict[value] = category
                                
                                heading = details.get("Heading")
                                if heading:
                                    headings_dict[heading] = category

                            return headings_dict, data_dict
                        # Initialize both heading dictionaries
                        # Call for each table
                        self.detail_summary_table_headings_dict, self.detail_summary_table_data = initialize_table_data(self.detail_summary_table)
                        self.detail_holdings_table_headings_dict, self.detail_holdings_table_data = initialize_table_data(self.detail_holdings_table)
                        self.detail_transactions_table_headings_dict, self.detail_transactions_table_data = initialize_table_data(self.detail_transactions_table)
                        self.statement_dates_type_1_headings_dict, self.statement_dates_type_1_data = initialize_table_data(self.statement_dates_type_1)

                        sql_transaction_query = self.generate_transaction_sql_query(partyid,partyclientid)

                        def batch_processor(transactions_data_list, currency, sectiontype, sectionheader):
                            # Convert raw rows into your expected data format

                            if currency and currency.strip():
                                if currency not in self.currency_output:
                                    self.currency_output[currency] = copy.deepcopy(self.driver_layout[0]["customer"]["documents"][0]["document"]["details"][0])

                                    # For every category in each table, create a fresh copy of detail_record per currency
                                    for category in self.detail_summary_table_headings_dict.values():
                                        self.records[f"{category}_summary_record"] = copy.deepcopy(self.driver_layout[0]["detail_record"])

                                    for category in self.detail_holdings_table_headings_dict.values():
                                        self.records[f"{category}_holdings_record"] = copy.deepcopy(self.driver_layout[0]["detail_record"])

                                    for category in self.detail_transactions_table_headings_dict.values():
                                        self.records[f"{category}_transactions_record"] = copy.deepcopy(self.driver_layout[0]["detail_record"])

                                if sectiontype == 'Holdings' and sectionheader in self.holdings_single_section_headers:
                                    if sectionheader in self.detail_holdings_table_headings_dict:
                                        category = self.detail_holdings_table_headings_dict[sectionheader]
                                        category_details = self.detail_holdings_table_data.get(category)
                                        group_row = []
                                        for transaction in transactions_data_list:
                                            record_entry = self.create_holdings_record(category, category_details, transaction)

                                            if record_entry:  # Ensure record_entry is not empty before appending
                                                group_row.extend(record_entry)
                                                                        
                                        write_section_to_temp_file(self, unique_party_id, currency, category, group_row, sectionheader, "holdings")

                                elif sectiontype == 'Activity' and sectionheader in self.transactions_single_section_headers:
                                    if sectionheader in self.detail_transactions_table_headings_dict:
                                        category = self.detail_transactions_table_headings_dict[sectionheader]
                                        category_details = self.detail_transactions_table_data.get(category)
                                        group_row = []
                                        for transaction in transactions_data_list:
                                            record_entry = self.create_transactions_record(category, category_details, transaction)
                                            if record_entry:
                                                group_row.extend(record_entry)

                                        write_section_to_temp_file(self, unique_party_id, currency, category, group_row, sectionheader, "transactions")
                                        
                                elif sectiontype == 'Activity' and sectionheader in self.transactions_multi_section_headers:
                                    if sectionheader in self.detail_transactions_table_headings_dict:
                                        category = self.detail_transactions_table_headings_dict[sectionheader]
                                        category_details = self.detail_transactions_table_data.get(category)

                                    grouped = defaultdict(list)
                                    for txn in transactions_data_list:
                                        key = txn.get("SecurityGroup")
                                        grouped[key].append(txn)

                                    all_grouped_data = []
        
                                    for key, grouped_txns in grouped.items():
                                        head_rows = []
                                        begin_rows = []
                                        detail_rows = []
                                        end_rows = []

                                        for txn in grouped_txns:
                                            record_entry = self.create_transactions_record(category, category_details, txn)
                                            if record_entry:
                                                record_type = txn.get("recordtype")
                                                if isinstance(record_entry, list):
                                                    for entry in record_entry:
                                                        if record_type == "Head":
                                                            head_rows.append(entry)
                                                        elif record_type == "Begin":
                                                            begin_rows.append(entry)
                                                        elif record_type == "Detail":
                                                            if any(str(v).strip() not in ["", "0", "0.00", "None", "null"] for v in entry.values()):
                                                                detail_rows.append(entry)
                                                        elif record_type in ["End", "Total"]:
                                                            end_rows.append(entry)
                                                else:
                                                    if record_type == "Head":
                                                        head_rows.append(record_entry)
                                                    elif record_type == "Begin":
                                                        begin_rows.append(record_entry)
                                                    elif record_type == "Detail":
                                                        if any(str(v).strip() not in ["", "0", "0.00", "None", "null"] for v in record_entry.values()):
                                                            detail_rows.append(record_entry)
                                                    elif record_type in ["End", "Total"]:
                                                        end_rows.append(record_entry)

                                        group_row = begin_rows + detail_rows + end_rows
                                        all_grouped_data.append({
                                            "sub_section_column_values": head_rows,
                                            "row": group_row
                                        })

                                    write_section_to_temp_file(self, unique_party_id, currency, category, all_grouped_data, sectionheader, "transactions")                            

                                elif sectiontype == 'Summary' and sectionheader in self.summary_single_section_headers:
                                    if sectionheader in self.detail_summary_table_headings_dict:
                                        category = self.detail_summary_table_headings_dict[sectionheader] # Get the corresponding category name
                                        category_details = self.detail_summary_table_data.get(category)
                                        group_row = []
                                        for transaction in transactions_data_list:
                                            record_entry = self.create_summary_record(category, category_details, transaction)

                                            if record_entry:  # Ensure record_entry is not empty before appending
                                                group_row.extend(record_entry)
                                
                                        write_section_to_temp_file(self, unique_party_id, currency, category, group_row, sectionheader, "summary")
                        
                        def write_section_to_temp_file(self, unique_party_id, currency, category, group_row, sectionheader, section_type):
                            if not group_row:
                                return  # no data, skip writing

                            section_key = f"{unique_party_id}_{currency}_{section_type}_{category}"
                            # print"section_key : ", section_key)
                            temp_file_path = f"/tmp/{section_key}.json"
                            print("temp_file_path : ", temp_file_path)

                            #Track this section_key under the currency for final combination later
                            if currency not in party_currency_sections["currency_sections"]:
                                party_currency_sections["currency_sections"][currency] = set()
                            party_currency_sections["currency_sections"][currency].add(section_key)

                            # print"party_currency_sections : ", party_currency_sections)

                            party_currency_sections["section_headers"][section_key] = sectionheader

                            if section_key not in self.section_file_handles:
                                os.makedirs(os.path.dirname(temp_file_path), exist_ok=True)
                                f = open(temp_file_path, "w", encoding="utf-8")
                                self.section_file_handles[section_key] = f
                                self.section_row_flags[section_key] = True  # first row to write
                            else:
                                f = self.section_file_handles[section_key]

                            is_first_batch = self.section_row_flags[section_key]

                            # If not first batch, write comma before the first row of this batch
                            if not is_first_batch:
                                f.write(",")

                            for i, row in enumerate(group_row):
                                if i > 0:
                                    f.write(",")  # add comma between rows
                                f.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")))

                            self.section_row_flags[section_key] = False

                        def close_section_files(self):
                            # Close all open section temp files by writing proper closing JSON brackets
                            for section_key, f in self.section_file_handles.items():
                                f.close()
                            self.section_file_handles = {}
                            self.section_row_flags = {}

                        #def print_disk_usage(self, path="/tmp"):
                            #total, used, free = shutil.disk_usage(path)
                            #print(f"[Disk] Total: {total // (1024**3)} GB, Used: {used // (1024**3)} GB, Free: {free // (1024**3)} GB")                            
                            #print(f"[Disk Usage] (in MB)")
                            #print(f"  Total: {total // (1024 ** 2)} MB")
                            #print(f"  Used:  {used // (1024 ** 2)} MB")
                            #print(f"  Free:  {free // (1024 ** 2)} MB")

                            #print(f"[Disk Usage] (in KB) Total: {total // 1024} KB, Used: {used // 1024} KB, Free: {free // 1024} KB")

                        def get_category_from_key(k, section_type):
                            return k.split(f"_{section_type}_")[1]

                        def sort_keys_by_order(keys, section_type, order_list):
                            return sorted(
                                [k for k in keys if get_category_from_key(k, section_type) in order_list],
                                key=lambda k: order_list.index(get_category_from_key(k, section_type))
                            )
                        
                        def delete_temp_file(self, file_path):
                            try:
                                if os.path.exists(file_path):
                                    os.remove(file_path)
                                    # printf"Deleted temp file: {file_path}")
                                    #print_disk_usage(self, "/tmp")
                            except Exception as e:
                                print(f"Failed to delete temp file {file_path}: {e}")

                        def has_details_to_write(self, currency_sections):
                            for currency, section_keys in currency_sections.items():
                                for k in section_keys:
                                    if "_summary_" in k or "_holdings_" in k or "_transactions_" in k:
                                        return True
                            return False
                        
                        def stream_single_document_to_final_file(self, partydata_list):

                            # print"partydata_list : ", partydata_list)
                            # Derive household flag and primary ID for filename
                            #household_flag = "Y" if len(partydata_list) > 1 else "N"
                            household_flag = "N"
                            primary_party = partydata_list[0]  # use first party for filename

                            # print"primary_party : ", primary_party)
                            
                            final_file_path = f"/tmp/{primary_party['party_clientid']}_" \
                                            f"{primary_party['party_identifierreference']}_" \
                                            f"{primary_party['party_id']}_" \
                                            f"{primary_party['party_type']}.json"
                            
                            documents_written = 0

                            #sister_account_numbers = ",".join([
                            #    party.get("party_identifierreference", "")
                            #    for party in partydata_list[1:]
                            #    if party.get("party_identifierreference")
                            #])

                            with open(final_file_path, "w", encoding="utf-8") as final_f:
                                final_f.write(f'{{"customer":{{"household_flag":"{household_flag}","run_type":"{self.run_type}","documents":[')
                                #final_f.write(f'{{"customer":{{"household_flag":"{household_flag}","documents":[')

                                for idx, partydata in enumerate(partydata_list):
                                    # Setup temp state variables (section headers, etc.)
                                    #party_section = next((p for p in self.currency_sections if p["party_id"] == partydata["party_identifierreference"]), None)
                                    party_section = next((p for p in self.currency_sections if p["party_id"] == partydata["party_identifierreference"] and p["party_clientid"] == partydata["party_clientid"]), None)
                                    # print"party_section 1 : ", party_section)
                                    currency_sections = party_section["currency_sections"]
                                    section_headers = party_section["section_headers"]

                                    if not has_details_to_write(self, currency_sections):
                                        continue

                                    if documents_written > 0:
                                        final_f.write(',')

                                    final_f.write('{"document":')

                                    # sequence = str(idx + 1)
                                    sequence = str(documents_written + 1)
                                    #is_master = partydata.get("party_type") == "HouseHoldMasterAccount"
                                    statement_type = 'Single'

                                    stream_single_document(self, final_f, partydata, currency_sections, section_headers, statement_type, sister_account_numbers=[], sequence=sequence)

                                    final_f.write('}')
                                    documents_written += 1

                                final_f.write(']}}')  # close documents, customer, root

                            if documents_written == 0:
                                os.remove(final_file_path)
                                return None
                            
                            return final_file_path

                        def stream_household_document_to_final_file(self, partydata_list):

                            # print"partydata_list : ", partydata_list)
                            # Derive household flag and primary ID for filename
                            household_flag = "Y" if len(partydata_list) > 1 else "N"
                            #household_flag = "Y"
                            primary_party = partydata_list[0]  # use first party for filename

                            # print"primary_party : ", primary_party)
                            
                            final_file_path = (
                                f"/tmp/{primary_party['party_clientid']}_"
                                f"{primary_party['party_identifierreference']}_"
                                f"{primary_party['party_id']}_"
                                f"HouseHoldMasterAccount.json"
                            )

                            sister_account_numbers = ",".join([
                                party.get("party_identifierreference", "")
                                for party in partydata_list[1:]
                                if party.get("party_identifierreference")
                            ])

                            documents_written = 0

                            with open(final_file_path, "w", encoding="utf-8") as final_f:
                                final_f.write(f'{{"customer":{{"household_flag":"{household_flag}","run_type":"{self.run_type}","documents":[')
                                #final_f.write(f'{{"customer":{{"household_flag":"{household_flag}","documents":[')

                                for idx, partydata in enumerate(partydata_list):
                                    # Setup temp state variables (section headers, etc.)
                                    #party_section = next((p for p in self.currency_sections if p["party_id"] == partydata["party_identifierreference"]), None)
                                    party_section = next((p for p in self.currency_sections if p["party_id"] == partydata["party_identifierreference"] and p["party_clientid"] == partydata["party_clientid"]), None)
                                    currency_sections = party_section["currency_sections"]
                                    section_headers = party_section["section_headers"]

                                    if not has_details_to_write(self, currency_sections):
                                        continue
                                    
                                    if documents_written > 0:
                                        final_f.write(',')

                                    final_f.write('{"document":')

                                    sequence = str(documents_written + 1)
                                    is_master = partydata.get("party_type") == "HouseHoldMasterAccount"
                                    statement_type = 'HouseHold'

                                    stream_single_document(self, final_f, partydata, currency_sections, section_headers, statement_type, sister_account_numbers=sister_account_numbers if is_master else [], sequence=sequence)

                                    final_f.write('}')
                                    documents_written += 1

                                final_f.write(']}}')  # close documents, customer, root

                            if documents_written == 0:
                                os.remove(final_file_path)
                                return None
                            
                            return final_file_path

                        def stream_single_document(self, final_f, partydata, currency_sections, section_headers, statement_type, sister_account_numbers=None, sequence="1"):
                            
                            documentInfo = self.driver_layout[0]["document_info"]
                            cmPickFields = self.driver_layout[0].get("cm_pick_fields", {})
                            userVariables = self.driver_layout[0].get("user_variables", {})
                            docNops = self.driver_layout[0].get("doc_nops", {})
                            urAttributes = self.driver_layout[0].get("ur_attributes", {})
                            mailingAddress = self.driver_layout[0]["customer_mailing_address"]
                            advisorInfo = self.driver_layout[0]["advisor_info"]
                            interestedpartyInfo = self.driver_layout[0]["interested_party_info"]
                            statementDates = self.driver_layout[0]["document_heading"]
                            test = statement_type
                
                            # Process transactions for this party
                            communication_data = process_communication(partydata['party_identifiervalue'],partydata['party_clientid'])
                            communicationdata = communication_data[0][list(communication_data[0].keys())[0]]

                            party_ref = partydata["party_identifierreference"]
                            temp_statement_file = f"/tmp/statement_date_{party_ref}.json"

                            statementdate_data = {}
                            document_heading_info = []
                            if os.path.exists(temp_statement_file):
                                with open(temp_statement_file, "r", encoding="utf-8") as f:
                                    loaded_data = json.load(f)

                                    if isinstance(loaded_data, list):
                                        if loaded_data:
                                            statementdate_data = loaded_data[0]
                                        elif isinstance(loaded_data, dict):
                                            statementdate_data = loaded_data

                                sectionheader = statementdate_data.get("sectionheader")
                                category = self.statement_dates_type_1_headings_dict.get(sectionheader)
                                category_details = self.statement_dates_type_1_data.get(category)

                                party_and_transaction_combined_data = {
                                    **statementdate_data,
                                    **partydata
                                }

                                # Create the record using the combined data
                                statement_data_record = self.create_statementdates_record(
                                    category, category_details, party_and_transaction_combined_data
                                )

                                # Only proceed if statement_data_record exists
                                if statement_data_record:
                                    statement_dict = statement_data_record[0]  # Get the dict inside the list

                                    for statementDatesRec in statementDates:
                                        key = next(iter(statementDatesRec))            # e.g. 'column1'
                                        lookup_key = statementDatesRec[key]            # e.g. 'statement_date_mmmyyyy'

                                        # Try fetching from statement_data_record
                                        line_value = statement_dict.get(lookup_key, '')

                                        # Fallback to partydata
                                        if not line_value:
                                            line_value = partydata.get(lookup_key, '')

                                        if line_value:
                                            document_heading_info.append({key: line_value})

                            # Build document_info dict dynamically
                            document_info = {}
                            for stmt_key, partydata_key in documentInfo.items():
                                if partydata_key == "party_identifiervalue":
                                    # Skip this key completely
                                    continue
                                if partydata_key in partydata:
                                    value = partydata[partydata_key]
                                elif partydata_key in communicationdata:
                                    value = communicationdata[partydata_key]
                                elif partydata_key in statementdate_data:
                                    value = statementdate_data[partydata_key]
                                else:
                                    value = ''
                                document_info[stmt_key] = value

                            if statement_type == 'Single':
                                if partydata.get("party_type") in ("AccountHolder","HouseHoldMasterAccount","HouseHoldSisterAccount"):
                                    document_info["sequence"] = sequence
                                    document_info["is_interested_party"] = "N"
                                elif partydata.get("party_type") == "InterestedParty":
                                    document_info["sequence"] = sequence
                                    document_info["is_interested_party"] = "Y"
                            elif statement_type == 'HouseHold':
                                if partydata.get("party_type") == "HouseHoldMasterAccount":
                                    document_info["sequence"] = sequence
                                    document_info["sister_accounts"] = sister_account_numbers
                                    document_info["is_interested_party"] = "N"
                                elif partydata.get("party_type") == "HouseHoldSisterAccount":
                                    document_info["sequence"] = sequence
                                    document_info["is_interested_party"] = "N"

                            # Build campaign_manager.cm_pick_fields
                            cm_pick_fields = {}
                            for cm_key, cmdata_key in cmPickFields.items():
                                value = communicationdata.get(cmdata_key, '')
                                cm_pick_fields[cm_key] = value if value is not None else ''

                            user_variables_fields = {}
                            for user_key, userdata_key in userVariables.items():
                                value = communicationdata.get(userdata_key, '')
                                user_variables_fields[user_key] = value if value is not None else ''

                            # Build ur_attributes
                            ur_attributes = {}
                            ####for ur_key, urdata_key in urAttributes.items():
                            ####    value = communicationdata.get(urdata_key, '')
                            ####    ur_attributes[ur_key] = value if value is not None else ''
                                # Append each address line from mailingAddress to customer_dict

                            for ur_key, urdata_key in urAttributes.items():
                                # First, check communicationdata
                                value = communicationdata.get(urdata_key, None)
                                # If not found in communicationdata, extend check to partydata
                                if value is None:
                                    value = partydata.get(urdata_key, '')
                                ur_attributes[ur_key] = value if value is not None else ''
                            doc_nop_fields = {}
                            for docnops_key, docnopsdata_key in docNops.items():
                                if isinstance(docnopsdata_key, str) and docnopsdata_key.startswith("!"):
                                    # Value is hardcoded — remove the marker and use as-is
                                    value = docnopsdata_key[1:]
                                else:
                                    # First, check communicationdata
                                    value = communicationdata.get(docnopsdata_key, None)
        
                                    # If not found in communicationdata, check partydata
                                    if value is None:
                                        value = partydata.get(docnopsdata_key, '')

                                doc_nop_fields[docnops_key] = value if value is not None else ''

                            customer_mailing_address = []
                            for address in mailingAddress:
                                # Use the key from the address dict
                                key = next(iter(address))  # Get the first key dynamically
                                line_value = partydata.get(address[key], '')  # Use the dynamic key
                                if line_value:  # Check if line_value is not empty or null
                                    customer_mailing_address.append({key: line_value})
                            
                            # Build advisor_info list
                            advisor_info = []
                            for advisorInfoRec in advisorInfo:
                                key = next(iter(advisorInfoRec))
                                line_value = partydata.get(advisorInfoRec[key], '')
                                if line_value:
                                    advisor_info.append({key: line_value})

                            # Build interested_party_info if partytype matches
                            interested_party_info = []
                            if partydata.get('party_classificationvalue') == 'InterestedParty':
                                for interestedpartyInfoRec in interestedpartyInfo:
                                    key = next(iter(interestedpartyInfoRec))
                                    line_value = partydata.get(interestedpartyInfoRec[key], '')
                                    if line_value:
                                        interested_party_info.append({key: line_value})

                            # Build duplicate_copies list
                            duplicate_copies_list = []
                            if getattr(self, "duplicate_copy_sent_to_type_1", None):
                                duplicate_parties = [party for party in (partydata.get('duplicate_copies') or []) if party is not None]
                                if duplicate_parties:
                                    duplicate_parties_record = copy.deepcopy(self.driver_layout[0]["detail_record"])
                                    duplicate_parties_record["heading_1"] = self.duplicate_copy_sent_to_type_1.get("DuplicateCopies", {}).get("Heading", "")
                                    duplicate_parties_record["sub_detail_column_values"].append({"row": duplicate_parties})
                                    duplicate_copies_list.append(duplicate_parties_record)

                            # ========== Write document ==========
                            final_f.write('{')
                            final_f.write(f'"document_info":{json.dumps(document_info, separators=(",", ":"))},')
                            final_f.write(f'"doc_nops":{json.dumps(doc_nop_fields, separators=(",", ":"))},')
                            final_f.write(f'"ur_attributes":{json.dumps(ur_attributes, separators=(",", ":"))},')
                            final_f.write('"campaign_manager":{"cm_pick_fields":')
                            final_f.write(json.dumps(cm_pick_fields, separators=(",", ":")))
                            final_f.write(',"user_variables":')
                            final_f.write(json.dumps(user_variables_fields, separators=(",", ":")))
                            final_f.write('},')
                            value = partydata.get("primary_logo")
                            if value: # checks for None, empty string, etc.
                                final_f.write(f'"primary_logo": "{value}",')
                            else:
                                final_f.write('"primary_logo":"",')
                            value = partydata.get("return_address")
                            if value:
                                return_address_list = value.split(";")
                                final_f.write(f'"return_address": {json.dumps(return_address_list)},')
                            else:
                                final_f.write('"return_address":[],')
                            value = partydata.get("return_address_second")
                            if value:
                                return_address_sec_list = value.split(";")
                                final_f.write(f'"return_address_secondary": {json.dumps(return_address_sec_list)},')
                            value = partydata.get("graphic_message_page1")
                            if value:
                                final_f.write(f'"graphic_message_page1": "{value}",')
                            value = partydata.get("statement_backer")
                            if value:
                                final_f.write(f'"statement_backer": "{value}",')
                            value = partydata.get("partyorigin")
                            if value:
                                final_f.write(f'"input_file_name": "{value}",')
                            value = partydata.get("rep_num")
                            if value:
                                final_f.write(f'"rep_num": "{value}",')
                            final_f.write('"document_date":"",')
                            ####final_f.write('"return_address":[],')
                            final_f.write(f'"customer_mailing_address":{json.dumps(customer_mailing_address, separators=(",", ":"))},')
                            final_f.write(f'"advisor_info":{json.dumps(advisor_info, separators=(",", ":"))},')
                            final_f.write(f'"document_heading":{json.dumps(document_heading_info, separators=(",", ":"))},')
                            final_f.write(f'"interested_party_info":{json.dumps(interested_party_info, separators=(",", ":"))},')
                            final_f.write(f'"duplicate_copies":{json.dumps(duplicate_copies_list, separators=(",", ":"))},')

                            final_f.write('"details":[')

                            # ========== Write details from temp files ==========
                            currencies = list(currency_sections.keys())
                            currencies.sort(key=lambda c: (0 if c == 'USD' else 1, c))

                            first_currency_written = True
                            for currency in currencies:
                                section_keys = sorted(currency_sections[currency])

                                summary_keys = sort_keys_by_order([k for k in section_keys if "_summary_" in k], "summary", self.summary_section_order)
                                holdings_keys = sort_keys_by_order([k for k in section_keys if "_holdings_" in k], "holdings", self.holdings_section_order)
                                transactions_keys = sort_keys_by_order([k for k in section_keys if "_transactions_" in k], "transactions", self.transactions_section_order)

                                if not (summary_keys or holdings_keys or transactions_keys):
                                    continue

                                if not first_currency_written:
                                    final_f.write(',')
                                first_currency_written = False

                                final_f.write('{')
                                first_section = True

                                def write_sections(label, keys, heading_1_value):
                                    nonlocal first_section
                                    if not keys:
                                        return
                                    if not first_section:
                                        final_f.write(',')
                                    first_section = False

                                    final_f.write(f'"{label}":[')
                                    section_level_counter = 1
                                    for i, section_key in enumerate(keys):
                                        category = get_category_from_key(section_key, label)
                                        temp_file_path = f"/tmp/{section_key}.json"
                                        sectionheader = section_headers.get(section_key, "")

                                        heading_2_value = currency if i == 0 else ""
                                        heading_1_val = heading_1_value if i == 0 else ""
                                        section_level_str = str(section_level_counter)
                                        section_level_counter += 1

                                        final_f.write('{')
                                        final_f.write(f'"heading_1":"{heading_1_val}",')
                                        final_f.write(f'"heading_2":"{heading_2_value}",')
                                        final_f.write(f'"section_level_indicator":"{section_level_str}",')
                                        final_f.write(f'"sub_heading_1":"{sectionheader}",')
                                        final_f.write(f'"sub_heading_2":"",')
                                        final_f.write(f'"sub_detail_row_indicator":"",')
                                        final_f.write(f'"reference_table_name":"{self.client_name}_{label}_{category.lower()}",')

                                        if sectionheader in ['Deposit Activities', 'Money Market Mutual Fund Activity', 'Statement of Billing Fees Collected - This is Not A Bill']:
                                            with open(temp_file_path, "r", encoding="utf-8") as section_file:
                                                content = section_file.read().strip()
                                            final_f.write('"sub_detail_column_values":[')
                                            final_f.write(content)
                                            final_f.write(']')
                                        else:
                                            final_f.write('"sub_detail_column_values":[{"row":[')
                                            with open(temp_file_path, "r", encoding="utf-8") as section_file:
                                                first_row = True
                                                for line in section_file:
                                                    line = line.strip()
                                                    if not line:
                                                        continue
                                                    if not first_row:
                                                        final_f.write(',')
                                                    final_f.write(line)
                                                    first_row = False
                                            final_f.write(']}]')

                                        final_f.write('}')
                                        if i < len(keys) - 1:
                                            final_f.write(',')

                                    final_f.write(']')  # end of label array

                                write_sections("summary", summary_keys, "Account Summary")
                                write_sections("holdings", holdings_keys, "Holdings")
                                write_sections("transactions", transactions_keys, "Transactions")

                                final_f.write('}')  # end of currency object

                            final_f.write(']')  # end of "details"
                            final_f.write('}')  # end of "document"

                        def upload_to_s3(self, file_or_buffer, s3_key):
                            s3 = boto3.client('s3')

                            if isinstance(file_or_buffer, (str, bytes, os.PathLike)):
                                # Assume it's a file path
                                with open(file_or_buffer, "rb") as f:
                                    s3.upload_fileobj(f, self.s3_bucket, s3_key)
                            else:
                                # Assume it's a file-like object like BytesIO
                                s3.upload_fileobj(file_or_buffer, self.s3_bucket, s3_key)

                            print(f"Uploaded file to s3://{self.s3_bucket}/{s3_key}")
                            
                        def get_zip_category(self, record):
                            for combo in self.combinations:
                                all_match = True

                                for rule in combo.get("rules", []):
                                    field = rule.get("field")
                                    operator = rule.get("operator")
                                    values = [str(v) for v in rule.get("values", [])]

                                    field_value = str(record.get(field, ""))

                                    if operator == "in":
                                        if field_value not in values:
                                            all_match = False
                                            break

                                    elif operator == "equals":
                                        if field_value != values[0]:
                                            all_match = False
                                            break

                                    elif operator == "startswith":
                                        if not any(field_value.startswith(v) for v in values):
                                            all_match = False
                                            break

                                    elif operator == "not_startswith":
                                        if any(field_value.startswith(v) for v in values):
                                            all_match = False
                                            break

                                    elif operator == "endswith":
                                        if not any(field_value.endswith(v) for v in values):
                                            all_match = False
                                            break

                                    elif operator == "contains":
                                        if not any(v in field_value for v in values):
                                            all_match = False
                                            break

                                    elif operator == "not_in":
                                        if field_value in values:
                                            all_match = False
                                            break

                                    else:
                                        print(f"Unknown operator '{operator}' in rule for {field}")
                                        all_match = False
                                        break

                                if all_match:
                                    return combo.get("category")

                            return None
                        
                        def upload_files_and_insert_staging(self, file_groups, accounts_list):

                            print("file_groups : ", file_groups)

                            staging_records = []   # collect rows for DB insert
                            BATCH_SIZE = 500       # safe batch size for inserts

                            for category, file_paths in file_groups.items():
                                if not file_paths:
                                    continue

                                for path in file_paths:
                                    try:
                                        filename = os.path.basename(path)

                                        # S3 key built by category + unique job ID + filename
                                        #s3_key = f"{self.s3_wip}/{self.unique_id}/{category}/{filename}"
                                        s3_key = f"{self.s3_wip}/{filename}"

                                        # Upload file to S3 in streaming mode (no memory issue)
                                        with open(path, "rb") as f:
                                            upload_to_s3(self, f, s3_key)

                                        # Prepare staging record
                                        staging_records.append({
                                            "category": category,
                                            "s3path": s3_key
                                        })

                                        # After uploading → remove local file
                                        try:
                                            os.remove(path)
                                        except Exception as e:
                                            print(f"Failed to delete local file {path}: {e}")

                                    except Exception as e:
                                        print(f"Error processing file {path}: {e}")

                            # Insert staging records into DB in batches
                            if staging_records:
                                staging_table = Table(
                                    self.stagingtable,
                                    self.meta,
                                    autoload=True,
                                    autoload_with=self.engine
                                )

                                with self.engine.connect() as connection:
                                    connection.execute(text(f"SET search_path TO {self.schema}"))
                                    for i in range(0, len(staging_records), BATCH_SIZE):
                                        batch = staging_records[i:i + BATCH_SIZE]
                                        connection.execute(insert(staging_table).values(batch))
                                    connection.commit()

                            print(f"Uploaded {len(staging_records)} files and inserted into staging table.")
                            account_info_json = json.dumps(accounts_list)

                            #with self.engine.begin() as connection:  # auto-commit
                                # Set schema
                            #    connection.execute(text(f"SET search_path TO {self.schema}"))

                                #connection.execute(
                                #    text(self.update_recon_table_query),
                                #    {
                                #        "accountinfo": account_info_json
                                #    }
                                #)
                        
                        self.execute_transaction_sql_query(sql_transaction_query, partydata, batch_size=10000, batch_processor=batch_processor)

                        self.currency_sections.append(party_currency_sections)

                        print("self.currency_sections : ", self.currency_sections)

                        # Close all section temp files properly
                        close_section_files(self)

                    #if partytype in ('AccountHolder', 'InterestedParty', 'HouseHoldMasterAccount', 'HouseHoldSisterAccount'):
                    if partytype in self.individual_statements_for:
                    #if partytype in ('AccountHolder', 'InterestedParty'):
                        final_file_path = stream_single_document_to_final_file(self, [partydata])
                        #print("final_file_path : ", final_file_path)
                        if final_file_path:
                            category = get_zip_category(self, partydata)
                            #print("category: ", category)
                            if category:
                                zip_file_groups[category].append(final_file_path)
                                account_info_list_entry = {
                                        "account_number": partydata.get("party_identifierreference"),
                                        "client_id": partydata.get("party_clientid"),
                                        "party_type": partydata.get("party_type")
                                    }
                            
                                if account_info_list_entry not in account_info_list:
                                    account_info_list.append(account_info_list_entry)

                if self.is_household == 'Y':
                    for partydata in group_list:
                        if partydata['party_type'] == 'HouseHoldMasterAccount':
                            # Collect all sisters for this master from the group
                            master_identifier = partydata.get('party_identifierreference')
                            master_clientid = partydata.get('party_clientid')
                            
                            sisters = [p for p in group_list if p['party_type'] == 'HouseHoldSisterAccount']
                            # Order sisters
                            ordered_sisters = []

                            ordered_sisters = sorted(
                                sisters,
                                key=lambda x: (
                                    x["party_identifierreference"] != master_identifier,  # master identifier first
                                    x["party_identifierreference"],                        # then by identifier
                                    str(x["party_clientid"]) != str(master_clientid),      # master client first
                                    str(x["party_clientid"])                               # then by clientid
                                    )
                                )
                            
                            final_path = stream_household_document_to_final_file(self, [partydata] + ordered_sisters)
                            category = get_zip_category(self, partydata)
                            if category:
                                zip_file_groups[category].append(final_path)

                                account_info_list_entry = {
                                    "account_number": partydata.get("party_identifierreference"),
                                    "client_id": partydata.get("party_clientid"),
                                    "party_type": partydata.get("party_type")
                                }
                            
                                if account_info_list_entry not in account_info_list:
                                    account_info_list.append(account_info_list_entry)

                                for sister in sisters:
                                    account_info_list_entry = {
                                        "account_number": sister.get("party_identifierreference"),
                                        "client_id": sister.get("party_clientid"),
                                        "party_type": sister.get("party_type")
                                    }

                                    if account_info_list_entry not in account_info_list:
                                        account_info_list.append(account_info_list_entry)
                
                end_time = time.time()
                time_taken = end_time - start_time
                print(f"Execution time for account: {time_taken:.4f} seconds")

            #print("zip_file_groups : ", zip_file_groups)
            print("account_info_list : ", account_info_list)
            upload_files_and_insert_staging(self, zip_file_groups, account_info_list)

        except SQLAlchemyError as e:
            # Handle generic SQLAlchemy errors
            message = f"Account extraction process failed due to SQLAlchemy error. {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': self.trigger_file_name, 'Message': message}, get_run_id())
            raise Exception(message)

        except Exception as e:
            error_details = traceback.format_exc()
            print(f"Error occurred at:\n{error_details}")
            message = f"Account extraction process failed. {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': self.trigger_file_name, 'Message': message}, get_run_id())
            raise Exception(message)

def set_job_params_as_env_vars():
    # Loop through all command-line arguments starting from the second argument (skip the script name)
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]  # Remove the leading '--'
            value = sys.argv[i + 1]
            os.environ[key] = value

# main:
def main():
    try:
        set_job_params_as_env_vars()
        
    except Exception as e:
        message = f"failed to retrieve input parameters. {str(e)}"
        splunk.log_message({'Status': 'failed', 'InputFileName': 'Test', 'Message':message }, get_run_id())
        raise Exception(message)
    
    try:
        dp=DataPostgres()
        partyids_list = dp.extract_partyids()
        dp.extract_records(partyids_list)

    except Exception as e:
        message = f"Account extraction process failed. {str(e)}"
        splunk.log_message({'Status': 'failed', 'InputFileName': 'Test', 'Message': message}, get_run_id())
        raise Exception(message)

if __name__ == '__main__':
    main()    