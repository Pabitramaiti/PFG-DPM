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
import jsonformatter
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
import json
import time
import os
import xmltodict
import re
import copy
import traceback
import sys

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
        self.client_name = os.getenv("client_name")
        self.query_to_extract_accounts = json.loads(os.getenv("query_to_extract_accounts"))
        #self.accounts_with_fa_sql_query = json.loads(os.getenv("query_to_extract_accounts_with_fa"))
        #self.accounts_without_fa_sql_query = json.loads(os.getenv("query_to_extract_accounts_without_fa"))
        self.transaction_sql_query = json.loads(os.getenv("query_to_extract_transaction"))
        self.driver_layout = json.loads(os.getenv("driver_layout"))
        self.formats = json.loads(os.getenv("formats"))
        self.partyid_list_values = json.loads(os.getenv("partyids_list"))
        self.historical_table_name = self.partyid_list_values.get('historical_table_name')
        print("self.historical_table_name : ", self.historical_table_name)
        self.region = os.getenv("region")
        self.dbkey = os.getenv("dbkey")
        self.dbname=os.getenv("dbname")
        self.stagingtable_print = os.getenv("table_name_print")
        self.stagingtable_presentment = os.getenv("table_name_presentment")
        self.schema=os.getenv("schema")
        self.ishouseholdneeded = os.getenv("ishouseholdneeded")
        self.ismulticurrency = os.getenv("ismulticurrency")
        self.deliverytype = os.getenv("deliverytype")
        self.message = ""
        self.index = 1
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
            if self.client_name.lower() in ['wfs']:
                account_partyids = [account_party_list['party_identifiervalue'] for account_party_list in extracted_account_data]
            else:
                account_partyids = [account_party_list['party_id'] for account_party_list in extracted_account_data]          
            return account_partyids

        except Exception as e:
            message = f"Account partyid extraction failed. {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': self.trigger_file_name, 'Message': message}, get_run_id())
            raise Exception(message)

    def generate_account_sql_query(self, partyid_list):
        try:
            party_ids = ', '.join(map(str, partyid_list))
            
            sql_account_query = self.query_to_extract_accounts.get("command", "")
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
            sql_transaction_query = self.transaction_sql_query.get("command", "")

            # If you have multiple party IDs, you can do something like this:
            #party_ids = [consolidated_partyids_str]  # or ['ID1', 'ID2', 'ID3']

            # Prepare the formatted string
            #party_ids_str = ", ".join(f"'{id}'" for id in party_ids)
            #sql_transaction_query = sql_transaction_query.replace("IN partyid", f"IN ({', '.join(map(repr,consolidated_partyids_str))})").replace("schema", self.schema)
            #sql_transaction_query = sql_transaction_query.replace("IN partyid", f"IN ({party_ids_str})").replace("schema", self.schema)

            #sql_transaction_query = sql_transaction_query.format(schema=self.schema, partyid=partyid_str)
            sql_transaction_query = sql_transaction_query.format(partyid=partyid_str,clientidentifier=partyclientid_str,historicaltablename=self.historical_table_name)

            return sql_transaction_query

        except Exception as e:
            message = f"Transaction sql query generation failed. {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': self.trigger_file_name, 'Message': message}, get_run_id())
            raise Exception(message)

    def execute_transaction_sql_query(self, sql_transaction_query):
        try:
            # with self.engine.connect() as connection:
            with self.engine.begin() as connection:
                # Set the schema dynamically
                connection.execute(text(f"SET search_path TO {self.schema}"))

                # Execute SQL query
                extracted_transaction_data_rows = connection.execute(text(sql_transaction_query))

            return extracted_transaction_data_rows

        except Exception as e:
            message = f"Transaction sql query execution failed. {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': self.trigger_file_name, 'Message': message}, get_run_id())
            raise Exception(message)

    def generate_communication_sql_query(self, partyid_str, partyclientid_str):
        try:
            sql_communication_query = self.query_to_extract_accounts.get("communication", "")

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
            #if 'OpeningBalance' in transaction.get('transactionamounts', {}):
            if transaction.get("transactioncategory") == "OpeningBalance":
                detail_items = category_details.get("OpeningBalance", [])
            #elif 'ClosingBalance' in transaction.get('transactionamounts', {}):
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
            account_column_names = extracted_account_data_rows.keys()
                
            # Create a dict using the column names and account data.
            accounts_list = [dict(zip(account_column_names, account_list)) for account_list in extracted_account_data_rows_list]

            # 1. First, sort the accounts list by party_identifierreference, then by party_type
            sorted_accounts = sorted(
                accounts_list,
                key=lambda acc: (
                    acc.get('party_clientid') or '',  # Primary sort key
                    acc.get('party_identifierreference') or '',  # Handle None
                    0 if acc.get('party_type') in ('AccountHolder','HouseHoldMasterAccount','HouseHoldSisterAccount') else 1
                )
            )

            # 2. Then group by party_identifierreference
            accounts_grouped_by_reference = groupby(
                sorted_accounts,
                key=lambda acc: (
                    acc.get('party_clientid'),
                    acc.get('party_identifierreference')
                )
            )

            #account_partyids = [account_party_list['partyid'] for account_party_list in extracted_account_data_rows_list]
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

            # Custom sorting function to prioritize 'USD' first
            def custom_sort_with_currency(record):
                currency = record.get('transactionamountcurrency', '')
                if currency == "USD":
                    return (0, currency)  # 'USD' will have priority
                return (1, currency)  # Other currencies will follow

            def process_transactions(party_id,party_clientid):
                # Generate and execute SQL query for transactions
                sql_transaction_query = self.generate_transaction_sql_query(party_id,party_clientid)

                extracted_transactions_data_rows = self.execute_transaction_sql_query(sql_transaction_query)
                extracted_transactions_rows_list = list(extracted_transactions_data_rows)
                transactions_column_names = extracted_transactions_data_rows.keys()
                transactions_consolidated_list = [dict(zip(transactions_column_names, extracted_transactions_row_list)) for extracted_transactions_row_list in extracted_transactions_rows_list]
    
                return transactions_consolidated_list

            def process_communication(party_id,party_clientid):
                # Generate and execute SQL query for transactions
                sql_communication_query = self.generate_communication_sql_query(party_id,party_clientid)

                extracted_communications_data_rows = self.execute_communication_sql_query(sql_communication_query)
                extracted_communications_rows_list = list(extracted_communications_data_rows)
                communications_column_names = extracted_communications_data_rows.keys()
                communications_consolidated_list = [dict(zip(communications_column_names, extracted_communications_row_list)) for extracted_communications_row_list in extracted_communications_rows_list]
    
                return communications_consolidated_list

            def process_category_record(category_record, category_type):
                category_record["heading_1"] = category_type
                category_record["heading_2"] = currency

                # Directly append each entry to the ROW list
                #for key,value,attrib,formatstr,cond in pairs["Detail"]:
                    #header = "DTL_" + value.replace("row", "HEADER") + "_TXT"
                    #category_record[header] = key
                
                return category_record

            def convert_to_float(value):
                """Convert string to float, handling parentheses for negative numbers."""
                value = str(value).replace(',', '')  # Remove commas
                if value.startswith('(') and value.endswith(')'):  # Check for parentheses
                    value = value[1:-1]  # Remove the parentheses
                    return -float(value)  # Convert and make it negative
                
                return float(value)  # Convert normally if no parentheses

            def format_datavalue(value):
                """Format float as currency with commas and parentheses for negative numbers."""
                if value < 0:
                    return f"({abs(value):,.2f})"  # Format negative values with parentheses
                else:
                    return f"{value:,.2f}"  # Format positive values with commas

            output = []

            # 3. Iterate through the groups
            # for identifier_ref, group in accounts_grouped_by_reference:
            for (clientid, identifier_ref), group in accounts_grouped_by_reference:
                group_list = list(group)  # materialize the group into a list
                # print(f"\nGroup - Client ID: {clientid}, Reference: {identifier_ref}")
                for partydata in group_list:

                    #if "impact" in self.trigger_file_name.lower() or "wfs" in self.trigger_file_name.lower():
                    if self.client_name.lower() in ['wfs', 'impact']:
                        partyid = partydata['party_identifierreference']
                        partyidvalue = partydata['party_identifiervalue']
                    else:
                        partyid = partydata['party_id']
                
                    partyclientid = partydata['party_clientid']
                    partytype = partydata['party_type']

                    # Initialize STMT_REC
                    STMT_REC = {
                        "customer": {}
                    }    
            
                    customer_dict = copy.deepcopy(self.driver_layout[0]["customer"])
                    # Empty the detail_table list
                    customer_dict["documents"][0]["document"]["details"] = []

                    documentInfo = copy.deepcopy(self.driver_layout[0]["document_info"])
                    cmPickFields = copy.deepcopy(self.driver_layout[0].get("cm_pick_fields", {}))
                    urAttributes = copy.deepcopy(self.driver_layout[0].get("ur_attributes", {}))
                    mailingAddress = copy.deepcopy(self.driver_layout[0]["customer_mailing_address"])
                    advisorInfo = copy.deepcopy(self.driver_layout[0]["advisor_info"])
                    interestedpartyInfo = copy.deepcopy(self.driver_layout[0]["interested_party_info"])
                    statementDates = copy.deepcopy(self.driver_layout[0]["document_heading"])
                    statementdate_data = {}
 
                    # Process transactions for this party
                    communication_data = process_communication(partyidvalue,partyclientid)
                    communicationdata = communication_data[0][list(communication_data[0].keys())[0]]

                    if partytype in ('AccountHolder', 'HouseHoldMasterAccount', 'HouseHoldSisterAccount'):
                        transactions = process_transactions(partyid,partyclientid)
                        transactions_data = transactions[0][list(transactions[0].keys())[0]]

                    elif partytype == 'InterestedParty':
                        pass

                    if not transactions_data:
                        message = (
                            f"No transactions data found for the combination "
                            f"partyidentifiervalue {partyid}, "
                            f"partyidentifierreference {partyidvalue} "
                            f"and clientid {partyclientid}"
                        )

                        splunk.log_message(
                            {
                                'Status': 'failed',
                                'InputFileName': self.trigger_file_name,  
                                'Message': message
                            },
                            get_run_id()
                        )
                        #ys.exit()  # Exits the program
                        continue


                    # Further group transactions by currency for this party
                    # Sort by 'transactionamountcurrency' before grouping
                    #transactions_data_sorted = sorted(transactions_data, key=lambda x: x.get('transactionamountcurrency', None))
                    transactions_data_sorted = sorted(transactions_data, key=custom_sort_with_currency)
                
                    #transactions_by_currency = groupby(transactions_data, key=lambda x: x['transactionamountcurrency'])
                    transactions_by_currency = groupby(transactions_data_sorted, key=lambda x: x.get('transactionamountcurrency', None))

                    # Initialize an output dictionary to hold currency-specific records
                    currency_output = {}
                    header_date_transaction = {}

                    for currency, currency_transactions in transactions_by_currency:
                        currency_transactions = list(currency_transactions)

                        if currency is not None and currency.strip() != "":
                            currency_output[currency] = copy.deepcopy(self.driver_layout[0]["customer"]["documents"][0]["document"]["details"][0])
                            template = self.driver_layout[0].get("template", {}).get("Template", {})

                            detail_summary_table = self.driver_layout[0].get("template", {}).get("detail_summary_table", {})
                            detail_holdings_table = self.driver_layout[0].get("template", {}).get("detail_holdings_table", {})
                            detail_transactions_table = self.driver_layout[0].get("template", {}).get("detail_transactions_table", {})
                            statement_dates_type_1 = self.driver_layout[0].get("template", {}).get("statement_dates_type_1", {})
                            duplicate_copy_sent_to_type_1 = self.driver_layout[0].get("template", {}).get("duplicate_copy_sent_to_type_1", {})
                        
                            # Initialize both heading dictionaries
                            detail_summary_table_headings_dict = {}
                            detail_summary_table_data = {}
                            detail_summary_table_record = {}

                            detail_holdings_table_headings_dict = {}
                            detail_holdings_table_data = {}
                            detail_holdings_table_record = {}

                            detail_transactions_table_headings_dict = {}
                            detail_transactions_table_data = {}
                            detail_transactions_table_record = {}

                            statement_dates_type_1_headings_dict = {}                        
                            statement_dates_type_1_data = {}
                            statement_dates_type_1_record = {}

                            # Process detail_summary_table
                            if detail_summary_table:
                                for category, details in detail_summary_table.items():
                                    detail_summary_table_data[category] = {}
                                    globals()[f"{category}_summary_record"] = copy.deepcopy(self.driver_layout[0]["detail_record"])

                                    # Iterate through each item (Heading, Detail, Total) in details
                                    for key, value in details.items():
                                        template_key = details.get(key, None)
                                        if template_key:
                                            # Use the get_nested_value function
                                            template_data = self.get_nested_value(template, template_key)
                                            if template_data is not None and template_data not in [None, "", []]:
                                                detail_summary_table_data[category][key] = copy.deepcopy(template_data)
                                            else:
                                                detail_summary_table_data[category][key] = value
                                                detail_summary_table_headings_dict[value] = category  # Map heading to category name

                            # Process detail_holdings_table
                            if detail_holdings_table:
                                for category, details in detail_holdings_table.items():
                                    detail_holdings_table_data[category] = {}
                                    globals()[f"{category}_holdings_record"] = copy.deepcopy(self.driver_layout[0]["detail_record"])

                                    # Iterate through each item (Heading, Detail, Total) in details
                                    for key, value in details.items():
                                        template_key = details.get(key, None)
                                        if template_key:
                                            # Use the get_nested_value function
                                            template_data = self.get_nested_value(template, template_key)
                                            if template_data is not None and template_data not in [None, "", []]:
                                                detail_holdings_table_data[category][key] = copy.deepcopy(template_data)
                                            else:
                                                detail_holdings_table_data[category][key] = value
                                                detail_holdings_table_headings_dict[value] = category  # Map heading to category name

                            # Process detail_transactions_table
                            if detail_transactions_table:
                                for category, details in detail_transactions_table.items():
                                    detail_transactions_table_data[category] = {}
                                    globals()[f"{category}_transactions_record"] = copy.deepcopy(self.driver_layout[0]["detail_record"])

                                    # Iterate through each item (Heading, Detail, Total) in details
                                    for key, value in details.items():
                                        template_key = details.get(key, None)
                                        if template_key:
                                            # Use the get_nested_value function
                                            template_data = self.get_nested_value(template, template_key)
                                            if template_data is not None and template_data not in [None, "", []]:
                                                detail_transactions_table_data[category][key] = copy.deepcopy(template_data)
                                            else:
                                                detail_transactions_table_data[category][key] = value
                                                detail_transactions_table_headings_dict[value] = category  # Map heading to category name

                            # Process statement_dates_type_1
                            if statement_dates_type_1:
                                for category, details in statement_dates_type_1.items():
                                    statement_dates_type_1_data[category] = {}
                                    globals()[f"{category}_record"] = {}

                                    # Iterate through each item (Heading, Detail, Total) in details
                                    for key, value in details.items():
                                        template_key = details.get(key, None)
                                        if template_key:
                                            # Use the get_nested_value function
                                            template_data = self.get_nested_value(template, template_key)
                                            if template_data is not None and template_data not in [None, "", []]:
                                                statement_dates_type_1_data[category][key] = copy.deepcopy(template_data)
                                            else:
                                                statement_dates_type_1_data[category][key] = value
                                                statement_dates_type_1_headings_dict[value] = category  # Map heading to category name

                            # Group the currency transactions by category
                            #transactions_by_category = groupby(currency_transactions, key=lambda x: x['sectiontype'])
                            #transactions_by_category = groupby(currency_transactions, key=lambda x: (x['sectiontype'], x['sectionheader']))
                            #transactions_by_category = groupby(currency_transactions, key=lambda x: (x['sectiontype'], x['sectionheader']))
                            currency_transactions.sort(key=lambda x: (x.get('sectiontype', ' '), x.get('sectionheader', ' ')))
                            transactions_by_category = groupby(currency_transactions, key=lambda x: (x.get('sectiontype', ' '), x.get('sectionheader', ' ')))

                            #Once Abhishek is done with BRx changes, above line should be removed and below line should be included
                            #transactions_by_category = groupby(currency_transactions, key=lambda x: (x['transactionclassificationtype'], x['transactionflagtype']))

                            #for category, category_transactions in transactions_by_category:
                            for (sectiontype, sectionheader), category_transactions in transactions_by_category:
                                category_transactions = list(category_transactions)

                                if sectiontype == 'Holdings':
                                    # Initialize a group_row for this section (even though there is no grouping for Holdings and Summary)
                                    group_row = []
                                    for transaction in category_transactions:
                                        if transaction['sectionheader'] in detail_holdings_table_headings_dict:
                                            category = detail_holdings_table_headings_dict[transaction['sectionheader']] # Get the corresponding category name
                                            category_details = detail_holdings_table_data.get(category)
                                            record_entry = self.create_holdings_record(category, category_details, transaction)

                                            # Append the created record_entry for the current transaction to group_row
                                            if record_entry:  # Ensure record_entry is not empty before appending
                                                # group_row.append(record_entry)
                                                group_row.extend(record_entry)
                                
                                    record_type = "holdings" 
                                    category_name = category 

                                    globals()[f"{category_name}_{record_type}_record"]["sub_detail_column_values"].append({"row": group_row})

                                elif sectiontype == 'Activity' and sectionheader not in ['Deposit Activities', 'Money Market Mutual Fund Activity', 'Statement of Billing Fees Collected - This is Not A Bill']:
                                    # Initialize a group_row for this section (even though there is no grouping for Holdings and Summary)
                                    group_row = []
                                
                                    for transaction in category_transactions:
                                        if transaction['sectionheader'] in detail_transactions_table_headings_dict:
                                            category = detail_transactions_table_headings_dict[transaction['sectionheader']] # Get the corresponding category name
                                            category_details = detail_transactions_table_data.get(category)
                                            record_entry = self.create_transactions_record(category, category_details, transaction)

                                            # Append the created record_entry for the current transaction to group_row
                                            if record_entry:  # Ensure record_entry is not empty before appending
                                                # group_row.append(record_entry)
                                                group_row.extend(record_entry)
                                
                                    record_type = "transactions" 
                                    category_name = category 

                                    globals()[f"{category_name}_{record_type}_record"]["sub_detail_column_values"].append({"row": group_row})

                                elif sectiontype == 'Activity' and sectionheader in ['Deposit Activities', 'Money Market Mutual Fund Activity', 'Statement of Billing Fees Collected - This is Not A Bill']:

                                    grouped = defaultdict(list)  # Initialize a dictionary to group transactions by ADPNumber

                                    # Loop through all transactions for the given sectiontype
                                    for txn in category_transactions:
                                        if sectionheader == 'Statement of Billing Fees Collected - This is Not A Bill':
                                            key = txn.get("GeneralNarrative")
                                        else:
                                            key = txn.get("ADPNumber")

                                        grouped[key].append(txn)  # Add the transaction to the group for this ADPNumber

                                    # Now process each group of transactions (grouped by ADPNumber)
                                    for adp, grouped_txns in grouped.items():
                                        transaction = grouped_txns[0]  # Take the first transaction in the group as a representative one

                                        # Check if the sectionheader is in the headings dictionary for 'Activity'
                                        if transaction['sectionheader'] in detail_transactions_table_headings_dict:
                                            # Get the category corresponding to the sectionheader
                                            category = detail_transactions_table_headings_dict[transaction['sectionheader']]
                                            # Get the details for that category from the 'detail_transactions_table_data'
                                            category_details = detail_transactions_table_data.get(category)

                                            # Separate lists for each recordtype
                                            head_rows = []
                                            begin_rows = []
                                            detail_rows = []
                                            end_rows = []

                                            # grouped_txns.sort(key=lambda x: x.get("row_num", 0))
                                            
                                            # Loop through the grouped transactions and generate the transaction records
                                            for txn in grouped_txns:
                                                # Call the create_transactions_record function to handle the record creation for each transaction
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
                                                        # record_entry is a single dict
                                                        if record_type == "Head":
                                                            head_rows.append(record_entry)
                                                        elif record_type == "Begin":
                                                            begin_rows.append(record_entry)
                                                        elif record_type == "Detail":
                                                            if any(str(v).strip() not in ["", "0", "0.00", "None", "null"] for v in record_entry.values()):
                                                                detail_rows.append(record_entry)
                                                        elif record_type in ["End", "Total"]:
                                                            end_rows.append(record_entry)

                                            # Initialize group_row and append the rows in the specified order
                                            group_row = []
                                            group_row.extend(begin_rows)
                                            group_row.extend(detail_rows)
                                            group_row.extend(end_rows)
                                        
                                            # After processing all transactions in the current group, append the group_row to the appropriate global variable
                                            record_type = "transactions"  # Assuming the record type is "transactions", you can adjust if needed
                                            category_name = category  # This will be the name of the category (e.g., "Deposit Activities")

                                            globals()[f"{category_name}_{record_type}_record"]["sub_detail_column_values"].append({"sub_section_column_values": head_rows, "row": group_row})

                                elif sectiontype == 'Summary':
                                    # Initialize a group_row for this section (even though there is no grouping for Holdings and Summary)
                                    group_row = []
                                    for transaction in category_transactions:
                                        if transaction['sectionheader'] in detail_summary_table_headings_dict:
                                            category = detail_summary_table_headings_dict[transaction['sectionheader']] # Get the corresponding category name
                                            category_details = detail_summary_table_data.get(category)
                                            record_entry = self.create_summary_record(category, category_details, transaction)

                                            # Append the created record_entry for the current transaction to group_row
                                            if record_entry:  # Ensure record_entry is not empty before appending
                                                # group_row.append(record_entry)
                                                group_row.extend(record_entry)
                                
                                    record_type = "summary" 
                                    category_name = category 

                                    globals()[f"{category_name}_{record_type}_record"]["sub_detail_column_values"].append({"row": group_row})

                            # Initialize the counter variable before the loop
                            summary_section_level_counter = 1

                            # Optionally remove detail_transactions_table if it also remains empty
                            for category, pairs in detail_summary_table.items():
                                category_record = globals().get(f"{category}_summary_record")

                                if category_record and isinstance(category_record.get("sub_detail_column_values", []), list):
                                    # Remove 'sub_section_column_values' if it's empty
                                    if category_record["sub_detail_column_values"] and any(item.get("row") for item in category_record["sub_detail_column_values"]):
                                        category_record["section_level_indicator"] = str(summary_section_level_counter)

                                        if summary_section_level_counter == 1:
                                            category_record = process_category_record(category_record, "Account Summary")

                                        category_record["sub_heading_1"] = pairs.get("Heading")
                                        category_record["sub_heading_2"] = pairs.get("SubHeading2", "")
                                        #category_record["reference_table_name"] = pairs.get("refTable", "")
                                        category_record["reference_table_name"] = f"{self.client_name}_summary_{category.lower()}"
                                        currency_output[currency]["summary"].append(category_record)
                                        # Increment the section_level_counter for the next iteration
                                        summary_section_level_counter += 1

                            if not currency_output[currency]["summary"]:
                                del currency_output[currency]["summary"]

                            # Initialize the counter variable before the loop
                            holdings_section_level_counter = 1
                        
                            for category, pairs in detail_holdings_table.items():
                                category_record = globals().get(f"{category}_holdings_record")

                                if category_record and isinstance(category_record.get("sub_detail_column_values", []), list):
                                    # Remove 'sub_section_column_values' if it's empty
                                    if category_record["sub_detail_column_values"] and any(item.get("row") for item in category_record["sub_detail_column_values"]):
                                        category_record["section_level_indicator"] = str(holdings_section_level_counter)

                                        if holdings_section_level_counter == 1: #This condition has been added to include heading only for the first section
                                            category_record = process_category_record(category_record, "Holdings")

                                        category_record["sub_heading_1"] = pairs.get("Heading")
                                        category_record["sub_heading_2"] = pairs.get("SubHeading2", "")
                                        #category_record["reference_table_name"] = pairs.get("refTable", "")
                                        category_record["reference_table_name"] = f"{self.client_name}_holdings_{category.lower()}"
                                        currency_output[currency]["holdings"].append(category_record)
                                        # Increment the section_level_counter for the next iteration
                                        holdings_section_level_counter += 1

                            # Optionally remove detail_transactions_table if it also remains empty
                            if not currency_output[currency]["holdings"]:
                                del currency_output[currency]["holdings"]

                            # Initialize the counter variable before the loop
                            transactions_section_level_counter = 1

                            for category, pairs in detail_transactions_table.items():
                                category_record = globals().get(f"{category}_transactions_record")

                                # Check for TradeActivities category - This logic is for Impact Statement
                                if category == "TradeActivities":
                                    # Check if there are more than 2 records in the ROW

                                    sub_detail_values = category_record.get("sub_detail_column_values", [])
                                    if isinstance(sub_detail_values, list) and sub_detail_values:
                                        row_records = sub_detail_values[0].get("row", [])
                                    else:
                                        row_records = []

                                    if len(row_records) <= 2:  # Less than or equal to 2 means we likely don't have TradeActivities. Only Opening Balance and closing Balance records are present.
                                        #category_record["sub_detail_column_values"]["row"] = []  # Clear the ROW list
                                        category_record["sub_detail_column_values"] = [{"row": []}]

                                if category_record and isinstance(category_record.get("sub_detail_column_values", []), list):
                                    # Remove 'sub_section_column_values' if it's empty
                                    if category_record["sub_detail_column_values"] and any(item.get("row") for item in category_record["sub_detail_column_values"]):
                                        category_record["section_level_indicator"] = str(transactions_section_level_counter)
                                    
                                        if transactions_section_level_counter == 1:
                                            category_record = process_category_record(category_record, "Transactions")

                                        category_record["sub_heading_1"] = pairs.get("Heading")
                                        category_record["sub_heading_2"] = pairs.get("SubHeading2", "")
                                        #category_record["reference_table_name"] = pairs.get("refTable", "")
                                        category_record["reference_table_name"] = f"{self.client_name}_transactions_{category.lower()}"

                                        currency_output[currency]["transactions"].append(category_record)
                                        # Increment the section_level_counter for the next iteration
                                        transactions_section_level_counter += 1
                        
                            # Optionally remove detail_transactions_table if it also remains empty
                            if not currency_output[currency]["transactions"]:
                                del currency_output[currency]["transactions"]
                        else:
                            #if currency_transactions[0]["transactioncategory"] == "Header Date":
                            #if currency_transactions[0].get("transactioncategory") == "Header Date":
                            if currency_transactions and currency_transactions[0].get("transactioncategory", "").lower() == "header date".lower():

                                header_date_transaction = currency_transactions[0]
                                category = "StatementDates"
                                category_details = statement_dates_type_1_data.get(category)

                                # Check if partyid in currency_transactions[0] matches party_id in partydata
                                if currency_transactions[0]["partyid"] == str(partydata["party_id"]) or currency_transactions[0]["partyid"] == str(partydata["party_identifierreference"]):
                                    # Combine the data
                                    party_and_transaction_combined_data = {**currency_transactions[0], **partydata}

                                    # Pass the combined data to create_statementdates_record
                                    statementdate_data = self.create_statementdates_record(category, category_details, party_and_transaction_combined_data)
                                else:
                                    # If partyid doesn't match, handle the error or pass only currency_transactions[0]
                                    statementdate_data = self.create_statementdates_record(category, category_details, currency_transactions[0])
                            else:
                                statementdate_data = {}
                                header_date_transaction = {}

                    for stmt_key, partydata_key in documentInfo.items():
                        if partydata_key in partydata:
                            value = partydata[partydata_key]
                        elif partydata_key in header_date_transaction:
                            value = header_date_transaction[partydata_key]
                        else:
                            value = ''
                        customer_dict["documents"][0]["document"]["document_info"][stmt_key] = value

                    for cm_key, cmdata_key in cmPickFields.items():
                        value = communicationdata.get(cmdata_key, None)
                        customer_dict["documents"][0]["document"]["campaign_manager"]["cm_pick_fields"][cm_key] = value if value is not None else ''

                    for ur_key, urdata_key in urAttributes.items():
                        value = communicationdata.get(urdata_key, None)
                        customer_dict["documents"][0]["document"]["ur_attributes"][ur_key] = value if value is not None else ''

                    # Handle search_keys
                    #for search_key in searchKeys:
                        #customer_dict["search_keys"][search_key] = partydata.get(search_key, '')                

                    # Handle inserts
                    #for insert_key in inserts:
                        #customer_dict["inserts"][insert_key] = partydata.get(insert_key, '')

                    # Handle user_variables
                    #for user_var_key in userVariables:
                        #customer_dict["user_variables"][user_var_key] = partydata.get(user_var_key, '')

                    # Append each address line from mailingAddress to customer_dict
                    for address in mailingAddress:
                        # Use the key from the address dict
                        key = next(iter(address))  # Get the first key dynamically
                        line_value = partydata.get(address[key], '')  # Use the dynamic key
                        if line_value:  # Check if line_value is not empty or null
                            customer_dict["documents"][0]["document"]["customer_mailing_address"].append({key: line_value}) 

                    # Append each address line from advisorInfo to customer_dict
                    for advisorInfoRec in advisorInfo:
                        # Use the key from the advisorInfoRec dict
                        key = next(iter(advisorInfoRec))  # Get the first key dynamically
                        line_value = partydata.get(advisorInfoRec[key], '')  # Use the dynamic key
                        if line_value:  # Check if line_value is not empty or null
                            customer_dict["documents"][0]["document"]["advisor_info"].append({key: line_value}) 

                    # Append each address line from interestedpartyInfo to customer_dict if an account is interested party
                    if partydata.get('party_classificationvalue') == 'InterestedParty':
                        for interestedpartyInfoRec in interestedpartyInfo:
                            # Use the key from the interestedpartyInfoRec dict
                            key = next(iter(interestedpartyInfoRec))  # Get the first key dynamically
                            line_value = partydata.get(interestedpartyInfoRec[key], '')  # Use the dynamic key
                            if line_value:  # Check if line_value is not empty or null
                                customer_dict["documents"][0]["document"]["interested_party_info"].append({key: line_value}) 

                    if statementdate_data:
                        statement_dict = statementdate_data[0]
                        for statementDatesRec in statementDates:
                            key = next(iter(statementDatesRec)) 
                            # line_value = statementdate_data.get(statementDatesRec[key], '')
                            line_value = statement_dict.get(statementDatesRec[key], '')
                            # If line_value is empty or null, check partydata
                            if not line_value:
                                line_value = partydata.get(statementDatesRec[key], '')

                            if line_value:  # Check if line_value is not empty or null
                                customer_dict["documents"][0]["document"]["document_heading"].append({key: line_value}) 
                
                    STMT_REC["customer"] = customer_dict.copy()

                    for currency, records in currency_output.items():
                        if records.get("summary") or records.get("holdings") or records.get("transactions"):
                            STMT_REC["customer"]["documents"][0]["document"]["details"].append(records)

                    # Only proceed if duplicate_copy_sent_to_type_1 is not None or empty
                    if duplicate_copy_sent_to_type_1:
                        # Get the duplicate parties from partydata, and remove any None values in the list
                        duplicateParties = [party for party in (partydata.get('duplicate_copies') or []) if party is not None]

                        # If there are valid duplicate parties, process them
                        if duplicateParties:
                            duplicateParties_record = copy.deepcopy(self.driver_layout[0]["detail_record"])
                            duplicateParties_record["heading_1"] = duplicate_copy_sent_to_type_1.get("DuplicateCopies", {}).get("Heading", "")

                            # Add the duplicate party rows to the record
                            for interested_party_copy in duplicateParties:
                                duplicateParties_record["sub_detail_column_values"].append({"row": interested_party_copy})

                            # Append the duplicate parties to the document in the statement record
                            STMT_REC["customer"]["documents"][0]["document"]["duplicate_copies"].append(duplicateParties_record)

                    # Append party output to the list
                    output.append(STMT_REC)
            
            # Function to find children of a given parent_id
            def find_children(parent_id):
                children = []
                sister_account_numbers = []  # List to hold the account numbers
                sequence_value = 1
                for record in output:
                    stmt = record['customer']
                    if (stmt["documents"][0]["document"]['document_info']['parent_id'] == parent_id and stmt["documents"][0]["document"]['document_info']['account_type'] == 'HouseHoldSisterAccount'):
                        document = stmt["documents"][0]["document"]
                        
                        sequence_value += 1  # Increment sequence value for next document
                        document['document_info']['sequence'] = str(sequence_value)
                        document['document_info']['is_interested_party'] = 'N'

                        # Add the account number to the account_numbers list
                        sister_account_number = document['document_info']['account_number']
                        sister_account_numbers.append(sister_account_number)
                        
                        children.append({"document": document})
                return children,sister_account_numbers

            def create_presentment_statement_list(item):
                # Logic to add individual statements for presentment
                stmt = item['customer']
                acct_num = stmt['documents'][0]['document']['document_info']['account_number']
                client_id = stmt['documents'][0]['document']['document_info']['client_id']
                item['customer']['household_flag'] = 'N'
                item['customer']['documents'][0]['document']['document_info']['sequence'] = '1'

                #xmldata = xmltodict.unparse({'Wealth': item}, pretty=True, short_empty_elements=True)
                # Remove lines containing <?xml version="1.0" encoding="utf-8">, <root>, and </root>
                #filtered_xmldata = re.sub(r'<\?xml.*?\?>\n|<root>\n|</root>', '', xmldata)

                presentment_driver = {
                    'partyid': acct_num,
                    'partyidentifier': acct_num,
                    'clientid': client_id,
                    'statementtype': 'Individual',
                    #'xml_driver': filtered_standalone_xmldata,
                    #'statement': json.dumps({"customer": item["customer"]}, cls=DecimalEncoder)
                    'statement': {"customer": item["customer"]}
                }

                presentment_statement_list.append(presentment_driver)

            def create_print_statement_list(item, party_id_number):
                # Logic to add household and individual statements for print
                stmt = item['customer']
                stmt_id = stmt['documents'][0]['document']['document_info']['id']
                hh_indicator = stmt['documents'][0]['document']['document_info']['account_type']
                client_id = stmt['documents'][0]['document']['document_info']['client_id']
                acct_num = stmt['documents'][0]['document']['document_info']['account_number']   # Either partyid or account number
                if hh_indicator == 'HouseHoldMasterAccount':
                    household_statement_list = []
                    children, sister_account_numbers = find_children(stmt_id)
                    item['customer']['household_flag'] = 'Y'
                    item['customer']['documents'][0]['document']['document_info']['sequence'] = '1'
                    item['customer']['documents'][0]['document']['document_info']['sister_accounts'] = ','.join(sister_account_numbers)
                    item['customer']['documents'][0]['document']['document_info']['is_interested_party'] = 'N'
                    item['customer']['documents'].extend(children)

                    #household_xmldata = xmltodict.unparse({'Wealth': sub_output}, pretty=True, short_empty_elements=True)
                    #filtered_household_xmldata = re.sub(r'<\?xml.*?\?>\n|<root>\n|</root>', '', household_xmldata)
                    print_driver = {
                        'partyid': str(party_id_number),
                        'partyidentifier': acct_num,
                        'clientid': client_id,
                        'statementtype': hh_indicator,
                        #'xml_driver': filtered_household_xmldata,
                        #'statement': json.dumps({"customer": item["customer"]}, cls=DecimalEncoder)
                        'statement': {"customer": item["customer"]}
                        #'statement': json.dumps({"customer": item["customer"]}, cls=DecimalEncoder, ensure_ascii=False).encode('utf-8')
                    }
                    print_statement_list.append(print_driver)

                elif hh_indicator == 'AccountHolder':
                    item['customer']['household_flag'] = 'N'
                    item['customer']['documents'][0]['document']['document_info']['sequence'] = '1'
                    item['customer']['documents'][0]['document']['document_info']['is_interested_party'] = 'N'
                    #standalone_xmldata = xmltodict.unparse({'Wealth': item}, pretty=True, short_empty_elements=True)
                    # Remove lines containing <?xml version="1.0" encoding="utf-8">, <root>, and </root>
                    #filtered_standalone_xmldata = re.sub(r'<\?xml.*?\?>\n|<root>\n|</root>', '', standalone_xmldata)

                    print_driver = {
                        'partyid': str(party_id_number),
                        'partyidentifier': acct_num,
                        'clientid': client_id,
                        'statementtype': hh_indicator,
                        #'xml_driver': filtered_standalone_xmldata,
                        #'statement': json.dumps({"customer": item["customer"]}, cls=DecimalEncoder)
                        'statement': {"customer": item["customer"]}
                        #'statement': json.dumps({"customer": item["customer"]}, cls=DecimalEncoder, ensure_ascii=False).encode('utf-8')
                    }
                    print_statement_list.append(print_driver)

                elif hh_indicator == 'InterestedParty':
                    item['customer']['household_flag'] = 'N'
                    item['customer']['documents'][0]['document']['document_info']['sequence'] = '1'

                    item['customer']['documents'][0]['document']['document_info']['is_interested_party'] = 'Y'
                    #standalone_xmldata = xmltodict.unparse({'Wealth': item}, pretty=True, short_empty_elements=True)
                    # Remove lines containing <?xml version="1.0" encoding="utf-8">, <root>, and </root>
                    #filtered_standalone_xmldata = re.sub(r'<\?xml.*?\?>\n|<root>\n|</root>', '', standalone_xmldata)

                    print_driver = {
                        'partyid': str(party_id_number),
                        'partyidentifier': acct_num,
                        'clientid': client_id,
                        'statementtype': hh_indicator,
                        #'xml_driver': filtered_standalone_xmldata,
                        #'statement': json.dumps({"customer": item["customer"]}, cls=DecimalEncoder)
                        'statement': {"customer": item["customer"]}
                        #'statement': json.dumps({"customer": item["customer"]}, cls=DecimalEncoder, ensure_ascii=False).encode('utf-8')
                    }
                    print_statement_list.append(print_driver)

            presentment_statement_list = []
            print_statement_list = []

            #for item in output:
            for idx, item in enumerate(output, start=1):
                if self.deliverytype == 'P':
                    # Logic to add household and individual statements for print
                    create_print_statement_list(item, idx)

                elif self.deliverytype == 'A':
                    # Logic to add individual statements for presentment
                    create_presentment_statement_list(item)

                elif self.deliverytype == 'PA':
                    # Logic to add individual statements for presentment
                    create_presentment_statement_list(item)
                    # Logic to add household and individual statements for print
                    create_print_statement_list(item, idx)

            with self.engine.connect() as connection:     
                account_staging_table_presentment = Table(self.stagingtable_presentment, self.meta, autoload=True, autoload_with=self.engine)
                account_staging_table_print = Table(self.stagingtable_print, self.meta, autoload=True, autoload_with=self.engine)
                if presentment_statement_list and (self.deliverytype == 'A' or self.deliverytype == 'PA'):
                    connection.execute(insert(account_staging_table_presentment).values(presentment_statement_list))
                    connection.commit()
                if print_statement_list and (self.deliverytype == 'P' or self.deliverytype == 'PA'):
                    connection.execute(insert(account_staging_table_print).values(print_statement_list))
                    connection.commit()
                
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
