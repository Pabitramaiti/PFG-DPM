import json, sys,boto3,re, traceback, math
import psycopg2
from awsglue.utils import getResolvedOptions
from itertools import groupby
import uuid
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

# simplefilestorage to get data in s3 object in bytes
glue_client = boto3.client('glue')
s3 = boto3.client('s3')

def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:store_auxrecords_postgres:" + account_id

def get_secret_values(secret_name):
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId = secret_name)
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
        self.account_sql_query = json.loads(os.getenv("query_to_extract_account"))
        self.transaction_sql_query = json.loads(os.getenv("query_to_extract_transaction"))
        self.driver_layout = json.loads(os.getenv("driver_layout"))
        self.formats = json.loads(os.getenv("formats"))
        self.advisors_list_values = json.loads(os.getenv("advisors_list"))
        self.region = os.getenv("region")
        self.dbkey = os.getenv("dbkey")
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
            self.engine = create_engine(f"postgresql://{host['username']}:{host['password']}@{host['host']}:{host['port']}/{host['dbname']}", future=True)
            
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

    def extract_advisor_partyids(self):
        try:            
            # Logic to extract list of advisor partyids from the config data
            advisor_partyids_list = self.advisors_list_values.get('partyid_list', [])
            #advisor_partyids_list = self.advisors_list_values.get('partyidentifiervalue_list', [])
            return advisor_partyids_list

        except Exception as e:
            message = f"Advisor partyid extraction failed. {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': 'Test', 'Message': message}, get_run_id())
            raise Exception(message)

    def extract_account_partyids(self, extracted_account_data):
        try:            
            account_partyids = [account_party_list['party_id'] for account_party_list in extracted_account_data]
            #account_partyids = [account_party_list['partyidentifier_value'] for account_party_list in extracted_account_data]
            return account_partyids

        except Exception as e:
            message = f"Account partyid extraction failed. {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': 'Test', 'Message': message}, get_run_id())
            raise Exception(message)

    def generate_account_sql_query(self, partyid_list):
        try:            
            partyids_str = map(str,partyid_list)
            sql_account_query = self.account_sql_query.get("command", "")
            sql_account_query = sql_account_query.replace("IN partyid_list", f"IN ({', '.join(map(repr,partyids_str))})").replace("schema", self.schema)

            return sql_account_query

        except Exception as e:
            message = f"Account sql query generation failed. {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': 'Test', 'Message': message}, get_run_id())
            raise Exception(message)

    def execute_account_sql_query(self, sql_query):
        try:            
            with self.engine.connect() as connection:
                # Execute SQL query
                extracted_account_data_rows = connection.execute(text(sql_query))

            return extracted_account_data_rows

        except Exception as e:
            message = f"Account sql query execution failed. {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': 'Test', 'Message': message}, get_run_id())
            raise Exception(message)

    def generate_transaction_sql_query(self, consolidated_partyids_str):
        try:            
            sql_transaction_query = self.transaction_sql_query.get("command", "")

            # If you have multiple party IDs, you can do something like this:
            party_ids = [consolidated_partyids_str]  # or ['ID1', 'ID2', 'ID3']

            # Prepare the formatted string
            party_ids_str = ", ".join(f"'{id}'" for id in party_ids)
            #sql_transaction_query = sql_transaction_query.replace("IN partyid", f"IN ({', '.join(map(repr,consolidated_partyids_str))})").replace("schema", self.schema)
            sql_transaction_query = sql_transaction_query.replace("IN partyid", f"IN ({party_ids_str})").replace("schema", self.schema)
            return sql_transaction_query

        except Exception as e:
            message = f"Transaction sql query generation failed. {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': 'Test', 'Message': message}, get_run_id())
            raise Exception(message)

    def execute_transaction_sql_query(self, sql_transaction_query):
        try:            
            with self.engine.connect() as connection:
                start_time_for_transaction_query = datetime.now()
                # Execute SQL query
                extracted_transaction_data_rows = connection.execute(text(sql_transaction_query))
                end_time_for_transaction_query = datetime.now()
                
                execution_time_for_transaction_query = end_time_for_transaction_query - start_time_for_transaction_query

            return extracted_transaction_data_rows

        except Exception as e:
            message = f"Transaction sql query execution failed. {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': 'Test', 'Message': message}, get_run_id())
            raise Exception(message)

    def create_incomesummary_record(self, transaction):
        try:
            # Initialize the dictionary
            incomesummary_entry = {}

            for key,value,attrib,format_type,cond in IncomeSummary:
                if attrib:
                    value_from_transaction = transaction.get(attrib)
                    if cond.strip():  # Check if the condition is not empty
                        column_name, expected_value = cond.split('|')  # Split the condition into column name and expected value
                        if transaction.get(column_name) == expected_value:
                            formatted_value = self.format_value(value_from_transaction, format_type)
                            incomesummary_entry[value] = formatted_value
                    else:
                        formatted_value = self.format_value(value_from_transaction, format_type)
                        incomesummary_entry[value] = formatted_value

                else:
                    incomesummary_entry[value] = ''

            # Append column values to the corresponding entry in the dictionary
            IncomeSummary_holdings_record["dtl_column_values"]["row"].append(incomesummary_entry)

        except Exception as e:
            message = f"Income summary record creation failed. {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': 'Test', 'Message': message}, get_run_id())
            raise Exception(message)

    def format_value(self, value, format_type):
        # Check if format_type is an empty string
        if not format_type:
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
                    zero_handling = format_config[sub_type].get("zeroHandling", None)

                    if isinstance(value, (int, float, Decimal)):  # Check if value is numeric for formatting
                        if value == 0:
                            if zero_handling == "remove":
                                return ''
                            elif zero_handling == "retain":
                                return format_str % value
                        if value < 0:
                            formatted_value = format_str % abs(value)  # Get the absolute value
                            if negative_sign_format == "signEnclosed":
                                return f"({formatted_value})"
                            elif negative_sign_format == "signRight":
                                return f"{formatted_value}-"
                            elif negative_sign_format == "signLeft":
                                return f"-{formatted_value}"
                        else:
                            return format_str % value if value is not None else ''
                    else:
                        return ""

                elif isinstance(value, str):
                    return value

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

    def create_summary_record(self, category_name, category_details, transaction):
        try:
            summary_detail_entry = {}

            if transaction.get("recordtype") == "Detail":
                detail_items = category_details.get("Detail", [])
            elif transaction.get("recordtype") == "Total":
                detail_items = category_details.get("Total", [])
            # This else part has to be removed once Abhishek is done with detail/total differentiation
            else:
                detail_items = category_details.get("Detail", [])

            for detail_item in detail_items:
                label, column, template, fields, condition = detail_item
                clear_template = "N"
                # Check conditions if specified
                if condition:
                    check_type = condition.get("check")
                    check_value = self.get_nested_value(transaction, condition["field"])

                    # Evaluate based on the specified condition
                    if check_type == "null" and check_value is None:
                        clear_template = "Y"  # Clear template if null
                    elif check_type == "greater" and check_value <= condition.get("value", 0):
                        clear_template = "Y"  # Clear template if not greater
                    elif check_type == "lesser" and check_value >= condition.get("value", 0):
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
                        summary_detail_entry[column] = formatted_output
                    else:
                        summary_detail_entry[column] = ""
                else:
                    summary_detail_entry[column] = label
            # Append the entry to the detail record dictionary
            if summary_detail_entry:
                globals()[f"{category_name}_summary_record"]["dtl_column_values"]["row"].append(summary_detail_entry)

        except Exception as e:
            message = f"Summary - {category_name.lower()} record creation failed. {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': 'Test', 'Message': message}, get_run_id())
            raise Exception(message)

    def create_transactions_record(self, category_name, category_details, transaction):
        try:            
            # Initialize the dictionary
            transactions_detail_entry = {}

            # Determine which detail items to use based on the presence of OpeningBalance or ClosingBalance
            if transaction.get("recordtype") == "Detail":
                if 'OpeningBalance' in transaction.get('transactionamounts', {}):
                    detail_items = category_details.get("OpeningBalance", [])
                elif 'ClosingBalance' in transaction.get('transactionamounts', {}):
                    detail_items = category_details.get("ClosingBalance", [])
                else:  
                    detail_items = category_details.get("Detail", [])
            elif transaction.get("recordtype") == "Total":
                detail_items = category_details.get("Total", [])

            # Process Detail records
            for detail_item in detail_items:
                label, column, template, fields, condition = detail_item
                clear_template = "N"
                # Check conditions if specified
                if condition:
                    check_type = condition.get("check")
                    check_value = self.get_nested_value(transaction, condition["field"])

                    # Evaluate based on the specified condition
                    if check_type == "null" and check_value is None:
                        clear_template = "Y"  # Clear template if null
                    elif check_type == "greater" and check_value <= condition.get("value", 0):
                        clear_template = "Y"  # Clear template if not greater
                    elif check_type == "lesser" and check_value >= condition.get("value", 0):
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
                        transactions_detail_entry[column] = formatted_output
                    else:
                        transactions_detail_entry[column] = ""
                else:
                    transactions_detail_entry[column] = label

            # Append the entry to the detail record dictionary
            if transactions_detail_entry:
                globals()[f"{category_name}_transactions_record"]["dtl_column_values"]["row"].append(transactions_detail_entry)

        except Exception as e:
            message = f"Transactions - {category_name.lower()} record creation failed. {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': 'Test', 'Message': message}, get_run_id())
            raise Exception(message)

    def create_holdings_record(self, category_name, category_details, transaction):
        try:
            holdings_detail_entry = {}

            if transaction.get("recordtype") == "Detail":
                detail_items = category_details.get("Detail", [])
            elif transaction.get("recordtype") == "Total":
                detail_items = category_details.get("Total", [])

            for detail_item in detail_items:
                label, column, template, fields, condition = detail_item
                clear_template = "N"
                # Check conditions if specified
                if condition:
                    check_type = condition.get("check")
                    check_value = self.get_nested_value(transaction, condition["field"])

                    # Evaluate based on the specified condition
                    if check_type == "null" and check_value is None:
                        clear_template = "Y"  # Clear template if null
                    elif check_type == "greater" and check_value <= condition.get("value", 0):
                        clear_template = "Y"  # Clear template if not greater
                    elif check_type == "lesser" and check_value >= condition.get("value", 0):
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
                        holdings_detail_entry[column] = formatted_output
                    else:
                        holdings_detail_entry[column] = ""
                else:
                    holdings_detail_entry[column] = label
            # Append the entry to the detail record dictionary
            if holdings_detail_entry:
                globals()[f"{category_name}_holdings_record"]["dtl_column_values"]["row"].append(holdings_detail_entry)

        except Exception as e:
            message = f"Holdings - {category_name.lower()} record creation failed. {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': 'Test', 'Message': message}, get_run_id())
            raise Exception(message)

    def create_statementdates_record(self, category_name, category_details, transaction):
        try:            
            # Initialize the dictionary
            statementdates_entry = {}

            detail_items = category_details.get("StatementDate", [])

            # Process Detail records
            for detail_item in detail_items:
                label, column, template, fields, condition = detail_item
                clear_template = "N"
                # Check conditions if specified
                if condition:
                    check_type = condition.get("check")
                    check_value = self.get_nested_value(transaction, condition["field"])

                    # Evaluate based on the specified condition
                    if check_type == "null" and check_value is None:
                        clear_template = "Y"  # Clear template if null
                    elif check_type == "greater" and check_value <= condition.get("value", 0):
                        clear_template = "Y"  # Clear template if not greater
                    elif check_type == "lesser" and check_value >= condition.get("value", 0):
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
                        statementdates_entry[column] = formatted_output
                    else:
                        statementdates_entry[column] = ""
                else:
                    statementdates_entry[column] = label

            return statementdates_entry
            # Append the entry to the detail record dictionary
            #if statementdates_entry:
                #globals()[f"{category_name}_record"] = {**statementdates_entry}

        except Exception as e:
            message = f"Statementdate record creation failed. {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': 'Test', 'Message': message}, get_run_id())
            raise Exception(message)

    def extract_records(self, advisor_partyids_list):
        try:            
            sql_account_query = self.generate_account_sql_query(advisor_partyids_list)

            extracted_account_data_rows = self.execute_account_sql_query(sql_account_query)
                
            # Copy extracted account data to a list. This list can be used multiple times for processing.
            extracted_account_data_rows_list = list(extracted_account_data_rows)
            account_column_names = extracted_account_data_rows.keys()
                
            # Create a dict using the column names and account data.
            accounts_list_for_advisor = [dict(zip(account_column_names, account_list)) for account_list in extracted_account_data_rows_list]

            #account_partyids = [account_party_list['partyid'] for account_party_list in extracted_account_data_rows_list]
            account_partyids = self.extract_account_partyids(extracted_account_data_rows_list)

            if self.ishouseholdneeded == 'Y':
                sql_account_query = self.generate_account_sql_query(account_partyids)
                extracted_household_account_data_rows = self.execute_account_sql_query(sql_account_query)
                extracted_household_account_data_rows_list = list(extracted_household_account_data_rows)

                if extracted_household_account_data_rows_list:
                    # Create a dict using the column names and account data.
                    accounts_list_for_master = [dict(zip(account_column_names, master_account_list)) for master_account_list in extracted_household_account_data_rows_list]
                    Household_account_partyids = [master_account_party_list['party_id'] for master_account_party_list in extracted_household_account_data_rows_list]
                    accounts_list = accounts_list_for_advisor + accounts_list_for_master
                    consolidated_partyids = account_partyids + Household_account_partyids                    

            else:
                accounts_list = accounts_list_for_advisor
                consolidated_partyids = account_partyids

            # Check if consolidated_partyids_str is empty
            if not consolidated_partyids:
                raise Exception("Account level retrieval failed since no account is associated with the financial advisor")

            consolidated_partyids_str = map(str,consolidated_partyids)

            # Define a function to handle None during sorting
            def custom_sort_with_currency(record):
                # Assign a rank based on currency, putting USD first
                currency_rank = 0 if record['transactionamountcurrency'] == "USD" else 1
                return (
                    record['partyid'],
                    currency_rank,
                    record['transactionamountcurrency'] or '',  # Replace None with an empty string
                    record['sectionheader'] or '',  # Replace None with an empty string
                )

            def process_transactions(party_id):
                # Generate and execute SQL query for transactions
                sql_transaction_query = self.generate_transaction_sql_query(party_id)
                extracted_transactions_data_rows = self.execute_transaction_sql_query(sql_transaction_query)
                extracted_transactions_rows_list = list(extracted_transactions_data_rows)
    
                transactions_column_names = extracted_transactions_data_rows.keys()
                transactions_consolidated_list = [dict(zip(transactions_column_names, extracted_transactions_row_list)) for extracted_transactions_row_list in extracted_transactions_rows_list]
    
                sorted_transactions = sorted(transactions_consolidated_list, key=custom_sort_with_currency)
    
                return sorted_transactions    
    
            def process_category_record(category_record, category_type):
                category_record["heading1"] = category_type
                category_record["heading2"] = currency
                category_record["dtl_section_and_subsection_name"] = pairs["Heading"]

                # Directly append each entry to the ROW list
                #for key,value,attrib,formatstr,cond in pairs["Detail"]:
                    #header = "DTL_" + value.replace("row", "HEADER") + "_TXT"
                    #category_record[header] = key
                
                return category_record

            output = []

            for partydata in accounts_list:
                #party_id = partydata['partyidentifier_value']
                partyid = partydata['party_id']
    
                # Initialize STMT_REC
                STMT_REC = {
                    "document": {}
                }    
            
                stmt_record_dict = copy.deepcopy(self.driver_layout[0]["stmt_record"])
                # Empty the detail_table list
                stmt_record_dict["detail_table"] = []

                stmtHeader = copy.deepcopy(self.driver_layout[0]["stmt_header"])
                searchKeys = copy.deepcopy(self.driver_layout[0]["search_keys"])
                userVariables = copy.deepcopy(self.driver_layout[0]["user_variables"])
                inserts = copy.deepcopy(self.driver_layout[0]["inserts"])
                mailingAddress = copy.deepcopy(self.driver_layout[0]["mailing_address"])
                repInfo = copy.deepcopy(self.driver_layout[0]["rep_info"])
                statementDates = copy.deepcopy(self.driver_layout[0]["statement_dates"])
 
                # Process transactions for this party
                transactions = process_transactions(partyid)
                
                # Further group transactions by currency for this party
                transactions_by_currency = groupby(transactions, key=lambda x: x['transactionamountcurrency'])

                # Initialize an output dictionary to hold currency-specific records
                currency_output = {}

                for currency, currency_transactions in transactions_by_currency:
                    currency_transactions = list(currency_transactions)          

                    if currency is not None and currency.strip() != "":
                        currency_output[currency] = copy.deepcopy(self.driver_layout[0]["stmt_record"]["detail_table"][0])
                        template = self.driver_layout[0].get("template", {}).get("Template", {})

                        detail_table_type_1 = self.driver_layout[0].get("template", {}).get("detail_table_type_1", {})
                        detail_table_type_2 = self.driver_layout[0].get("template", {}).get("detail_table_type_2", {})
                        detail_table_type_3 = self.driver_layout[0].get("template", {}).get("detail_table_type_3", {})
                        statement_dates_type_1 = self.driver_layout[0].get("template", {}).get("statement_dates_type_1", {})
                        
                        # Initialize both heading dictionaries
                        detail_table_type_1_headings_dict = {}
                        detail_table_type_1_data = {}
                        detail_table_type_1_record = {}

                        detail_table_type_2_headings_dict = {}
                        detail_table_type_2_data = {}
                        detail_table_type_2_record = {}

                        detail_table_type_3_headings_dict = {}
                        detail_table_type_3_data = {}
                        detail_table_type_3_record = {}

                        statement_dates_type_1_headings_dict = {}                        
                        statement_dates_type_1_data = {}
                        statement_dates_type_1_record = {}

                        # Process detail_table_type_1
                        if detail_table_type_1:
                            for category, details in detail_table_type_1.items():
                                detail_table_type_1_data[category] = {}
                                globals()[f"{category}_summary_record"] = copy.deepcopy(self.driver_layout[0]["detail_table_type_record"])

                                # Iterate through each item (Heading, Detail, Total) in details
                                for key, value in details.items():
                                    template_key = details.get(key, None)
                                    if template_key:
                                        # Use the get_nested_value function
                                        template_data = self.get_nested_value(template, template_key)
                                        if template_data is not None and template_data not in [None, "", []]:
                                            detail_table_type_1_data[category][key] = copy.deepcopy(template_data)
                                        else:
                                            detail_table_type_1_data[category][key] = value
                                            detail_table_type_1_headings_dict[value] = category  # Map heading to category name


                        # Process detail_table_type_2
                        if detail_table_type_2:
                            for category, details in detail_table_type_2.items():
                                detail_table_type_2_data[category] = {}
                                globals()[f"{category}_holdings_record"] = copy.deepcopy(self.driver_layout[0]["detail_table_type_record"])

                                # Iterate through each item (Heading, Detail, Total) in details
                                for key, value in details.items():
                                    template_key = details.get(key, None)
                                    if template_key:
                                        # Use the get_nested_value function
                                        template_data = self.get_nested_value(template, template_key)
                                        if template_data is not None and template_data not in [None, "", []]:
                                            detail_table_type_2_data[category][key] = copy.deepcopy(template_data)
                                        else:
                                            detail_table_type_2_data[category][key] = value
                                            detail_table_type_2_headings_dict[value] = category  # Map heading to category name

                        # Process detail_table_type_3
                        if detail_table_type_3:
                            for category, details in detail_table_type_3.items():
                                detail_table_type_3_data[category] = {}
                                globals()[f"{category}_transactions_record"] = copy.deepcopy(self.driver_layout[0]["detail_table_type_record"])

                                # Iterate through each item (Heading, Detail, Total) in details
                                for key, value in details.items():
                                    template_key = details.get(key, None)
                                    if template_key:
                                        # Use the get_nested_value function
                                        template_data = self.get_nested_value(template, template_key)
                                        if template_data is not None and template_data not in [None, "", []]:
                                            detail_table_type_3_data[category][key] = copy.deepcopy(template_data)
                                        else:
                                            detail_table_type_3_data[category][key] = value
                                            detail_table_type_3_headings_dict[value] = category  # Map heading to category name

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
                        transactions_by_category = groupby(currency_transactions, key=lambda x: (x['sectiontype'], x['sectionheader']))
                        #Once Abhishek is done with BRx changes, above line should be removed and below line should be included
                        #transactions_by_category = groupby(currency_transactions, key=lambda x: (x['transactionclassificationtype'], x['transactionflagtype']))

                        #for category, category_transactions in transactions_by_category:
                        for (sectiontype, sectionheader), category_transactions in transactions_by_category:
                            category_transactions = list(category_transactions)
                        
                            if sectiontype == 'Holdings':
                                for transaction in category_transactions:
                                    if transaction['sectionheader'] in detail_table_type_2_headings_dict:
                                        category = detail_table_type_2_headings_dict[transaction['sectionheader']] # Get the corresponding category name
                                        category_details = detail_table_type_2_data.get(category)
                                        self.create_holdings_record(category, category_details, transaction)

                            elif sectiontype == 'Activity':
                                for transaction in category_transactions:
                                    if transaction['sectionheader'] in detail_table_type_3_headings_dict:
                                        category = detail_table_type_3_headings_dict[transaction['sectionheader']] # Get the corresponding category name
                                        category_details = detail_table_type_3_data.get(category)
                                        self.create_transactions_record(category, category_details, transaction)

                            elif sectiontype == 'Summary':
                                for transaction in category_transactions:
                                    if transaction['sectionheader'] in detail_table_type_1_headings_dict:
                                        category = detail_table_type_1_headings_dict[transaction['sectionheader']] # Get the corresponding category name
                                        category_details = detail_table_type_1_data.get(category)
                                        self.create_summary_record(category, category_details, transaction)

                        # Optionally remove detail_table_type_3 if it also remains empty
                        for category, pairs in detail_table_type_1.items():
                            category_record = globals().get(f"{category}_summary_record")

                            if category_record and category_record.get("dtl_column_values", {}).get("row"):
                                category_record = process_category_record(category_record, "Summary")

                                currency_output[currency]["detail_table_type_1"].append(category_record)

                        if not currency_output[currency]["detail_table_type_1"]:
                            del currency_output[currency]["detail_table_type_1"]

                        for category, pairs in detail_table_type_2.items():
                            category_record = globals().get(f"{category}_holdings_record")

                            if category_record and category_record.get("dtl_column_values", {}).get("row"):
                                category_record = process_category_record(category_record, "Holdings")

                                currency_output[currency]["detail_table_type_2"].append(category_record)

                        # Optionally remove detail_table_type_3 if it also remains empty
                        if not currency_output[currency]["detail_table_type_2"]:
                            del currency_output[currency]["detail_table_type_2"]                                

                        for category, pairs in detail_table_type_3.items():
                            category_record = globals().get(f"{category}_transactions_record")

                            # Check for TradeActivities category - This logic is for Impact Statement
                            if category == "TradeActivities":
                                # Check if there are more than 2 records in the ROW
                                row_records = category_record.get("dtl_column_values", {}).get("row", [])

                                if len(row_records) <= 2:  # Less than or equal to 2 means we likely don't have TradeActivities. Only Opening Balance and closing Balance records are present.
                                    category_record["dtl_column_values"]["row"] = []  # Clear the ROW list

                            if category_record and category_record.get("dtl_column_values", {}).get("row"):
                                category_record = process_category_record(category_record, "Transactions")

                                currency_output[currency]["detail_table_type_3"].append(category_record)
                        
                        # Optionally remove detail_table_type_3 if it also remains empty
                        if not currency_output[currency]["detail_table_type_3"]:
                            del currency_output[currency]["detail_table_type_3"]
                    else:
                        if currency_transactions[0]["transactioncategory"] == "Header Date":
                            category = "StatementDates"
                            category_details = statement_dates_type_1_data.get(category)
                            statementdate_data = self.create_statementdates_record(category, category_details, currency_transactions[0])
                        else:
                            statementdate_data = {}

                for stmt_key, partydata_key in stmtHeader.items():
                    value = partydata.get(partydata_key, None)
                    stmt_record_dict[stmt_key] = value if value is not None else ''

                # Handle search_keys
                for search_key in searchKeys:
                    stmt_record_dict["search_keys"][search_key] = partydata.get(search_key, '')                

                # Handle inserts
                for insert_key in inserts:
                    stmt_record_dict["inserts"][insert_key] = partydata.get(insert_key, '')

                # Handle user_variables
                for user_var_key in userVariables:
                    stmt_record_dict["user_variables"][user_var_key] = partydata.get(user_var_key, '')

                # Append each address line from mailingAddress to stmt_record_dict
                for address in mailingAddress:
                    # Use the key from the address dict
                    key = next(iter(address))  # Get the first key dynamically
                    line_value = partydata.get(address[key], '')  # Use the dynamic key
                    if line_value:  # Check if line_value is not empty or null
                        stmt_record_dict["mailing_address"].append({key: line_value}) 

                # Append each address line from repInfo to stmt_record_dict
                for repInfoRec in repInfo:
                    # Use the key from the repInfoRec dict
                    key = next(iter(repInfoRec))  # Get the first key dynamically
                    line_value = partydata.get(repInfoRec[key], '')  # Use the dynamic key
                    if line_value:  # Check if line_value is not empty or null
                        stmt_record_dict["rep_info"].append({key: line_value}) 

                for key, value in statementDates.items():
                    line_value = statementdate_data.get(value, '')
                    if line_value:  # Check if line_value is not empty or null
                        stmt_record_dict["statement_dates"][key] = line_value 

                STMT_REC["document"] = stmt_record_dict.copy()

                for currency, records in currency_output.items():
                    if records.get("detail_table_type_1") or records.get("detail_table_type_2") or records.get("detail_table_type_3"):
                        STMT_REC["document"]["detail_table"].append(records)

                # Append party output to the list
                output.append(STMT_REC)
                                            
            # Function to find children of a given parent_id
            def find_children(parent_id):
                children = []
                for record in output:
                    stmt = record['document']
                    if stmt['parent_id'] == parent_id:
                        children.append(record)
                return children

            def create_presentment_statement_list(item):
                # Logic to add individual statements for presentment
                acct_num = item['document']['account_number']   # Either partyid or account number
                json_data = item
                #xmldata = xmltodict.unparse({'Wealth': item}, pretty=True, short_empty_elements=True)
                # Remove lines containing <?xml version="1.0" encoding="utf-8">, <root>, and </root>
                #filtered_xmldata = re.sub(r'<\?xml.*?\?>\n|<root>\n|</root>', '', xmldata)
                    
                driver_data = {
                    'partyid': acct_num,
                    #'xml_driver': filtered_xmldata,
                    'json_driver': json.dumps({"mailpiece": [json_data]}, cls=DecimalEncoder)
                }
                presentment_statement_list.append(driver_data)

            def create_print_statement_list(item):
                # Logic to add household and individual statements for print
                stmt = item['document']
                stmt_id = stmt['id']
                hh_indicator = stmt['hh_ind']
                if hh_indicator == 'HouseHoldMasterAccount':
                    acct_num = item['document']['account_number']   # Either partyid or account number
                    household_statement_list = []
                    children = find_children(stmt_id)
                    household_statement_list = [item] + children
                    sub_output = {}
                    for entry in household_statement_list:
                        for key,value in entry.items():
                            if key in sub_output:
                                sub_output[key].append(value)
                            else:
                                sub_output[key] = [value]
                    #household_xmldata = xmltodict.unparse({'Wealth': sub_output}, pretty=True, short_empty_elements=True)
                    #filtered_household_xmldata = re.sub(r'<\?xml.*?\?>\n|<root>\n|</root>', '', household_xmldata)
                    print_driver = {
                        'partyid': acct_num,
                        #'xml_driver': filtered_household_xmldata,
                        'json_driver': json.dumps({"mailpiece": [sub_output]}, cls=DecimalEncoder)
                    }
                    print_statement_list.append(print_driver)

                elif hh_indicator == 'AccountHolder' or hh_indicator == 'InterestedParty':
                    acct_num = item['document']['account_number']   # Either partyid or account number
                    #standalone_xmldata = xmltodict.unparse({'Wealth': item}, pretty=True, short_empty_elements=True)
                    # Remove lines containing <?xml version="1.0" encoding="utf-8">, <root>, and </root>
                    #filtered_standalone_xmldata = re.sub(r'<\?xml.*?\?>\n|<root>\n|</root>', '', standalone_xmldata)

                    print_driver = {
                        'partyid': acct_num,
                        #'xml_driver': filtered_standalone_xmldata,
                        'json_driver': json.dumps({"mailpiece": [item]}, cls=DecimalEncoder)
                    }
                    print_statement_list.append(print_driver)

            presentment_statement_list = []
            print_statement_list = []
                
            for item in output:

                if self.deliverytype == 'P':
                    # Logic to add household and individual statements for print
                    create_print_statement_list(item)

                elif self.deliverytype == 'A':
                    # Logic to add individual statements for presentment
                    create_presentment_statement_list(item)

                elif self.deliverytype == 'PA':
                    # Logic to add individual statements for presentment
                    create_presentment_statement_list(item)
                    # Logic to add household and individual statements for print
                    create_print_statement_list(item)

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
            splunk.log_message({'Status': 'failed', 'InputFileName': 'Test', 'Message': message}, get_run_id())
            raise Exception(message)

        except Exception as e:
            message = f"Account extraction process failed. {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': 'Test', 'Message': message}, get_run_id())
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
        advisor_partyids_list = dp.extract_advisor_partyids()
        dp.extract_records(advisor_partyids_list)

    except Exception as e:
        message = f"Account extraction process failed. {str(e)}"
        splunk.log_message({'Status': 'failed', 'InputFileName': 'Test', 'Message': message}, get_run_id())
        raise Exception(message)

if __name__ == '__main__':
    main()    
