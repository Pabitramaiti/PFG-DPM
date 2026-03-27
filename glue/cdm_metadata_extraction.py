import json, sys,boto3,os
from awsglue.utils import getResolvedOptions
from sqlalchemy import Table, Column, create_engine, text, Text, String, MetaData, Numeric, Integer, Float, BigInteger, JSON, insert, select, func, inspect, bindparam, ARRAY, and_, Boolean, Date, DateTime, Time, Enum, delete
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import splunk  # import the module from the s3 bucket
import json
from s3fs import S3FileSystem
import time
import re

# Initialize Boto3 clients
glue_client = boto3.client('glue')
s3 = boto3.client('s3')
ssm_client = boto3.client("secretsmanager")

# simplefilestorage to get data in s3 object in bytes
sfs = S3FileSystem()

def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:extract_advisor_from_cdm:" + account_id

def get_secret_values(secret_name):
    response = ssm_client.get_secret_value(SecretId = secret_name)
    secret_value = response['SecretString']
    json_secret_value = json.loads(secret_value)

    return json_secret_value

class DataPostgres:
    def __init__(self):
        self.conn = None
        self.engine = None
        self.tableinfo=json.loads(os.getenv("tableInfo"))
        self.region = self.tableinfo.get("region")
        self.dbkey=self.tableinfo.get("dbkey")
        self.dbname=self.tableinfo.get("dbname")
        self.schema=self.tableinfo.get("schema")        
        self.s3_bucket=os.getenv("s3_bucket")
        self.s3_wip=os.getenv("s3_wip")
        self.s3_accountoutputfile=os.getenv("s3_accountoutputfile")
        self.s3_outputfiletype=os.getenv("s3_outputfiletype")
        self.sql_queries = json.loads(os.getenv("sql_queries"))
        self.trigger_file_name = os.getenv("trigger_file_name")
        self.message = ""
        self.index = 1
        self.create_connection()
        self.execution_time = str(time.time())
    
    def create_connection(self):
        
        try:
            #creates engine connection
            ssm = boto3.client('ssm', self.region)
            ssmdbkey = self.dbkey
            secret_name = ssm.get_parameter(Name=ssmdbkey, WithDecryption=True)
            secret_name=secret_name['Parameter']['Value']
            host = get_secret_values(secret_name)
            #self.engine = create_engine(f"postgresql://{host['username']}:{host['password']}@{host['host']}:{host['port']}/{host['dbname']}", future=True)
            self.engine = create_engine(f"postgresql://{host['username']}:{host['password']}@{host['host']}:{host['port']}/{self.dbname}", future=True)

            self.conn = self.engine.connect()
            
            result = splunk.log_message({'Status': 'success', 'InputFileName': self.trigger_file_name,'Message': 'Connection with Aurora Postgres Cluster is established'},  get_run_id())
            
        except Exception as e:
            self.message = "Connection failed due to error "+ str(e)

            result = splunk.log_message({
                'InputFileName': self.trigger_file_name,
                'Status':'failed',
                'Message': self.message},  get_run_id())

            raise Exception(self.message)

    # Function to dynamically fetch the SQLAlchemy type based on the type string
    def get_sqlalchemy_type(self, type_string):
        # Map type strings to SQLAlchemy types
        type_mapping = {
            "String": String,      # Variable-length string
            "Text": Text,          # Variable-length string (larger than String)
            "Integer": Integer,    # Integer
            "Float": Float,        # Floating-point number
            "Numeric": Numeric,    # Fixed-point number
            "Boolean": Boolean,    # Boolean (True/False)
            "Date": Date,          # Date
            "DateTime": DateTime,  # Date and time
            "Time": Time,          # Time
            "JSON": JSON,          # JSON data
            "Enum": Enum           # Enumerated type
            # Add more types as needed
        }

        if type_string in type_mapping:
            return type_mapping[type_string]
        else:
            raise ValueError(f"Unsupported column type: {type_string}")

    def get_accounts_from_trigger(self):
        try:
            # Convert S3 file path to S3 URI format
            if not self.trigger_file_name.startswith("s3://"):
                trigger_file_uri = "s3://" + self.trigger_file_name
            else:
                trigger_file_uri = self.trigger_file_name

            # Read the trigger file
            with sfs.open(trigger_file_uri, 'r') as f:
                content = f.read().strip()

            # Handle empty file — valid case
            if not content:
                # raise ValueError("No accountnumbers found in trigger file.")
                splunk.log_message({
                    'Status': 'success',
                    'InputFileName': self.trigger_file_name,
                    'Message': 'Trigger file is empty — proceeding without account filters.'
                }, get_run_id())
                return []

            # Parse JSON content if not empty
            trigger_content = json.loads(content)

            account_numbers = trigger_content.get("accountnumbers", [])

            return account_numbers

        except Exception as e:
            message = f"Failed to read or parse trigger file: {str(e)}"
            splunk.log_message({
                'Status': 'failed',
                'InputFileName': self.trigger_file_name,
                'Message': message
            }, get_run_id())
            raise Exception(message)

    def extract_advisor_records(self):
        try:
            sql_queries = json.loads(os.getenv("sql_queries"))
            sql_account_query = self.sql_queries.get("metadata_query", "")
            create_base_historical_tables_query = self.sql_queries.get("create_base_historical_tables", "")
            limit = self.sql_queries.get("limit", "")

            # Read account numbers from trigger file
            account_numbers = self.get_accounts_from_trigger()
            # Format for SQL ARRAY — e.g. ARRAY['1AA00095','1AA08011']::TEXT[]
            if account_numbers:
                formatted_account_numbers = ",".join([f"'{acc}'" for acc in account_numbers])
                sql_account_query = sql_account_query.format(account_numbers=f"ARRAY[{formatted_account_numbers}]")
            else:
                sql_account_query = sql_account_query.format(account_numbers='ARRAY[]')
            
            # Get columns list
            if sql_queries.get("columns_list"):
                column_list = [(column_list_value.strip()) for column_list_value in sql_queries["columns_list"].split(",")]
            
            # Define a function to process the extracted data rows and limit the values
            def process_data_rows(extracted_data_rows, column_list, limit):
                conditions_map = {column_name: [] for column_name in column_list}
    
                # Process each row and populate the conditions_map
                for row in extracted_data_rows:
                    for column_name in column_list:
                        conditions_map[column_name].append(row[column_name])
    
                # Create lists with a limit of n values per list for each key
                result_list = []
                keys = conditions_map.keys()
    
                # Loop through the conditions_map and create result_dict for each chunk of `limit` size
                for i in range(0, len(next(iter(conditions_map.values()))), limit):
                    result_dict = {key: conditions_map[key][i:i + limit] for key in keys}
                    result_list.append(result_dict)
    
                return result_list
            
            # Execute the SQL command to extract advisor data
            with self.engine.begin() as connection:
                connection.execute(text(f"SET search_path TO {self.schema}"))
                connection.execute(text(create_base_historical_tables_query))

                extracted_account_metadata = connection.execute(text(sql_account_query))
                # Fetch all the rows
                extracted_account_metadata_rows = extracted_account_metadata.fetchall()

            limited_accounts_list = process_data_rows(extracted_account_metadata_rows, column_list, limit)

            # Extract UUID from the s3_wip path
            uuid_part = self.s3_wip.split("/")[-1].replace("-", "_")
            sql_create_historical_table_query = self.sql_queries.get("create_historical_table", "")
            sql_create_staging_table_query = self.sql_queries.get("create_staging_table", "")

            with self.engine.begin() as connection:
                connection.execute(text(f"SET search_path TO {self.schema}"))
                # Add historical_table_name to each dict
                for i, item in enumerate(limited_accounts_list, start=1):
                    if sql_create_historical_table_query:
                        item['historical_table_name'] = f"historical_table_{uuid_part}_{i}"
                        formatted_historical_sql = sql_create_historical_table_query.format(historicaltablename=item['historical_table_name'], schemaname=self.schema)
                        connection.execute(text(formatted_historical_sql))
                    
                    if sql_create_staging_table_query:
                        item['staging_table_name'] = f"staging_table_{uuid_part}_{i}"
                        formatted_staging_sql = sql_create_staging_table_query.format(stagingtablename=item['staging_table_name'], schemaname=self.schema)
                        connection.execute(text(formatted_staging_sql))

                return limited_accounts_list
            
        except SQLAlchemyError as e:
            # Handle generic SQLAlchemy errors
            message = f"Account extraction process failed due to SQLAlchemy error. {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': self.trigger_file_name, 'Message': message}, get_run_id())
            raise Exception(message)

    def write_output_file(self, accounts_list):
        try:

            if not accounts_list: 
                raise ValueError(f"Write operation to S3 bucket {self.s3_bucket} failed with error: Contents to output file is empty.")
        
            if accounts_list:
                for idy, account in enumerate(accounts_list):
                    # Construct the output file URI
                    account_output_file = f"{self.s3_wip}/{self.s3_accountoutputfile}_{idy + 1}.{self.s3_outputfiletype}"
                    account_output_file_uri = f"s3://{self.s3_bucket}/{account_output_file}"

                    # Write each account to a separate file
                    with sfs.open(account_output_file_uri, 'w') as out_file:
                        out_file.write(json.dumps([account], indent=4))

        except FileNotFoundError as e:
            message = f"Write operation for the file in S3 bucket {self.s3_bucket} failed with not found error."
            splunk.log_message({'Status': 'failed', 'InputFileName': self.trigger_file_name, 'Message': message}, get_run_id())
            raise Exception(message)

        except OSError as e:
            message = f"Write operation for the file in S3 bucket {self.s3_bucket} failed with error: {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': self.trigger_file_name, 'Message': message}, get_run_id())
            raise Exception(message)

        except Exception as e:
            message = f"Write Operation process failed. {str(e)}"
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
        accounts_list = dp.extract_advisor_records()

    except Exception as e:
        message = f"Advisor extraction process failed. {str(e)}"
        splunk.log_message({'Status': 'failed', 'InputFileName': 'Test', 'Message': message}, get_run_id())
        raise Exception(message)

    dp.write_output_file(accounts_list)

if __name__ == '__main__':
    main()