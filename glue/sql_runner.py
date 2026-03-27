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
        self.engine = None
        self.tableinfo=json.loads(os.getenv("tableInfo"))
        self.region = self.tableinfo.get("region")
        self.dbkey=self.tableinfo.get("dbkey")
        self.dbname=self.tableinfo.get("dbname")
        self.schema=self.tableinfo.get("schema")        
        self.s3_sql_folder = os.getenv("s3_sql_folder")
        self.s3_bucket = os.getenv("s3_bucket")
        self.message = ""
        self.create_connection()
    
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
            
            result = splunk.log_message({'Status': 'success', 'InputFileName': 'N/A','Message': 'Connection with Aurora Postgres Cluster is established'},  get_run_id())
            
        except Exception as e:
            self.message = "Connection failed due to error "+ str(e)

            result = splunk.log_message({
                'InputFileName': 'N/A',
                'Status':'failed',
                'Message': self.message},  get_run_id())

            raise Exception(self.message)

    def get_sql_files_in_folder(self):
        s3_path = f"s3://{self.s3_bucket}/{self.s3_sql_folder}"

        files = sfs.ls(s3_path)
        sql_files = sorted([f for f in files if f.lower().endswith(".sql")])
        return sql_files

    def execute_sql_file(self, sql_file):
        """Read and execute a single SQL file"""
        try:
            function_name = os.path.basename(sql_file).replace(".sql", "")
            #drop_sql = f"DROP FUNCTION IF EXISTS {function_name}(TEXT[]);"
            drop_sql = f"DROP FUNCTION IF EXISTS {function_name};"
            with sfs.open(sql_file, 'r') as f:
                sql_content = f.read().strip()
            if not sql_content:
                splunk.log_message({
                    'Status': 'success',
                    'InputFileName': sql_file,
                    'Message': 'SQL file is empty, skipping.'
                }, get_run_id())
                return

            with self.engine.begin() as conn:
                conn.execute(text(f"SET search_path TO {self.schema}"))
                conn.execute(text(drop_sql))
                conn.execute(text(sql_content))

            splunk.log_message({
                'Status': 'success',
                'InputFileName': sql_file,
                'Message': 'SQL executed successfully.'
            }, get_run_id())

        except SQLAlchemyError as e:
            message = f"SQLAlchemy error executing {sql_file}: {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': sql_file, 'Message': message}, get_run_id())
            raise

        except Exception as e:
            message = f"Error executing {sql_file}: {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': sql_file, 'Message': message}, get_run_id())
            raise

    def execute_all_sql_files(self):
        sql_files = self.get_sql_files_in_folder()
        if not sql_files:
            message = f"No SQL files found in folder {self.s3_sql_folder}"
            splunk.log_message({'Status': 'failed', 'InputFileName': self.s3_sql_folder, 'Message': message}, get_run_id())
            raise Exception(message)

        for sql_file in sql_files:
            self.execute_sql_file(sql_file)        

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
        dp.execute_all_sql_files()

    except Exception as e:
        message = f"Advisor extraction process failed. {str(e)}"
        splunk.log_message({'Status': 'failed', 'InputFileName': 'Test', 'Message': message}, get_run_id())
        raise Exception(message)

if __name__ == '__main__':
    main()