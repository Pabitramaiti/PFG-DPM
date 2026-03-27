import sys
import boto3
import json
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
from awsglue.utils import getResolvedOptions
import splunk
import re
import pandas as pd
import ast
import time
from sqlalchemy import *
import psycopg2

# boto3 client
client = boto3.client("secretsmanager")

#####################################################################################
def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return f"arn:dpm:glue:{program_name}:{account_id}"

#####################################################################################
# define function to load input parameters for glue
def get_configs():
    args = getResolvedOptions(sys.argv, [
        'client_name', 'data_base', 'schemas', 'exclude_tables', 'ssmdbkey'
    ])
    return args

##############################################################################
def get_db_values(secret_name):
    try:
        response = client.get_secret_value(SecretId=secret_name)
        os.environ['json_db_value'] = response['SecretString']
        message = f'{program_name} secret values retrieved successfully'
        splunk.log_message({'Status': 'SUCCESS', 'Message': message}, get_run_id())
    except Exception as e:
        message = f'{program_name} error retrieving secret values : {str(e)}'
        splunk.log_message({'Status': 'ERROR', 'Message': message}, get_run_id())
        raise e


def truncate_tables(ssmdbkey, client_name, data_base, schemas, exclude_tables=None):
    #database = data_base
    ssm = boto3.client('ssm', region_name='us-east-1')

    # Retrieve DB secret name from SSM
    param_name = ssm.get_parameter(Name=ssmdbkey, WithDecryption=True)
    param_value = param_name['Parameter']['Value']

    # Load DB credentials from Secrets Manager
    get_db_values(param_value)
    host = json.loads(os.getenv('json_db_value'))

    try:
        connection = psycopg2.connect(
            user=host['username'],
            password=host['password'],
            host=host['host'],
            port=host['port'],
            database=data_base
        )
        cursor = connection.cursor()

        # Call the Postgres function
        cursor.callproc(
            f'{schemas}.{client_name}_truncate_schema',
            [client_name, data_base, exclude_tables if exclude_tables else None]
        )

        result = cursor.fetchone()
        if result:
            return_text = result[0]   # The TEXT returned by the function
        connection.commit()

        splunk.log_message({'Status': 'SUCCESS', 'Message': return_text}, get_run_id())
        
        message = f'{program_name} executed truncate_schema with result: {result}'
        splunk.log_message({'Status': 'SUCCESS', 'Message': message}, get_run_id())

    except Exception as e:
        message = f'{program_name} failed to truncate tables for client {client_name}: {str(e)}'
        splunk.log_message({'Status': 'ERROR', 'Message': message}, get_run_id())
        raise ValueError(message)

    finally:
        if connection:
            cursor.close()
            connection.close()

#######################################################################################

if __name__ == '__main__':

    today_time = datetime.now()
    program = os.path.basename(sys.argv[0])
    program_name = program.split('.')[0]

    iparm = get_configs()
    ssmdbkey = iparm['ssmdbkey']
    client_name = iparm['client_name']
    data_base = iparm['data_base']
    schemas = iparm['schemas']
    exclude_tables = iparm['exclude_tables']

    if not data_base or not client_name or not schemas or not ssmdbkey:
        message = f"{program_name} failed at missing required parameters !!!"
        splunk.log_message({'Status': 'ERROR', 'Message': message}, get_run_id())
        raise ValueError(message)

    message = f"Starting {program_name} to truncate tables for {client_name} at {today_time} !!!"
    splunk.log_message({'Status': 'Starting', 'Message': message}, get_run_id())

    truncate_tables(ssmdbkey, client_name, data_base, schemas, exclude_tables) #program_name

    message = f'Finished {program_name} processing at {datetime.now()} !!!'
    splunk.log_message({'Status': 'Finished', 'Message': message}, get_run_id())
