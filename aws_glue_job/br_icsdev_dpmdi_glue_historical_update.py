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
        'data_base', 'schemas', 'historical', 'historical_pre', 'ssmdbkey'
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


###################################################################
def historical_update(ssmdbkey, data_base, schemas, input_arg1, input_arg2):
    database = data_base
    ssm = boto3.client('ssm', region_name='us-east-1')

    param_name = ssm.get_parameter(Name=ssmdbkey, WithDecryption=True)
    param_value = param_name['Parameter']['Value']

    get_db_values(param_value)
    host = json.loads(os.getenv('json_db_value'))
    # Connect to PostgreSQL database
    try:
        connection = psycopg2.connect(
            user=host['username'],
            password=host['password'],
            host=host['host'],
            port=host['port'],
            database=database
        )

        cursor = connection.cursor()
        # Execute the HISTORIAL_UPDATE function
        '''
        schemas = 'AUX'
        input_arg1 = 'HISTORICAL_BY_SECID'
        input_arg2 = 'HISTORICAL_BY_SECID_2025_01'
        '''
        input_arg2 = input_arg2.removesuffix(".trigger")
        # Using psycopg2 to call a PostgreSQL function
        cursor.callproc(f'"{schemas}"."HISTORICAL_UPDATE"', [schemas, input_arg1, input_arg2])
        pgadmin_result = cursor.fetchone()
        pgadmin_message = pgadmin_result[0]
        connection.commit()
        #print(f"This is pgadmin message -->>> {pgadmin_message}")
        if f"{input_arg2} has been processed before" in pgadmin_message:
            message = f'Error!!!  {input_arg2} has been processed before'
            splunk.log_message({'Status': 'ERROR', 'Message': message}, get_run_id())
            #print(f"{message}")
            raise ValueError(message)
        else:
            message = f'{program_name} process {input_arg2} with return message from pgAdmin {pgadmin_message}'
            splunk.log_message({'Status': 'SUCCESS', 'Message': message}, get_run_id())
            #print(f"{message}")

    except Exception as e:
        message = f'{program_name} failed to process {input_arg2}  at select "{schemas}"."HISTORICAL_UPDATE" : {str(e)}'
        splunk.log_message({'Status': 'ERROR', 'Message': message}, get_run_id())
        raise ValueError(message)
    finally:
        # closing database connection
        if connection:
            cursor.close()
            connection.close()


#######################################################################################

if __name__ == '__main__':

    today_time = datetime.now()
    program = os.path.basename(sys.argv[0])
    program_name = program.split('.')[0]

    ##   ssmdbkey  /dpmtest/dev/database-secret
    iparm = get_configs()
    data_base = iparm['data_base']
    schemas = iparm['schemas']
    historical = iparm['historical']
    historical_pre = iparm['historical_pre']
    ssmdbkey = iparm['ssmdbkey']

    if not data_base or not schemas or not historical or not historical_pre or not ssmdbkey:
        message = f"{program_name} failed at missing required parameters !!!"
        splunk.log_message({'Status': 'ERROR', 'Message': message}, get_run_id())
        raise ValueError(message)

    message = f"Starting {program_name} to process {historical_pre} at {today_time} !!!"
    splunk.log_message({'Status': 'Starting', 'Message': message}, get_run_id())
    ################################################################
    historical_update(ssmdbkey, data_base, schemas, historical, historical_pre)
    ################################################################
    message = f'Finished {program_name} processing {historical_pre} at {datetime.now()} !!!'
    splunk.log_message({'Status': 'Starting', 'Message': message}, get_run_id())