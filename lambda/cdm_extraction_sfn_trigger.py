import boto3
import json
import re
import os
import datetime
from dpm_splunk_logger_py import splunk

# Access environment variables
track_records = os.getenv("TRACK_RECORDS")
config_table = os.getenv("CONFIG_TABLE_DYNAMO")

# Initialize AWS clients
stepfunctions = boto3.client("stepfunctions")
dynamodb = boto3.client('dynamodb')
s3_client = boto3.client('s3')
Lambda = boto3.client('lambda')


def lambda_handler(event, context):
    try:

        splunk.log_message({'Message': "Initial Lambda triggered successfully"}, context)
        transaction_start_time = get_transaction_start_time()

        # retrieve config data for client from dynamodb
        client_name = "impact"
        client_configdata_json = retrieve_config_data(config_table, client_name)
        splunk.log_message({"client_configdata_json": client_configdata_json, "Status": "logging"}, context)

        if type(client_configdata_json) is not dict:
            message = "failed due to configuration not found for the client: " + client_name
            raise ValueError(message)

        # extract file information from config
        print("client_configdata_json", client_configdata_json)

        sf_info = get_sf_info(client_configdata_json)

        if not sf_info:
            message = "failed since step Functions configuration not found for the client: " + client_name
            raise ValueError(message)

        # trigger stepfunction
        result = execute_step_functions(sf_info, context, transaction_start_time, client_name)

        if result == 200:
            splunk.log_message(
                {'Message': "Stepfunction triggered successfully", "FileName": client_name, "Status": "success"}, context)

        return result

    except Exception as e:
        splunk.log_message({'Message': str(e), "FileName": client_name, "Status": "failed"}, context)
        raise e

def execute_step_functions(sf_info, context, transaction_start_time, client_name):
    '''
    main logic which triggers the specific stepfunction
    from config
    '''
    try:
        step_functions = sf_info['StepFunction']
        init_sf_key = ''

        if not step_functions:
            raise ValueError("failed due to step functions configuration not found")

        flags = {
            'bucket': "",
            'fileId': "",
            'isRejected': "",
            'key': "",
            'fileName': "",
            'documentType': "",
            'destinationBucketName': "",
            'transactionId': context.aws_request_id,
            'startDateTime': transaction_start_time,
            'endDateTime': "",
            'clientName': client_name,
            "stepFunctions": [],
            'clientID': sf_info['clientId'],
            'trackrecords': track_records,
            'decrypt_flag': "",
            'uncompress_flag': "",
            "arnStepName": "",
            "sfInstance": "",
            "sfStatus": "",
            "sfStartTime": "",
            "sfEndTime": "",
            "sfInputFile": "",
            "sfOutputFile": ""
        }

        flags['archivalFile'] = ""
        flags['archival_source_key'] = ""
        if sf_info['tableInfo']:
            flags['tableInfo'] = sf_info['tableInfo']

        for sf_step in step_functions:
            sf_step_name = sf_step['name']
            sf_order = sf_step['sfKey']

            splunk.log_message(
                {'Message': "Stepfunction in progress is - sf_order", "Stepfunction": sf_order, "Status": "pending"},
                context)

            flags['stepFunctions'].append({'arnStepName': sf_step_name})

            if sf_order == "SM_ARN_ADVISOREXTRACTION":
                #splunk.log_message({'Message': "sf_order", "sf_order": sf_order, "Status": "logging"}, context)
                #splunk.log_message(
                    #{'Message': "sfStepsToDone", "sfStepsToDone": sf_step["sfStepsToDone"], "Status": "logging"},
                    #context)
                sf_step['sfOutputFile'] = ''
                if 'createadvisorlist' in sf_step['sfStepsToDone']:
                    sfStepsToDone = sf_step["sfStepsToDone"]
                    createadvisorlist = sfStepsToDone["createadvisorlist"]
                    advisor = createadvisorlist["advisor"]
                    columns = createadvisorlist["columns"]
                    #splunk.log_message(
                        #{'Message': "createadvisorlist", "createadvisorlist": createadvisorlist, "Status": "logging"}, context)
                    createadvisorlist['wipPath'] = createadvisorlist['wipPath'] + '/' + flags['transactionId']
                    flags['bucket'] = createadvisorlist['bucket']
                    flags['wipPath'] = createadvisorlist['wipPath']
                    flags['nextsfInputFile'] = createadvisorlist['outputFile']
   
                    createadvisorlist['advisor'] = json.dumps(advisor)
                    createadvisorlist['columns'] = json.dumps(columns)
                    #splunk.log_message(
                        #{'Message': "createadvisorlist", "sf_step": sf_step, "Status": "logging"}, context)

            if sf_order == "SM_ARN_ACCOUNTEXTRACTION":
                #splunk.log_message({'Message': "sf_order", "sf_order": sf_order, "Status": "logging"}, context)
                #splunk.log_message(
                    #{'Message': "sfStepsToDone", "sfStepsToDone": sf_step["sfStepsToDone"], "Status": "logging"},
                    #context)
                sf_step['sfOutputFile'] = ''
                if 'extractaccounts' in sf_step['sfStepsToDone']:
                    sfStepsToDone = sf_step["sfStepsToDone"]
                    extractaccounts = sfStepsToDone["extractaccounts"]
                    account = extractaccounts["account"]
                    transaction = extractaccounts["transaction"]
                    driverlayout = extractaccounts["driverlayout"]
                    #splunk.log_message(
                        #{'Message': "extractaccounts", "extractaccounts": extractaccounts, "Status": "logging"}, context)
                    #sf_step['sfOutputFile'] = extractaccounts['bucket_name'] + '/' + sf_step['sfStepsToDone']['extractaccounts']['wipPath'] + '/' + \
                                              #flags['transactionId'] + '/' + extractaccounts['output_filename']
                    extractaccounts['account'] = json.dumps(account)
                    extractaccounts['transaction'] = json.dumps(transaction)
                    extractaccounts['driverlayout'] = json.dumps(driverlayout)
                    extractaccounts['bucket'] = flags['bucket']
                    extractaccounts['inputFile'] = flags['wipPath'] + '/' + flags['nextsfInputFile']
                    flags['nextsfInputFile'] = ''
                    #splunk.log_message(
                        #{'Message': "extractaccounts", "sf_step": sf_step, "Status": "logging"}, context)

            if sf_order == "SM_ARN_DRIVERCREATION":
                #splunk.log_message({'Message': "sf_order", "sf_order": sf_order, "Status": "logging"}, context)
                #splunk.log_message(
                    #{'Message': "sfStepsToDone", "sfStepsToDone": sf_step["sfStepsToDone"], "Status": "logging"},
                    #context)
                if 'drivercreation' in sf_step['sfStepsToDone']:
                    sfStepsToDone = sf_step["sfStepsToDone"]
                    drivercreation = sfStepsToDone["drivercreation"]
                    extraction = drivercreation["extraction"]
                    #splunk.log_message(
                        #{'Message': "extractaccounts", "extractaccounts": extractaccounts, "Status": "logging"}, context)
                    #sf_step['sfOutputFile'] = extractaccounts['bucket_name'] + '/' + sf_step['sfStepsToDone']['extractaccounts']['wipPath'] + '/' + \
                                              #flags['transactionId'] + '/' + extractaccounts['output_filename']
                    drivercreation['extraction'] = json.dumps(extraction)
                    drivercreation["wipPath"] = flags['wipPath']
                    drivercreation['bucket'] = flags['bucket']
                    #splunk.log_message(
                        #{'Message': "extractaccounts", "sf_step": sf_step, "Status": "logging"}, context)

            if 'order' in sf_step and sf_step['order'] != "" and sf_step['order'] == "1":
                init_sf_key = os.getenv(sf_step['sfKey'])

            if 'sfKey' in sf_step and sf_step['sfKey'] != "":
                sf_step['sfKey'] = os.getenv(sf_step['sfKey'])
                # flags["stepFunctions"].append({"sfName": sf_step['sfKey']})

            if 'sfNextKey' in sf_step and sf_step['sfNextKey'] != "":
                sf_step['sfNextKey'] = os.getenv(sf_step['sfNextKey'])

            sf_steps_to_done = sf_step
            flags[sf_order] = sf_steps_to_done
            flags[sf_order]["sf_step_name"] = sf_step_name

        sf_input = {"StatePayload": flags}
        print("sf_input", sf_input)

        STATE_MACHINE_ARN = init_sf_key

        # triggering stepfunction
        sf_response = stepfunctions.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            input=json.dumps(sf_input)
        )

        execution_arn = sf_response.get('executionArn')
        status_code = sf_response.get('ResponseMetadata', {}).get('HTTPStatusCode')

        return status_code

    except Exception as e:
        raise ValueError("Failed to execute Step Functions: " + str(e))

def get_sf_info(config_data):
    '''
    function to get the details of the file from config
    '''
    result = {}
    try:
        clientId = config_data['clientId']
        tableInfo = config_data['tableInfo']
        file_config = config_data.get('filesToProcess', [])[0]
        # new object with StepFunction, sf_key, sf_key_next, and fileType
        result = {
            'clientId': clientId,
            'StepFunction': file_config['StepFunction']
        }

        if tableInfo:  # Check if tableInfo is not empty or None
            result['tableInfo'] = tableInfo
        
        return result
    except Exception as e:
        return result

def retrieve_config_data(config_table, client_name):
    try:
        dynamodb_response = dynamodb.get_item(
            TableName=config_table,
            Key={"clientName": {"S": client_name}}
        )
        config_data = json.loads(dynamodb_response.get("Item", {}).get("clientConfig", {}).get("S", ""))

        return config_data

    except Exception as e:
        raise ValueError("Failed to retrieve config data from DynamoDB: " + str(e))

def get_transaction_start_time():
    '''
    function to generete the transaction start time
    '''
    try:
        startDateTime = datetime.datetime.now()
        startDateTimeStr = startDateTime.strftime("%Y-%m-%d %H:%M:%S:%f")
        return startDateTimeStr
    except Exception as e:
        raise ValueError("failed to get transaction start time: " + str(e))
