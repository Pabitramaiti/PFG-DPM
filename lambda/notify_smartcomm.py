import json
import boto3
import re
import os
import datetime
import inspect
import datetime
from dpm_splunk_logger_py import splunk
from dpmdev_di_layer_common_functions import client_config
from dpmdev_di_layer_common_functions import functions

table_name = os.environ.get('CONFIG_TABLE_DYNAMO')

# Initialize AWS clients
stepfunctions = boto3.client("stepfunctions")


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


def lambda_handler(event, context):
    
    try:
        curr_fn = inspect.currentframe().f_code.co_name
        body_data = json.loads(event["Records"][0]["body"])
        fileName = body_data["Records"][0]["s3"]["object"]["key"]
        filename_split = fileName.split('/')
        if len(filename_split) == 2:
            system_name = os.environ.get('SYSTEM')
        else:
            system_name =  filename_split[0]
        splunk.log_message({'Status': 'success', 'InputFileName': fileName, "FunctionCalled": curr_fn,
                            'Message': f'The SQS message is recived from the Transmission and Lambda  function is invoked.'}, context)
    
        file_name_done = fileName + ".transmission.done"
        transaction_db_file_name = fileName
        step_function = os.environ.get('SM_ARN_INVOKESMARTCOMM')
        config = functions.retrieve_config_data(client_config.ClientConfig(table_name, system_name, file_name_done))
        configdata_json={}
        for configdata in config["filesToProcess"]:
            if configdata["filename"].endswith(".done"):
                configdata_json = configdata["StepFunction"][0]["sfParams"]
                timestampinminutes = configdata["StepFunction"][0]["sfStepsToDone"]["invokeSmartComm"]["timestampinminutes"]
                createFileMonitorFlag=configdata["StepFunction"][0]["sfStepsToDone"]["invokeSmartComm"]["createFileMonitorFlag"]
                fileExpiryDurationInMinutes=configdata["StepFunction"][0]["sfStepsToDone"]["invokeSmartComm"]["fileExpiryDurationInMinutes"]

                nextstepname = configdata["StepFunction"][0]["sfStepsToDone"]["invokeSmartComm"]["nextstepname"]
                if 'transFileExtension' in configdata["StepFunction"][0]["sfStepsToDone"]["invokeSmartComm"]:
                    transaction_db_file_name = fileName.rsplit('.', 1)[0] + configdata["StepFunction"][0]["sfStepsToDone"]["invokeSmartComm"]['transFileExtension']
                break
        input_params = {
            "fileName":  fileName.rsplit('/', 1)[-1],
            "configdata_json":configdata_json,
            "system_name":system_name,
            "StatePayload": {
                "fileExpiryDurationInMinutes":fileExpiryDurationInMinutes,
                "createFileMonitorFlag":createFileMonitorFlag,
                "nextstepname":nextstepname,
                "timestampinminutes":timestampinminutes,
                "name": "invokesmartcomm",
                "fileName": transaction_db_file_name.rsplit('/', 1)[-1],
                "fileId":  transaction_db_file_name.rsplit('/', 1)[-1],
                "isRejected": False,
                "documentType": "",
                "transactionId": "placeholder-from-smart-comm-api",
                "startDateTime": get_transaction_start_time(),
                "endDateTime": "",
                "clientName": config["clientName"],
                "stepFunctions": [{"arnStepName": "invokesmartcomm"}],
                "clientID": config["clientId"],
                "arnStepName": "sfkey",
                "sfInstance": "",
                "sfStatus": "success",
                "sfLog": "",
                "sfStartTime": "",
                "sfEndTime": "",
                "sfInputFile": "",
                "sfOutputFile": "",
                "fileLog": "",
                "fileStatus": "success",
                "initStep": False,
                "endStep": True,
                "inputPath": config['inputPath']
            }
        }
        response = stepfunctions.start_execution(
            stateMachineArn=step_function,
            input=json.dumps(input_params) if input_params else "{}"  # Pass input data as JSON string
        )
        execution_arn = response.get('executionArn')
        status_code = response.get('ResponseMetadata', {}).get('HTTPStatusCode')
        splunk.log_message({'Status': 'success', 'InputFileName': fileName, "FunctionCalled": curr_fn,
                            'Message': f'{context.function_name} Lambda function invoked.'},
                           context)

    except  Exception as e:
        splunk.log_message({'Status': 'Failed', 'InputFileName': fileName, "FunctionCalled": curr_fn,
                            'Message': f'{context.function_name} {e} Lambda function invoked.'},
                          context)
        raise e

    return {
        'statusCode': 200,
        'object_key': file_name_done
    }
