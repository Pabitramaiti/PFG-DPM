import json
import boto3
import os
import inspect
import requests
from requests_oauthlib import OAuth1
from dpm_splunk_logger_py import splunk
from dpmdev_di_layer_common_functions import client_config
from dpmdev_di_layer_common_functions import functions

secret_name = os.environ.get("SECRET_NAME")
table_name = os.environ.get("DDBCONFIGTBL")
system = os.environ.get("SYSTEM")
env = os.environ.get("SDLC_ENV")

client = boto3.client('secretsmanager')
dynamodb = boto3.client("dynamodb")


def lambda_handler(event, context):
    global secret_name
    curr_fn = inspect.currentframe().f_code.co_name
    extract_event_setenviron(event, context)    
    object_key = os.environ.get("fileName")
    system_name = os.environ.get("system_name")
    transactionDataType=""
    # print(f"system_name : {env} : {system_name}")
    api_params = json.loads(os.environ.get("configdata_json"))
    splunk.log_message({'Status': 'success', 'InputFileName': object_key, "FunctionCalled": curr_fn,
                        'Message': f'{context.function_name} Lambda function invoked.'},
                       context)
    if "json" in object_key.lower():
        transactionDataType="application/json"
    if not system_name in secret_name:
        secret_name = f"{env}/{system_name}"
        # print(f"Updated secret_name : {secret_name}")
    # connect lambda to secrets manager
    response = client.get_secret_value(SecretId=secret_name)
    secret_string = response['SecretString']
    secret_dict = json.loads(secret_string)

    # Define the API URL, Consumer Key and Consumer Secret
    consumer_key = secret_dict["consumer_key"]
    consumer_secret = secret_dict["consumer_secret"]

    api_url = secret_dict["api_url"]
    # configdata_json = {}
    # configdata = functions.retrieve_config_data(client_config.ClientConfig(table_name, system, event["fileName"]))
    # for config in configdata["filesToProcess"]:
    #     if config["filename"].endswith(".done"):
    #         configdata_json = config["StepFunction"][0]["sfParams"]
    #         break
    try:

        # XML body content
        xml_body = f"""
        <jobRequest>
            <queue>{api_params["queue"]}</queue>
            <type>{api_params["type"]}</type>
            <input>{object_key}</input>
            <config>{api_params["config"]}</config>
            <name>{api_params["name"]}</name>
            <transactionDataType>{transactionDataType}</transactionDataType>
        </jobRequest>
        """

        splunk.log_message({'Status': 'success', 'InputFileName': object_key,

                            'Message': f'{context.function_name} Lambda function Initialized .'},
                           context)
    except Exception as e:
        splunk.log_message({'Status': 'Failed', 'InputFileName': object_key,

                            'Message': f'{context.function_name} Lambda function cannot find the config file with specified file .'},
                           context)
        raise

    try:
        # Step 2: Create OAuth1 authentication headers
        auth = OAuth1(consumer_key, consumer_secret, signature_type='auth_header')
        splunk.log_message({'InputFileName': object_key, 'Status': 'success',

                            'message': f'{context.function_name}  Lambda function does Auth1 Successfully',
                            "ModuleName": curr_fn}, context)

        # Step 3: Send an HTTP POST request with the provided headers and body
        headers = {
            "Content-Type": "application/xml"
        }
        splunk.log_message({'InputFileName': object_key, 'Status': 'success',

                            'message': f'{context.function_name}  Lambda function is sending an HTTP POST request',
                            "ModuleName": curr_fn}, context)

        response = requests.post(api_url, headers=headers, data=xml_body, auth=auth)

        # Check the response status code
        if response.status_code == 201:
            # Successful API call
            splunk.log_message({'InputFileName': object_key, 'Status': 'success',

                                'message': f'Job submitted', "statusCode": response.status_code,
                                "ModuleName": curr_fn}, context)
            return {
                "statusCode": 201,
                "body": "Job submitted"
            }
        elif response.status_code == 400:
            splunk.log_message({'InputFileName': object_key, 'Status': 'failed',

                                'message': f'Some parameters used in the job body are invalid. Please check and correct them.',
                                "statusCode": response.status_code,
                                "ModuleName": curr_fn}, context)
            raise ValueError( "Some parameters used in the job body are invalid. Please check and correct them.")
            # return {
            #     "statusCode": 400,
            #     "body": "Some parameters used in the job body are invalid. Please check and correct them."
            # }
        elif response.status_code == 401:
            splunk.log_message({'InputFileName': object_key, 'Status': 'failed',
                                'message': f'You are not authorised. Please check your credentials or authorization method used and try again.',
                                "statusCode": response.status_code,
                                "ModuleName": curr_fn}, context)
            raise ValueError("You are not authorised. Please check your credentials or authorization method used and try again.")
            # return {
            #     "statusCode": 401,
            #     "body": "You are not authorised. Please check your credentials or authorization method used and try again."
            # }
        elif response.status_code == 403:
            splunk.log_message({'InputFileName': object_key, 'Status': 'failed',

                                'message': f'Access forbidden. Please check your user roles and feature assignment. Otherwise please check the job is submitted correctly using a valid Queue.',
                                "statusCode": response.status_code,
                                "ModuleName": curr_fn}, context)
            raise ValueError("Access forbidden. Please check your user roles and feature assignment. Otherwise please check the job is submitted correctly using a valid Queue.")
            # return {
            #     "statusCode": 403,
            #     "body": "Access forbidden. Please check your user roles and feature assignment. Otherwise please check the job is submitted correctly using a valid Queue."
            # }
        elif response.status_code == 429:
            # Successful API call
            splunk.log_message({'InputFileName': object_key, 'Status': 'failed',

                                'message': f'Too many requests. Please wait 60 seconds, then retry your request.',
                                "statusCode": response.status_code,
                                "ModuleName": curr_fn}, context)
            raise ValueError("Too many requests. Please wait 60 seconds, then retry your request.")
            # return {
            #     "statusCode": 429,
            #     "body": "Too many requests. Please wait 60 seconds, then retry your request."
            # }
        elif response.status_code == 503:
            splunk.log_message({'InputFileName': object_key, 'Status': 'failed',

                                'message': f'Service unavailable. Please wait 120 seconds, then retry your request. Please note that a maintenance window can last an hour or more during a milestone upgrade.',
                                "statusCode": response.status_code,
                                "ModuleName": curr_fn}, context)
            raise ValueError("Service unavailable. Please wait 120 seconds, then retry your request. Please note that a maintenance window can last an hour or more during a milestone upgrade.")
            # return {
            #     "statusCode": 503,
            #     "body": "Service unavailable. Please wait 120 seconds, then retry your request. Please note that a maintenance window can last an hour or more during a milestone upgrade."
            # }

        else:
            # Handle errors
            splunk.log_message({'InputFileName': object_key, 'Status': 'failed',

                                'message': f'API request failed', "statusCode": response.status_code,
                                "ModuleName": curr_fn}, context)
            raise ValueError("API request failed")
            # return {
            #     "statusCode": response.status_code,
            #     "body": "API request failed"
            # }

    except Exception as e:
        # Handle exceptions
        splunk.log_message({'Status': 'failed',
                            'Message': f'{context.function_name}  Lambda function failed due to {e}',
                            "statusCode": response.status_code,
                            'ClientName': system, "ClientID": system, "FunctionCalled": curr_fn},
                           context)
        raise ValueError(f"{str(e)}")

def extract_event_setenviron(event_data, context):
    try:
        for key, value in event_data.items():
            if type(value) == dict :
                value = json.dumps(value)                
            os.environ[key] = value        
    except Exception as e:
        raise ValueError("Failed to extract event info: " + str(e))
