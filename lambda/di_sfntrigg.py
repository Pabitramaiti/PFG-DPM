import boto3
import json
import re
import os
import datetime

# Access environment variables
TRACK_RECORDS = os.getenv("TRACK_RECORDS")
CONFIG_TABLE = os.getenv("CONFIG_TABLE_DYNAMO")

# Initialize AWS clients
stepfunctions = boto3.client("stepfunctions")
dynamodb = boto3.client('dynamodb')


def lambda_handler(event, context):
    try:
        # Extract information from the S3 event
        print("Received S3 event: ", event)
        print("Lambda Request ID:", context.aws_request_id)

        bucket_name, object_key, decrypt_flag, uncompress_flag = extract_s3_event_info(event)

        if not object_key.startswith("data-ingress-input/"):
            raise ValueError("Object is not in the expected folder")

        client_name, file_name = extract_client_and_file_name(object_key)
        print(f"client_name, file_name :{client_name, file_name}")
        transaction_start_time = get_transaction_start_time()

        client_configdata_json = retrieve_config_data(CONFIG_TABLE, client_name)

        print(f"client_configdata_json :{client_configdata_json}")

        if not client_configdata_json:
            raise ValueError("Configuration not found for the client: " + client_name)
        
        client_name_parts = object_key.split('/')[1].split('.')[:4]
        client_name_concat = '.'.join(client_name_parts)

        sf_info = get_sf_info(client_configdata_json, client_name_concat)

        print(f"sf_info :{sf_info}")

        if not sf_info:
            raise ValueError("Step Functions configuration not found for the file: " + object_key)

        execute_step_functions(sf_info, bucket_name, object_key, file_name, context, transaction_start_time,decrypt_flag, uncompress_flag, client_name)

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return {
            'error': str(e)
        }


def extract_s3_event_info(event):
    try:
        s3_event = event['Records'][0]['s3']
        bucket_name = s3_event['bucket']['name']
        object_key = s3_event['object']['key']
        decrypt_flag = should_decrypt(object_key)
        uncompress_flag = should_uncompress(object_key)
        return bucket_name, object_key, decrypt_flag, uncompress_flag
    except Exception as e:
        raise ValueError("Failed to extract S3 event info: " + str(e))


def extract_client_and_file_name(object_key):
    try:
        client_name = extract_client_name(object_key)
        file_name = get_file_name(object_key)
        return client_name, file_name
    except Exception as e:
        raise ValueError("Failed to extract client and file name: " + str(e))


def execute_step_functions(sf_info, bucket_name, object_key, file_name, context, transaction_start_time,decrypt_flag, uncompress_flag, client_name):
    try:
        step_functions = sf_info['StepFunction']
        sf_key = ""
        sf_key_next = sf_info['sf_key_next']
        file_type = sf_info['fileType']

        

        if not step_functions:
            raise ValueError("Step Functions configuration not found for the file: " + object_key)

        flags = {
            'bucket': bucket_name,
            'sf_key': sf_key,
            'sf_key_next': sf_key_next,
            'key': object_key,
            'fileName': file_name,
            'documentType': file_type,
            'transcationId': context.aws_request_id,
            'startDateTime': transaction_start_time,
            'endDateTime': "",
            'clientName': client_name,
            "stepFunctions": [],
            'clientID': "",
            'documentType': "",
            'trackrecords': TRACK_RECORDS,
            'decrypt_flag': decrypt_flag,
            'uncompress_flag': uncompress_flag,
        }
        
        for sf_step in step_functions:
            sf_step_name = sf_step['name']
            sf_order = sf_step['sf_key']
            
            print(f"sf next key before {sf_step['sf_next_key']}")
            if 'sf_next_key' in sf_step and sf_step['sf_next_key'] != "":
                sf_step['sf_next_key_arn'] = os.getenv(sf_step['sf_next_key'])
            
            if 'order' in sf_step and sf_step['order'] != "" and sf_step['order'] == "1":
                sf_key = sf_step['sf_key']
            sf_steps_to_done = sf_step
            flags[sf_order] = sf_steps_to_done
            flags[sf_order]["sf_step_name"] = sf_step_name
        
        sf_input = {"StatePayload" : flags }
        
        STATE_MACHINE_ARN = os.getenv(sf_key)
        
        sf_response = stepfunctions.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            input=json.dumps(sf_input)
        )

        execution_arn = sf_response.get('executionArn')
        status_code = sf_response.get('ResponseMetadata', {}).get('HTTPStatusCode')

        return status_code

    except Exception as e:
        raise ValueError("Failed to execute Step Functions: " + str(e))


def retrieve_config_data(table_name, client_name):
    try:
        dynamodb_response = dynamodb.get_item(
            TableName=table_name,
            Key={"clientName": {"S": client_name}}
        )
        configdata = dynamodb_response.get("Item", {}).get("clientConfig", {}).get("S", "")

        return json.loads(configdata)

    except Exception as e:
        raise ValueError("Failed to retrieve config data from DynamoDB: " + str(e))


def extract_client_name(filename):
    try:
        pattern = r'\/([^\.]+)\.'
        match = re.search(pattern, filename)

        if match:
            client_name = match.group(1)
            return client_name
        else:
            raise ValueError("Invalid file format")

    except Exception as e:
        raise ValueError("Failed to extract client name: " + str(e))


def should_decrypt(filename):
    decryption_formats = ['.pgp', '.gpg', '.aes', '.enc']
    return any(ext in filename for ext in decryption_formats)


def should_uncompress(filename):
    compression_formats = ['.zip', '.tar', '.gz', '.bz2', '.rar', '.7z']
    return any(filename.endswith(ext) for ext in compression_formats)


def get_file_name(file_name):
    try:
        match = re.search(r'/([^/]+)$', file_name)
        if match:
            file_name = match.group(1)
            return file_name
        else:
            raise ValueError("File name not found in: " + file_name)
    except Exception as e:
        raise ValueError("Failed to get file name: " + str(e))


def get_transaction_start_time():
    try:
        startDateTime = datetime.datetime.now()
        startDateTimeStr = startDateTime.strftime("%Y-%m-%d %H:%M:%S")
        return startDateTimeStr
    except Exception as e:
        raise ValueError("Failed to get transaction start time: " + str(e))


def get_sf_info(config_data, client_name_concat):

    print(f"config_data, client_name_concat {config_data, client_name_concat}")
    try:
        files_to_process_sf = config_data.get('FilesToProcessSf', [])
        for file_config in files_to_process_sf:
            if file_config['filename'] == client_name_concat:
                # Create a new object with StepFunction, sf_key, sf_key_next, and fileType
                result = {
                    'StepFunction': file_config.get('StepFunction', ""),
                    'sf_key': file_config.get('sf_key', ""),
                    'sf_key_next': file_config.get('sf_next_key', ""),
                    'fileType': file_config.get('fileType', "")
                }
                

        return result  # Return an empty dictionary if no matching file is found
    except Exception as e:
        # Handle exceptions here, you can print an error message or log the exception
        print(f"An error occurred: {str(e)}")
        return str(e)