import boto3
import json
import re
import os
import datetime
from dpm_splunk_logger_py import splunk
from dpmdev_di_layer_common_functions import client_config
from dpmdev_di_layer_common_functions import functions

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
        
        file_name = ""
        splunk.log_message({'Message': "Initial Lambda for afp processing triggered successfully"}, context)

        bucket_name, object_key, decrypt_flag, uncompress_flag, client_name, file_name = extract_s3_event_info(event)
        
        # get transaction start time    
        transaction_start_time = get_transaction_start_time()
        
        # retrieve config data for client from dynamodb
        # client_configdata_json = retrieve_config_data(config_table, client_name.upper())
        client_configdata_json = functions.retrieve_config_data(client_config.ClientConfig(config_table, client_name, file_name))
        
        if not client_configdata_json:
            message = "failed due to configuration not found for the client: " + client_name
            splunk.log_message({ 'Message': message, "FileName": file_name, "Status": "failed"}, context)
            raise ValueError(message)
        
        client_name_parts = object_key.split('/')[1].split('.')[:4]
        client_name_concat = '.'.join(client_name_parts)
        
        # extract file information from config
        sf_info = get_sf_info(client_configdata_json, file_name)

        if not sf_info:
            message = "failed since step Functions configuration not found for the file: " + object_key
            splunk.log_message({ 'Message': message, "FileName": file_name, "Status": "failed"}, context)
            raise ValueError(message)
            
        is_rejected = False
        
        match = re.search(r'(test|TEST|prod|PROD)', file_name)
                
        if  match[1].lower() in sf_info['runType']:
            run_type = match[1]
        else:
            run_type = None
        
        file_type = ""
        extension = file_name.split('.')[-1]
        
        for key, value in sf_info['fileType'].items():
            if key in file_name:
                file_type = value
                break
        if file_type == "":
            file_type = file_name.split('.')[-1]
            
        # extract date and time from file name    
        date, time = extract_date_time(file_name)
        
        # trigger stepfunction
        result = execute_step_functions(sf_info, bucket_name, object_key, file_name, context, transaction_start_time,decrypt_flag, uncompress_flag, client_name, is_rejected, file_type, date, time, run_type)
        
    
        if result == 200:
            splunk.log_message({ 'Message': "stepfunction triggered successfully", "FileName": file_name, "Status": "success"}, context)
        
        return result
        
    except Exception as e:
        splunk.log_message({ 'Message': str(e), "FileName": file_name, "Status": "failed"}, context)
        raise e

def extract_s3_event_info(event):
    '''
    function to extract the required params
    from s3 event
    '''
    try:
        s3_event = event['Records'][0]['s3']
        bucket_name = s3_event['bucket']['name']
        object_key = s3_event['object']['key']
        decrypt_flag = should_decrypt(object_key)
        uncompress_flag = should_uncompress(object_key)
        client_name = object_key.split('/')[0]
        file_name = object_key.split('/')[-1]
        return bucket_name, object_key, decrypt_flag, uncompress_flag, client_name, file_name
    except Exception as e:
        raise ValueError("Failed to extract S3 event info: " + str(e))

def execute_step_functions(sf_info, bucket_name, object_key, file_name, context, transaction_start_time,decrypt_flag, uncompress_flag, client_name, is_rejected, file_type, date, time, run_type):
    '''
    main logic which triggers the specific stepfunction
    from config
    '''
    try:
        step_functions = sf_info['StepFunction']
        init_sf_key = ''
        file_xsd_path = ""
        file_xml_path = ""

        if not step_functions:
            raise ValueError("failed due to step functions configuration not found for the file: " + object_key)
        
        flags = {
            'bucket': bucket_name,
            'fileId': file_name,
            'isRejected': is_rejected,
            'key': object_key,
            'fileName': file_name,
            'documentType': file_type,
            'destinationBucketName': bucket_name,
            'transactionId': context.aws_request_id,
            'startDateTime': transaction_start_time,
            'endDateTime': "2023-09-23 03:12:58",
            'clientName': client_name,
            "stepFunctions": [],
            'clientID': sf_info['clientId'],
            'trackrecords': track_records,
            'decrypt_flag': decrypt_flag,   
            'uncompress_flag': uncompress_flag,
            "arnStepName" : "",
            "sfInstance" : "",
            "sfStatus" : "",
            "sfStartTime" : "",
            "sfEndTime" : "",
            "sfInputFile" : bucket_name + "/" + object_key,
            "sfOutputFile" : ""
        }
        
        flags['archivalFile'] = ""
        flags['archival_source_key'] = ""
        for sf_step in step_functions:
            sf_step_name = sf_step['name']
            sf_order = sf_step['sfKey']
            flags['stepFunctions'].append({'arnStepName': sf_step_name})
            
            if sf_order == "SM_ARN_NDMTOONPREM":
                if 'sfStepsToDone' in sf_step:
                    file_xml_path = sf_step["sfStepsToDone"]["ndmjson"]["xmlPath"]
                    afp_pattern = sf_info['fileRegex']
                    xml_file_name = find_matching_file(bucket_name,file_xml_path, file_name, afp_pattern)
                    if xml_file_name is None:
                        raise KeyError("Failed to fetch xml file")
                    flags['archival_source_key'] = xml_file_name
                    flags['xmlFile'] = xml_file_name.split('/')[-1]
                    flags['archivalFile'] = flags['xmlFile']
                    sf_step['sfOutputFile'] = bucket_name + "/" + sf_step['sfParams']['to_internal_uri'] + file_name
                    flags['fileId'] = flags['xmlFile']
                    # flags['fileId'] = client_name+"."+file_type+"."+date+"."+time+"."+extension+"."+run_type
            
            if sf_order == "SM_ARN_ARCHIVAL":
                sf_step['sfInputFile'] = bucket_name + flags['archival_source_key']
                flags['archival_destination_key'] = sf_step['archivalDestinationKey'] + flags['archivalFile']
                sf_step['sfOutputFile'] = bucket_name + flags['archival_destination_key']
            
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
        
        sf_input = {"StatePayload" : flags }
        
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


def retrieve_config_data(table_name, client_name):
    '''
    function to retrieve config data from dynamodb
    '''
    try:
        dynamodb_response = dynamodb.get_item(
            TableName=table_name,
            Key={"clientName": {"S": client_name}}
        )
        configdata = dynamodb_response.get("Item", {}).get("clientConfig", {}).get("S", "")

        return json.loads(configdata)

    except Exception as e:
        raise ValueError("Failed to retrieve config data from DynamoDB: " + str(e))

def should_decrypt(filename):
    '''
    function to check if file should be decrypted
    '''
    decryption_formats = ['.pgp', '.gpg', '.aes', '.enc']
    return any(ext in filename for ext in decryption_formats)


def should_uncompress(filename):
    '''
    function to check if file should be uncompressed
    '''
    compression_formats = ['.zip', '.tar', '.gz', '.bz2', '.rar', '.7z']
    return any(filename.endswith(ext) for ext in compression_formats)

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

def get_sf_info(config_data, client_name):
    '''
    function to get the details of the file from config
    '''
    result = {}
    try:
        clientId = config_data['clientId']
        runType = config_data['runType']
        fileType = config_data['fileType']
        file_config = config_data.get('filesToProcess', [])[0]
        # new object with StepFunction, sf_key, sf_key_next, and fileType
        result = {
                'clientId': clientId,
                'runType': runType,
                'fileType': fileType,
                'StepFunction': file_config['StepFunction'],
                'groupStep': file_config['StepFunction'][0]['name'],
                'fileRegex': file_config['filename']
        }

        return result
    except Exception as e:
        return result
    
def extract_date_time(file_name):
    '''
    function to retrieve date and time from file name
    '''
    base_name_pattern = r'(\d{14})'
    match = re.search(base_name_pattern, file_name)
    if not match:
        return None
    datetime = match.group(1)
    date = datetime[:8]
    time = datetime[8:]
    
    return date, time
    
def find_matching_file(bucket_name, path, file_name, pattern):
    '''
    function to extract matching file from the specified path
    '''
    
    match = re.search(pattern, file_name)
    if not match:
        return None
    
    
    group1 = match.group(1)
    group2 = match.group(2)
    group3 = match.group(3)
    
    base_name = ''
    
    if file_name.endswith('.afp'):
        base_name = group2 + "_" + group3 + "_" + group1
    # Ensure the path ends with a '/' to list contents inside it
    if not path.endswith('/'):
        path += '/'

    # Get the list of objects in the S3 bucket at the specified path
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=path)
    
    # Check if 'Contents' is in the response (it's not if the path is empty)
    if 'Contents' not in response:
        return None

    # Loop through all files in the specified path
    for obj in response['Contents']:
        file = obj['Key'].split('/')[-1]
        if file.endswith('.xml') and base_name in file:
            return obj['Key']

    return None