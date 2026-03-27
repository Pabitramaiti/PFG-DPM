import boto3
import json
import re
import os
import datetime
from dpm_splunk_logger_py import splunk
from boto3.dynamodb.conditions import Key, Attr
from dpmdev_di_layer_common_functions import client_config
from dpmdev_di_layer_common_functions import functions
import sys
import traceback
from ddb_handler import dbhandler
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError

# Access environment variables
track_records = os.getenv("TRACK_RECORDS")
config_table = os.getenv("CONFIG_TABLE_DYNAMO")

# Initialize AWS clients
stepfunctions = boto3.client("stepfunctions")
dynamodb = boto3.client('dynamodb')
s3_client = boto3.client('s3')
Lambda = boto3.client('lambda')


def checkFile_isPresent(bucket_name, object_key, file_name, context):
    try:
        # Perform the head_object call and check the response status directly
        s3_client.head_object(Bucket=bucket_name, Key=object_key)

        print(f"File {file_name} exists in the bucket {bucket_name}.")
        return True
    except ClientError as e:
        # If the error is NoSuchKey, it means the file does not exist
        if e.response['Error']['Code'] == 'NoSuchKey':
            print(f"File {file_name} not found in the bucket {bucket_name}.")
            return False
        # Handle other errors (e.g., permission issues)
        print(f"Error checking file {file_name}: {e}")
        return False
    except (NoCredentialsError, PartialCredentialsError) as e:
        print("AWS credentials are missing or incomplete.")
        return False
    except Exception as e:
        # Catch any other exceptions and log them
        print(f"Unexpected error: {e}")
        return False
def set_job_params_as_env_vars(json_data, context):
    # Extracting the region and account_id from the context
    region = context.invoked_function_arn.split(":")[3]
    account_id = context.invoked_function_arn.split(":")[4]
    for key, value in json_data.items():
        revalue =value.replace("{env}",os.getenv('SDLC_ENV')).replace("{region}", region).replace("{account_id}",account_id)
        os.environ[key] = revalue
    print("Environment variables have been set successfully.")

def lambda_handler(event, context):
    try:

        splunk.log_message({'Message': "Initial Lambda triggered successfully"}, context)

        bucket_name, object_key, decrypt_flag, uncompress_flag, client_name, file_name = extract_s3_event_info(event)

        # get transaction start time
        transaction_start_time = get_transaction_start_time()

        print(f"bucket_name: {bucket_name}, object_key: {object_key}, decrypt_flag: {decrypt_flag}, "
              f"uncompress_flag: {uncompress_flag}, client_name: {client_name}, file_name: {file_name}, transaction_start_time: {transaction_start_time}")
        splunk.log_message({'Message': "init_lambda_trigger_start", "bucket_name" : bucket_name, "object_key": object_key,
                            "client_name": client_name, "file_name": file_name, "decrypt_flag": decrypt_flag,
                            "uncompress_flag": uncompress_flag, "transaction_start_time": transaction_start_time}, context)

        # retrieve config data for client from dynamodb
        try:
            client_configdata_json = functions.retrieve_config_data(
                client_config.ClientConfig(config_table, client_name, file_name))
            if not client_configdata_json.get("filesToProcess"):
                client_configdata_json = functions.retrieve_config_data(
                    client_config.ClientConfig(
                        config_table,
                        client_name + "_" + object_key.split('/')[1],
                        file_name
                    )
                )

        except Exception as e:
            try:
                client_configdata_json = functions.retrieve_config_data(
                    client_config.ClientConfig(config_table, client_name + "_" + object_key.split('/')[1], file_name))

            except Exception as e:
                message = f"failed to retrieve configuration for client: {client_name}, file_name : {file_name}, bucket_name : {bucket_name}, object_key: {object_key} due to error: {str(e)}, traceback : {traceback.format_exc()}"
                raise Exception(message) from e

        if type(client_configdata_json) is not dict:
            message = f"client_configdata_json is not dict for client: {client_name}, file_name : {file_name}, bucket_name : {bucket_name}, object_key: {object_key}"
            raise ValueError(message)
            
        
        # check Sum logic(65 - 76)
        if ("filesToProcess" in client_configdata_json and len(client_configdata_json["filesToProcess"]) > 0 and
                "checkSum" in client_configdata_json["filesToProcess"][0] and
                client_configdata_json["filesToProcess"][0]["checkSum"]):

            checksum_ext = ".sha256"
            data_ext = "." + client_configdata_json["filesToProcess"][0]["fileType"]

            # ---- Case A : SHA256 uploaded ----
            if file_name.endswith(checksum_ext):
                data_file_name = file_name.replace(checksum_ext, data_ext)
                data_object_key = object_key.replace(checksum_ext, data_ext)

                if not checkFile_isPresent(bucket_name, data_object_key, data_file_name, context):
                    # checksum arrived first ? wait for data
                    print(f"Checksum arrived first, waiting for data file: {data_file_name}")
                    splunk.log_message(
                        {"Message": "Checksum file uploaded, waiting for data file",
                         "FileName": file_name, "Status": "waiting"}, context)
                    return {"status": "waiting_for_data"}
                else:
                    # txt already exists ? trigger now, but correct identifiers to txt
                    print("Checksum uploaded after data file ? triggering Step Function for data file.")
                    file_name = data_file_name
                    object_key = data_object_key
                    # proceed to trigger Step Function using data file context

            # ---- Case B : Data file uploaded ----
            else:
                checksum_file_name = file_name.replace(data_ext, checksum_ext)
                checksum_object_key = object_key.replace(data_ext, checksum_ext)

                if not checkFile_isPresent(bucket_name, checksum_object_key, checksum_file_name, context):
                    # data file arrived first ? wait for checksum
                    print(f"Data file uploaded first, waiting for checksum: {checksum_file_name}")
                    splunk.log_message(
                        {'Message': "The .SHA256 is not uploaded yet", "FileName": file_name, "Status": "success"},
                        context)
                    sys.exit("The .SHA256 is not uploaded yet")
        if "sfn_info" in client_configdata_json and len(client_configdata_json["sfn_info"])>0:
            
            set_job_params_as_env_vars(client_configdata_json["sfn_info"], context)
        
        if "db_info" in client_configdata_json and len(client_configdata_json["db_info"])>0:

            set_job_params_as_env_vars(client_configdata_json["db_info"], context)

        if "cluster_info" in client_configdata_json and len(client_configdata_json["cluster_info"])>0:

            set_job_params_as_env_vars(client_configdata_json["cluster_info"], context)

        if "task_definition_info" in client_configdata_json and len(client_configdata_json["task_definition_info"])>0:

            set_job_params_as_env_vars(client_configdata_json["task_definition_info"], context)
            
        # extract file information from config
        sf_info = get_sf_info(client_configdata_json, file_name)

        if not sf_info:
            input_path = object_key.rsplit('/', 1)[0] + '/'
            if 'input' in input_path:
                destination_path = input_path.replace('input', 'failed')
                file_monitoring_config = client_configdata_json.get("file_monitoring", {})
                fm_table_dynamo = os.getenv("FILE_MONITORING_TABLE_DYNAMO","")
                if fm_table_dynamo and file_monitoring_config:
                    fm_path = input_path.replace('input', 'monitoring')
                    is_group_file_match = send_file_to_file_monitoring(fm_table_dynamo,file_monitoring_config,bucket_name,fm_path,file_name,client_name,object_key.split('/')[1],context)
                    if is_group_file_match:
                        move_file_to_failed(bucket_name, input_path, fm_path, file_name, context)
                    else:
                        destination_path = input_path.replace('input', 'failed')
                        move_file_to_failed(bucket_name, input_path, destination_path, file_name, context)
                        message = f"failed since step Functions configuration and file monitoring not found for the client: {client_name}, file_name : {file_name}, bucket_name : {bucket_name}, object_key: {object_key}"
                        raise ValueError(message)
                else:
                    destination_path = input_path.replace('input', 'failed')
                    move_file_to_failed(bucket_name, input_path, destination_path, file_name, context)
                    message = f"failed since step Functions configuration not found for the client: {client_name}, file_name : {file_name}, bucket_name : {bucket_name}, object_key: {object_key}"
                    raise ValueError(message)

        is_rejected = False

        file_type = ""

        for key, value in sf_info['fileType'].items():
            if key in file_name:
                file_type = value
                break
        if file_type == "":
            file_type = file_name.split('.')[-1]

        # invoke duplicate file check lambda
        response = functions.is_duplicate_file(os.getenv("TRANSACTION_TABLE"), file_name)

        if response:
            input_path = sf_info['inputPath']
            destination_path = input_path.replace('input', 'failed')
            move_file_to_failed(bucket_name, input_path, destination_path, file_name, context)
            message = f"failed in initial triggering lambda due to duplicate file error for the client: {client_name}, file_name : {file_name}, bucket_name : {bucket_name}, object_key: {object_key}"
            raise ValueError(message)

        # trigger stepfunction
        result = execute_step_functions(sf_info, bucket_name, object_key, file_name, context, transaction_start_time,
                                        decrypt_flag, uncompress_flag, client_name, is_rejected, file_type)

        if result == 200:
            message = f"init_lambda_trigger_success for client: {client_name}, file_name : {file_name}, bucket_name : {bucket_name}, object_key: {object_key}"
            print(message)
            splunk.log_message({'Message': "init_lambda_trigger_success", "bucket_name" : bucket_name, "object_key": object_key,
                            "client_name": client_name, "file_name": file_name, "decrypt_flag": decrypt_flag,
                            "uncompress_flag": uncompress_flag, "transaction_start_time": get_transaction_start_time()}, context)

        return result

    except Exception as e:
        message = f"init_lambda_trigger_failed for client: {client_name}, file_name : {file_name}, bucket_name : {bucket_name}, object_key: {object_key} due to exception: {str(e)}, traceback : {traceback.format_exc()}"
        print(message)
        splunk.log_message({'Message': "init_lambda_trigger_failed", "bucket_name" : bucket_name, "object_key": object_key,
                        "client_name": client_name, "file_name": file_name, "decrypt_flag": decrypt_flag,
                        "uncompress_flag": uncompress_flag, "transaction_start_time": get_transaction_start_time(), "error_msg" : str(e), "traceback": traceback.format_exc()}, context)
        raise e


def extract_s3_event_info(event):
    '''
    function to extract the required params
    from s3 event
    '''
    try:
        if 'detail' in event:
            bucket_name = event['detail']['bucket']['name']
            object_key = event['detail']['object']['key']
            client_name = object_key.split('/')[0]
            file_name = object_key.split('/')[-1]
            decrypt_flag = should_decrypt(object_key)
            uncompress_flag = should_uncompress(object_key)
            return bucket_name, object_key, decrypt_flag, uncompress_flag, client_name, file_name
        else:
            s3_event = event['Records'][0]['s3']
            bucket_name = s3_event['bucket']['name']
            object_key = s3_event['object']['key']
            decrypt_flag = should_decrypt(object_key)
            uncompress_flag = should_uncompress(object_key)
            client_name = object_key.split('/')[0]
            file_name = object_key.split('/')[-1]
            return bucket_name, object_key, decrypt_flag, uncompress_flag, client_name, file_name
    except Exception as e:
        raise Exception("Failed to extract S3 event info: " + str(e)) from e


def execute_step_functions(sf_info, bucket_name, object_key, file_name, context, transaction_start_time, decrypt_flag,
                           uncompress_flag, client_name, is_rejected, file_type):
    '''
    main logic which triggers the specific stepfunction
    from config
    '''
    try:
        step_functions = sf_info['StepFunction']
        init_sf_key = ''
        xml_tag_name = ""
        xml_tag_value = ""
        sec_index_key_name = ""
        dynamo_sec_index = ""

        if not step_functions:
            input_path = object_key.rsplit('/', 1)[0] + '/'
            if 'input' in input_path:
                destination_path = input_path.replace('input', 'failed')
                move_file_to_failed(bucket_name, input_path, destination_path, file_name, context)
            raise ValueError("failed due to step functions configuration not found for the file: " + object_key)

        flags = {
            'region': os.getenv('AWS_REGION'),
            'bucket': bucket_name,
            'sdlc_env' : os.getenv('SDLC_ENV'),
            'fileId': file_name,
            'isRejected': is_rejected,
            'key': object_key,
            'fileName': file_name,
            'documentType': file_type,
            'destinationBucketName': bucket_name,
            'transactionId': context.aws_request_id,
            'startDateTime': transaction_start_time,
            'endDateTime': "",
            'clientName': client_name,
            "stepFunctions": [],
            'clientID': sf_info['clientId'],
            'teamsId': sf_info.get("teamsId", " "),
            'applicationName': sf_info['applicationName'],
            'trackrecords': track_records,
            'decrypt_flag': decrypt_flag,
            'uncompress_flag': uncompress_flag,
            "arnStepName": "",
            "sfInstance": "",
            "sfStatus": "",
            "sfStartTime": "",
            "sfEndTime": "",
            "sfInputFile": bucket_name + "/" + object_key,
            "sfOutputFile": ""
        }

        flags['archivalFile'] = file_name
        flags['archival_source_key'] = object_key
        source_key = object_key
        source_file = object_key.rsplit('/', 1)[1]
        for sf_step in step_functions:
            sf_step_name = sf_step['name']
            sf_order = sf_step['sfKey']

            splunk.log_message(
                {'Message': "Stepfunction in progress is - sf_order", "Stepfunction": sf_order, "Status": "pending"},
                context)
            splunk.log_message(
                {'Message': "Stepfunction in progress is - sf_step_name", "NAME": sf_step_name, "Status": "pending"},
                context)

            splunk.log_message({'Message': "file_name", "NAME": file_name, "Status": "logging"}, context)
            splunk.log_message({'Message': "object_key", "NAME": object_key, "Status": "logging"}, context)
            splunk.log_message({'Message': "client_name", "NAME": client_name, "Status": "logging"}, context)

            flags['stepFunctions'].append({'arnStepName': sf_step_name})
            if sf_order == "SM_ARN_VALIDATION":
                sf_step['sfOutputFile'] = ''
                if sf_step['sfStepsToDone']['ACKEmail']:
                    splunk.log_message({'Message': "sfStepsToDone", "ACKEmail": sf_step["sfStepsToDone"]["ACKEmail"],
                                        "Status": "logging"}, context)
                    # populate dummy values for optional parameters if they are not done as part of the configuration parameters.
                    if 'email_input_format' not in sf_step['sfStepsToDone']['ACKEmail']:
                        sf_step['sfStepsToDone']['ACKEmail']['email_input_format'] = ""
                    if 'email_input_file' not in sf_step['sfStepsToDone']['ACKEmail']:
                        sf_step['sfStepsToDone']['ACKEmail']['email_input_file'] = ""
                    if 'email_output_format' not in sf_step['sfStepsToDone']['ACKEmail']:
                        sf_step['sfStepsToDone']['ACKEmail']['email_output_format'] = ""
                    if 'email_output_delimiter' not in sf_step['sfStepsToDone']['ACKEmail']:
                        sf_step['sfStepsToDone']['ACKEmail']['email_output_delimiter'] = ""
                    if 'report_attachment_flag' not in sf_step['sfStepsToDone']['ACKEmail']:
                        sf_step['sfStepsToDone']['ACKEmail']['report_attachment_flag'] = ""
                    if 'attachment_flag' not in sf_step['sfStepsToDone']['ACKEmail']:
                        sf_step['sfStepsToDone']['ACKEmail']['attachment_flag'] = ""
                    if 'failureMailFlag' not in sf_step['sfStepsToDone']['ACKEmail']:
                        sf_step['sfStepsToDone']['ACKEmail']['failureMailFlag'] = False
                    if 'multiple_files' not in sf_step['sfStepsToDone']['ACKEmail']:
                        sf_step['sfStepsToDone']['ACKEmail']['multiple_files'] = ""
                    if 'duplicate_report_flag' not in sf_step['sfStepsToDone']['ACKEmail']:
                        sf_step['sfStepsToDone']['ACKEmail']['duplicate_report_flag'] = False
                    if 'return_template_flag' not in sf_step['sfStepsToDone']['ACKEmail']:
                        sf_step['sfStepsToDone']['ACKEmail']['return_template_flag'] = False
                    if 'template_key' not in sf_step['sfStepsToDone']['ACKEmail']:
                        sf_step['sfStepsToDone']['ACKEmail']['template_key'] = ""
                    if 'report_data_key' not in sf_step['sfStepsToDone']['ACKEmail']:
                        sf_step['sfStepsToDone']['ACKEmail']['report_data_key'] = ""
                    if not (('report_attachment_flag' in sf_step['sfStepsToDone']['ACKEmail']) and (sf_step['sfStepsToDone']['ACKEmail']['report_attachment_flag'])):
                        sf_step['sfStepsToDone']['ACKEmail']['report_attachment_flag'] = ""
                        sf_step['sfStepsToDone']['ACKEmail'][
                        'subject'] = f"{source_file} received and processed successfully!"
                        sf_step['sfStepsToDone']['ACKEmail'][
                        'body'] = f"{source_file} received and processed successfully @ {transaction_start_time}"
                    sf_step['sfInputFile'] = bucket_name + '/' + source_key
                    sf_step['sfOutputFile'] = ""
                    flags['archival_source_key'] = source_key
                    flags['archivalFile'] = source_file
                    flags['secIndexKeyName'] = sec_index_key_name
                    flags['secIndexKeyValue'] = xml_tag_value

                if 'ValidationSuppFiles' in sf_step['sfStepsToDone']:
                    sfStepsToDone = sf_step['sfStepsToDone']
                    ValidationSuppFiles = sfStepsToDone["ValidationSuppFiles"]
                    run_type = os.getenv("RUN_TYPE")
                    sf_step['sfStepsToDone']['ValidationSuppFiles']['run_type'] = run_type
                    if 'file_key' not in sf_step['sfStepsToDone']['ValidationSuppFiles']:
                        sf_step['sfStepsToDone']['ValidationSuppFiles']['file_key'] = source_key

                    sf_step['sfInputFile'] = bucket_name + '/' + source_key
                
                if 'MetadataValidation' in sf_step['sfStepsToDone']:
                    sfStepsToDone = sf_step['sfStepsToDone']
                    update_wip_paths(sf_step['sfStepsToDone']['MetadataValidation'], flags["transactionId"])
                    if 'file_path' in sf_step['sfStepsToDone']['MetadataValidation'] and 'file_pattern' in \
                            sf_step['sfStepsToDone']['MetadataValidation']:
                        sf_step['sfStepsToDone']['MetadataValidation']['file_key'] = " "
                    else:
                        if 'file_key' not in sf_step['sfStepsToDone']['MetadataValidation']:
                            sf_step['sfStepsToDone']['MetadataValidation']['file_key'] = source_key
                        sf_step['sfStepsToDone']['MetadataValidation']['file_path'] = " "
                        sf_step['sfStepsToDone']['MetadataValidation']['file_pattern'] = " "
                    

                if 'ValidationZipFiles' in sf_step['sfStepsToDone']:
                    sfStepsToDone = sf_step['sfStepsToDone']
                    ValidationZipFiles = sfStepsToDone["ValidationZipFiles"]
                    splunk.log_message({'Message': "sf_order", "sf_order": sf_order, "Status": "logging"}, context)
                    splunk.log_message(
                    {'Message': "sfStepsToDone", "sfStepsToDone": sf_step["sfStepsToDone"], "Status": "logging"},
                    context)

                    sf_step['sfInputFile'] = bucket_name + '/' + source_key
                    
                if 'ValidationEBCDIC' in sf_step["sfStepsToDone"]:
                    if sf_step['sfStepsToDone']['ACKEmail']:
                        sf_step['sfStepsToDone']['ACKEmail'][
                            'subject'] = f"{source_file} received and processed successfully!"
                        sf_step['sfStepsToDone']['ACKEmail'][
                            'body'] = f"{source_file} received and processed successfully @ {transaction_start_time}"
                    update_wip_paths(sf_step['sfStepsToDone']['ValidationEBCDIC'], flags["transactionId"])

                    if 'ebcdicPath' in sf_step['sfStepsToDone']['ValidationEBCDIC'] and 'ebcdicPattern' in \
                            sf_step['sfStepsToDone']['ValidationEBCDIC']:
                        sf_step['sfStepsToDone']['ValidationEBCDIC']['ebcdicKey'] = " "
                    else:
                        if 'ebcdicKey' not in sf_step['sfStepsToDone']['ValidationEBCDIC']:
                            sf_step['sfStepsToDone']['ValidationEBCDIC']['ebcdicKey'] = source_key
                        sf_step['sfStepsToDone']['ValidationEBCDIC']['ebcdicPath'] = " "
                        sf_step['sfStepsToDone']['ValidationEBCDIC']['ebcdicPattern'] = " "

                    if 'copy_flag' not in sf_step['sfStepsToDone']['ValidationEBCDIC']:
                        sf_step['sfStepsToDone']['ValidationEBCDIC']['copy_flag'] = "False"
                        sf_step['sfStepsToDone']['ValidationEBCDIC']['destination_path'] = " "

                    # Check if copy_flag is True and destination_path is provided
                    elif sf_step['sfStepsToDone']['ValidationEBCDIC']['copy_flag'] == 'True':
                        if 'destination_path' not in sf_step['sfStepsToDone']['ValidationEBCDIC']:
                            raise ValueError(f"For 'True' copy_flag, destination_path must be provided")
                    sf_step['sfInputFile'] = bucket_name + '/' + source_key
                    sf_step['sfOutputFile'] = bucket_name + '/' + sf_step['sfStepsToDone']['ValidationEBCDIC'][
                        'ebcdicPath'] + source_file

                if 'multiFileEbcdicValidation' in sf_step["sfStepsToDone"]:
                    sfStepsToDone = sf_step["sfStepsToDone"]
                    multiFileEbcdicValidation = sfStepsToDone["multiFileEbcdicValidation"]
                    splunk.log_message(
                        {'Message': "multiFileEbcdicValidation", "multiFileEbcdicValidation": multiFileEbcdicValidation, "Status": "logging"}, context)
                    # Process each file in the fileList
                    if 'fileList' in multiFileEbcdicValidation:
                        config_file_key = multiFileEbcdicValidation["config_file_key"] if "config_file_key" in multiFileEbcdicValidation else " "
                        validation_config = multiFileEbcdicValidation["validation_config"] if "validation_config" in multiFileEbcdicValidation else " "
                        ebcdic_path = multiFileEbcdicValidation["ebcdicPath"] if "ebcdicPath" in multiFileEbcdicValidation else " "
                        destination_path = multiFileEbcdicValidation["destination_path"] if "destination_path" in multiFileEbcdicValidation else " "
                        if "allow_empty_file" not in multiFileEbcdicValidation:
                            multiFileEbcdicValidation["allow_empty_file"] = "False"
                        allow_empty_file = multiFileEbcdicValidation["allow_empty_file"]
                        update_wip_paths(multiFileEbcdicValidation,flags["transactionId"])
                        update_wip_paths(sf_step['sfStepsToDone']['ACKEmail'], flags["transactionId"])
                        for file_config in multiFileEbcdicValidation['fileList']:
                            if sf_step['sfStepsToDone']['ACKEmail']:
                                if not (('report_attachment_flag' in sf_step['sfStepsToDone']['ACKEmail']) and (sf_step['sfStepsToDone']['ACKEmail']['report_attachment_flag'])):
                                    sf_step['sfStepsToDone']['ACKEmail']['report_attachment_flag'] = ""
                                    sf_step['sfStepsToDone']['ACKEmail'][
                                    'subject'] = f"{source_file} received and processed successfully!"
                                    sf_step['sfStepsToDone']['ACKEmail'][
                                    'body'] = f"{source_file} received and processed successfully @ {transaction_start_time}"
                                
                            file_config["config_file_key"] = config_file_key
                            file_config["validation_config"] = validation_config
                            file_config["ebcdicPath"] = ebcdic_path
                            file_config["destination_path"] = destination_path
                            file_config["allow_empty_file"] = allow_empty_file
                            update_wip_paths(file_config, flags["transactionId"])
                            if 'ebcdicPath' in file_config and 'ebcdicPattern' in \
                                    file_config:
                                file_config['ebcdicKey'] = " "
                            else:
                                if 'ebcdicKey' not in file_config:
                                    file_config['ebcdicKey'] = source_key
                                file_config['ebcdicPath'] = " "
                                file_config['ebcdicPattern'] = " "
                            if 'skip_wh_validation' not in file_config:
                                file_config['skip_wh_validation'] = "False"
                            if 'copy_flag' not in file_config:
                                file_config['copy_flag'] = "False"
                                file_config['destination_path'] = " "
                            # Check if copy_flag is True and destination_path is provided
                            elif file_config['copy_flag'] == 'True':
                                if 'destination_path' not in file_config:
                                    raise ValueError(f"For 'True' copy_flag, destination_path must be provided")
                            sf_step['sfInputFile'] = bucket_name + '/' + source_key
                            sf_step['sfOutputFile'] = bucket_name + '/' + file_config[
                                'ebcdicPath'] + source_file

                if 'ValidationJSON' in sf_step["sfStepsToDone"]:
                    if sf_step['sfStepsToDone']['ACKEmail']:
                        sf_step['sfStepsToDone']['ACKEmail'][
                            'subject'] = f"{source_file} received and processed successfully!"
                        sf_step['sfStepsToDone']['ACKEmail'][
                            'body'] = f"{source_file} received and processed successfully @ {transaction_start_time}"

                    # Add transaction_id to jsonPath if it contains 'wip'
                    update_wip_paths(sf_step['sfStepsToDone']['ValidationJSON'], flags["transactionId"])
                    if 'crossfileValidation' not in sf_step['sfStepsToDone']['ValidationJSON']:
                        sf_step['sfStepsToDone']['ValidationJSON']['crossfileValidation'] = []

                    # if 'jsonKey' not in sf_step['sfStepsToDone']['ValidationJSON']:
                    #     sf_step['sfStepsToDone']['ValidationJSON']['jsonKey'] = " "

                    if "jsonPattern" not in sf_step['sfStepsToDone']['ValidationJSON']:
                        if "jsonKey" not in sf_step['sfStepsToDone']['ValidationJSON']:
                            sf_step['sfStepsToDone']['ValidationJSON']['jsonKey'] = flags['key']
                        sf_step['sfStepsToDone']['ValidationJSON']['jsonPath'] = " "
                        sf_step['sfStepsToDone']['ValidationJSON']['jsonPattern'] = " "
                    else:
                        sf_step['sfStepsToDone']['ValidationJSON']['jsonKey'] = " "

                    if "jsonSchemaPattern" not in sf_step['sfStepsToDone']['ValidationJSON']:
                        sf_step['sfStepsToDone']['ValidationJSON']['jsonSchemaPath'] = " "
                        sf_step['sfStepsToDone']['ValidationJSON']['jsonSchemaPattern'] = " "
                    else:
                        sf_step['sfStepsToDone']['ValidationJSON']['jsonSchemaKey'] = " "

                    sf_step['sfInputFile'] = bucket_name + '/' + source_key
                    sf_step['sfOutputFile'] = bucket_name + '/' + sf_step['sfStepsToDone']['ValidationJSON'][
                        'jsonPath'] + source_file

                if 'ValidationXML' in sf_step["sfStepsToDone"]:
                    flags['validation_source_key'] = source_key
                    flags['validation_source_file'] = source_file
                    if 'secondaryIndexKey' in sf_step['sfStepsToDone']['ValidationXML']:
                        # retrieve xmltag from config
                        xml_tag_name = sf_step['sfStepsToDone']['ValidationXML']['xmlTag']
                        # retrieve secondary index key from config
                        sec_index_key_name = sf_step['sfStepsToDone']['ValidationXML']['secondaryIndexKey']
                        xml_tag_value = fetch_xml_tag(bucket_name, object_key, xml_tag_name)
                    flags['secIndexKeyName'] = sec_index_key_name
                    flags['secIndexKeyValue'] = xml_tag_value
                    flags['xml_destination'] = sf_step['sfStepsToDone']['ValidationXML']['wipPath'] + flags[
                        'transactionId'] + '/' + source_file
                    sf_step['sfInputFile'] = bucket_name + '/' + source_key
                    sf_step['sfOutputFile'] = bucket_name + '/' + sf_step['sfStepsToDone']['ValidationXML']['wipPath'] + \
                                              flags['transactionId'] + '/' + source_file
                if 'PositionalValidation' in sf_step["sfStepsToDone"]:
                    sfStepsToDone = sf_step["sfStepsToDone"]
                    validation_delimited = sfStepsToDone["PositionalValidation"]
                    flags['return_report_config_str'] = get_json_string(validation_delimited["return_report_config"],
                                                                        source_file, context)
                    output_folder = source_key.replace("input", "to_internal")
                    output_file = output_folder.rsplit(".", 1)[0] + "_acknowledgment.json"
                    sf_step['sfInputFile'] = bucket_name + '/' + source_key
                    sf_step['sfOutputFile'] = output_file
                    flags['secIndexKeyName'] = sec_index_key_name
                    flags['secIndexKeyValue'] = xml_tag_value

                    if sf_step['sfStepsToDone']['ACKEmail']:
                        sf_step['sfStepsToDone']['ACKEmail'][
                            'subject'] = f"{source_file} received and processed successfully!"
                        sf_step['sfStepsToDone']['ACKEmail'][
                            'body'] = f"{source_file} received and processed successfully @ {transaction_start_time}"
                        sf_step['sfStepsToDone']['ACKEmail']['email_input_file'] = output_file
                        splunk.log_message({'Message': "Stepfunction in progress is - sf_order",
                                            "AckEmail": sf_step['sfStepsToDone']['ACKEmail'], "Status": "pending"},
                                           context)
                if 'ValidationDelimited' in sf_step["sfStepsToDone"]:
                    sfStepsToDone = sf_step["sfStepsToDone"]
                    validation_delimited = sfStepsToDone["ValidationDelimited"]
                    flags['return_report_config_str'] = get_json_string(validation_delimited["return_report_config"],
                                                                        source_file, context)
                    output_folder = source_key.replace("input", "to_internal")
                    output_file = output_folder.rsplit(".", 1)[0] + "_acknowledgment.json"
                    sf_step['sfInputFile'] = bucket_name + '/' + source_key
                    sf_step['sfOutputFile'] = output_file
                    flags['secIndexKeyName'] = sec_index_key_name
                    flags['secIndexKeyValue'] = xml_tag_value

                    if sf_step['sfStepsToDone']['ACKEmail']:
                        sf_step['sfStepsToDone']['ACKEmail'][
                            'subject'] = f"{source_file} received and processed successfully!"
                        sf_step['sfStepsToDone']['ACKEmail'][
                            'body'] = f"{source_file} received and processed successfully @ {transaction_start_time}"
                        sf_step['sfStepsToDone']['ACKEmail']['email_input_file'] = output_file
                        splunk.log_message({'Message': "Stepfunction in progress is - sf_order",
                                            "AckEmail": sf_step['sfStepsToDone']['ACKEmail'], "Status": "pending"},
                                           context)
                if 'validationColumner' in sf_step['sfStepsToDone']:
                    sf_step['sfStepsToDone']['validationColumner']['dataAttributes'] = json.dumps(
                        sf_step['sfStepsToDone']['validationColumner']['dataAttributes'])
                    sf_step['sfInputFile'] = bucket_name + '/' + source_key
                    sf_step['sfOutputFile'] = bucket_name + '/' + sf_step['sfStepsToDone']['validationColumner'][
                        'outputPath'] + '/' + \
                                              source_file.split('.')[0] + \
                                              sf_step['sfStepsToDone']['validationColumner']['outputExtension']

                if 'jsonTotalsValidation' in sf_step['sfStepsToDone']:
                    sfStepsToDone = sf_step["sfStepsToDone"]
                    output_folder = source_key.replace("input", "output")
                    ##  output_file = output_folder.rsplit(".", 1)[0] + "-TRAILER.json"
                    output_file = output_folder + "-TRAILER.json"

                    sf_step['sfStepsToDone']['jsonTotalsValidation']['total_json_file'] = output_file

            if sf_order == "SM_ARN_POST_VALIDATION":
                sf_step['sfOutputFile'] = ''
                if sf_step['sfStepsToDone']['ACKEmail']:
                    splunk.log_message({'Message': "sfStepsToDone", "ACKEmail": sf_step["sfStepsToDone"]["ACKEmail"],
                                        "Status": "logging"}, context)
                    # populate dummy values for optional parameters if they are not done as part of the configuration parameters.
                    if 'email_input_format' not in sf_step['sfStepsToDone']['ACKEmail']:
                        sf_step['sfStepsToDone']['ACKEmail']['email_input_format'] = ""
                    if 'email_input_file' not in sf_step['sfStepsToDone']['ACKEmail']:
                        sf_step['sfStepsToDone']['ACKEmail']['email_input_file'] = ""
                    if 'email_output_format' not in sf_step['sfStepsToDone']['ACKEmail']:
                        sf_step['sfStepsToDone']['ACKEmail']['email_output_format'] = ""
                    if 'email_output_delimiter' not in sf_step['sfStepsToDone']['ACKEmail']:
                        sf_step['sfStepsToDone']['ACKEmail']['email_output_delimiter'] = ""
                    if 'return_template_flag' not in sf_step['sfStepsToDone']['ACKEmail']:
                        sf_step['sfStepsToDone']['ACKEmail']['return_template_flag'] = False
                    if 'template_key' not in sf_step['sfStepsToDone']['ACKEmail']:
                        sf_step['sfStepsToDone']['ACKEmail']['template_key'] = ""
                    if 'report_data_key' not in sf_step['sfStepsToDone']['ACKEmail']:
                        sf_step['sfStepsToDone']['ACKEmail']['report_data_key'] = ""
                    if not (('report_attachment_flag' in sf_step['sfStepsToDone']['ACKEmail']) and (sf_step['sfStepsToDone']['ACKEmail']['report_attachment_flag'])):
                        sf_step['sfStepsToDone']['ACKEmail']['report_attachment_flag'] = ""
                        sf_step['sfStepsToDone']['ACKEmail'][
                            'subject'] = f"{source_file} received and processed successfully!"
                        sf_step['sfStepsToDone']['ACKEmail'][
                            'body'] = f"{source_file} received and successfully passed all Post-Validation checks @ {transaction_start_time}"
                    sf_step['sfInputFile'] = bucket_name + '/' + source_key
                    sf_step['sfOutputFile'] = ""
                    flags['archival_source_key'] = source_key
                    flags['archivalFile'] = source_file
                    flags['secIndexKeyName'] = sec_index_key_name
                    flags['secIndexKeyValue'] = xml_tag_value
                    

                if 'postgressQueryValidation' in sf_step['sfStepsToDone']:
                    sfStepsToDone = sf_step["sfStepsToDone"]
                    postgressQueryValidation = sfStepsToDone["postgressQueryValidation"]
                    splunk.log_message({'Message': postgressQueryValidation, "sf_order": sf_order, "Status": "logging"}, context)
                    splunk.log_message(
                    {'Message': postgressQueryValidation, "sfStepsToDone": sfStepsToDone, "Status": "logging"},
                    context)

                    update_wip_paths(sf_step['sfStepsToDone']['postgressQueryValidation'], flags["transactionId"])
                    sf_step['sfInputFile'] = bucket_name + '/' + source_key
                    
                if "Notification_Mail" in sf_step["sfStepsToDone"]:
                    if "include_env" in sf_step["sfStepsToDone"]["Notification_Mail"] :
                        if sf_step["sfStepsToDone"]["Notification_Mail"]["include_env"]:
                            sf_step["sfStepsToDone"]["Notification_Mail"]["subject"] =  os.getenv('SDLC_ENV').upper() + " : " +sf_step["sfStepsToDone"]["Notification_Mail"]["subject"]
                    sf_step["sfStepsToDone"]["Notification_Mail"]["subject"] =  sf_step["sfStepsToDone"]["Notification_Mail"]["subject"] + " - " + source_file
                    print("updated subject",sf_step["sfStepsToDone"]["Notification_Mail"]["subject"]) 
                    for attachment in sf_step["sfStepsToDone"]["Notification_Mail"]["attachment_files"]:
                        if 'file_path' in attachment and 'file_regex' in attachment:
                            attachment['file_key'] = " "
                        else:
                            if 'file_key' not in attachment:
                                attachment['file_key'] = source_key
                            attachment['file_path'] = " "
                            attachment['file_regex'] = " "
                    update_wip_paths(sf_step["sfStepsToDone"]["Notification_Mail"], flags["transactionId"])


                if 'PostValidationEBCDIC_post' in sf_step['sfStepsToDone']:
                    splunk.log_message({'Message': "sf_order", "sf_order": sf_order, "Status": "logging"}, context)
                    splunk.log_message(
                    {'Message': "sfStepsToDone", "sfStepsToDone": sf_step["sfStepsToDone"], "Status": "logging"},
                     context)
                    update_wip_paths(sf_step['sfStepsToDone']['PostValidationEBCDIC_post'], flags["transactionId"])
                    if 'ascii_file_path' not in sf_step['sfStepsToDone']['PostValidationEBCDIC_post']:
                        sf_step['sfStepsToDone']['PostValidationEBCDIC_post']['ascii_file_path'] = " "
                    if 'output_folder' not in sf_step['sfStepsToDone']['PostValidationEBCDIC_post']:
                        sf_step['sfStepsToDone']['PostValidationEBCDIC_post']['output_folder'] = " "
                    if 'config_file' not in sf_step['sfStepsToDone']['PostValidationEBCDIC_post']:
                        sf_step['sfStepsToDone']['PostValidationEBCDIC_post']['config_file'] = " "

            # ---------------------------------------------------------------------
            #  Step function trigger for using Brx as task instead of service
            # ---------------------------------------------------------------------
            if sf_order == "SM_ARN_BRXTRIGGERSFN":

                # Pick up your brx task configuration block
                brx_config = sf_step["sfStepsToDone"].get("brxTrigger", {})

                # Attach the extra parameters to the flags dictionary
                flags["input_file"] = brx_config.get("input_file", "")
               # flags["demo_s3_input"]   = brx_config.get("s3_input_file_path", "")
               # flags["demo_temp_path"]  = brx_config.get("temp_file_path", "")
                flags["TASK_LIST"]  = brx_config.get("TASK_LIST", [])

            if sf_order == "SM_ARN_POLLINGSFN":
                polling_cfg = sf_step["sfStepsToDone"]["polling"]
                flags["PollingParams"] = polling_cfg["PollingParams"]
                flags["LambdaParams"] = polling_cfg["LambdaParams"]

            if sf_order == "SM_ARN_TRANSMISSION":
                update_wip_paths(sf_step['sfStepsToDone']['Transmission'], flags["transactionId"])
                if "file_path" not in sf_step['sfStepsToDone']['Transmission']:
                    sf_step['sfStepsToDone']['Transmission']['file_path'] = " "
                    sf_step['sfStepsToDone']['Transmission']['file_pattern'] = " "
                    flags['transmission_source_file'] = source_file
                    flags['transmission_source_key'] = source_key
                else:
                    sf_step['sfStepsToDone']['Transmission']['transmission_source_file'] = " "
                    flags['transmission_source_file'] = " "
                    flags['transmission_source_key'] = " "
                flags['transmission_destination_key'] = bucket_name + '/' + sf_step["sfStepsToDone"]['Transmission'][
                    'transmissionDestinationKey'] + source_file
                sf_step['sfInputFile'] = bucket_name + '/' + source_key
                sf_step['sfOutputFile'] = bucket_name + '/' + sf_step["sfStepsToDone"]['Transmission'][
                    'transmissionDestinationKey'] + source_file

            if sf_order == "SM_ARN_NDMTOONPREM":
                if 'sfStepsToDone' in sf_step:
                    input_path = ''
                    input_file_name = ''
                    update_wip_paths(sf_step['sfStepsToDone']['ndmjson'], flags["transactionId"])
                    if not ("ndmFilePath" in sf_step['sfStepsToDone']['ndmjson'] and
                            sf_step['sfStepsToDone']['ndmjson']['ndmFilePath'] != '' and
                            "ndmFilePattern" in sf_step['sfStepsToDone']['ndmjson'] and
                            sf_step['sfStepsToDone']['ndmjson']['ndmFilePattern'] != ''):
                        sf_step['sfStepsToDone']['ndmjson']['ndmFilePath'] = ""
                        sf_step['sfStepsToDone']['ndmjson']['ndmFilePattern'] = ""
                    if not ("moveFlag" in sf_step['sfStepsToDone']['ndmjson'] and sf_step['sfStepsToDone']['ndmjson'][
                        'moveFlag'] != ''):
                        sf_step['sfStepsToDone']['ndmjson']['moveFlag'] = "False"

                    # Handle version flag
                    # Save in flags so Step Function can dynamically reference it
                    # Use base ARN from environment
                    version = sf_step['sfStepsToDone']['ndmjson'].get("version", "")
                    base_arn = os.getenv("NDMTOONPREM_BASE")
                    target_lambda_arn = base_arn + version if version else base_arn
                    flags["NDM_TARGET_LAMBDA"] = target_lambda_arn

                    input_path = sf_info['inputPath']
                    if "afp" in sf_step['sfStepsToDone']['ndmjson']['Type']:
                        if "secondaryIndexKey" in sf_step['sfStepsToDone']['ndmjson']:
                            file_pattern = sf_info['fileRegex']
                            # match the afp pattern with filename
                            match = re.search(file_pattern, source_file)
                            # retrieve secondary index key from config
                            sec_index_key_name = sf_step['sfStepsToDone']['ndmjson']['secondaryIndexKey']
                            # retrieve secondary index name from config
                            dynamo_sec_index = sf_step['sfStepsToDone']['ndmjson']['globalSecondaryIndex']
                            # group the matched elements to extract xmltag
                            index_value = match.group(1) + match.group(2)
                            # extract the file using the secondary index key and index value
                            response_items = extract_file_content(dynamo_sec_index, sec_index_key_name, index_value)
                            input_file_name = response_items[0]['fileName']

                        if input_file_name is not None and input_file_name != '':
                            flags['inputFile'] = input_file_name
                            flags['archival_source_key'] = input_path + input_file_name
                            flags['archivalFile'] = flags['inputFile']

                        if input_file_name is None or input_file_name == '':
                            delimited_file_name = find_matching_file(bucket_name, input_path, source_file)
                            if delimited_file_name is not None:
                                flags['archival_source_key'] = input_path + delimited_file_name
                                flags['archivalFile'] = delimited_file_name
                                input_file_name = delimited_file_name
                                flags['inputFile'] = input_file_name
                            else:
                                flags['archival_source_key'] = source_key
                                flags['archivalFile'] = source_file
                                input_file_name = source_file
                                flags['inputFile'] = source_file

                        if input_file_name is None or input_file_name == '':
                            raise KeyError("Failed to fetch input file")
                        sf_step['sfInputFile'] = bucket_name + '/' + source_key
                        sf_step['sfOutputFile'] = bucket_name + "/" + sf_step['sfParams'][
                            'to_internal_uri'] + source_file
                        flags['fileId'] = flags['inputFile']
                        # flags['fileId'] = client_name+"."+file_type+"."+date+"."+time+"."+extension+"."+run_type
                    elif 'ndmFilePatternOverride' in sf_step['sfStepsToDone']['ndmjson']:
                        sf_step['sfStepsToDone']['ndmjson']['ndmFilePattern'] = source_file
                        sf_step['sfInputFile'] = bucket_name + '/' + source_key
                        sf_step['sfOutputFile'] = bucket_name + "/" + sf_step['sfParams'][
                            'to_internal_uri'] + source_file
                        flags['inputFile'] = flags['fileName']
                        flags['archivalFile'] = flags['fileName']
                    else:
                        sf_step['sfInputFile'] = bucket_name + '/' + source_key
                        sf_step['sfOutputFile'] = bucket_name + "/" + sf_step['sfParams'][
                            'to_internal_uri'] + source_file
                        flags['inputFile'] = flags['fileName']
                        flags['archivalFile'] = flags['fileName']

            if sf_order == "SM_ARN_BATCHING":
                splunk.log_message({'Message': "sf_order", "NAME": sf_order, "Status": "logging"}, context)

                key = flags["key"]
                path, filename = key.rsplit('/', 1)
                if "mergeFiles" in sf_step["sfStepsToDone"]:
                    path = f"{path}/{filename.split('.', 1)[0]}/"
                    update_wip_paths(sf_step['sfStepsToDone']['mergeFiles'], flags["transactionId"])
                if 'input_dir' in sf_step['sfStepsToDone']['moveFiles']:
                    path = sf_step['sfStepsToDone']['moveFiles']['input_dir']
                if 'move_key_flag' not in sf_step['sfStepsToDone']['moveFiles']:
                    sf_step['sfStepsToDone']['moveFiles']['move_key_flag'] = "False"

                # filename = filename.split('.', 1)[1]

                # Add transaction_id to jsonPath if it contains 'wip'
                update_wip_paths(sf_step['sfStepsToDone']['moveFiles'], flags["transactionId"])

                if 'CalculativeMergeFiles' in sf_step['sfStepsToDone']:
                    update_wip_paths(sf_step['sfStepsToDone']['CalculativeMergeFiles'], flags["transactionId"])

                if 'CalculativeMergeXMLFiles' in sf_step['sfStepsToDone']:
                    update_wip_paths(sf_step['sfStepsToDone']['CalculativeMergeXMLFiles'], flags["transactionId"])
                    if 'done_file_flag' not in sf_step['sfStepsToDone']['CalculativeMergeXMLFiles']:
                        sf_step['sfStepsToDone']['CalculativeMergeXMLFiles']['done_file_flag'] = "false"
                        sf_step['sfStepsToDone']['CalculativeMergeXMLFiles']['done_file_field'] = " "
                        sf_step['sfStepsToDone']['CalculativeMergeXMLFiles']['done_file_key'] = " "
                        sf_step['sfStepsToDone']['CalculativeMergeXMLFiles']['metadata_config'] = []
                    else:
                        if 'done_file_key' not in sf_step['sfStepsToDone']['CalculativeMergeXMLFiles']:
                            sf_step['sfStepsToDone']['CalculativeMergeXMLFiles']['done_file_key'] = \
                                sf_step['sfStepsToDone']['CalculativeMergeXMLFiles']['input_path'] + filename

                    if 'metadata_config' not in sf_step['sfStepsToDone']['CalculativeMergeXMLFiles']:
                        sf_step['sfStepsToDone']['CalculativeMergeXMLFiles']['metadata_config'] = []

                if 'moveFiles' in sf_step["sfStepsToDone"]:
                    sf_step['sfStepsToDone']['moveFiles']['input_dir'] = path
                    sf_step['sfStepsToDone']['moveFiles']['manifest_file_path'] = f"{path}{filename}"
                # if 'mergeFiles' in sf_step["sfStepsToDone"]:
                #  sf_step['sfStepsToDone']['mergeFiles']['path']+=f"{filename.split('.', 1)[0]}/"

                # Read the manifest JSON file from S3
                try:
                    if('useDoneFilePath' in sf_step['sfStepsToDone'] and sf_step['sfStepsToDone']['useDoneFilePath']=='True'):
                        splunk.log_message({"Message": "Avoiding the done.json content in the sf_input"}, context)
                    else :
                        # Download the file content from S3
                        response = s3_client.get_object(Bucket=bucket_name, Key=f"{path}{filename}")
                        content = response['Body'].read().decode('utf-8')
 
                        # Parse the JSON content
                        json_data = json.loads(content)
                        # print(f"json_data : {json_data}")
                        sf_step['manifestData'] = json_data

                except Exception as e:
                    raise ValueError(f"Error reading JSON file from S3: {str(e)}")
                sf_step['sfInputFile'] = bucket_name + '/' + f"{path}{filename}"
                sf_step[
                    'sfOutputFile'] = bucket_name + '/' + f"{sf_step['sfStepsToDone']['moveFiles']['output_dir']}{filename}"

            # if sf_order == "SM_ARN_ARCHIVAL":
            #     sf_step['sfInputFile'] = bucket_name + '/' + flags['archival_source_key']
            #     flags['archival_destination_key'] = sf_step['archivalDestinationKey'] + flags['archivalFile']
            #     sf_step['sfOutputFile'] = bucket_name + '/' + flags['archival_destination_key']

            if sf_order == "SM_ARN_CONVERSION":
                splunk.log_message({'Message': "sf_order", "sf_order": sf_order, "Status": "logging"}, context)
                splunk.log_message(
                    {'Message': "sfStepsToDone", "sfStepsToDone": sf_step["sfStepsToDone"], "Status": "logging"},
                    context)
                sf_step['sfOutputFile'] = ''
                if 'createb1config' in sf_step['sfStepsToDone']:
                    sfStepsToDone = sf_step["sfStepsToDone"]
                    createb1config = sfStepsToDone["createb1config"]
                    csv_read_params = createb1config["csv_read_params"]
                    b1_file_header = createb1config["b1_file_header"]
                    match = re.search(r'_(\d+)_', source_file)
                    if match:
                        if match.group(1) in createb1config['Client_ids']:
                            sf_step['sfStepsToDone']['createb1config']['client_id'] = match.group(1)
                    print("Setting wipOutputPath")
                    wipOutputPath = sf_step['sfStepsToDone']['createb1config']['wipPath'] + \
                                    flags['transactionId'] + '/' + source_file.rsplit(".", 1)[0] + ".json"
                    sf_step['sfOutputFile'] = bucket_name + '/' + wipOutputPath
                    createb1config['csv_read_params'] = json.dumps(csv_read_params)
                    createb1config['b1_file_header'] = json.dumps(b1_file_header)

                # new logic begin for columnar to json conversion used for RBC KYC
                if 'columnartojson' in sf_step["sfStepsToDone"]:
                    sfStepsToDone = sf_step["sfStepsToDone"]
                    columnartojson = sfStepsToDone["columnartojson"]
                    splunk.log_message(
                        {'Message': "columnartojson", "columnartojson": columnartojson,
                         "Status": "logging"}, context)
                    

                # new logic begin for brx json to smartcom json converion
                if 'jsontojson' in sf_step["sfStepsToDone"]:
                    # Add transaction_id to jsonPath if it contains 'wip'
                    update_wip_paths(sf_step['sfStepsToDone']['jsontojson'], flags["transactionId"])
                    if 'input_path' in sf_step['sfStepsToDone']['jsontojson'] and 'input_regex' in \
                            sf_step['sfStepsToDone']['jsontojson']:
                        sf_step['sfStepsToDone']['jsontojson']['file_key'] = " "
                    else:
                        if 'file_key' not in sf_step['sfStepsToDone']['jsontojson']:
                            sf_step['sfStepsToDone']['jsontojson']['file_key'] = source_key
                        sf_step['sfStepsToDone']['jsontojson']['input_path'] = " "
                        sf_step['sfStepsToDone']['jsontojson']['input_regex'] = " "
                    if 'output_file_postfix' not in sf_step['sfStepsToDone']['jsontojson'] or sf_step['sfStepsToDone']['jsontojson']['output_file_postfix']=="":
                        sf_step['sfStepsToDone']['jsontojson']['output_file_postfix']=" "
                # new logic begin for json to json converion with additional attributes
                if 'docSortKey' in sf_step["sfStepsToDone"]:
                    # Add transaction_id to jsonPath if it contains 'wip'
                    update_wip_paths(sf_step['sfStepsToDone']['docSortKey'], flags["transactionId"])
                    if 'filePath' in sf_step['sfStepsToDone']['docSortKey'] and 'filePattern' in \
                            sf_step['sfStepsToDone']['docSortKey']:
                        sf_step['sfStepsToDone']['docSortKey']['fileKey'] = " "
                    else:
                        if 'fileKey' not in sf_step['sfStepsToDone']['docSortKey']:
                            sf_step['sfStepsToDone']['docSortKey']['fileKey'] = source_key
                        sf_step['sfStepsToDone']['docSortKey']['filePath'] = " "
                        sf_step['sfStepsToDone']['docSortKey']['filePattern'] = " "
                    if 'outputFilePostfix' not in sf_step['sfStepsToDone']['docSortKey'] or sf_step['sfStepsToDone']['docSortKey']['outputFilePostfix']=="":
                        sf_step['sfStepsToDone']['docSortKey']['outputFilePostfix']=""                        
                # new logic end for brx json to smartcom json converion

                if 'ebcdictoascii' in sf_step["sfStepsToDone"]:
                    sfStepsToDone = sf_step["sfStepsToDone"]
                    ebcdictoascii = sfStepsToDone["ebcdictoascii"]
                    splunk.log_message(
                        {'Message': "ebcdictoascii", "ebcdictoascii": ebcdictoascii, "Status": "logging"}, context)

                    update_wip_paths(sf_step['sfStepsToDone']['ebcdictoascii'], flags["transactionId"])

                    if 'file_path' in sf_step['sfStepsToDone']['ebcdictoascii'] and 'file_pattern' in \
                            sf_step['sfStepsToDone']['ebcdictoascii']:
                        sf_step['sfStepsToDone']['ebcdictoascii']['file_key'] = " "
                    else:
                        if 'file_key' not in sf_step['sfStepsToDone']['ebcdictoascii']:
                            sf_step['sfStepsToDone']['ValidationEBCDIC']['file_key'] = source_key
                        sf_step['sfStepsToDone']['ebcdictoascii']['file_path'] = " "
                        sf_step['sfStepsToDone']['ebcdictoascii']['file_pattern'] = " "

                    if 'out_ASCII_path' in sf_step['sfStepsToDone']['ebcdictoascii'] and 'out_ASCII_pattern' in \
                            sf_step['sfStepsToDone']['ebcdictoascii']:
                        sf_step['sfStepsToDone']['ebcdictoascii']['output_ascii_file_key'] = " "

                    if 'delimeter_flag' not in sf_step['sfStepsToDone']['ebcdictoascii']:
                        sf_step['sfStepsToDone']['ebcdictoascii']['delimeter_flag'] = "False"
                    if 'isOptional' not in sf_step['sfStepsToDone']['ebcdictoascii']:
                        sf_step['sfStepsToDone']['ebcdictoascii']['isOptional'] = "False"
                    if not ('ASCII_Mapping_Key' in sf_step['sfStepsToDone']['ebcdictoascii'] and
                            sf_step['sfStepsToDone']['ebcdictoascii']['ASCII_Mapping_Key'].strip() != ""):
                        sf_step['sfStepsToDone']['ebcdictoascii']['input_ascii_flag'] = "False"
                        sf_step['sfStepsToDone']['ebcdictoascii']['ASCII_Mapping_Key'] = " "

                    else:
                        sf_step['sfStepsToDone']['ebcdictoascii']['input_ascii_flag'] = "True"

                    sf_step['sfInputFile'] = bucket_name + '/' + source_key
                    sf_step['sfOutputFile'] = bucket_name + '/' + sf_step['sfStepsToDone']['ebcdictoascii'][
                        'file_path'] + source_file

                # Handle MultiFileEbcdicToAscii conversion
                if 'MultiFileEbcdicToAscii' in sf_step["sfStepsToDone"]:
                    sfStepsToDone = sf_step["sfStepsToDone"]
                    multiFileEbcdicToAscii = sfStepsToDone["MultiFileEbcdicToAscii"]
                    splunk.log_message(
                        {'Message': "MultiFileEbcdicToAscii", "MultiFileEbcdicToAscii": multiFileEbcdicToAscii,
                         "Status": "logging"}, context)
                    error_report_path = set()
                    # Process each file in the fileList
                    if 'fileList' in multiFileEbcdicToAscii:
                        for file_config in multiFileEbcdicToAscii['fileList']:
                            # Update wip paths for each file configuration
                            update_wip_paths(file_config, flags["transactionId"])

                            # Handle file_key logic same as single file
                            if 'file_path' in file_config and 'file_pattern' in file_config:
                                # For multi-file, we still want to set file_key to space if using path/pattern
                                file_config['file_key'] = " "
                            else:
                                if 'file_key' not in file_config:
                                    file_config['file_key'] = source_key
                                file_config['file_path'] = " "
                                file_config['file_pattern'] = " "

                            # Handle output_ascii_file_key logic same as single file
                            if 'out_ASCII_path' in file_config and 'out_ASCII_pattern' in file_config:
                                file_config['output_ascii_file_key'] = " "

                            # Set default values if not present
                            if 'delimeter_flag' not in file_config:
                                file_config['delimeter_flag'] = "True"  # Default to True as seen in mapping
                                
                            #Handle isOptional and set default value as False
                            if 'isOptional' not in file_config:
                                file_config['isOptional'] = "False"   #Default to False
                            
                            # Handle ASCII mapping and input_ascii_flag logic
                            if not ('ASCII_Mapping_Key' in file_config and
                                    file_config['ASCII_Mapping_Key'].strip() != ""):
                                file_config['input_ascii_flag'] = "False"
                                if 'ASCII_Mapping_Key' not in file_config:
                                    file_config['ASCII_Mapping_Key'] = " "
                            else:
                                file_config['input_ascii_flag'] = "True"

                            # Set CheckOutputAsciiFlag default if not present
                            if 'CheckOutputAsciiFlag' not in file_config:
                                file_config['CheckOutputAsciiFlag'] = "false"  # Default to false as seen in mapping
                            error_report_path.add(file_config['out_ASCII_path'].rstrip('/') + '/failed_conversion/')
                    error_report_path = list(error_report_path)
                    sfStepsToDone["MultiFileEbcdicToAscii"]['error_report_path'] = error_report_path
                    # Set input and output files for multi-file conversion
                    sf_step['sfInputFile'] = bucket_name + '/' + source_key
                    sf_step['sfOutputFile'] = ''  # Multi-file doesn't have single output file

                if 'JsonToUrXml' in sf_step["sfStepsToDone"]:
                    sfStepsToDone = sf_step["sfStepsToDone"]
                    JsonToUrXml = sfStepsToDone["JsonToUrXml"]
                    splunk.log_message(
                        {'Message': "JsonToUrXml", "JsonToUrXml": JsonToUrXml, "Status": "logging"}, context)
                    # Add transaction_id to jsonPath if it contains 'wip'
                    update_wip_paths(sf_step['sfStepsToDone']['JsonToUrXml'], flags["transactionId"])

                    if 'batching' in sf_step['sfStepsToDone']['JsonToUrXml']:
                        if 'transactionFlag' in sf_step['sfStepsToDone']['JsonToUrXml']['batching']:
                            sf_step['sfStepsToDone']['JsonToUrXml']['batching']['transactionId'] = flags[
                                'transactionId']
                    else:
                        sf_step['sfStepsToDone']['JsonToUrXml']['batching'] = {}

                    if "xmlConfigPattern" not in sf_step['sfStepsToDone']['JsonToUrXml']:
                        sf_step['sfStepsToDone']['JsonToUrXml']['xmlConfigPath'] = " "
                        sf_step['sfStepsToDone']['JsonToUrXml']['xmlConfigPattern'] = " "
                    else:
                        sf_step['sfStepsToDone']['JsonToUrXml']['xmlConfigKey'] = " "

                    if "jsonPattern" not in sf_step['sfStepsToDone']['JsonToUrXml']:
                        sf_step['sfStepsToDone']['JsonToUrXml']['jsonPath'] = " "
                        sf_step['sfStepsToDone']['JsonToUrXml']['jsonPattern'] = " "
                    else:
                        sf_step['sfStepsToDone']['JsonToUrXml']['jsonKey'] = " "

                    if 'pdfFileMapping' not in sf_step['sfStepsToDone']['JsonToUrXml']:
                        sf_step['sfStepsToDone']['JsonToUrXml']['pdfFileMapping'] = {}

                    sf_step['sfInputFile'] = bucket_name + '/' + source_key
                    sf_step['sfOutputFile'] = bucket_name + '/' + sf_step['sfStepsToDone']['JsonToUrXml'][
                        'jsonPath'] + source_file

                if 'xmltojson' in sf_step['sfStepsToDone']:
                    sf_step['sfOutputFile'] = (bucket_name + "/" +
                                               sf_step['sfStepsToDone']['xmltojson']['xmlDestination'] +
                                               source_file.rsplit(".", 1)[0] + ".json")
                if 'copybooktojson' in sf_step['sfStepsToDone']:
                    sf_step['sfOutputFile'] = bucket_name + "/" + source_file.rsplit(".", 1)[0] + ".json"

                if 'PositionalToJSON' in sf_step['sfStepsToDone']:
                    sfStepsToDone = sf_step["sfStepsToDone"]
                    PositionalToJSON = sfStepsToDone["PositionalToJSON"]
                    output_folder = source_key.replace("input", "output")
                    output_file = output_folder.rsplit(".", 1)[0] + ".json"
                    sf_step['sfOutputFile'] = output_file
                    splunk.log_message(
                        {'Message': "PositionalToJSON", "PositionalToJSON": PositionalToJSON, "Status": "logging"},
                        context)
                    # set the file needed for the removal of duplicated , should be the same as the output of the validation step
                    sf_step['config_str'] = get_json_string(PositionalToJSON["config"], source_file, context)

                if 'delimitedtojson' in sf_step['sfStepsToDone']:
                    sfStepsToDone = sf_step["sfStepsToDone"]
                    delimitedtojson = sfStepsToDone["delimitedtojson"]
                    output_folder = source_key.replace("input", "output")
                    output_file = output_folder.rsplit(".", 1)[0] + ".json"
                    sf_step['sfOutputFile'] = output_file
                    splunk.log_message(
                        {'Message': "delimitedtojson", "delimitedtojson": delimitedtojson, "Status": "logging"},
                        context)
                    # set the file needed for the removal of duplicated , should be the same as the output of the validation step
                    sf_step['config_str'] = get_json_string(delimitedtojson["config"], source_file, context)

                if 'delimitedtoxml' in sf_step['sfStepsToDone']:
                    sfStepsToDone = sf_step["sfStepsToDone"]
                    delimitedtoxml = sfStepsToDone["delimitedtoxml"]
                    splunk.log_message(
                        {'Message': "delimitedtoxml", "delimitedtoxml": delimitedtoxml, "Status": "logging"}, context)
                    conversion_parameters = json.loads(
                        get_json_string(delimitedtoxml['conversionParameters'], source_file, context))
                    if 'additionalTagsRootTop' in conversion_parameters:
                        match = re.search(sf_info['fileRegex'], source_file)
                        if 'additionalTagsRootTop' in conversion_parameters and match:
                            conversion_parameters['additionalTagsRootTop'] = {'formName': match.group(
                                conversion_parameters['additionalTagsRootTop']['formNameIndex']),
                                'outFileName': source_file.rsplit('.', 1)[
                                                   0] +
                                               conversion_parameters[
                                                   'additionalTagsRootTop'][
                                                   'outFileExtension']}
                        if 'rootTag' in conversion_parameters:
                            conversion_parameters['rootTag'] = "tax" + conversion_parameters['additionalTagsRootTop'][
                                                                           'formName'][:4] + "List"
                        delimitedtoxml['conversionParameters'] = json.dumps(conversion_parameters)
                    output_folder = bucket_name + '/' + sf_step['sfStepsToDone']['delimitedtoxml']['wipPath'] + flags[
                        'transactionId'] + '/' + source_file
                    sf_step['sfOutputFile'] = output_folder.rsplit(".", 1)[0] + ".xml"
                    source_key = sf_step['sfStepsToDone']['delimitedtoxml']['wipPath'] + flags['transactionId'] + '/' + \
                                 source_file.rsplit('.', 1)[0] + '.xml'
                    sf_step['delimited_to_xml_destination'] = source_key
                    source_file = source_key.rsplit('/', 1)[1]
                    splunk.log_message(
                        {'Message': "delimitedtoxml", "sf_step": sf_step, "Status": "logging"}, context)
                ###########   2078_3190_3318 ###########################################################
                if 'purchaseandbatch' in sf_step['sfStepsToDone']:

                    sfStepsToDone = sf_step["sfStepsToDone"]
                    purchaseandbatch = sfStepsToDone["purchaseandbatch"]
                    output_file = purchaseandbatch["output_file"]
                    if output_file == "":
                        output_folder = source_key
                        # output_file = output_folder.rsplit(".", 1)[0] + ".csv"
                        output_file = file_name.rsplit(".", 1)[0] + ".csv"
                        print(f"this is output file {output_file}")
                        purchaseandbatch["output_file"] = output_file
                #########################################################################
                if 'b228HeaderPrep' in sf_step['sfStepsToDone']:
                    sfStepsToDone = sf_step["sfStepsToDone"]

                    b228HeaderPrep = sfStepsToDone["b228HeaderPrep"]
                    sf_step['sfStepsToDone']['b228HeaderPrep']['config'] = get_json_string(b228HeaderPrep["config"],
                                                                                           source_file, context)

                    sf_step['sfStepsToDone']['b228HeaderPrep']['header_input_json'] = source_key
                    sf_step['sfStepsToDone']['b228HeaderPrep']['header_output_json'] = source_key.replace("input",
                                                                                                          "output")

                if 'b228Conversion' in sf_step['sfStepsToDone']:
                    sfStepsToDone = sf_step["sfStepsToDone"]
                    b228Conversion = sfStepsToDone["b228Conversion"]
                    #update transactionid in queue_folder path
                    sf_step['sfStepsToDone']['b228Conversion']['queue_folder'] = update_wip_paths(sf_step['sfStepsToDone']['b228Conversion']['queue_folder'], flags["transactionId"])
####rbc specific changes .so passing false as default for other client for backward compatibility and also added the acf and branch file key with empty value for non rbc client
                    if 'rbcfilterenable' not in sf_step['sfStepsToDone']['b228Conversion']:
                        sf_step['sfStepsToDone']['b228Conversion']['rbcfilterenable'] = "false"
                    if 'rbcacffile' not in sf_step['sfStepsToDone']['b228Conversion']:
                        sf_step['sfStepsToDone']['b228Conversion']['rbcacffile'] = " "
                    if 'rbcbranchfile' not in sf_step['sfStepsToDone']['b228Conversion']:
                        sf_step['sfStepsToDone']['b228Conversion']['rbcbranchfile'] = " "

                    temp_config_bucket = sfStepsToDone['b228Conversion']['config_bucket']

                    temp_output_bucket = sfStepsToDone['b228Conversion']['output_bucket']

                    if len(temp_config_bucket.strip()) == 0:
                        sf_step['sfStepsToDone']['b228Conversion']['config_bucket'] = bucket_name

                    if len(temp_output_bucket.strip()) == 0:
                        sf_step['sfStepsToDone']['b228Conversion']['output_bucket'] = bucket_name

                if 'java_functions' in sf_step['sfStepsToDone']:
                    sfStepsToDone = sf_step["sfStepsToDone"]
                    java_functions = sfStepsToDone["java_functions"]
                    java_functions['java_bucket_name']=bucket_name
                    java_functions['bucket_name']=bucket_name
                    if java_functions['java_function'] == "ebcdic_to_json":
                        table_name = flags['transactionId'] + '_ebcidic_ascii'
                        if not java_functions.get('java_table_name'):
                            java_functions['java_table_name'] = table_name
                        java_functions['java_s3_if'] = source_key
                    if java_functions['java_function'] == "columnar_to_json":
                        table_name = flags['transactionId'] + '_columnar_json'
                        java_functions['java_table_name'] = table_name
                        java_functions['java_s3_if'] = source_key
                        java_functions['java_table_config_path']=f"{client_name}/config/{flags['transactionId']}_table_config.json"
                if 'cost_basis' in sf_step['sfStepsToDone']:
                    sfStepsToDone = sf_step["sfStepsToDone"]
                    cost_basis = sfStepsToDone["cost_basis"]
                    cost_basis['src_table'] = flags['transactionId'] + '_ebcidic_ascii'
                
                 ########## new logic for convertion of brx json to smartcom required json #########
                if 'BrxjsontoSmartcomjson' in sf_step["sfStepsToDone"]:
                    # Add transaction_id to jsonPath if it contains 'wip'
                    update_wip_paths(sf_step['sfStepsToDone']['BrxjsontoSmartcomjson'], flags["transactionId"])
                    if 'file_path' in sf_step['sfStepsToDone']['BrxjsontoSmartcomjson'] and 'file_pattern' in \
                            sf_step['sfStepsToDone']['BrxjsontoSmartcomjson']:
                        sf_step['sfStepsToDone']['BrxjsontoSmartcomjson']['file_key'] = " "
                    else:
                        if 'file_key' not in sf_step['sfStepsToDone']['BrxjsontoSmartcomjson']:
                            sf_step['sfStepsToDone']['BrxjsontoSmartcomjson']['file_key'] = source_key
                        sf_step['sfStepsToDone']['BrxjsontoSmartcomjson']['file_path'] = " "
                        sf_step['sfStepsToDone']['BrxjsontoSmartcomjson']['file_pattern'] = " "
                ######### new logic end for convertion of brx json to smartcom required json ##########
                
                if 'columnarToCsv' in sf_step['sfStepsToDone']:
                    sfStepsToDone = sf_step["sfStepsToDone"]

                    temp_file = bucket_name + '/' + source_key
                    sf_step['sfStepsToDone']['columnarToCsv']['s3_output_file'] = temp_file.replace(".DAT", ".csv")
                

            if sf_order == "SM_ARN_B228_FULL_CONVERT":
                if 'b228FullRunConvert' in sf_step['sfStepsToDone']:
                    sfStepsToDone = sf_step["sfStepsToDone"]
                    sf_step['sfStepsToDone']['b228FullRunConvert']['queue_folder'] = update_wip_paths(sf_step['sfStepsToDone']['b228FullRunConvert']['queue_folder'], flags["transactionId"])
                   ####rbc specific changes .so passing false as default for other client for backward compatibility and also added the acf and branch file key with empty value for non rbc client
                    if 'rbcfilterenable' not in sf_step['sfStepsToDone']['b228FullRunConvert']:
                        sf_step['sfStepsToDone']['b228FullRunConvert']['rbcfilterenable'] = "false"
                    if 'rbcacffile' not in sf_step['sfStepsToDone']['b228FullRunConvert']:
                        sf_step['sfStepsToDone']['b228FullRunConvert']['rbcacffile'] = " "
                    if 'rbcbranchfile' not in sf_step['sfStepsToDone']['b228FullRunConvert']:
                        sf_step['sfStepsToDone']['b228FullRunConvert']['rbcbranchfile'] = " "

                    temp_config_bucket = sfStepsToDone['b228FullRunConvert']['config_bucket']
                    temp_output_bucket = sfStepsToDone['b228FullRunConvert']['output_bucket']

                    if len(temp_config_bucket.strip()) == 0:
                        sf_step['sfStepsToDone']['b228FullRunConvert']['config_bucket'] = bucket_name

                    if len(temp_output_bucket.strip()) == 0:
                        sf_step['sfStepsToDone']['b228FullRunConvert']['output_bucket'] = bucket_name

            if sf_order == "SM_ARN_MERGE_FILES":
                merge_info_value = sf_step["sfStepsToDone"].get("mergeInfo")

                if merge_info_value is None:
                    print("mergeInfo not found, skipping")
                else:
                    if not isinstance(merge_info_value, str):
                        print("mergeInfo is a list/dict, converting to JSON string")
                        merge_info_str = json.dumps(merge_info_value)
                        sf_step["sfStepsToDone"]["mergeInfo"] = merge_info_str
                        print(f"Converted mergeInfo JSON string: {merge_info_str}")
                    else:
                        print("mergeInfo is already a string")

                merge_insert_info_value = sf_step["sfStepsToDone"].get("mergeInsertInfo")

                # Convert if mergeInsertInfo is a dict or list — not already a string
                if merge_insert_info_value is None:
                    print("mergeInsertInfo not found, skipping")
                else:
                    if not isinstance(merge_insert_info_value, str):
                        print("mergeInsertInfo is a list/dict, converting to JSON string")
                        merge_insert_info_str = json.dumps(merge_insert_info_value)
                        sf_step["sfStepsToDone"]["mergeInsertInfo"] = merge_insert_info_str
                        print(f"Converted mergeInsertInfo JSON string: {merge_insert_info_str}")
                    else:
                        print("mergeInsertInfo is already a string")


            if sf_order == "SM_ARN_COPY_FILES":
                copy_info_value = sf_step["sfStepsToDone"].get("copyInfo")

                # Convert if copy_info_value is a dict or list — not already a string
                if not isinstance(copy_info_value, str):
                    print("copyInfo is a list/dict, converting to JSON string")
                    copy_info_str = json.dumps(copy_info_value)
                    sf_step["sfStepsToDone"]["copyInfo"] = copy_info_str
                    print(f"Converted copyInfo JSON string: {copy_info_str}")
                else:
                    print("copyInfo is already a string")

            if sf_order == "SM_ARN_UR_ATTRIBUTES":
                splunk.log_message({'Message': "sf_order", "sf_order": sf_order, "Status": "logging"}, context)
                splunk.log_message(
                    {'Message': "sfStepsToDone", "sfStepsToDone": sf_step["sfStepsToDone"], "Status": "logging"},
                    context)
                sf_step['sfOutputFile'] = ''
                update_wip_paths(sf_step['sfStepsToDone'], flags["transactionId"])

                if 'propertyFileCreation' in sf_step['sfStepsToDone']:
                    filePathList = []
                    if "propertyConfigPattern" not in sf_step['sfStepsToDone']['propertyFileCreation']:
                        sf_step['sfStepsToDone']['propertyFileCreation']['propertyConfigPath'] = " "
                        sf_step['sfStepsToDone']['propertyFileCreation']['propertyConfigPattern'] = " "
                    else:
                        sf_step['sfStepsToDone']['propertyFileCreation']['propertyConfigKey'] = " "

                sf_step['sfInputFile'] = bucket_name + '/' + source_key
                sf_step['sfOutputFile'] = bucket_name + '/' + sf_step['sfStepsToDone']['zipFileCreation'][
                    'zipDestination'] + source_file

            if sf_order == "SM_ARN_FILE_BASED_TRIGGER":
                update_wip_paths(sf_step['sfStepsToDone'], flags["transactionId"])
                splunk.log_message({'Message': "sf_order", "sf_order": sf_order, "Status": "logging"}, context)
                splunk.log_message(
                    {'Message': "sfStepsToDone", "sfStepsToDone": sf_step["sfStepsToDone"], "Status": "logging"},
                    context)                

                if 'fileBasedTrigger' in sf_step['sfStepsToDone']:
                    if "fileKey" not in sf_step['sfStepsToDone']['fileBasedTrigger']:
                        sf_step['sfStepsToDone']['fileBasedTrigger']["fileKey"]=source_key

                sf_step['sfInputFile'] = bucket_name + '/' + source_key
                sf_step['sfOutputFile'] = bucket_name + '/' + sf_step['sfStepsToDone']['fileBasedTrigger'][
                    'secondaryFilePath'] + source_file


            if sf_order == "SM_ARN_LOADSTAGE":
                if 'LoadStage' in sf_step['sfStepsToDone']:
                    tmp_file_name = sf_step['sfStepsToDone']['LoadStage']['fileName']
                    tmp_object_key = sf_step['sfStepsToDone']['LoadStage']['object_key']
                    if len(tmp_file_name.strip()) < 1:
                        sf_step['sfStepsToDone']['LoadStage']['fileName'] = source_file.rsplit(".", 1)[0] + ".json"
                    if len(tmp_object_key.strip()) < 1:
                        new_object_key = source_key.replace("input", "output")
                        sf_step['sfStepsToDone']['LoadStage']['object_key'] = new_object_key.rsplit(".", 1)[0] + ".json"

                    sf_step['sfOutputFile'] = new_object_key.rsplit(".", 1)[0] + ".json"
                    sfStepsToDone = sf_step["sfStepsToDone"]
                    LoadStage = sfStepsToDone["LoadStage"]
                    splunk.log_message({'Message': "LoadStage", "LoadStage": LoadStage, "Status": "logging"}, context)
                    sf_step['sfStepsToDone']['LoadStage']['key_location'] = get_json_string(LoadStage["key_location"],
                                                                                            source_file, context)
                elif 'delimitedLoadStage' in sf_step['sfStepsToDone']:
                    print("delimitedLoadStage in sf_step['sfStepsToDone']")
                    print(
                        f"sf_step['sfStepsToDone']['delimitedLoadStage']: {sf_step['sfStepsToDone']['delimitedLoadStage']}")
                    splunk.log_message({'Message': "delimitedLoadStage",
                                        "delimitedLoadStage": sf_step['sfStepsToDone']['delimitedLoadStage'],
                                        "Status": "logging"}, context)
                    tmp_file_name = sf_step['sfStepsToDone']['delimitedLoadStage']['fileName']
                    tmp_object_key = sf_step['sfStepsToDone']['delimitedLoadStage']['object_key']
                    print(f"tmp_file_name: {tmp_file_name}")
                    print(f"tmp_object_key: {tmp_object_key}")
                    if len(tmp_file_name.strip()) < 1:
                        sf_step['sfStepsToDone']['delimitedLoadStage']['fileName'] = source_file.rsplit(".", 1)[
                                                                                         0] + ".csv"
                    if len(tmp_object_key.strip()) < 1:
                        sf_step['sfStepsToDone']['delimitedLoadStage']['object_key'] = source_key.rsplit(".", 1)[
                                                                                           0] + ".csv"

                    new_object_key = source_key.replace("input", "output")
                    sf_step['sfOutputFile'] = new_object_key.rsplit(".", 1)[0] + ".csv"
                    sfStepsToDone = sf_step["sfStepsToDone"]
                    delimitedLoadStage = sfStepsToDone["delimitedLoadStage"]
                    splunk.log_message({'Message': "delimitedLoadStage", "delimitedLoadStage": delimitedLoadStage,
                                        "Status": "logging"}, context)
                    sf_step['sfStepsToDone']['delimitedLoadStage']['key_location'] = get_json_string(
                        delimitedLoadStage["key_location"],
                        source_file, context)
                    print(f"sf_step key_location: {sf_step['sfStepsToDone']['delimitedLoadStage']['key_location']}")
                    print(f"sf_step fileName: {sf_step['sfStepsToDone']['delimitedLoadStage']['fileName']}")
                    print(f"sf_step object_key: {sf_step['sfStepsToDone']['delimitedLoadStage']['object_key']}")
                    print(f"sf_step sfOutputFile: {sf_step['sfOutputFile']}")
                    print(f"sf_step : {sf_step}")

                elif 'csvLoadStage' in sf_step['sfStepsToDone']:
                    tmp_file_name = sf_step['sfStepsToDone']['csvLoadStage']['fileName']
                    tmp_object_key = sf_step['sfStepsToDone']['csvLoadStage']['object_key']
                    if len(tmp_file_name.strip()) < 1:
                        sf_step['sfStepsToDone']['csvLoadStage']['fileName'] = source_file.rsplit(".", 1)[0] + ".csv"
                    if len(tmp_object_key.strip()) < 1:
                        sf_step['sfStepsToDone']['csvLoadStage']['object_key'] = source_key.rsplit(".", 1)[0] + ".csv"

                    new_object_key = source_key.replace("input", "output")
                    sf_step['sfOutputFile'] = new_object_key.rsplit(".", 1)[0] + ".csv"
                    sfStepsToDone = sf_step["sfStepsToDone"]
                    csvLoadStage = sfStepsToDone["csvLoadStage"]
                    sf_step['sfStepsToDone']['csvLoadStage']['key_location'] = get_json_string(csvLoadStage["key_location"], source_file, context)
                    print(f"sf_step key_location: {sf_step['sfStepsToDone']['csvLoadStage']['key_location']}")
                    print(f"sf_step fileName: {sf_step['sfStepsToDone']['csvLoadStage']['fileName']}")
                    print(f"sf_step object_key: {sf_step['sfStepsToDone']['csvLoadStage']['object_key']}")
                    print(f"sf_step sfOutputFile: {sf_step['sfOutputFile']}")
                    print(f"sf_step : {sf_step}")

                elif 'LoadStageToDB' in sf_step['sfStepsToDone']:
                    sfStepsToDone = sf_step['sfStepsToDone']
                    LoadStageToDB = sfStepsToDone['LoadStageToDB']
                    if wipOutputPath is None or not wipOutputPath:
                        LoadStageToDB['object_key'] = object_key
                    else:
                        LoadStageToDB['object_key'] = wipOutputPath
                    splunk.log_message({'Message': "LoadStage", "LoadStage": LoadStageToDB, "Status": "logging"},
                                       context)

                if 'EnhancedLoadToDB' in sf_step['sfStepsToDone']:
                    sfStepsToDone = sf_step["sfStepsToDone"]
                    EnhancedLoadToDB = sfStepsToDone["EnhancedLoadToDB"]
                    splunk.log_message(
                        {'Message': "EnhancedLoadToDB", "EnhancedLoadToDB": EnhancedLoadToDB, "Status": "logging"}, context)
                    files_to_process = sf_step['sfStepsToDone']['EnhancedLoadToDB']["filesToProcess"]
                    for file_dict in files_to_process:
                        if 'file_path' in file_dict and 'file_pattern' in file_dict:
                            file_dict['fileKey'] = " "
                        else:
                            if 'fileKey' not in file_dict:
                                file_dict['fileKey'] = source_key
                            file_dict['file_path'] = " "
                            file_dict['file_pattern'] = " "

                    update_wip_paths(sf_step['sfStepsToDone']['EnhancedLoadToDB'], flags["transactionId"])
                elif 'SelectToInsert' in sf_step['sfStepsToDone']:
                    sfStepsToDone = sf_step["sfStepsToDone"]
                    select_to_db = sfStepsToDone["SelectToInsert"]
                    select_to_db['table_config_path']=f"{client_name}/config/{flags['transactionId']}_table_config.json"
                    splunk.log_message(
                        {'Message': "SelectToInsert", "SelectToInsert": select_to_db, "Status": "logging"}, context)
                elif 'SelectToCsv' in sf_step['sfStepsToDone']:
                    sfStepsToDone = sf_step["sfStepsToDone"]
                    select_to_csv = sfStepsToDone["SelectToCsv"]
                    select_to_csv['table_config_path']=f"{client_name}/config/{flags['transactionId']}_table_config.json"
                    splunk.log_message(
                        {'Message': "SelectToCsv", "SelectToCsv": select_to_csv, "Status": "logging"}, context)

                if 'EnhancedLoadToDB_Spark' in sf_step['sfStepsToDone']:
                    sfStepsToDone = sf_step["sfStepsToDone"]
                    EnhancedLoadToDB_Spark = sfStepsToDone["EnhancedLoadToDB_Spark"]
                    splunk.log_message(
                        {'Message': "EnhancedLoadToDB_Spark", "EnhancedLoadToDB_Spark": EnhancedLoadToDB_Spark, "Status": "logging"}, context)
                    files_to_process = sf_step['sfStepsToDone']['EnhancedLoadToDB_Spark']["filesToProcess"]
                    for file_dict in files_to_process:
                        if 'file_path' in file_dict and 'file_pattern' in file_dict:
                            file_dict['fileKey'] = " "
                        else:
                            if 'fileKey' not in file_dict:
                                file_dict['fileKey'] = source_key
                            file_dict['file_path'] = " "
                            file_dict['file_pattern'] = " "

                    update_wip_paths(sf_step['sfStepsToDone']['EnhancedLoadToDB_Spark'], flags["transactionId"])
                            
            if sf_order == "SM_ARN_MULTIFILE_VALIDATION":
                if 'multiFileValidation' in sf_step['sfStepsToDone']:
                    sfStepsToDone = sf_step['sfStepsToDone']
                    multiFileValidation = sfStepsToDone["multiFileValidation"]
                    sf_step['sfStepsToDone']['multiFileValidation']['file_name'] = file_name
                    sf_step['sfStepsToDone']['multiFileValidation']['multifileConfig'] = get_json_string(
                        sf_info['multifileData'], source_file, context)
                    sf_step['sfStepsToDone']['multiFileValidation']['multifileTableName'] = get_json_string(
                        sf_info['multifileTableName'], source_file, context)
                    sf_step['sfStepsToDone']['multiFileValidation']['periodDateRange'] = get_json_string(
                        sf_info['periodDateRange'], source_file, context)

            if sf_order == "SM_ARN_DEBATCH":

                if 'debatchAscii' in sf_step["sfStepsToDone"]:
                    sfStepsToDone = sf_step["sfStepsToDone"]
                    debatchAscii = sfStepsToDone["debatchAscii"]
                    splunk.log_message(
                        {'Message': "debatchAscii", "debatchAscii": debatchAscii, "Status": "logging"}, context)
                    update_wip_paths(sf_step['sfStepsToDone']['debatchAscii'], flags["transactionId"])
                    run_type = os.getenv("RUN_TYPE")
                    sf_step['sfStepsToDone']['debatchAscii']['run_type'] = run_type

                    if 'ascii_file_path' in sf_step['sfStepsToDone']['debatchAscii'] and 'ascii_file_pattern' in \
                            sf_step['sfStepsToDone']['debatchAscii']:
                        sf_step['sfStepsToDone']['debatchAscii']['ascii_file_key'] = " "
                    else:
                        sf_step['sfStepsToDone']['debatchAscii']['ascii_file_path'] = " "
                        sf_step['sfStepsToDone']['debatchAscii']['ascii_file_pattern'] = " "

                    if (sf_step['sfStepsToDone']['debatchAscii']["one_record_per_row"]).lower() == "True":
                        sf_step['sfStepsToDone']['debatchAscii']["end_record_type"] = " "
                        sf_step['sfStepsToDone']['debatchAscii']["start_record_type"] = " "

                    if (sf_step['sfStepsToDone']['debatchAscii']["header_skip_flag"]).lower() == "True":
                        sf_step['sfStepsToDone']['debatchAscii']["header_in_file_flag"] = "False"
                    if "end_record_type" not in sf_step['sfStepsToDone']['debatchAscii']:
                        sf_step['sfStepsToDone']['debatchAscii']["end_record_type"] = " "
                    if "start_record_type" not in sf_step['sfStepsToDone']['debatchAscii']:
                        sf_step['sfStepsToDone']['debatchAscii']["start_record_type"] = " "
                    if "header_rec" not in sf_step['sfStepsToDone']['debatchAscii']:
                        sf_step['sfStepsToDone']['debatchAscii']["header_rec"] = " "
                    if "done_file_flag" not in sf_step['sfStepsToDone']['debatchAscii']:
                        sf_step['sfStepsToDone']['debatchAscii']["done_file_flag"] = "False"
                    if "configFile" not in sf_step['sfStepsToDone']['debatchAscii'] or not sf_step['sfStepsToDone']['debatchAscii']["configFile"]:
                        sf_step['sfStepsToDone']['debatchAscii']["configFile"] = " "
                    if "header_record_type" not in sf_step['sfStepsToDone']['debatchAscii']:
                        sf_step['sfStepsToDone']['debatchAscii']["header_record_type"] = " "
                    if "tailer_record_type" not in sf_step['sfStepsToDone']['debatchAscii']:
                        sf_step['sfStepsToDone']['debatchAscii']["tailer_record_type"] = " "
                    if "use_end_record_only" not in sf_step['sfStepsToDone']['debatchAscii']:
                        sf_step['sfStepsToDone']['debatchAscii']["use_end_record_only"] = "False"
                    if "use_greedy_assignment" not in sf_step['sfStepsToDone']['debatchAscii']:
                        sf_step['sfStepsToDone']['debatchAscii']["use_greedy_assignment"] = "False"
                    if "group_key_positions" not in sf_step['sfStepsToDone']['debatchAscii']:
                        sf_step['sfStepsToDone']['debatchAscii']["group_key_positions"] = " "

                if "debatch" in sf_step['sfStepsToDone']:
                    if "config" in sf_step['sfStepsToDone']['debatch']:
                        sf_step['sfStepsToDone']['debatch']['config'] = json.dumps(
                            sf_step['sfStepsToDone']['debatch']['config'])

                if "check_empty_file" in sf_step['sfStepsToDone']:
                    sfStepsToDone = sf_step['sfStepsToDone']
                    update_wip_paths(sf_step['sfStepsToDone']['check_empty_file'], flags["transactionId"])
                    if 'file_path' in sf_step['sfStepsToDone']['check_empty_file'] and 'file_pattern' in \
                            sf_step['sfStepsToDone']['check_empty_file']:
                        sf_step['sfStepsToDone']['check_empty_file']['file_key'] = " "
                    else:
                        if 'file_key' not in sf_step['sfStepsToDone']['check_empty_file']:
                            sf_step['sfStepsToDone']['check_empty_file']['file_key'] = source_key
                        sf_step['sfStepsToDone']['check_empty_file']['file_path'] = " "
                        sf_step['sfStepsToDone']['check_empty_file']['file_pattern'] = " "
                    if sf_step['sfStepsToDone']['ACKEmail']:
                        splunk.log_message({'Message': "sfStepsToDone", "ACKEmail": sf_step["sfStepsToDone"]["ACKEmail"],
                                        "Status": "logging"}, context)
                    # populate dummy values for optional parameters if they are not done as part of the configuration parameters.
                        if 'email_input_format' not in sf_step['sfStepsToDone']['ACKEmail']:
                            sf_step['sfStepsToDone']['ACKEmail']['email_input_format'] = ""
                        if 'email_input_file' not in sf_step['sfStepsToDone']['ACKEmail']:
                            sf_step['sfStepsToDone']['ACKEmail']['email_input_file'] = ""
                        if 'email_output_format' not in sf_step['sfStepsToDone']['ACKEmail']:
                            sf_step['sfStepsToDone']['ACKEmail']['email_output_format'] = ""
                        if 'email_output_delimiter' not in sf_step['sfStepsToDone']['ACKEmail']:
                            sf_step['sfStepsToDone']['ACKEmail']['email_output_delimiter'] = ""
                        if not (('report_attachment_flag' in sf_step['sfStepsToDone']['ACKEmail']) and (sf_step['sfStepsToDone']['ACKEmail']['report_attachment_flag'])):
                            sf_step['sfStepsToDone']['ACKEmail']['report_attachment_flag'] = ""
                            sf_step['sfStepsToDone']['ACKEmail']['subject'] = f"{source_file} - empty file received"
                            sf_step['sfStepsToDone']['ACKEmail']['body'] = f"{source_file} - empty file received @ {transaction_start_time}"
                    flags['archival_source_key'] = source_key
                    flags['archivalFile'] = source_file
                    flags['secIndexKeyName'] = sec_index_key_name
                    flags['secIndexKeyValue'] = xml_tag_value
                    
                sf_step['sfInputFile'] = bucket_name + '/' + source_key
                sf_step['sfOutputFile'] = ""

                ############ glue trigger code added  for bundle to json for B228 debatching  ##################
                if 'BundletoJson' in sf_step["sfStepsToDone"]:
                    sfStepsToDone = sf_step["sfStepsToDone"]
                    BundletoJson = sfStepsToDone["BundletoJson"]
                    splunk.log_message(
                        {'Message': "BundletoJson", "BundletoJson": BundletoJson, "Status": "logging"}, context)
                  #  update_wip_paths(sf_step['sfStepsToDone']['BundletoJson']['input_path'], flags["transactionId"])
                    sf_step['sfStepsToDone']['BundletoJson']['input_path'] = update_wip_paths(sf_step['sfStepsToDone']['BundletoJson']['input_path'], flags["transactionId"])

                  #  run_type = os.getenv("RUN_TYPE")
                  #  sf_step['sfStepsToDone']['debatchAscii']['run_type'] = run_type
                  #By default masking flag will be false if not provided in the config
                    if "data_masking_enable" not in sf_step['sfStepsToDone']['BundletoJson']:
                        sf_step['sfStepsToDone']['BundletoJson']["data_masking_enable"] = "False"

                    # Set default value for mask_data_file if not present or empty
                    if "mask_data_file" not in sf_step['sfStepsToDone']['BundletoJson']:
                        sf_step['sfStepsToDone']['BundletoJson']["mask_data_file"] = ""

                    if 'input_path'not in sf_step['sfStepsToDone']['BundletoJson'] or 'kafka_topic_prefix' not in \
                            sf_step['sfStepsToDone']['BundletoJson']:
                            splunk.log_message(
                                                    {'Message': "input_path or kafka_topic_prefix is missing from client config.", "BundletoJson": BundletoJson, "Status": "failed"}, context)
                            raise ValueError("input_path or kafka_topic_prefix is missing from client config.")
                    
                    

            if sf_order == "SM_ARN_UNCOMPRESSION":
                flags['key'] = source_key
                flags['bucket'] = bucket_name
                flags['trackrecords'] = track_records
                flags['inputFile'] = source_key
                flags['transactioninputfile'] = source_file
                # object_key.split('/')[-1]
                # flags['uncompress_flag'] = False
                # flags['decrypt_flag'] = False
                update_wip_paths(sf_step['sfStepsToDone']['unzip'], flags["transactionId"])
                if sf_step['sfStepsToDone']['decrypt_flag'] and 'uncompress_flag' not in sf_step['sfStepsToDone']:
                    sf_step['decrypt_flag'] = sf_step['sfStepsToDone']['decrypt_flag']
                    sf_step['input_destination'] = sf_step['sfStepsToDone']['unzip']['inputZipDestination']
                    sf_step['output_destination'] = sf_step['sfStepsToDone']['unzip']['unzipFileDestination']
                sf_step['sfInputFile'] = bucket_name + '/' + source_key
                file_extension = source_file.split('.')[-1]
                sf_step['sfOutputFile'] = bucket_name + '/' + source_file.replace(file_extension, 'dpg')

            if sf_order == "SM_ARN_ADVISOREXTRACTION":
                if 'sfStepsToDone' in sf_step:
                    splunk.log_message({'Message': "sf_order", "sf_order": sf_order, "Status": "logging"}, context)
                    splunk.log_message(
                        {'Message': "sfStepsToDone", "sfStepsToDone": sf_step["sfStepsToDone"], "Status": "logging"},
                        context)
                    # sf_step['sfOutputFile'] = ''
                    if 'createadvisorlist' in sf_step['sfStepsToDone']:
                        # sf_step['sfInputFile'] = flags['inputFileBucket'] + '/' + flags['inputPath'] + flags['fileName']
                        sf_step['sfInputFile'] = bucket_name + '/' + os.path.splitext(flags['key'])[0] + '.txt'
                        # sf_step['sfInputFile'] = flags['inputFileBucket'] + '/' + flags['inputPath'] + os.path.splitext(flags['fileName'])[0] + '.json'
                        sfStepsToDone = sf_step["sfStepsToDone"]
                        createadvisorlist = sfStepsToDone["createadvisorlist"]
                        sql_queries = createadvisorlist["sql_queries"]
                        columns = createadvisorlist["columns"]
                        createadvisorlist['wipPath'] = createadvisorlist['wipPath'] + '/' + flags['transactionId']
                        flags['wipPath'] = createadvisorlist['wipPath']
                        flags['nextsfInputFile'] = ''
                        flags['fileName'] = flags['fileName'].replace(".trigger", ".txt").replace(".TRIGGER", ".txt")
                        sf_step['sfOutputFile'] = flags['wipPath']

                        createadvisorlist['sql_queries'] = json.dumps(sql_queries)
                        createadvisorlist['columns'] = json.dumps(columns)

            if sf_order == "SM_ARN_ACCOUNTEXTRACTION":
                if 'sfStepsToDone' in sf_step:
                    splunk.log_message({'Message': "sf_order", "sf_order": sf_order, "Status": "logging"}, context)
                    splunk.log_message(
                        {'Message': "sfStepsToDone", "sfStepsToDone": sf_step["sfStepsToDone"], "Status": "logging"},
                        context)
                    # sf_step['sfOutputFile'] = ''
                    if 'extractaccounts' in sf_step['sfStepsToDone']:
                        sf_step['sfOutputFile'] = flags['wipPath']
                        sf_step['sfInputFile'] = bucket_name + '/' + os.path.splitext(flags['key'])[0] + '.txt'
                        # sf_step['sfInputFile'] = flags['inputFileBucket'] + '/' + flags['inputPath'] + os.path.splitext(flags['fileName'])[0] + flags['inputExtension']
                        sfStepsToDone = sf_step["sfStepsToDone"]
                        extractaccounts = sfStepsToDone["extractaccounts"]
                        accounts_with_fa = extractaccounts["accounts_with_fa"]
                        accounts_without_fa = extractaccounts["accounts_without_fa"]
                        transaction = extractaccounts["transaction"]
                        driverlayout = extractaccounts["driverlayout"]
                        extractaccounts['accounts_with_fa'] = json.dumps(accounts_with_fa)
                        extractaccounts['accounts_without_fa'] = json.dumps(accounts_without_fa)
                        extractaccounts['transaction'] = json.dumps(transaction)
                        extractaccounts['driverlayout'] = json.dumps(driverlayout)
                        extractaccounts['bucket'] = flags['bucket']
                        extractaccounts['inputFile'] = ''
                        flags['nextsfInputFile'] = ''
                        flags['fileName'] = flags['fileName'].replace(".trigger", ".txt").replace(".TRIGGER", ".txt")

            if sf_order == "SM_ARN_METADATAEXTRACTION":
                if 'sfStepsToDone' in sf_step:
                    splunk.log_message({'Message': "sf_order", "sf_order": sf_order, "Status": "logging"}, context)
                    splunk.log_message(
                        {'Message': "sfStepsToDone", "sfStepsToDone": sf_step["sfStepsToDone"], "Status": "logging"},
                        context)
                    # sf_step['sfOutputFile'] = ''
                    if 'extractmetadata' in sf_step['sfStepsToDone']:
                        # sf_step['sfInputFile'] = flags['inputFileBucket'] + '/' + flags['inputPath'] + flags['fileName']
                        sf_step['sfInputFile'] = bucket_name + '/' + os.path.splitext(flags['key'])[0] + '.txt'
                        # sf_step['sfInputFile'] = flags['inputFileBucket'] + '/' + flags['inputPath'] + os.path.splitext(flags['fileName'])[0] + '.json'
                        sfStepsToDone = sf_step["sfStepsToDone"]
                        extractmetadata = sfStepsToDone["extractmetadata"]
                        sql_queries = extractmetadata["sql_queries"]
                        tableInfo = extractmetadata["tableInfo"]
                        extractmetadata['wipPath'] = extractmetadata['wipPath'] + '/' + flags['transactionId']
                        flags['wipPath'] = extractmetadata['wipPath']
                        flags['nextsfInputFile'] = ''
                        flags['fileName'] = flags['fileName'].replace(".trigger", ".txt").replace(".TRIGGER", ".txt")
                        sf_step['sfOutputFile'] = flags['wipPath']

                        extractmetadata['sql_queries'] = json.dumps(sql_queries)
                        extractmetadata['tableInfo'] = json.dumps(tableInfo)

            if sf_order == "SM_ARN_STATEMENTDATAEXTRACTION":
                if 'sfStepsToDone' in sf_step:
                    splunk.log_message({'Message': "sf_order", "sf_order": sf_order, "Status": "logging"}, context)
                    splunk.log_message(
                        {'Message': "sfStepsToDone", "sfStepsToDone": sf_step["sfStepsToDone"], "Status": "logging"},
                        context)
                    # sf_step['sfOutputFile'] = ''
                    if 'extractaccounts' in sf_step['sfStepsToDone']:
                        sf_step['sfOutputFile'] = flags['wipPath']
                        sf_step['sfInputFile'] = bucket_name + '/' + os.path.splitext(flags['key'])[0] + '.txt'
                        # sf_step['sfInputFile'] = flags['inputFileBucket'] + '/' + flags['inputPath'] + os.path.splitext(flags['fileName'])[0] + flags['inputExtension']
                        sfStepsToDone = sf_step["sfStepsToDone"]
                        extractaccounts = sfStepsToDone["extractaccounts"]
                        run_type = os.getenv("RUN_TYPE")
                        sf_step['sfStepsToDone']['extractaccounts']['runType'] = run_type
                        #sql_queries = extractaccounts["sql_queries"]
                        #tableInfo = extractaccounts["tableInfo"]
                        #transaction = extractaccounts["transaction"]
                        #driverlayout = extractaccounts["driverlayout"]
                        #dataformats = extractaccounts["formats"]
                        #extractaccounts['sql_queries'] = json.dumps(sql_queries)
                        #extractaccounts['transaction'] = json.dumps(transaction)
                        #extractaccounts['driverlayout'] = json.dumps(driverlayout)
                        #extractaccounts['tableInfo'] = json.dumps(tableInfo)
                        #extractaccounts['formats'] = json.dumps(dataformats)
                        extractaccounts['bucket'] = flags['bucket']
                        extractaccounts['inputFile'] = ''
                        flags['nextsfInputFile'] = ''
                        flags['fileName'] = flags['fileName'].replace(".trigger", ".txt").replace(".TRIGGER", ".txt")

            if sf_order == "SM_ARN_BRXEXTRACTION":
                if 'sfStepsToDone' in sf_step:
                    splunk.log_message({'Message': "sf_order", "sf_order": sf_order, "Status": "logging"}, context)
                    splunk.log_message(
                        {'Message': "sfStepsToDone", "sfStepsToDone": sf_step["sfStepsToDone"], "Status": "logging"},
                        context)
                    if 'createbrxextractiondata' in sf_step['sfStepsToDone']:
                        createbrxextractiondata = sf_step['sfStepsToDone']['createbrxextractiondata']
                        sf_step['sfStepsToDone']['createbrxextractiondata']['wipPath'] = \
                            sf_step['sfStepsToDone']['createbrxextractiondata']['wipPath'] + '/' + flags[
                                'transactionId']

                        if 'wipPath' not in createbrxextractiondata:
                            raise ValueError("wipPath is missing in createbrxextractiondata config")

                        flags['wipPath'] = createbrxextractiondata['wipPath']  # Set it here
                        sf_step['sfOutputFile'] = flags['wipPath'] + '/' + flags['transactionId']
                        sf_step['sfInputFile'] = bucket_name + '/' + os.path.splitext(flags['key'])[0] + '.txt'

                        # To fetch the runtype
                        run_type = os.getenv("RUN_TYPE")
                        sf_step['sfStepsToDone']['createbrxextractiondata']['runType'] = run_type

                        createbrxextractiondata['bucket'] = flags['bucket']
                        createbrxextractiondata['inputFile'] = ''
                        flags['nextsfInputFile'] = ''
                        flags['fileName'] = flags['fileName'].replace(".trigger", ".txt").replace(".TRIGGER", ".txt")

            if sf_order == "SM_ARN_CDMEXTRACTION":
                if 'sfStepsToDone' in sf_step:
                    splunk.log_message({'Message': "sf_order", "sf_order": sf_order, "Status": "logging"}, context)
                    splunk.log_message(
                        {'Message': "sfStepsToDone", "sfStepsToDone": sf_step["sfStepsToDone"], "Status": "logging"},
                        context)
                    if 'createbrxextractiondata' in sf_step['sfStepsToDone']:
                        createbrxextractiondata = sf_step['sfStepsToDone']['createbrxextractiondata']
                        sf_step['sfStepsToDone']['createbrxextractiondata']['wipPath'] = \
                            sf_step['sfStepsToDone']['createbrxextractiondata']['wipPath'] + '/' + flags[
                                'transactionId']

                        if 'wipPath' not in createbrxextractiondata:
                            raise ValueError("wipPath is missing in createbrxextractiondata config")

                        flags['wipPath'] = createbrxextractiondata['wipPath']  # Set it here
                        sf_step['sfOutputFile'] = flags['wipPath'] + '/' + flags['transactionId']
                        sf_step['sfInputFile'] = bucket_name + '/' + os.path.splitext(flags['key'])[0] + '.txt'

                        # To fetch the runtype
                        run_type = os.getenv("RUN_TYPE")
                        sf_step['sfStepsToDone']['createbrxextractiondata']['runType'] = run_type

                        createbrxextractiondata['bucket'] = flags['bucket']
                        createbrxextractiondata['inputFile'] = ''
                        flags['nextsfInputFile'] = ''
                        flags['fileName'] = flags['fileName'].replace(".trigger", ".txt").replace(".TRIGGER", ".txt")
                        flags['filePattern'] = sf_info['fileRegex']

            if sf_order == "SM_ARN_DRIVERCREATION":
                if 'sfStepsToDone' in sf_step:
                    splunk.log_message({'Message': "sf_order", "sf_order": sf_order, "Status": "logging"}, context)
                    splunk.log_message(
                        {'Message': "sfStepsToDone", "sfStepsToDone": sf_step["sfStepsToDone"], "Status": "logging"},
                        context)
                    if 'drivercreation' in sf_step['sfStepsToDone']:
                        sf_step['sfOutputFile'] = flags['wipPath']
                        # sf_step['sfInputFile'] = bucket_name + '/' + flags['key']
                        sf_step['sfInputFile'] = bucket_name + '/' + os.path.splitext(flags['key'])[0] + '.txt'
                        # sf_step['sfInputFile'] = flags['inputFileBucket'] + '/' + flags['inputPath'] + os.path.splitext(flags['fileName'])[0] + flags['inputExtension']
                        sfStepsToDone = sf_step["sfStepsToDone"]
                        drivercreation = sfStepsToDone["drivercreation"]
                        sql_queries = drivercreation["sql_queries"]
                        tableInfo = drivercreation["tableInfo"]
                        drivercreation['sql_queries'] = json.dumps(sql_queries)
                        drivercreation['tableInfo'] = json.dumps(tableInfo)

                        flags['fileName'] = flags['fileName'].replace(".trigger", ".txt").replace(".TRIGGER", ".txt")
 
            if sf_order == "SM_ARN_INVOKESMARTCOMM":
                if 'sfStepsToDone' in sf_step and 'invokeProspectusSmartcomm' in sf_step['sfStepsToDone']:
                    update_wip_paths(sf_step['sfStepsToDone'], flags["transactionId"])
                    sf_step['sfInputFile'] = bucket_name + '/' + source_key
                    sf_step['sfOutputFile'] = bucket_name + '/' + source_key
                    flags["name"]="invokeProspectusSmartcomm"
                    flags["sfInputFile"]=sf_step['sfInputFile']
                    flags["sfOutputFile"]=sf_step['sfOutputFile']
                    flags["initStep"]=sf_step["initStep"]
                    flags["endStep"]=sf_step["endStep"]
                    flags["timestampinminutes"] = sf_step["sfStepsToDone"]["invokeProspectusSmartcomm"]["timestampinminutes"]
                    flags["createFileMonitorFlag"]=sf_step["sfStepsToDone"]["invokeProspectusSmartcomm"]["createFileMonitorFlag"]
                    flags["fileExpiryDurationInMinutes"]=sf_step["sfStepsToDone"]["invokeProspectusSmartcomm"]["fileExpiryDurationInMinutes"]
                    flags["nextstepname"] = sf_step["sfStepsToDone"]["invokeProspectusSmartcomm"]["nextstepname"]

            if sf_order == "SM_ARN_ARCHIVAL":
                # flags['archival_source_key'] = flags['fileName'].replace(".trigger", ".txt").replace(".TRIGGER", ".txt")
                sf_step['sfInputFile'] = bucket_name + '/' + flags['archival_source_key']
                #flags['archivalFile'] = flags['fileName'].replace(".trigger", ".txt").replace(".TRIGGER", ".txt")
                flags['archival_destination_key'] = sf_step['archivalDestinationKey']
                sf_step['sfOutputFile'] = bucket_name + '/' + flags['archival_destination_key']
                flags['archival_file_prefix'] = os.path.splitext(flags['key'])[0]
                flags['operation_type'] = "list_and_copy_object"

            if sf_order == "SM_ARN_ORDER_GROUPING":
                file_content = get_file_content(bucket_name, object_key, context)
                if file_content != '':
                    match = re.search(sf_info['fileRegex'], source_file)
                    job_list = [job.strip() for job in
                                file_content.split(',')] if ',' in file_content else [file_content]
                    if sf_info['destinationType'] != {}:
                        if job_list[0] in sf_info['destinationType']:
                            sf_step["sfStepsToDone"]['orderGroupingFunction']['destinationType'] = sf_info['destinationType'][job_list[0]]
                        elif sf_info['destinationType']['regex']:
                            destination_type = get_destination_type(sf_info['destinationType']['regex'], job_list[0])
                            if destination_type is not None:
                                sf_step["sfStepsToDone"]['orderGroupingFunction']['destinationType'] = destination_type
                            else:
                                sf_step["sfStepsToDone"]['orderGroupingFunction']['destinationType'] = sf_info['destinationType']['default']

                cronConfig = sf_info.get("cronConfig", {})
                print(f"cronConfig: {cronConfig}")

                # Convert if cronConfig is a dict or list — not already a string
                if cronConfig is None:
                    print("cronConfig not found, skipping")
                else:
                    if not isinstance(cronConfig, str):
                        print("cronConfig is a list/dict, converting to JSON string")
                        cronConfig_str = json.dumps(cronConfig)
                        sf_step["sfStepsToDone"]['orderGroupingFunction']['sf_cronConfig'] = cronConfig_str
                    else:
                        print("cronConfig is already a string")
                        sf_step["sfStepsToDone"]['orderGroupingFunction']['sf_cronConfig'] = cronConfig


                print(f"sf_cronConfig: {sf_step["sfStepsToDone"]['orderGroupingFunction']['sf_cronConfig']}")

                sf_step['sfInputFile'] = bucket_name + '/' + source_key
                sf_step['sfOutputFile'] = ''
                flags['archival_source_key'] = sf_info['inputPath'] + flags['fileName']
                flags['archivalFile'] = flags['fileName']

            if sf_order == "SM_ARN_DOCUMENT_AGGREGATION":
                if "sfStepsToDone" in sf_step:
                    if "moveCompletedOrderDocuments" in sf_step["sfStepsToDone"]:
                        sf_step["sfStepsToDone"]['moveCompletedOrderDocuments']['outputDir'] = \
                            sf_step["sfStepsToDone"]['moveCompletedOrderDocuments']['outputDir'] + flags[
                                'transactionId'] + '/'
                    if "brxTrigger" in sf_step["sfStepsToDone"]:
                        if 'triggerFileName' in sf_step["sfStepsToDone"]['brxTrigger']:
                            destination_file_split = sf_step["sfStepsToDone"]['brxTrigger']['triggerFileName'].split(
                                '.')
                            destination_file_name = destination_file_split[0] + '_' + flags['transactionId'] + '.' + \
                                                    destination_file_split[-1]
                            sf_step['sfStepsToDone']['brxTrigger']['triggerFileName'] = destination_file_name
                            sf_step['sfOutputFile'] = bucket_name + '/' + sf_step['sfStepsToDone']['brxTrigger'][
                                'destinationKey'] + '/' + destination_file_name
                            sf_step["sfStepsToDone"]['brxTrigger']['brxOutputPath'] = \
                                sf_step["sfStepsToDone"]['brxTrigger']['brxOutputPath'] + flags['transactionId']
                            sf_step["sfStepsToDone"]['brxTrigger']['brxOutputTriggerFile'] = flags['fileName']
                            if os.environ['SDLC_ENV'] == 'prd':
                                environment = 'PROD'
                            else:
                                environment = 'TEST'
                            match = re.search(sf_info['fileRegex'], source_file)
                            if match.group(1) in sf_info['corps']:
                                job_or_corp_id = sf_info['corps'][match.group(1)]
                            else:
                                job_or_corp_id = match.group(1)
                            dt = datetime.datetime.strptime(match.group(3), "%m%d%Y%H%M%S%f")
                            # Format it to YYYYMMDDHHMMSS
                            formatted_dt = dt.strftime("%Y%m%d%H%M%S%f")[:14]
                            sf_step["sfStepsToDone"]['brxTrigger']['onpremOutputFileName'] = flags['clientName'] + '_' + \
                                flags['applicationName'] + \
                                '_' + formatted_dt + '_' + job_or_corp_id + '.' + match.group(2) + '_' + flags['transactionId'] + '.json'
                            sf_step["sfStepsToDone"]['brxTrigger']['jobName'] = job_or_corp_id
                            if sf_info['destinationType'] != {}:
                                if match.group(1) in sf_info['destinationType']:
                                    sf_step["sfStepsToDone"]['brxTrigger']['destinationType'] = \
                                    sf_info['destinationType'][match.group(1)]
                                elif sf_info['destinationType']['regex']:
                                    destination_type = get_destination_type(sf_info['destinationType']['regex'], match.group(1))
                                    if destination_type is not None:
                                        sf_step["sfStepsToDone"]['brxTrigger']['destinationType'] = destination_type
                                    else:
                                        sf_step["sfStepsToDone"]['brxTrigger']['destinationType'] = sf_info['destinationType']['default']
                            if sf_step["sfStepsToDone"]['brxTrigger']['destinationType'] == "marcomm":
                                if sf_info['marcommSLA'] != {} and match.group(1) in sf_info['marcommSLA']:
                                    sf_step["sfStepsToDone"]['brxTrigger']['marcommSLA'] = sf_info['marcommSLA'][match.group(1)]
                                elif sf_info['marcommSLA'] != {} and 'default' in sf_info['marcommSLA']:
                                    sf_step["sfStepsToDone"]['brxTrigger']['marcommSLA'] = sf_info['marcommSLA']['default']
                            elif sf_info['marcommSLA'] != {} and 'default' in sf_info['marcommSLA']:
                                sf_step["sfStepsToDone"]['brxTrigger']['marcommSLA'] = sf_info['marcommSLA']['default']
                if 'secondaryIndexKey' in sf_step:
                    flags['secIndexKeyName'] = sf_step['secondaryIndexKey']
                    match = re.search(sf_info['fileRegex'], source_file)
                    flags['secIndexKeyValue'] = flags['clientName'] + '_' + match.group(3)
                sf_step['sfInputFile'] = bucket_name + '/' + source_key
                flags['archival_source_key'] = sf_info['inputPath'] + flags['fileName']
                flags['archivalFile'] = flags['fileName']

            if sf_order == "SM_ARN_COMPRESSION":
                flags['destinationType'] = ''
                sf_step['sfInputFile'] = bucket_name + '/' + source_key
                if "secondaryIndexKey" in sf_step:
                    sec_index_key_name = sf_step['secondaryIndexKey']
                    dynamo_sec_index = sf_step['globalSecondaryIndex']
                    match = re.search(sf_info['fileRegex'], source_file)
                    index_value = flags['clientName'] + '_' + match.group(2)
                    flags['destinationType'] = match.group(4)
                    response_items = extract_file_content(dynamo_sec_index, sec_index_key_name, index_value)
                    if response_items is not None:
                        flags['fileName'] = response_items[0]['fileName']
                        flags['transactionId'] = response_items[0]['transactionId']
                if 'sfStepsToDone' in sf_step:
                    if os.environ['SDLC_ENV'] == 'prd':
                        environment = 'PROD'
                    else:
                        environment = 'TEST'
                    if "zipFileCreation" in sf_step['sfStepsToDone']:
                        if "zipFileName" in sf_step['sfStepsToDone']['zipFileCreation']:
                            sf_step['sfStepsToDone']['zipFileCreation'][
                                'zipFileName'] = "Principal" + '_' + match.group(2) + '_' + environment
                            flags['compressFileName'] = sf_step['sfStepsToDone']['zipFileCreation']['zipFileName'] + '.zip'
                        sf_step['sfOutputFile'] = bucket_name + '/' + sf_step['sfStepsToDone']['zipFileCreation'][
                            'zipDestination'] + sf_step['sfStepsToDone']['zipFileCreation']['zipFileName'] + '.zip'
                        if "filePathList" in sf_step['sfStepsToDone']['zipFileCreation']:
                            file_path_list = []
                            for path in sf_step['sfStepsToDone']['zipFileCreation']['filePathList']:
                                if 'wip' in path:
                                    file_path_list.append(path + flags['transactionId'])
                                else:
                                    file_path_list.append(path)
                            sf_step['sfStepsToDone']['zipFileCreation']['filePathList'] = file_path_list
                    if 'tarCreation' in sf_step['sfStepsToDone']:
                        if "tarFileName" in sf_step['sfStepsToDone']['tarCreation']:
                            dt = datetime.datetime.strptime(match.group(2), "%m%d%Y%H%M%S%f")
                            # Format it to YYYYMMDDHHMMSS
                            formatted_dt = dt.strftime("%Y%m%d%H%M%S%f")[:14]
                            job, bulk_check = match.group(1).split('.')
                            if job in sf_info['corps']:
                                job_or_corp_id = sf_info['corps'][job]
                            else:
                                job_or_corp_id = job
                            sf_step['sfStepsToDone']['tarCreation']['tarFileName'] = flags['clientName'] + '_' + flags['applicationName'] + \
                             '_' + formatted_dt + '_' + job_or_corp_id + '.' + bulk_check + '_' + flags['transactionId']
                            flags['compressFileName'] = sf_step['sfStepsToDone']['tarCreation']['tarFileName'] + '.tgz'
                            source_file = sf_step['sfStepsToDone']['tarCreation']['tarFileName'] + '.tgz'
                            source_key = sf_step['sfStepsToDone']['tarCreation']['tarDestinationPath'] + \
                                         sf_step['sfStepsToDone']['tarCreation']['tarFileName'] + '.tgz'
                        sf_step['sfOutputFile'] = bucket_name + '/' + sf_step['sfStepsToDone']['tarCreation'][
                            'tarDestinationPath'] + sf_step['sfStepsToDone']['tarCreation']['tarFileName'] + '.tgz'
                        if "filePathList" in sf_step['sfStepsToDone']['tarCreation']:
                            file_path_list = []
                            for path in sf_step['sfStepsToDone']['tarCreation']['filePathList']:
                                if 'wip' in path:
                                    file_path_list.append(path + flags['transactionId'])
                                else:
                                    file_path_list.append(path)
                            sf_step['sfStepsToDone']['tarCreation']['filePathList'] = file_path_list
                        #########   3045_Historical_update   ####################################
            if sf_order == "SM_ARN_HISTORICALUPDATE":
                if 'sfStepsToDone' in sf_step:
                    if 'historicalupdate' in sf_step['sfStepsToDone']:
                        splunk.log_message(
                            {'Message': "sf_order", "sf_order": sf_order, "Status": "logging"},
                            context)
                        splunk.log_message(
                            {'Message': "sfStepsToDone", "sfStepsToDone": sf_step["sfStepsToDone"],
                             "Status": "logging"},
                            context)
                        sfStepsToDone = sf_step["sfStepsToDone"]
                        historicalupdate = sfStepsToDone["historicalupdate"]
                        data_base = historicalupdate["data_base"]
                        schemas = historicalupdate["schemas"]
                        historical = historicalupdate["historical"]
                        ssmdbkey = historicalupdate["ssmdbkey"]
                        # historicalupdate["historical_pre"] = source_key
                        # historicalupdate["historical_pre"] = file_name
                        # print(f"{file_name}")

            if sf_order == "SM_ARN_TRUNCATESCHEMA":
                if 'sfStepsToDone' in sf_step:
                    if 'truncateschema' in sf_step['sfStepsToDone']:
                        splunk.log_message(
                            {'Message': "sf_order", "sf_order": sf_order, "Status": "logging"},
                            context)
                        splunk.log_message(
                            {'Message': "sfStepsToDone", "sfStepsToDone": sf_step["sfStepsToDone"],
                             "Status": "logging"},
                            context)
                        sfStepsToDone = sf_step["sfStepsToDone"]
                        truncateschema = sfStepsToDone["truncateschema"]
                        data_base = truncateschema["data_base"]
                        schemas = truncateschema["schemas"]
                        exclude_tables = truncateschema["exclude_tables"]
                        ssmdbkey = truncateschema["ssmdbkey"]

            if sf_order == "SM_ARN_STAGE":
                splunk.log_message({'Message': "sf_order", "sf_order": sf_order, "Status": "logging"}, context)
                if 'file_path' in sf_step['sfStepsToDone']['StageFiles'] and 'file_pattern' in sf_step['sfStepsToDone']['StageFiles']:
                        sf_step['sfStepsToDone']['StageFiles']['file_key'] = " "
                else:
                    if 'file_key' not in sf_step['sfStepsToDone']['StageFiles']:
                        sf_step['sfStepsToDone']['StageFiles']['file_key'] = source_key
                    sf_step['sfStepsToDone']['StageFiles']['file_path'] = " "
                    sf_step['sfStepsToDone']['StageFiles']['file_pattern'] = " "
                if "include_timestamp" not in sf_step["sfStepsToDone"]["StageFiles"]:
                    sf_step["sfStepsToDone"]["StageFiles"]["include_timestamp"] = True
                if "move_flag" not in sf_step["sfStepsToDone"]["StageFiles"]:
                    sf_step["sfStepsToDone"]["StageFiles"]["move_flag"] = False
                if 'rename_flag' not in sf_step['sfStepsToDone']['StageFiles']:
                    sf_step['sfStepsToDone']['StageFiles']['rename_flag'] = False
                if 'position_insertion' not in sf_step['sfStepsToDone']['StageFiles']:
                    sf_step['sfStepsToDone']['StageFiles']['position_insertion'] = {}
                if 'position_removal' not in sf_step['sfStepsToDone']['StageFiles']:
                    sf_step['sfStepsToDone']['StageFiles']['position_removal'] = []
                update_wip_paths(sf_step['sfStepsToDone'], flags["transactionId"])


            ##################   1492 ###########################################
            if sf_order == "SM_ARN_KAFKA":
                if 'sfStepsToDone' in sf_step:
                    if 'kafka_load' in sf_step['sfStepsToDone']:
                        splunk.log_message(
                            {'Message': "sf_order", "sf_order": sf_order, "Status": "logging"},
                            context)
                        splunk.log_message(
                            {'Message': "sfStepsToDone", "sfStepsToDone": sf_step["sfStepsToDone"],
                             "Status": "logging"},
                            context)
                        sfStepsToDone = sf_step["sfStepsToDone"]
                        kafka_load = sfStepsToDone["kafka_load"]

            #######################################################################

            if sf_order == "SM_ARN_MERGEFILES":
                splunk.log_message({'Message': "sf_order", "sf_order": sf_order, "Status": "logging"}, context)
                if "MergeFiles" in sf_step['sfStepsToDone']:
                    if "MergeFilePrefix" not in sf_step['sfStepsToDone']['MergeFiles']:
                        sf_step['sfStepsToDone']['MergeFiles']["MergeFilePrefix"] = ""
                    if "mergedFileName" not in sf_step['sfStepsToDone']['MergeFiles']:
                        sf_step['sfStepsToDone']['MergeFiles']["mergedFileName"] = ""
                    if "mergeFilePattern" not in sf_step['sfStepsToDone']['MergeFiles']:
                        sf_step['sfStepsToDone']['MergeFiles']["mergeFilePattern"] = ""
                    if "multiplePatternMerge" not in sf_step['sfStepsToDone']['MergeFiles']:
                        sf_step['sfStepsToDone']['MergeFiles']["multiplePatternMerge"] = []
                update_wip_paths(sf_step['sfStepsToDone'], flags["transactionId"])
                if 'mergeCustomAsciiFiles' in sf_step["sfStepsToDone"]:
                    #print(update_wip_paths(sf_step['sfStepsToDone']['mergeCustomAsciiFiles'], flags["transactionId"]))
                    output_path =f"{sf_step['sfStepsToDone']['mergeCustomAsciiFiles']['outputPath']}" 
                    sf_step['sfInputFile'] = bucket_name + '/' + source_key
                    sf_step['sfOutputFile'] = f"{bucket_name}/{output_path}"

            if sf_order == "SM_ARN_INVOKE_ORDER_STATUS":
                sf_step['sfInputFile'] = ''
                sf_step['sfOutputFile'] = ''
                

            if sf_order == "SM_ARN_REPORTING":
                splunk.log_message({'Message': "sf_order", "sf_order": sf_order, "Status": "logging"}, context)
                splunk.log_message(
                    {'Message': "sfStepsToDone", "sfStepsToDone": sf_step["sfStepsToDone"], "Status": "logging"},
                    context)
                
                if 'reportingChecks' in sf_step["sfStepsToDone"]:
                        update_wip_paths(sf_step['sfStepsToDone']['reportingChecks'], flags["transactionId"])
                        report_path =f"{sf_step['sfStepsToDone']['reportingChecks']['reportPathList'][0]}" 
                        # Add transaction_id to jsonPath if it contains 'wip'
                        # update_wip_paths(sf_step['sfStepsToDone']['reportingChecks'], flags["transactionId"])
                        if 'inputPathList' in sf_step['sfStepsToDone']['reportingChecks'] and 'inputPatternList' in \
                                sf_step['sfStepsToDone']['reportingChecks']:
                            sf_step['sfStepsToDone']['reportingChecks']['inputKeyList'] = []
                        else:
                            if 'inputKeyList' not in sf_step['sfStepsToDone']['reportingChecks']:
                                sf_step['sfStepsToDone']['reportingChecks']['inputKeyList'] = [source_key]
                            sf_step['sfStepsToDone']['reportingChecks']['inputPathList'] = []
                            sf_step['sfStepsToDone']['reportingChecks']['inputPatternList'] = []
                        sf_step['sfInputFile'] = bucket_name + '/' + source_key
                        sf_step['sfOutputFile'] = f"{bucket_name}/{report_path}"
                if 'reportingConfirms' in sf_step["sfStepsToDone"]:
                        output_s3_path =f"{sf_step['sfStepsToDone']['reportingConfirms']['output_s3_path']}"
                        update_wip_paths(sf_step['sfStepsToDone']['reportingConfirms'], flags["transactionId"])
                        # sf_step['sfInputFile'] = bucket_name + '/' + source_key
                        # sf_step['sfOutputFile'] = f"{bucket_name}/{output_s3_path}"ss
                        sf_step['sfStepsToDone']['reportingConfirms']['sfInputFile'] = f"{bucket_name}/{source_key}"
                        sf_step['sfStepsToDone']['reportingConfirms']['sfOutputFile'] = f"{bucket_name}/{sf_step['sfStepsToDone']['reportingConfirms']['output_s3_path']}"
                        sf_step['sfInputFile'] = bucket_name + '/' + source_key
                        sf_step['sfOutputFile'] = f"{bucket_name}/{output_s3_path}"

                if "reportingProspectus" in sf_step["sfStepsToDone"]:
                    if "xml_file_key" not in sf_step["sfStepsToDone"]["reportingProspectus"]:
                        sf_step["sfStepsToDone"]["reportingProspectus"]["xml_file_key"] = source_key

                    # xml_file_key = sf_step["sfStepsToDone"]["reportingProspectus"]["xml_file_key"]
                    print("xml_file_key", sf_step["sfStepsToDone"]["reportingProspectus"]["xml_file_key"])

                    update_wip_paths(sf_step["sfStepsToDone"]["reportingProspectus"], flags["transactionId"])
                    # sf_step["sfInputFile"] = f"{bucket_name}/{source_key}"
                    # sf_step["sfOutputFile"] = f"{bucket_name}/Janus_Prospectus_report"
                    sf_step['sfStepsToDone']['reportingProspectus']['sfInputFile'] = f"{bucket_name}/{source_key}"
                    sf_step['sfStepsToDone']['reportingProspectus']['sfOutputFile'] = f"{bucket_name}/Janus_Prospectus_report"
                if "reporting_mail" in sf_step["sfStepsToDone"]:
                    if "include_env" in sf_step["sfStepsToDone"]["reporting_mail"] :
                        if sf_step["sfStepsToDone"]["reporting_mail"]["include_env"]:
                            sf_step["sfStepsToDone"]["reporting_mail"]["subject"] =  os.getenv('SDLC_ENV').upper() + " : " +sf_step["sfStepsToDone"]["reporting_mail"]["subject"]
                    if "include_corp_details" in sf_step["sfStepsToDone"]["reporting_mail"] :
                        if sf_step["sfStepsToDone"]["reporting_mail"]["include_corp_details"]:
                            corp_delimiter = sf_step["sfStepsToDone"]["reporting_mail"]["corp_delimiter"]
                            corp_data = file_name.split(corp_delimiter)
                            corp_index = sf_step["sfStepsToDone"]["reporting_mail"]["corp_index"]
                            cycle_index = sf_step["sfStepsToDone"]["reporting_mail"]["cycle_index"]
                            corp_value = corp_data[corp_index]  
                            cycle_value = corp_data[cycle_index]
                            month = datetime.datetime.now().month
                            corp_message = "Corp    :  {}  Cycle :  {}  Month :  {}".format(corp_value, cycle_value, month)
                            sf_step["sfStepsToDone"]["reporting_mail"]["body_text"] = sf_step["sfStepsToDone"]["reporting_mail"]["body_text"] + "\n" + corp_message
                    for attachment in sf_step["sfStepsToDone"]["reporting_mail"]["attachment_files"]:
                        if 'file_path' in attachment and 'file_regex' in attachment:
                            attachment['file_key'] = " "
                        else:
                            if 'file_key' not in attachment:
                                attachment['file_key'] = source_key
                            attachment['file_path'] = " "
                            attachment['file_regex'] = " "
                    update_wip_paths(sf_step["sfStepsToDone"]["reporting_mail"], flags["transactionId"])

                if "reportingProspectusLambda" in sf_step["sfStepsToDone"]:
                    update_wip_paths(sf_step["sfStepsToDone"]["reportingProspectusLambda"], flags["transactionId"])
                    if "csv_key" not in sf_step["sfStepsToDone"]["reportingProspectusLambda"]:
                        sf_step["sfStepsToDone"]["reportingProspectusLambda"]["csv_key"] = source_key
                    sf_step["sfInputFile"] = f"{bucket_name}/{source_key}"
                    sf_step["sfOutputFile"] = f"{bucket_name}/Janus_Prospectus_updated_report"
                
                if 'reportingJanusTaxes' in sf_step["sfStepsToDone"]:
                    output_s3_path =f"{sf_step['sfStepsToDone']['reportingJanusTaxes']['output_path']}"
                    update_wip_paths(sf_step['sfStepsToDone']['reportingJanusTaxes'], flags["transactionId"])
                    if 'ACKEmail' in sf_step["sfStepsToDone"]:
                        if 'return_template_flag' not in sf_step['sfStepsToDone']['ACKEmail']:
                            sf_step['sfStepsToDone']['ACKEmail']['return_template_flag'] = False
                        if 'template_key' not in sf_step['sfStepsToDone']['ACKEmail']:
                            sf_step['sfStepsToDone']['ACKEmail']['template_key'] = ""
                        if 'report_data_key' not in sf_step['sfStepsToDone']['ACKEmail']:
                            sf_step['sfStepsToDone']['ACKEmail']['report_data_key'] = ""
                        update_wip_paths(sf_step['sfStepsToDone']['ACKEmail'], flags["transactionId"])
                    sf_step['sfInputFile'] = bucket_name + '/' + source_key
                    sf_step['sfOutputFile'] = f"{bucket_name}/{output_s3_path}"
                    if 'summary_report_file_name' not in sf_step['sfStepsToDone']['reportingJanusTaxes']:
                            sf_step['sfStepsToDone']['reportingJanusTaxes']['summary_report_file_name'] = "False"
                    if 'production_report_file_name' not in sf_step['sfStepsToDone']['reportingJanusTaxes']:
                            sf_step['sfStepsToDone']['reportingJanusTaxes']['production_report_file_name'] = "False"
                    if 'recon_report_file_name' not in sf_step['sfStepsToDone']['reportingJanusTaxes']:
                            sf_step['sfStepsToDone']['reportingJanusTaxes']['recon_report_file_name'] = "False"
                    if 'file_fields_order' not in sf_step['sfStepsToDone']['reportingJanusTaxes']:
                            sf_step['sfStepsToDone']['reportingJanusTaxes']['file_fields_order'] = {}
                    if 'csv_file_pattern' not in sf_step['sfStepsToDone']['reportingJanusTaxes']:
                            sf_step['sfStepsToDone']['reportingJanusTaxes']['csv_file_pattern'] = "False"
                    if 'file_type' not in sf_step['sfStepsToDone']['reportingJanusTaxes']:
                            sf_step['sfStepsToDone']['reportingJanusTaxes']['file_type'] = "False"
                
                if 'reportingJanusStmt' in sf_step["sfStepsToDone"]:
                    output_s3_path =f"{sf_step['sfStepsToDone']['reportingJanusStmt']['output_path']}"
                    update_wip_paths(sf_step['sfStepsToDone']['reportingJanusStmt'], flags["transactionId"])
                    if 'ACKEmail' in sf_step["sfStepsToDone"]:
                        if 'return_template_flag' not in sf_step['sfStepsToDone']['ACKEmail']:
                            sf_step['sfStepsToDone']['ACKEmail']['return_template_flag'] = False
                        if 'template_key' not in sf_step['sfStepsToDone']['ACKEmail']:
                            sf_step['sfStepsToDone']['ACKEmail']['template_key'] = ""
                        if 'report_data_key' not in sf_step['sfStepsToDone']['ACKEmail']:
                            sf_step['sfStepsToDone']['ACKEmail']['report_data_key'] = ""
                        update_wip_paths(sf_step['sfStepsToDone']['ACKEmail'], flags["transactionId"])
                    sf_step['sfInputFile'] = bucket_name + '/' + source_key
                    sf_step['sfOutputFile'] = f"{bucket_name}/{output_s3_path}"
                    if 'file_fields_order' not in sf_step['sfStepsToDone']['reportingJanusStmt']:
                            sf_step['sfStepsToDone']['reportingJanusStmt']['file_fields_order'] = {}

            if sf_order == "SM_ARN_BRXADAPTER":
                for key,value in sf_step["sfStepsToDone"].items():
                    if key in ["image_tag","task_revision"]:
                        sf_step["sfStepsToDone"][key] = value
                    else:
                        sf_step["sfStepsToDone"][key] = f'--{key}=\"{value}\"'
  
                flags['cluster'] = os.getenv("BRX_ADAPTER_CLUSTER")
                flags['task_definition'] = f"{os.getenv('BRX_ADAPTER_TASK_DEFINITION')}_{sf_step['sfStepsToDone']['image_tag']}:{str(sf_step['sfStepsToDone']['task_revision'])}"
                sf_step["sfStepsToDone"].update({"s3_bucket": f'--s3_bucket=\"{bucket_name}\"',"client_file_key" : f'--client_file_key=\"{object_key}\"'})
                
                update_wip_paths(sf_step['sfStepsToDone'], flags["transactionId"])
                
            if 'order' in sf_step and sf_step['order'] != "" and sf_step['order'] == "1":
                init_sf_key = os.getenv(sf_step['sfKey'])

            if 'sfKey' in sf_step and sf_step['sfKey'] != "":
                sf_step['sfKey'] = os.getenv(sf_step['sfKey'])

            if 'sfNextKey' in sf_step and sf_step['sfNextKey'] != "":
                sf_step['sfNextKey'] = os.getenv(sf_step['sfNextKey'])

            sf_steps_to_done = sf_step
            flags[sf_order] = sf_steps_to_done
            flags[sf_order]["sf_step_name"] = sf_step_name
            flags['inputPath'] = sf_info['inputPath']

        RetryParams = {
            "maxAttempts": 100,
            "backoffRate": 2,
            "intervalSeconds": 60,
            "attemptCounter": 0
        }

        sf_input = {"StatePayload": flags, "RetryParams": RetryParams}

        STATE_MACHINE_ARN = init_sf_key

        splunk.log_message({'Message': "sf_input - ", "INPUT": sf_input, "Status": "logging"}, context)
        splunk.log_message({'Message': "init_sf_key - ", "KEY": init_sf_key, "Status": "logging"}, context)

        # triggering stepfunction
        sf_response = stepfunctions.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            input=json.dumps(sf_input)
        )

        # sqs_client = boto3.client('sqs')
        # SQS_QUEUE_URL='https://sqs.us-east-1.amazonaws.com/187777304606/dpmdev-di-sqs-json_validation_against_jsonschema'
        # message_body = {
        #     "STATE_MACHINE_ARN": STATE_MACHINE_ARN,
        #     "payload": json.dumps(sf_input)
        # }
        # sf_response = sqs_client.send_message(
        #     QueueUrl=SQS_QUEUE_URL,
        #     MessageBody=json.dumps(message_body)
        # )

        status_code = sf_response.get('ResponseMetadata', {}).get('HTTPStatusCode')

        return status_code

    except Exception as e:
        raise Exception("Error in triggering Step Function: " + str(e)) from e


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
        applicationName = config_data['applicationName']
        runType = config_data['runType']
        fileType = config_data['fileType']
        destinationType = {}
        marcommSLA = {}
        cronConfig = {}
        teamsId = " "
        corps = {}
        if 'destinationType' in config_data:
            destinationType = config_data['destinationType']
        if 'corps' in config_data:
            corps = config_data['corps']
        if 'marcommSLA' in config_data:
            marcommSLA = config_data['marcommSLA']
        if 'cronConfig' in config_data:
            cronConfig = config_data['cronConfig']
        inputPath = config_data['inputPath']
        multifileData = config_data.get('multifileData')
        if multifileData is not None:
            result['multifileData'] = multifileData
            pass
        multifileTableName = config_data.get('multifileTableName')
        if multifileTableName is not None:
            result['multifileTableName'] = multifileTableName
            pass
        periodDateRange = config_data.get('periodDateRange')
        if periodDateRange is not None:
            result['periodDateRange'] = periodDateRange
            pass
        if 'teamsId' in config_data:
            teamsId = config_data['teamsId']

        file_config = config_data.get('filesToProcess', [])[0]
        # new object with StepFunction, sf_key, sf_key_next, and fileType
        print('file_config::', file_config)
        print('clientID::', clientId, runType, inputPath)
        result.update({
            'clientId': clientId,
            'applicationName': applicationName,
            'teamsId': teamsId,
            'runType': runType,
            'fileType': fileType,
            'destinationType': destinationType,
            'corps': corps,
            'marcommSLA': marcommSLA,
            'cronConfig': cronConfig,
            'inputPath': inputPath,
            'StepFunction': file_config['StepFunction'],
            'groupStep': file_config['StepFunction'][0]['name'],
            'fileRegex': file_config['filename']
        })
        return result
    except Exception as e:
        return result


def extract_date_time(file_name):
    '''
    function to retrieve date and time from file name
    '''
    if "-" in file_name:
        base_name_pattern = r'(\d{4}-\d{2}-\d{2}T\d{6})'

        match = re.search(base_name_pattern, file_name)
        if not match:
            return None
        datetime = match.group(1)
        date = datetime[:4] + datetime[4:2] + datetime[6:2]
        time = datetime[9:]
    else:
        base_name_pattern = r'(\d{14})'
        match = re.search(base_name_pattern, file_name)
        if not match:
            return None
        datetime = match.group(1)
        date = datetime[:8]
        time = datetime[8:]

    return date, time


def fetch_xml_tag(bucket, key, xml_tag_name):
    '''
    function to fetch the xml tag value from xml file
    '''

    s3_client.download_file(bucket, key, '/tmp/xml_file.xml')

    start_tag = f"<{xml_tag_name}>"
    end_tag = f"</{xml_tag_name}>"

    # Read file line by line
    with open('/tmp/xml_file.xml', 'r') as file:
        for line in file:
            if xml_tag_name in line:
                xml_tag_value = line.split(start_tag)[1].split(end_tag)[0]
                # Optional: Delete the temporary file
                os.remove('/tmp/xml_file.xml')
                return xml_tag_value
    # Optional: Delete the temporary file
    os.remove('/tmp/xml_file.xml')
    return ''


def extract_file_content(index_name, index_key, index_value):
    '''
    function to retrieve the xml file name from dynamodb
    '''

    dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table(os.getenv('TRANSACTION_TABLE'))

    response = table.query(
        IndexName=index_name,
        KeyConditionExpression=Key(index_key).eq(index_value)
    )

    if len(response['Items']) != 0:
        return response['Items']

    return None


def get_json_string(json_object, file_name, context):
    '''
    function to convert json to string
    '''
    try:
        if json_object:
            return json.dumps(json_object)
        else:
            raise ValueError("json object passed in as the parameter is null or empty ")

    except Exception as e:
        message = "Failed to convert the json object to a tring: " + json_object
        splunk.log_message({'Message': message, "FileName": file_name, "Status": "failed"}, context)
        raise ValueError(message)


# function to find the matching file for tax files
def find_matching_file(bucket_name, path, file_name):
    '''
    function to extract matching file from the specified path
    '''

    base_name = ''

    base_name = file_name.rsplit('.', 1)[0]
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
        if base_name in obj['Key']:
            file = obj['Key'].split('/')[-1]
            return file

    return None


# function to fmove file to failed folder
def move_file_to_failed(bucket_name, input_path, output_path, file_name, context):
    '''
    function to move the file which triggered the process to the failed folder
    '''
    try:
        # Get the list of objects in the S3 bucket at the specified path
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=input_path)

        # Check if 'Contents' is in the response (it's not if the path is empty)
        if 'Contents' not in response:
            message = f'{bucket_name} is empty'
            splunk.log_message({'Status': 'failed', 'Message': message}, context)
            raise KeyError(message)

        found = False

        # Loop through all files in the specified path
        for obj in response['Contents']:
            if file_name in obj['Key']:
                # copy the source file from the source S3 bucket
                found = True
                source_key = input_path + file_name
                destination_key = output_path + file_name
                copy_source = {'Bucket': bucket_name, 'Key': source_key}
                response = s3_client.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=destination_key)

                # Delete the source file from the source S3 bucket
                s3_client.delete_object(Bucket=bucket_name, Key=source_key)
                message = f'{file_name} is moved from {input_path} to {output_path} within {bucket_name}'
                splunk.log_message({'Status': 'success', 'Message': message}, context)

        if not found:
            message = f'{file_name} not found in {input_path}'
            splunk.log_message({'Status': 'failed', 'Message': message}, context)
            raise KeyError(message)
    except Exception as e:
        message = f'failed to move {file_name} from {input_path} to {output_path} within {bucket_name} due to: {str(e)}'
        splunk.log_message({'Status': 'failed', 'Message': message}, context)
        raise Exception(message)

    # function to update wip with wip/transaction_id


def update_wip_paths(obj, transaction_id):
    if isinstance(obj, dict):
        for key, value in obj.items():
            if isinstance(value, str) and "wip/" in value:
                obj[key] = value.replace("wip/", f"wip/{transaction_id}/")
            else:
                update_wip_paths(value, transaction_id)
    elif isinstance(obj, list):
        for index, item in enumerate(obj):
            if isinstance(item, str) and "wip/" in item:
                obj[index] = item.replace("wip/", f"wip/{transaction_id}/")
            else:
                update_wip_paths(item, transaction_id)

    elif isinstance(obj, str) and "wip/" in obj:
        obj = obj.replace("wip/", f"wip/{transaction_id}/")
    return obj


# Function to get file content from s3 bucket
def get_file_content(bucket_name, object_key, context):
    file_content = ''
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        file_content = response['Body'].read().decode('utf-8').strip()
        return file_content
    except Exception as e:
        splunk.log_message(
            {"Message": f"Error fetching file content from {object_key} from {bucket_name} bucket: {str(e)}"}, context)
        return file_content

# Function to get the destination type for a regex match
def get_destination_type(regex_map, job_name):
    for key, value in regex_map.items():
        if re.match(key, job_name):
            return value
    return None

def build_run_id(file_name, grp_name, sgrp_name,sgrp_pattern, run_template):
    try:
        m = re.compile(sgrp_pattern).match(file_name)
        if not m:
            return None
        rid = run_template
        for i, grp in enumerate(m.groups()[1:], start=1):
            rid = rid.replace(f"${i}", grp)
        rid = rid.replace("$GN", grp_name)
        sgrp_run_id = rid.replace("$SGN", sgrp_name)
        grp_run_id = ".".join(rid.split('.')[:-1])
        return grp_run_id,sgrp_run_id
    except Exception as e:
        raise ValueError("Failed to build file run ID: " + str(e))

def dynamodb_record_preparation(**kwrgs):
    try:
        record_data = {}
        record_data["id"] = kwrgs.get("id","")
        record_data["client"] = kwrgs.get("client","")
        record_data["product"] = kwrgs.get("product","")
        record_data["bucket"] = kwrgs.get("bucket","")
        record_data["object_key"] = kwrgs.get("object_key","")
        record_data["file_name"] = kwrgs.get("file_name","")
        
        record_data["grp_id"] = kwrgs.get("grp_id","")
        record_data["grp_name"] = kwrgs.get("grp_name","")
        record_data["grp_count"] = kwrgs.get("grp_count","")
        record_data["grp_run_id"] = kwrgs.get("grp_run_id","")
        record_data["grp_status"] = kwrgs.get("grp_status","")
        record_data["grp_merge_status"] = kwrgs.get("grp_merge_status","")
        record_data["grp_actual_count"] = kwrgs.get("grp_actual_count","")
        
        record_data["sgrp_id"] = kwrgs.get("sgrp_id","")
        record_data["sgrp_name"] = kwrgs.get("sgrp_name","")
        record_data["sgrp_count"] = kwrgs.get("sgrp_count","")
        record_data["sgrp_pattern"] = kwrgs.get("sgrp_pattern","")
        record_data["sgrp_run_id"] = kwrgs.get("sgrp_run_id","")
        record_data["sgrp_status"] = kwrgs.get("sgrp_status","")
        record_data["sgrp_actual_count"] = kwrgs.get("sgrp_actual_count","")
        
        record_data["created_at"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        record_data["updated_at"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        record_data["wait_time"] = kwrgs.get("wait_time","")
        record_data["content_merge_status"] = kwrgs.get("content_merge_status","")
        record_data["grp_incomplete_action"] = kwrgs.get("grp_incomplete_action","")
        record_data["grp_complete_action"] = kwrgs.get("grp_complete_action","")
        record_data["output_prefix"] = kwrgs.get("grp_merge_path","")
        record_data["grp_initial_status"] = kwrgs.get("grp_initial_status","")
        record_data["grp_completion_status"] = kwrgs.get("grp_completion_status","")

        record_data["schedule"] = kwrgs.get("schedule", "")
        record_data["filetype"] = kwrgs.get("filetype", "")
        return record_data
    except Exception as e:
        print(f"[Error] Data Preparation Error : {e}","")
        raise e

def update_group_status(objFM,filter_field,filter_value, update_field, update_value,expected_field, expected_value,context):
    try:
        print(f"filter_field : {filter_field} | filter_value : {filter_value} | update_field : {update_field} | update_value: {update_value} |\
        expected_field : {expected_field} | expected_value : {expected_value}")
        objFM.update_all_group_items(filter_field,filter_value, update_field, update_value,expected_field, expected_value)
    except Exception as e:
        splunk.log_message({'Message': f"[Error] Getting Error  While Updating Status: {str(e)}", "Status": "failed"}, context)
        raise e

def send_file_to_file_monitoring(fm_table_dynamo,file_monitoring_config,bucket_name,object_key,file_name,client_name,product_name,context):
        try:
            objFM = dbhandler.DynamoDBHandler(fm_table_dynamo)
            splunk.log_message({'Message': "Sending File to File Monitoring", "FileName": file_name, "Status": "logging"}, context)
            file_groups = file_monitoring_config.get("file_groups",[])
            is_group_file_match = False
            for group in file_groups:
                    grp_id = group.get("grp_id")
                    grp_name = group.get("grp_name")
                    grp_list = group.get("grp_list", [])
                    wait_time = group.get("wait_time",0)
                    grp_incomplete_action = group.get("grp_incomplete_action", "")
                    grp_complete_action = group.get("grp_complete_action", "")
                    grp_count = group.get("grp_count", 0) 
                    grp_initial_status = group.get("grp_initial_status", "pending")
                    grp_merge_path = group.get("grp_merge_path", "merge/")
                    grp_completion_status = group.get("grp_completion_status", "completed")
                    output_prefix = group.get("grp_merge_path", "monitoring/merged/")
                    grp_merge_status = grp_initial_status
                    content_merge_status = grp_initial_status
                    schedule = group.get("schedule", "")
                    filetype = group.get("filetype", "")
                    # get matching sub group files
                    sub_group_matching_file = [group for group in grp_list if re.match(group['sgrp_pattern'],file_name)]
                    if sub_group_matching_file:
                        is_group_file_match = True
                        sub_group = sub_group_matching_file[0]
                        record_data = {}
                        sgrp_id = sub_group.get("sgrp_id", "")
                        sgrp_pattern = sub_group.get("sgrp_pattern")
                        sgrp_count = sub_group.get("sgrp_count",0)
                        sgrp_name = sub_group.get("sgrp_name",0)
                        comp_pattern = re.compile(sgrp_pattern)
                        unique_id = len(objFM.fetch_all_items())
                        if comp_pattern.match(file_name) and sgrp_id and sgrp_count or sgrp_name:
                            try:
                                grp_status = grp_initial_status
                                sgrp_status = grp_initial_status
                                # bulding run and sub group run id
                                grp_run_id, sgrp_run_id = build_run_id(file_name, grp_name, sgrp_name,sgrp_pattern, sub_group.get("sgrp_run_id", ""))

                                # get records on sgrp run id
                                grp_records_all = objFM.fetch_items_on_field("grp_run_id",grp_run_id)
                                if grp_records_all:
                                    # get not completed records on group status
                                    grp_records_pending = [i for i in grp_records_all if i.get("grp_status") != grp_completion_status]
                                    # get records on sgrp run id
                                    sgrp_records_all = objFM.fetch_items_on_field("sgrp_run_id",sgrp_run_id)
                                    # get not completed records on sub group status
                                    if not grp_records_pending:
                                        grp_records_pending = [{}]
                                    max_grp_actual_count = max(record.get("grp_actual_count", 0) for record in grp_records_pending)
                                    if sgrp_records_all:
                                        sgrp_records_pending = [i for i in sgrp_records_all if i.get("sgrp_status") != grp_completion_status]
                                        if sgrp_records_pending:
                                            max_sgrp_actual_count = max(record.get("sgrp_actual_count", 0) for record in sgrp_records_pending)
                                            sgrp_actual_count = max_sgrp_actual_count + 1
                                        else:
                                            if not (schedule and filetype == 'DLY'):
                                                continue
                                    else:
                                        sgrp_actual_count = 1
                                    grp_actual_count = max_grp_actual_count +1
                                else:
                                    grp_actual_count = 1
                                    sgrp_actual_count = 1
                                
                                if schedule and filetype == 'DLY':
                                    grp_actual_count = 1
                                    sgrp_actual_count = 1
                                
                                record_data = dynamodb_record_preparation(
                                        id = unique_id,
                                        client = client_name,
                                        product = product_name,
                                        bucket = bucket_name,
                                        object_key = object_key,
                                        file_name = file_name,
                                        grp_id = grp_id,
                                        grp_name = grp_name,
                                        grp_count = grp_count,
                                        grp_run_id = grp_run_id,
                                        grp_status = grp_status,
                                        grp_merge_status = grp_merge_status,
                                        grp_actual_count = grp_actual_count,
                                        sgrp_actual_count = sgrp_actual_count,
                                        sgrp_id = sgrp_id,
                                        sgrp_name = sgrp_name,
                                        sgrp_count = sgrp_count,
                                        sgrp_pattern = sgrp_pattern,
                                        sgrp_run_id = sgrp_run_id,
                                        sgrp_status = sgrp_status,
                                        wait_time = wait_time,
                                        grp_incomplete_action = grp_incomplete_action,
                                        grp_complete_action = grp_complete_action,
                                        grp_merge_path = grp_merge_path,
                                        content_merge_status = content_merge_status,
                                        output_prefix = output_prefix,
                                        grp_initial_status = grp_initial_status,
                                        grp_completion_status = grp_completion_status,
                                        schedule = schedule,
                                        filetype = filetype
                                        )
                                print(f"New record inserting for group : {grp_run_id} sub group : {sgrp_run_id}")
                                objFM.insert_item(record_data)
                                # time.sleep(1)

                                if sgrp_actual_count == sgrp_count:
                                    update_group_status(objFM,"sgrp_run_id",sgrp_run_id, "sgrp_status", grp_completion_status,"sgrp_status", grp_initial_status,context)
                        
                                if grp_actual_count == grp_count:
                                    update_group_status(objFM,"grp_run_id",grp_run_id, "grp_status", grp_completion_status,"grp_status", grp_initial_status,context)
                                    # status = grp_completion_status
                                    splunk.log_message({'Message': "File Group Completed", "Status": "logging"}, context)
                            except Exception as e:
                                splunk.log_message({'Message': str(e), "Status": "failed"}, context)
                                raise e
                        else:
                            splunk.log_message({'Message': "File Group not found", "Status": "logging"}, context)
                            print(f"matching file or {sgrp_id} or {sgrp_count} or {sgrp_name} missisng")    
            return is_group_file_match
        except Exception as e:
            splunk.log_message({'Message': str(e), "Status": "failed"}, context)
            raise e