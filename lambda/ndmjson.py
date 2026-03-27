import boto3
import json
import os
import re
import io
import traceback
from dpm_splunk_logger_py import splunk
from datetime import datetime, timezone
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig

#initialize s3 client
# dynamodb = boto3.client("dynamodb")
s3_client = boto3.client('s3')

# Get the table name from environment variable

env = os.environ['SDLC_ENV']

all_order_ids = []
client_name = None
clientID = None
application_name = None
execution_name = None
execution_id = None
execution_starttime = None
statemachine_name = None
statemachine_id = None
state_name = None
state_enteredtime = None
transactionId = None
teamsId = ""
source_path = None
dest_path = None
compress_file_name = ""

def get_run_id():
    try:
        account_id = boto3.client("sts").get_caller_identity()["Account"]
        return f"arn:dpm:lambda:ndmjson:{account_id}"
    except Exception as e:
        print(f"[ERROR] Failed to get run id/account: {str(e)}")
        raise e

def is_valid_email(email: str) -> bool:
    """Return True if email looks like a valid address."""
    if not email:
        return False
    email_regex = r"^[\w\.-]+@[\w\.-]+\.\w+$"
    return re.match(email_regex, email) is not None

def log_event(status, message, context):

    job_name = "lmbd_ndmjson"
    if statemachine_name:
        parts = statemachine_name.split("-")
        if len(parts) >= 3:
            prefix = "_".join(parts[0:3])
            job_name = f"{prefix}_{job_name}"

    event_data = {
        "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "transactionId": transactionId,
        "clientName": client_name,
        "clientID": clientID,
        "applicationName": application_name,
        "order_ids": all_order_ids if "all_order_ids" in globals() else [],
        "step_function": "ndmtoonprem",
        "component": "lambda",
        "job_name": job_name,
        "job_run_id": get_run_id(),
        "compressed_file": compress_file_name,
        "source_path": source_path,
        "dest_path": dest_path,
        "execution_name": execution_name,
        "execution_id": execution_id,
        "execution_starttime": execution_starttime,
        "statemachine_name": statemachine_name,
        "statemachine_id": statemachine_id,
        "state_name": state_name,
        "state_enteredtime": state_enteredtime,
        "status": status,
        "message": message,
        "aws_account_id": boto3.client("sts").get_caller_identity()["Account"],
    }

    print(json.dumps(event_data))

    try:
        splunk.log_message(event_data, context)
    except Exception as e:
        print(f"[ERROR] lmbd_ndmjson : log_event : Failed to write log to Splunk: {str(e)}")

    if status == "FAILED" and is_valid_email(teamsId):

        lambda_link = (
            f"https://us-east-1.console.aws.amazon.com/lambda/home?region=us-east-1"
            f"#/functions/{event_data['job_name']}"
        )

        stepfn_link = (
            f"https://us-east-1.console.aws.amazon.com/states/home?region=us-east-1"
            f"#/v2/executions/details/{event_data['execution_id']}"
        )

        subject = f"[ALERT] Lambda Function Failure - {event_data['job_name']} ({status})"

        body_text = (
            f"Lambda job failed.\n\n"
            f"Event Time: {event_data['event_time']}\n"
            f"Client: {event_data['clientName']} ({event_data['clientID']})\n"
            f"Job: {event_data['job_name']}\n"
            f"Order Ids: {event_data['order_ids']}\n"
            f"TransactionId: {event_data['transactionId']}\n"
            f"Status: {event_data['status']}\n"
            f"Message: {event_data['message']}\n"
            f"Lambda Function: {lambda_link}\n"
            f"Step Function Execution: {stepfn_link}\n"
        )

        body_html = f"""<html>
        <body>
          <h3>Lambda Job Failure Notification</h3>
          <ul>
            <li><b>Event Time:</b> {event_data['event_time']}</li>
            <li><b>Client:</b> {event_data['clientName']} ({event_data['clientID']})</li>
            <li><b>Job Name:</b> {event_data['job_name']}</li>
            <li><b>Order Ids:</b> {event_data['order_ids']}</li>
            <li><b>TransactionId:</b> {event_data['transactionId']}</li>
            <li><b>Status:</b> {event_data['status']}</li>
            <li><b>Message:</b> {event_data['message']}</li>
          </ul>
          <p><a href="{lambda_link}">View Lambda Function</a></p>
          <p><a href="{stepfn_link}">View Step Function Execution</a></p>
        </body>
        </html>"""

        sender = os.getenv("ALERT_SENDER_EMAIL", "noreply@broadridge.com")
        ses_client = boto3.client("ses", region_name="us-east-1")

        try:
            response = ses_client.send_email(
                Destination={"ToAddresses": [teamsId]},
                Message={
                    "Body": {
                        "Html": {"Charset": "UTF-8", "Data": body_html},
                        "Text": {"Charset": "UTF-8", "Data": body_text},
                    },
                    "Subject": {"Charset": "UTF-8", "Data": subject},
                },
                Source=sender,
            )
            print(f"Email sent! MessageId: {response['MessageId']}")
        except ClientError as e:
            print(f"[ERROR] Failed to send email: {str(e)}")

def lambda_handler(event, context):
    try:
        splunk.log_message({'Status': 'success', 'Message': f'{context.function_name} invoked'}, context)
            
        source_bucket = event["bucket"]
        source_key = event["object"]
        ndmFilePath = event["ndmFilePath"]
        ndmFilePattern = event["ndmFilePattern"]
        moveFlag = event["moveFlag"]

        global client_name, clientID, application_name, execution_name, execution_id, execution_starttime
        global statemachine_name, statemachine_id, state_name, state_enteredtime, transactionId, teamsId
        client_name = event.get("clientName", "")
        clientID = event.get("clientID", "")
        application_name = event.get("applicationName", "")
        execution_name = event.get("execution_name", "")
        execution_id = event.get("execution_id", "")
        execution_starttime = event.get("execution_starttime", "")
        statemachine_name = event.get("statemachine_name", "")
        statemachine_id = event.get("statemachine_id", "")
        state_name = event.get("state_name", "")
        state_enteredtime = event.get("state_enteredtime", "")
        transactionId = event.get("transactionId", "")
        teamsId = event.get("teamsId", "")

        log_event("STARTED", f"lmbd_ndmjson job started with event: {json.dumps(event)}", context)

        if ndmFilePath:
            source_key=find_ndm_file(source_bucket, ndmFilePath, ndmFilePattern,context)
        
        # splits the key into <system> <subfolder> <filename>
        key_dict = splitkey(source_key)
            
        jsonParams_json = event['ndmjsonparams']
            
        # copy file from inbound to toOnPrem S3
        response = copy_file(source_bucket, source_key, jsonParams_json, key_dict, moveFlag, event, context)
        
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            log_event("INFO", f"lmbd_ndmjson : InputFileName: {key_dict['filename']} : File move successful from {source_path} to {dest_path} ", context)

            # if the file is copied successfully then a json file is created and stored in toOnPrem_json
            create_json(source_bucket, source_key, jsonParams_json, key_dict, event, context)
                
        else:
            log_event("WARN", f"lmbd_ndmjson : InputFileName: {key_dict['filename']} : file failed to move from {source_path} to {dest_path} ", context)
            return {
                'statusCode': 500,
                'body': json.dumps('file failed to move to the transmission bucket')
            }
        
        log_event("ENDED", f"lmbd_ndmjson job successfully completed for file: {key_dict['filename']}", context)

        return {
                'statusCode': 200,
                'body': json.dumps('json file uploaded successfully!')
            }

    except Exception as e:
        log_event("FAILED", f"Error: {traceback.format_exc()}, Exception : {str(e)}, Context: {context}", context)
        raise e
        

def splitkey(source_key):
    '''
    This function splits the source_key into system
    subfolder and filename, if root is not present then
    only system, subsystem and filename
    '''

    words = re.split(r'\/', source_key)

    key = {
            "system": words[0],
            "subfolder": words[1],
            "filename": words[-1]
    }

    return key

def create_json(source_bucket, source_key, jsonParams_json, key_dict, event, context):
    '''
    This function tells the main purpose of this module.
    This function creates the json and uploads in the transmission
    bucket for the transmission team.
    '''

    global env

    output_json = {
        "mappings": [
            {
                "job_name": "",
                "s3_source": "",
                "archive": "",
                "sizeInMB": "",
                "brmf_destination": "",
                "runtask": "",
                "isGDG": "",
                "snode": "",
                "bros_destination": "",
                "filetype": "",
                "username": "",
                "password": "",
                "length": ""
            }]
    }
    
    try:
        fname = os.path.splitext(key_dict["filename"])[0]
        toOnPrem_bucket = source_bucket
        toOnPrem_key = jsonParams_json["to_internal_uri"]  + key_dict["filename"]
        toOnPrem_json_bucket = source_bucket
        toOnPrem_json_key = jsonParams_json["to_internal_json_uri"] +fname + ".json"
        
        snode = jsonParams_json["snode"][env]
    
        output_json["mappings"][0]["job_name"] = key_dict["system"] + "_" + jsonParams_json["job_name"] + "_" + fname
        output_json["mappings"][0]["s3_source"] = "s3://" + toOnPrem_bucket + "/" + toOnPrem_key
        output_json["mappings"][0]["archive"] = jsonParams_json["archive_uri"]
        output_json["mappings"][0]["snode"] = snode[0]
        output_json["mappings"][0]["username"] = snode[1]
        if ('s3_ndm' in jsonParams_json and jsonParams_json['s3_ndm']):
            output_json["mappings"][0]["bros_destination"] = f"s3://{jsonParams_json['s3_ndm'][env]['bucket']}/{jsonParams_json['s3_ndm'][env]['path']}{toOnPrem_key.split('/')[-1]}"
        elif ('is_composer_path' in jsonParams_json and jsonParams_json['is_composer_path'] and jsonParams_json['is_composer_path'].upper() == 'TRUE'):
            output_json["mappings"][0]["bros_destination"] = jsonParams_json['composer_onprem_path'] + snode[2] + "/" + jsonParams_json["onprem_system"] + jsonParams_json["onprem_subsystem"] + jsonParams_json['onprem_folder']
        else:
            output_json["mappings"][0]["bros_destination"] = "/dsto/cbu/" + snode[2] + "/" + jsonParams_json["onprem_system"] + "/" + jsonParams_json["onprem_subsystem"] + "/" + jsonParams_json['onprem_folder'] 
     
        # Convert data to a JSON string
        json_data = json.dumps(output_json)
    
        # Upload the JSON file to S3
        s3_client.upload_fileobj(
            io.BytesIO(json_data.encode('utf-8')),
            toOnPrem_json_bucket,
            toOnPrem_json_key
        )
    except Exception as e:
        raise Exception(f"Error creating JSON: {str(e)}") from e

def copy_file(source_bucket, source_key, jsonParams_json, key_dict, moveFlag, event, context):
    '''
    This function moves source file from inbound source bucket
    to destination onprem bucket
    '''
    # assign global variables
    global source_path, dest_path, compress_file_name
    source_path = f"{source_key}"
    dest_path = f"{jsonParams_json['to_internal_uri']}{key_dict['filename']}"
    compress_file_name = f"{key_dict['filename']}"

    toOnPrem_bucket = source_bucket
    toOnPrem_key = jsonParams_json["to_internal_uri"] + key_dict["filename"]

    GB = 1024 ** 3
    transfer_config = TransferConfig(
        multipart_threshold=4.9 * GB,
        multipart_chunksize=500 * 1024 ** 2,
        max_concurrency=10,
        use_threads=True
    )

    copy_source = {'Bucket': source_bucket, 'Key': source_key}

    try:
        s3_client.copy(
            copy_source,
            toOnPrem_bucket,
            toOnPrem_key,
            Config=transfer_config
        )

        response = {'ResponseMetadata': {'HTTPStatusCode': 200}}

        # Delete the source file if requested
        if moveFlag == 'True':
            s3_client.delete_object(Bucket=source_bucket, Key=source_key)

        return response

    except Exception as e:
        raise Exception(f"Copy operation failed: {str(e)}") from e
    

def find_ndm_file(bucket_name, ndmFilePath, ndmFilePattern, context):
    # Initialize S3 client
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    matched_files = []
    for page in paginator.paginate(Bucket=bucket_name, Prefix=ndmFilePath):
        for obj in page.get('Contents', []):
            file_key = obj['Key']
            filename = file_key.split('/')[-1]  # Get the file name
            # Check if the file name matches the regex pattern
            if re.match(ndmFilePattern, filename):
                matched_files.append(file_key)
    return matched_files[0] if matched_files else None