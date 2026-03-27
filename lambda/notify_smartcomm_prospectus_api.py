import json
import boto3
import os
import re
import inspect
import requests
from requests_oauthlib import OAuth1
from dpm_splunk_logger_py import splunk
from dpmdev_di_layer_common_functions import client_config
from dpmdev_di_layer_common_functions import functions
import xml.etree.ElementTree as ET
from datetime import datetime

secret_name = os.environ.get("SECRET_NAME")
table_name = os.environ.get("DDBCONFIGTBL")
system = os.environ.get("SYSTEM")
env = os.environ.get("SDLC_ENV")
client = boto3.client('secretsmanager')
dynamodb = boto3.client("dynamodb")
s3 = boto3.client('s3')
headers = { "Content-Type": "application/x-www-form-urlencoded" }

def get_matching_s3_keys(bucket, prefix='', pattern=''):
    try:    
        paginator = s3.get_paginator('list_objects_v2')
        keys = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = os.path.basename(obj['Key'])
                if re.match(pattern, key.rsplit('/', 1)[-1]):
                    keys.append(obj['Key'])
        if not keys:
            raise Exception("No matching S3 keys found.")
        return keys
    except Exception as e:
        message = f"Exception in get_matching_s3_keys: {e}"
        raise Exception(message)

def upload_to_smartcomm(auth, api_url, bucketName, filePath, filePattern, new_pid, folderId, logPath, logPrefix, curr_fn, context):
    """
    bucketName = "br-icsdev-dpmjayant-dataingress-us-east-1-s3"
    filePath = "jhi/" or "confirms/" or "pros/"
    """
    logData = ""
    # 1. List all PDF files under the prefix
    # file_list = s3.list_objects_v2(Bucket=bucketName, Prefix=filePath)
    file_list = get_matching_s3_keys(bucketName, filePath, filePattern)
    print(f"file_list : {file_list}")
    count = 0
    countDup = 0
    countPass = 0
    for key in file_list:
        print(f"Path : {key}")
        count+=1
        filename = key.split("/")[-1]
        local_path = f"/tmp/{filename}"
        # 2. Download each file
        s3.download_file(bucketName, key, local_path)
        # 3. Call SmartCOMM upload or next steps
        resourceName = filename
        create_resource_data = {
            "projectId": new_pid,
            "folderId": folderId,
            # "resourceName":f"{resourceName}_{timestamp}" ,
            "resourceName":f"{resourceName}",
            "resourceMimeType":"application/pdf"
        }
        create_resource_api_url=f"{api_url}resources"
        response = requests.post(create_resource_api_url, headers=headers, data=create_resource_data, auth=auth)
        if response.status_code == 201:
            countPass +=1
            logData += f"Sequence {count:05d} : File {resourceName} is uploaded to Smartcomm\n"
        elif response.status_code == 409:
            countDup +=1
            logData += f"Sequence {count:05d} : File {resourceName} is duplicate, Skipping upload\n"
            continue
        elif response.status_code != 201:
            splunk.log_message({'bucket Name ': bucketName, 'Status': 'failed',
                                'message': f"Failed to create resource : {resourceName}_{timestamp}",
                                "statusCode": response.status_code,
                                "ModuleName": curr_fn}, context)
            raise ValueError(f"Failed to create resource : {resourceName}_{timestamp}")
        xml_response = response.text
        # ---- Parse XML to extract <ids> ----
        root = ET.fromstring(xml_response)

        ids_values = [elem.text for elem in root.findall(".//ids")]

        # Get required values
        resource_version_id = ids_values[1]       # Second <ids>

        set_content_api_url=f"{api_url}versions/{resource_version_id}/content"
        
        with open(local_path, "rb") as fh:
            files = {"file": (filename, fh, "application/pdf")}
            response = requests.post(set_content_api_url, files=files, data={"performCheckout": "false"}, auth=auth)
            if response.status_code != 204:
                splunk.log_message({'bucket Name ': bucketName, 'Status': 'failed',
                                    'message': f"Failed to set content for resource : {resourceName}_{timestamp}",
                                    "statusCode": response.status_code,
                                    "ModuleName": curr_fn}, context)
                raise ValueError(f"Failed to set content for resource : {resourceName}_{timestamp}")
        check_in_api_url=f"{api_url}versions/{resource_version_id}/checkin"
        check_in_data = {
            "withdrawBusinessObjectRevision": "false"
        }
        response = requests.post(check_in_api_url, headers=headers, data=check_in_data, auth=auth)
        if response.status_code != 204:
            splunk.log_message({'bucket Name ': bucketName, 'Status': 'failed',
                                'message': f"Failed to check in resource : {resourceName}_{timestamp}",
                                "statusCode": response.status_code,
                                "ModuleName": curr_fn}, context)
            raise ValueError(f"Failed to check in resource : {resourceName}_{timestamp}")
    logData += f"\n{'-' * 20}\nDuplicate files : {countDup}\nUploaded files : {countPass}\nTotal files processed : {count}"        
    s3.put_object(
        Bucket=bucketName,
        Key=f"{logPath}{logPrefix}_{timestamp}.txt",
        Body=logData
    )
    print("Completed processing all PDF files.")



def lambda_handler(event, context):
        global secret_name
        global fileName
        global timestamp

        curr_fn = inspect.currentframe().f_code.co_name
        extract_event_setenviron(event, context)    
        fileName = os.environ.get("fileName")
        systemName = os.environ.get("systemName")
        bucketName = os.environ.get("bucketName")
        filePath = os.environ.get("filePath")
        filePattern = os.environ.get("filePattern")
        folderId = os.environ.get("folderId")
        logPath = os.environ.get("logPath")
        logPrefix = os.environ.get("logPrefix")
        print(f"systemName : {env} : {systemName}")

        splunk.log_message({'Status': 'success', 'InputFileName': fileName, "FunctionCalled": curr_fn,
                            'Message': f'{context.function_name} Lambda function invoked.'},
                        context)
        if not systemName in secret_name:
            secret_name = f"{env}/{systemName}"
        # connect lambda to secrets manager
        response = client.get_secret_value(SecretId=secret_name)
        secret_string = response['SecretString']
        secret_dict = json.loads(secret_string)

        # Define the API URL, Consumer Key and Consumer Secret
        consumer_key = secret_dict["consumer_key"]
        consumer_secret = secret_dict["consumer_secret"]
        project_manager_user_id = secret_dict["project_manager_user_id"]

        api_url = secret_dict["api_url"]
        splunk.log_message({'Status': 'success', 'InputFileName': fileName,
                            'Message': f'{context.function_name} Lambda function Initialized .'},
                            context)

        try:
            now = datetime.now()
            timestamp = now.strftime("%y%m%d_%H%M%S")
            # Step 2: Create OAuth1 authentication headers
            auth = OAuth1(consumer_key, consumer_secret, signature_type='auth_header')
            splunk.log_message({'InputFileName': fileName, 'Status': 'success',
                                'message': f'{context.function_name}  Lambda function does Auth1 Successfully',
                                "ModuleName": curr_fn}, context)

            # Step 3: Send an HTTP POST request with the provided headers and body
            splunk.log_message({'InputFileName': fileName, 'Status': 'success',
                                'message': f'{context.function_name}  Lambda function is sending an HTTP POST request',
                                "ModuleName": curr_fn}, context)

            create_project_api_url=f"{api_url}projects"
            create_project_data = {
                "name": f"{fileName}",
                # "name": f"{fileName}_{timestamp}",
                "projectManagerUserId": project_manager_user_id
            }
            #Create project
            response = requests.post(create_project_api_url, headers=headers, data=create_project_data, auth=auth)
            location_header = response.headers.get("Location", "")
            new_pid = location_header.split("/")[-1] if location_header else None
            # upload pdf files to smartcomm
            upload_to_smartcomm(auth, api_url, bucketName, filePath, filePattern, new_pid, folderId, logPath, logPrefix, curr_fn, context)
            #Release project
            project_release_api_url=f"{api_url}projects/{new_pid}/release"
            response = requests.post(project_release_api_url, headers=headers, auth=auth)
            # Check the response status code
            if response.status_code == 200:
                # Successful API call
                splunk.log_message({'InputFileName': fileName, 'Status': 'success',
                                    'message': f'Job submitted', "statusCode": response.status_code,
                                    "ModuleName": curr_fn}, context)
                return {
                    "statusCode": 200,
                    "body": "Job submitted"
                }
            elif response.status_code == 400:
                splunk.log_message({'InputFileName': fileName, 'Status': 'failed',

                                    'message': f'Some parameters used in the job body are invalid. Please check and correct them.',
                                    "statusCode": response.status_code,
                                    "ModuleName": curr_fn}, context)
                raise ValueError( "Some parameters used in the job body are invalid. Please check and correct them.")
            elif response.status_code == 401:
                splunk.log_message({'InputFileName': fileName, 'Status': 'failed',
                                    'message': f'You are not authorised. Please check your credentials or authorization method used and try again.',
                                    "statusCode": response.status_code,
                                    "ModuleName": curr_fn}, context)
                raise ValueError("You are not authorised. Please check your credentials or authorization method used and try again.")
            elif response.status_code == 403:
                splunk.log_message({'InputFileName': fileName, 'Status': 'failed',

                                    'message': f'Access forbidden. Please check your user roles and feature assignment. Otherwise please check the job is submitted correctly using a valid Queue.',
                                    "statusCode": response.status_code,
                                    "ModuleName": curr_fn}, context)
                raise ValueError("Access forbidden. Please check your user roles and feature assignment. Otherwise please check the job is submitted correctly using a valid Queue.")
            elif response.status_code == 429:
                # Successful API call
                splunk.log_message({'InputFileName': fileName, 'Status': 'failed',
                                    'message': f'Too many requests. Please wait 60 seconds, then retry your request.',
                                    "statusCode": response.status_code,
                                    "ModuleName": curr_fn}, context)
                raise ValueError("Too many requests. Please wait 60 seconds, then retry your request.")
            elif response.status_code == 503:
                splunk.log_message({'InputFileName': fileName, 'Status': 'failed',

                                    'message': f'Service unavailable. Please wait 120 seconds, then retry your request. Please note that a maintenance window can last an hour or more during a milestone upgrade.',
                                    "statusCode": response.status_code,
                                    "ModuleName": curr_fn}, context)
                raise ValueError("Service unavailable. Please wait 120 seconds, then retry your request. Please note that a maintenance window can last an hour or more during a milestone upgrade.")
            else:
                # Handle errors
                splunk.log_message({'InputFileName': fileName, 'Status': 'failed',
                                    'message': f'API request failed', "statusCode": response.status_code,
                                    "ModuleName": curr_fn}, context)
                raise ValueError("API request failed")
        except Exception as e:
            # Handle exceptions
            splunk.log_message({'Status': 'failed',
                                'Message': f'{context.function_name}  Lambda function failed due to {e}',
                                # "statusCode": response.status_code,
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
