import json
import boto3
import datetime
import re
import os
from dpm_splunk_logger_py import splunk

s3 = boto3.client('s3')
stage = os.getenv("STAGE")
inputFileName = None
context = None

def lambda_handler(event, context):
    
    inputFileName = event.get('inputFileName', 'unknown_file')
    bucketName = event['bucketName']
    propertyConfigPath = event['propertyConfigPath']
    propertyConfigPattern = event['propertyConfigPattern']
    propertyConfigKey = event['propertyConfigKey']
    propertyFileDestinationPath = event['propertyFileDestinationPath']
    propertyFileDestinationPrefix = event['propertyFileDestinationPrefix']
    urPayloadPath = event['urPayloadPath']
    urPayloadPattern = event['urPayloadPattern']
    
    try:
        splunk.log_message({'FileName': inputFileName, 'Message': event, "Status": "logging"}, context)

        # Initialize variables
        propertyConfigName = urPayloadName = ""

        if propertyConfigKey.strip() == "":
            propertyConfigKey, propertyConfigName = search_key(bucketName, propertyConfigPath, propertyConfigPattern,inputFileName,context)
        else:
            propertyConfigName = os.path.basename(propertyConfigKey)
            splunk.log_message({'FileName': inputFileName, 'Message': f"Property config name: {propertyConfigName}", "Status": "logging"}, context)
        
        if propertyConfigKey is None:
            raise ValueError("Property config file not found matching the pattern.")
        
        urPayloadKey, urPayloadName = search_key(bucketName, urPayloadPath, urPayloadPattern,inputFileName,context)
        
        if urPayloadKey is None:
            raise ValueError("UR Payload file not found matching the pattern.")
        
        # Get the content of the config file from S3
        configObject = s3.get_object(Bucket=bucketName, Key=propertyConfigKey)
        configData = configObject['Body'].read().decode('utf-8')
        
        # Parse the config file (assuming it's JSON)
        config = json.loads(configData)
        config = config[stage]

        # Generate the properties file content
        propertiesContent = create_properties_content(config)
        splunk.log_message({'FileName': inputFileName, 'Message': "Property file created!", "Status": "logging"}, context)
        
        # Generate the S3 key for the destination property file
        currentTime = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        propertyFileKey = f"{propertyFileDestinationPath}{urPayloadName}.transfer.properties"

        # Save the properties file back to S3
        s3.put_object(Bucket=bucketName, Key=propertyFileKey, Body=propertiesContent)
        
        return {
            'statusCode': 200,
            'secIndexKeyName': 'Secondary_Index_Key',
            "secIndexKeyValue": f'{urPayloadName}.zip',
            'body': json.dumps('Properties file created successfully!')
        }

    except Exception as e:
        splunk.log_message({'FileName': inputFileName, 'Message': str(e), "Status": "failed"}, context)
        raise

def search_key(bucketName, path, pattern, inputFileName, context):
    try:
        regexPattern = re.compile(pattern)
        paginator = s3.get_paginator('list_objects_v2')
        # List objects in the bucket using paginator
        for page in paginator.paginate(Bucket=bucketName, Prefix=path):
            for obj in page.get('Contents', []):
                key = obj['Key']
                filename = key.split('/')[-1]
                # print("key : ", key)
                if regexPattern.match(filename):
                    # print(f"file to be returned : {filename}\n")
                    return key, filename
        splunk.log_message({'FileName': inputFileName,'Message': f'Key for regex "{pattern}" not found in bucket "{bucketName}" at path "{path}"', "Status": "failed"}, context)
        raise ValueError(f'Key for regex "{pattern}" not found in bucket "{bucketName}" at path "{path}"')
    except Exception as e:
        splunk.log_message({'FileName': inputFileName,'Message': str(e), "Status": "failed"}, context)
        raise e

def create_properties_content(config):
    currentTime = datetime.datetime.now().strftime("%a %b %d %H:%M:%S %Y")
    
    # Assign default values if keys are missing
    urSuccessFeedback = config.get("UR_SUCCESS_FEEDBACK", False)
    urFailureFeedback = config.get("UR_FAILURE_FEEDBACK", False)
    urFeedbackAttributes = config.get("UR_FEEDBACK_ATTRIBUTES", False)
    snode = config.get("SNODE", "")
    snodeId = config.get("SNODE_ID", "")
    destPath = config.get("DEST_PATH", "")
    lockbox = config.get("LOCKBOX", "")
    inpPubKey = config.get("INP_PUB_KEY", "")
    inpUid = config.get("INP_UID", "")

    properties = f"""# This is a system generated file. DO NOT MODIFY!
# Created {currentTime}

[default]
# optional, return feedback communication to preprocessor
UR_SUCCESS_FEEDBACK={str(urSuccessFeedback).lower()}
UR_FAILURE_FEEDBACK={str(urFailureFeedback).lower()}
UR_FEEDBACK_ATTRIBUTES={str(urFeedbackAttributes).lower()}
# optional, NDM parameters to be used by UR for routing feedback file(s) to preprocessor
SNODE={snode}
SNODE_ID={snodeId}
DEST_PATH={destPath}

# optional, PGP public key to be utilized by UR AFP Loader for encrypting *feedback*
# file(s) prior to transmission to preprocessor.  The below parameters are
# optional if no PII data found in Client Input Data.
LOCKBOX={lockbox}
INP_PUB_KEY={inpPubKey}
INP_UID={inpUid}

# --- EOF ---
"""
    return properties
