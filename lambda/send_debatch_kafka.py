import json
import requests
import boto3
import traceback
from dpm_splunk_logger_py import splunk
import os

# Initialize boto3 client for S3
s3 = boto3.client('s3')

# Kafka API URL (set via AWS Lambda environment variables)



def fetch_debatch_config(bucket_name, config_file_path):
    """
    Fetch the debatch configuration file from S3.
    """
    try:
        response = s3.get_object(Bucket=bucket_name, Key=config_file_path)
        config_content = response['Body'].read().decode('utf-8')
        return json.loads(config_content)
    except Exception as e:
        raise ValueError(f"Error fetching debatch config: {str(e)}")

def send_to_kafka_api(context, topic , messages):
    """
    Publish message to Kafka via API call
    """
    success = True
    KAFKA_API_URL = os.environ["KAFKA_API_URL"]
    KAFKA_API_URL = KAFKA_API_URL + topic

    print(f"Attempting to send {len(messages)} messages to Kafka API")
    print(f"Kafka URL: {KAFKA_API_URL}")

    # Basic URL validation
    if not KAFKA_API_URL or not str(KAFKA_API_URL).strip():
        print("Kafka API URL is empty or None")
        return False

    kafka_url_str = str(KAFKA_API_URL).strip()
    if not (kafka_url_str.startswith('http://') or kafka_url_str.startswith('https://')):
        print("Kafka API URL may not be properly formatted (missing http/https)")

    for i, message in enumerate(messages):
        try:
            headers = {"Content-Type": "application/json", 'source': 'DebatchingLambda','Accept': 'application/json'}

            print(f"Sending message {i+1}/{len(messages)}")
            print(f"Message Payload: {message}")
            print(f"Headers: {headers}")

            response = requests.post(KAFKA_API_URL, json=message, headers=headers, timeout=30)

            print(f"Kafka API Response for message {i+1}")
            print(f"Status Code: {response.status_code}")
            print(f"Response Headers: {dict(response.headers)}")
            print(f"Response Body: {response.text[:1000]}")

            if response.status_code == 200:
                print(f"Successfully sent message {i+1} to Kafka")
            else:
                success = False
                print(f"Failed to send message {i+1}, Status Code: {response.status_code}, Response: {response.text}")

        except requests.exceptions.Timeout as e:
            success = False
            print(f"Timeout error for message {i+1}: {str(e)}")

        except requests.exceptions.ConnectionError as e:
            success = False
            print(f"Connection error for message {i+1}: {str(e)}")

        except requests.RequestException as e:
            success = False
            print(f"Request error for message {i+1}: {str(e)}")

    print(f"Kafka batch processing complete")
    print(f"Total Messages: {len(messages)}")
    print(f"Overall Success: {success}")

    return success

def lambda_handler(event, context):
    print("starting")
    """
    AWS Lambda function entry point.
    Expected event format:
    {
        "client_id": "client_123",
        "transaction_id": "trans_456",
        "output_folder": "jhi_confirms/wip/c74ee547-4301-4981-947b-fb80a3874a38/debatched_files",
        "bucket": "bucket_name"
    }
    """
    client_id = event.get("client_id")
    print("client_id",client_id)
    transaction_id = event.get("transaction_id")
    print("transaction_id",transaction_id)
    output_folder = event.get("output_folder")
    print("output_folder",output_folder)
    bucket_name = event.get("bucket")
    print("bucket_name",bucket_name)
    topic = event.get("topic")
    print("topic", topic)
    configFile = event.get("configFile") or " "
    print("configFile:", configFile)

    if not client_id or not transaction_id or not output_folder or not bucket_name :
        error_message = "Missing required parameter: client_id, transaction_id, output_folder, or bucket"
        print("Missing required parameter: client_id, transaction_id, output_folder, or bucket ")
        splunk.log_message({"Status": "Failed", "Message": error_message}, context)
        raise Exception(error_message)

    try:
        # Construct the path to debatch_config.json
        config_file_path = f"{output_folder}/debatch_config.json"
        print("config_file_path",config_file_path)
        # Fetch debatch config
        config_data = fetch_debatch_config(bucket_name, config_file_path)
        print("config_data",config_data)
        splunk.log_message({
            "Status": "Info", 
            "Message": "Successfully fetched debatch config",
            "ConfigData": config_data
        }, context)

        # Read the debatch config file
        s3_uris = config_data.get("s3_uris", [])
        record_counts = config_data.get("record_counts", [])
        
        # Validate that s3_uris and record_counts have the same length
        if len(s3_uris) != len(record_counts):
            error_message = f"Mismatch between s3_uris count ({len(s3_uris)}) and record_counts count ({len(record_counts)})"
            print(error_message)
            splunk.log_message({"Status": "Failed", "Message": error_message}, context)
            raise Exception(error_message)

        s3_bucket_loc = s3_uris[0].split('/', 3)[2]
        s3_folder_name = '/'.join(s3_uris[0].split('/', 3)[3:]).rsplit('/', 1)[0]

        formatted_messages = []

        for i, s3_uri in enumerate(s3_uris):
            file_name = s3_uri.rsplit('/', 1)[-1]
            file_record_count = record_counts[i] if i < len(record_counts) else 0
            
            formatted_message = {
                "s3BucketLoc": s3_bucket_loc,
                "s3FolderName": s3_folder_name,
                "metaData": {
                    "fileName": file_name,
                    "totalDocs": str(file_record_count), 
                    "doneFile": config_data.get("done_file_name", ""),
                    "requiredSegregation": config_data.get("requiredSegregation", ""),
                    "configFile": configFile
                }
            }
            formatted_messages.append(formatted_message)
            
            # NEW: Add detailed logging for each file
            print(f"File {i+1}: {file_name} - Records: {file_record_count}")

        # NEW: Add summary logging
        total_files = len(formatted_messages)
        total_records = sum(record_counts)
        print(f"Summary: {total_files} files, {total_records} total records")
        
        splunk.log_message({
            "Status": "Info", 
            "Message": f"Formatted {len(formatted_messages)} messages for Kafka",
            "TotalFiles": total_files,
            "TotalRecords": total_records,
            "RecordCounts": record_counts,
            "Messages": formatted_messages
        }, context)

        # Send Kafka messages
        success = send_to_kafka_api(context,topic, formatted_messages)
        print("success",success)
        if not success:
            error_message = "Failed to send Kafka messages"
            splunk.log_message({"Status": "Failed", "Message": error_message}, context)
            raise Exception(error_message)

        splunk.log_message({
            "Status": "Success", 
            "Message": "Kafka messages sent successfully", 
            "Count": len(formatted_messages),
            "TotalRecords": total_records,
            "RecordDistribution": record_counts
        }, context)

    except Exception as e:
        error_message = f"Error processing configuration file: {str(e)}"
        splunk.log_message({"Status": "Failed", "Message": error_message, "StackTrace": traceback.format_exc()}, context)
        raise Exception(error_message)

    return {
        "statusCode": 200, 
        "body": f"Processed {len(formatted_messages)} S3 URIs successfully with {sum(record_counts) if 'record_counts' in locals() else 0} total records"
    }