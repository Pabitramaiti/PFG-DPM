import json
import requests
import boto3
import traceback
from dpm_splunk_logger_py import splunk
from datetime import datetime, timezone
import os

# Initialize boto3 client for S3
s3 = boto3.client('s3')

file_date = datetime.now().strftime("%m%d%Y")
# def fetch_debatch_config(bucket_name, config_file_path):
#     """Fetch the debatch configuration file from S3."""
#     try:
#         response = s3.get_object(Bucket=bucket_name, Key=config_file_path)
#         config_content = response['Body'].read().decode('utf-8')
#         return json.loads(config_content)
#     except Exception as e:
#         raise ValueError(f"Error fetching debatch config: {str(e)}")

def fetch_debatch_config(bucket_name, config_file_path):
    """
    Fetch the debatch configuration file from S3.
    Handles both JSON configs (expected by default)
    and plain-text line files (like *_final_list_files.txt).
    """
    try:
        response = s3.get_object(Bucket=bucket_name, Key=config_file_path)
        config_content = response['Body'].read().decode('utf-8').strip()

        # Try parsing as JSON first
        try:
            parsed = json.loads(config_content)
            return parsed
        except json.JSONDecodeError:
            # If not JSON, treat content as plain text lines
            lines = [line.strip() for line in config_content.splitlines() if line.strip()]

            # Return in a consistent "dict-like" format
            return {"s3_uris": lines}

    except Exception as e:
        raise ValueError(f"Error fetching debatch config: {str(e)}")

def send_to_kafka_api(context, topic, messages):
    """Publish message to Kafka via API call."""
    success = True
    KAFKA_API_URL = os.environ["KAFKA_API_URL"]
    KAFKA_API_URL = KAFKA_API_URL + topic

    print(f"Attempting to send {len(messages)} messages to Kafka API")
    print(f"Kafka URL: {KAFKA_API_URL}")

    if not KAFKA_API_URL or not str(KAFKA_API_URL).strip():
        print("Kafka API URL is empty or None")
        return False

    kafka_url_str = str(KAFKA_API_URL).strip()
    if not (kafka_url_str.startswith('http://') or kafka_url_str.startswith('https://')):
        print("Kafka API URL may not be properly formatted (missing http/https)")

    for i, message in enumerate(messages):
        try:
            headers = {
                "Content-Type": "application/json",
                "source": "DebatchingLambda",
                "Accept": "application/json"
            }

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

    client_id = event.get("client_id")
    print("client_id:", client_id)
    in_file = event.get("s3_input_file")

    formatted_messages = []

    try:
        transaction_id = event.get("transaction_id")
        debatch_kafka  = event.get("debatch_kafka")
        bucket_name    = event.get("bucket")
        topic          = event.get("topic")
        kafka_info     = event.get("kafka_info")
        

        if not client_id or not transaction_id or not debatch_kafka or not bucket_name or not in_file:
            error_message = "Missing required parameter (special client flow)"
            print(error_message)
            splunk.log_message({"Status": "Failed", "Message": error_message}, context)
            raise Exception(error_message)

        file_name = in_file.rsplit("/", 1)[-1]
        base_name = file_name.rsplit(".", 1)[0]

        config_file_path = f"{debatch_kafka}/{base_name}_final_list_files.txt"
        print("config_file_path:", config_file_path)

        # Fetch debatch config
        config_data = fetch_debatch_config(bucket_name, config_file_path)
        print("config_data:", config_data)

        splunk.log_message({
            "Status": "Info",
            "Message": "Fetched special debatch config",
            "ConfigData": config_data
        }, context)

        client_name = kafka_info.get("client")

        # Here config_data is assumed to be a simple list for special client
        for i, fname in enumerate(config_data.get("s3_uris", [])):
            formatted_message = {
                "metadata": {
                    "transaction_id": transaction_id,
                    "client": client_name,
                    "product": "confirms",
                    "bucket": bucket_name,
                    "region": "us-east-1",
                    "file_name": fname
                    
                }
            }
            formatted_messages.append(formatted_message)
            print(f"[SPECIAL] File {i+1}: {fname}")

        total_files = len(formatted_messages)
        print(f"[SPECIAL] Summary: {total_files} files")

        splunk.log_message({
            "Status": "Info",
            "Message": f"[888888] Formatted {total_files} messages for Kafka",
            "TotalFiles": total_files,
            "Messages": formatted_messages
        }, context)

        # Send Kafka messages
        success = send_to_kafka_api(context, topic, formatted_messages)
        if not success:
            raise Exception("Failed to send Kafka messages for client 888888")

        # Common success response for both flows
        splunk.log_message({
            "Status": "Success",
            "Message": "Kafka messages sent successfully",
            "Count": len(formatted_messages)
        }, context)

        return {
            "statusCode": 200,
            "body": f"Processed {len(formatted_messages)} messages successfully"
        }

    except Exception as e:
        error_message = f"Error processing configuration: {str(e)}"
        splunk.log_message({
            "Status": "Failed",
            "Message": error_message,
            "StackTrace": traceback.format_exc()
        }, context)
        raise