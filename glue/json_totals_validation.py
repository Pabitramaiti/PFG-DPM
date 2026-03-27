import sys
import boto3
import json
import os
# import logging
from awsglue.utils import getResolvedOptions
import splunk

s3 = boto3.client('s3')


def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return f"arn:dpm:glue:Validating Json Total running with account id:{account_id}"


def set_job_params_as_env_vars():
    try:
        for i in range(1, len(sys.argv), 2):
            if sys.argv[i].startswith('--'):
                key = sys.argv[i][2:]  # Remove the leading '--'
                value = sys.argv[i + 1]
                os.environ[key] = value
                print(f'Set environment variable {key} to {value}')
    except IndexError as e:
        message = "Error in parsing command line arguments: Missing value for a parameter."
        splunk.log_message({'Status': 'Failed', 'Message': message}, get_run_id())
        print(message)
        raise ValueError("Invalid command line arguments format.") from e


def print_all_env_vars():
    for key, value in os.environ.items():
        print(f'{key}: {value}')


def read_json_from_s3(s3_uri):
    if not s3_uri.startswith('s3://'):
        raise ValueError("The S3 URI must start with 's3://'")

    s3_parts = s3_uri[5:].split('/', 1)
    if len(s3_parts) != 2:
        raise ValueError("The S3 URI does not contain enough components to extract bucket and key.")

    bucket_name, object_key = s3_parts

    try:
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        content = response['Body'].read()
        return json.loads(content)
    except Exception as e:
        message = f"Failed to read JSON from S3. Error: {e}"
        splunk.log_message({'Status': 'Failed', 'Message': message}, get_run_id())
        print(message)
        raise e


def json_totals_compare(s3_total_json_file):
    try:
        data = read_json_from_s3(s3_total_json_file)
        number_of_records = data["FILE"]["NUMBER-OF-RECORDS"]
        trailer_total = data["FILE"]["TRAILER-TOTAL"]

        if number_of_records == trailer_total:
            return True
        else:
            message = f"Total Number of Records : {number_of_records} are not equal to Trailer-Total: {trailer_total}"
            splunk.log_message({'Status': 'Failed', 'Message': message}, get_run_id())
            print(message)
            raise ValueError(message)

    except KeyError as e:
        message = f"Missing key in JSON file: {e}"
        splunk.log_message({'Status': 'Failed', 'Message': message}, get_run_id())
        print(message)
        raise KeyError(f"Missing key in JSON: {e}") from e
    

def main():
    message = "glue_json_totals_validation processing Started"
    splunk.log_message({'Status': 'success', 'Message': message}, get_run_id())

    try:
        set_job_params_as_env_vars()
        print_all_env_vars()
    except Exception as e:
        message = f"Error setting environment variables: {e}"
        print(message)
        splunk.log_message({'Status': 'Failed', 'Message': message}, get_run_id())
        raise ValueError(message)

    try:
        total_json_file = os.environ['total_json_file']

    except KeyError:
        message = "Environment variable 'total_json_file' not set."
        print(message)
        splunk.log_message({'Status': 'Failed', 'Message': message}, get_run_id())
        raise ValueError(message)

    try:
        bucket_name = os.environ['bucket_name']

    except KeyError:
        message = "Environment variable 'bucket_name' not set."
        print(message)
        splunk.log_message({'Status': 'Failed', 'Message': message}, get_run_id())
        raise ValueError(message)

    s3_total_json_file = "s3://" + bucket_name + "/" + total_json_file

    try:
        json_totals_compare(s3_total_json_file)
    except Exception as e:
        message = f"Error during JSON totals comparison: {e}"
        print(message)
        splunk.log_message({'Status': 'Failed', 'Message': message}, get_run_id())
        raise ValueError(message)


if __name__ == "__main__":
    # Set logging configuration
    ##    logging.basicConfig(level=logging.INFO)
    main()
