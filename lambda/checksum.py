import json

import hashlib
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
from dpm_splunk_logger_py import splunk

s3 = boto3.client('s3')


def compute_sha256_from_s3(bucket_name, file_key, context):
    sha256 = hashlib.sha256()
    try:

        message = f"Starting checksum calculation for file: {file_key} in bucket: {bucket_name}"
        splunk.log_message({"Status": "log", "Message": message}, context)
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        body = response['Body']
        while True:
            chunk = body.read(8192)
            if not chunk:
                break
            sha256.update(chunk)
        checksum = sha256.hexdigest()

        message = f"Checksum calculation completed for file: {file_key}. SHA-256 checksum: {checksum}"
        splunk.log_message({"Status": "log", "Message": message}, context)

        return checksum

    except NoCredentialsError:
        error_message = "AWS credentials not found."
        splunk.log_message({'Message': error_message, "Status": "failed"}, context)
        print(error_message)
        return None
    except PartialCredentialsError:
        error_message = "Incomplete AWS credentials."
        splunk.log_message({'Message': error_message, "Status": "failed"}, context)
        print(error_message)
        return None
    except ClientError as e:
        error_message = f"Client error occurred: {e}"
        splunk.log_message({'Message': error_message, "Status": "failed"}, context)
        print(error_message)
        return None
    except Exception as e:
        error_message = f"An unexpected error occurred: {e}"
        splunk.log_message({'Message': error_message, "Status": "failed"}, context)

        return None


def lambda_handler(event, context):
    try:
        bucket_name = event['bucket_name']
        file_key = event['file_key']
        checksum = compute_sha256_from_s3(bucket_name, file_key, context)
        shafile = file_key[:file_key.rfind(".")] + ".sha256"
        if checksum:
            # shafile = file_key[:file_key.rfind(".")] + ".sha256"
            try:
                response = s3.get_object(Bucket=bucket_name,
                                         Key=shafile)
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchBucket':
                    error_message = f"The bucket '{bucket_name}' does not exist."
                elif e.response['Error']['Code'] == 'NoSuchKey':
                    error_message = f"The file {shafile} does not exist in bucket '{bucket_name}'."
                else:
                    error_message = f"Unexpected error: {e}"
                raise Exception(error_message)

            expected_checksum = response['Body'].read().decode('utf-8').strip().split(" ")[0].lower()

            if checksum != expected_checksum:
                ### ICSBRCCPPI-3710
                # print(f"This is sha256 --->  {shafile}")
                # print(f"This is file_key --->  {file_key}")
                # print(f"This is bucket_name --->  {bucket_name}")
                source_key = shafile
                destination_key = source_key.replace('input', 'failed')
                copy_source = {'Bucket': bucket_name, 'Key': source_key}
                response = s3.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=destination_key)
                # Delete the source file from the source S3 bucket
                s3.delete_object(Bucket=bucket_name, Key=source_key)
                ########################
                error_message = f"Checksum validation failed.where Checksum file of {file_key} is ${checksum} and checksum provided by the client file of {shafile} is : ${expected_checksum}"
                splunk.log_message({'Message': error_message, "Status": "failed"}, context)
                raise Exception(f"{error_message}")

    except Exception as e:
        error_message = f"Process {shafile} checksum failed"
        splunk.log_message({'Message': error_message, "Status": "failed"}, context)
        raise e