import json
import boto3
import os
import logging
import re
import datetime
import urllib.parse
from dpm_splunk_logger_py import splunk

def lambda_handler(event, context):
    
    # Get the S3 event record
    print(f"s3 event print: {event}")
    print(f"printing context {context}")

    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    input_file_name = event["Records"][0]["s3"]["object"]["key"]

    splunk.log_message({'Status': 'success', 'InputFileName': input_file_name, 'InputBucketName': bucket_name, 'Message': 'Lambda function is invoked'}, context)
    print(f"Input bucket name is :{bucket_name}")
    print(f"Input file name is {input_file_name}")

    file_name = input_file_name.split('/')[-1].rsplit(".", 1)[0]
    output_file_name = "output_files/" + file_name + ".json"
    glue = boto3.client("glue")
    
    #Name of the glue job
    job_name = os.environ['GLUEJOB']
    
    # Define the parameters to pass to the Glue job
    parameters = {
        "--s3_bucket_input": bucket_name,
        "--s3_input_file": input_file_name,
        "--s3_bucket_output": bucket_name,
        "--s3_output_file": output_file_name
    }

    print(f"Glue job parameters are: {parameters}")

    try:
        response = glue.start_job_run(JobName=job_name, Arguments=parameters)
        
        return {
            'statusCode': 200,
            'body': 'glue job started successfully!!!'
        }

    except Exception as e:
        print(f'Error starting glue job: {str(e)}')

        return {
            'statusCode': 500,
            'body': 'Error starting glue job'
        }