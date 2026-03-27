import json, os
import boto3
from botocore.exceptions import ClientError
from dpm_splunk_logger_py import splunk
s3 = boto3.client('s3')

def move_objects_within_bucket(bucket_name, source_key, destination_key, context):
    """
    Move objects from one folder to another within an S3 bucket.

    :param bucket_name: Name of the S3 bucket
    :param source_keyr: Path of the source folder
    :param destination_key: Path of the destination folder
    """
    try:
        copy_source = {'Bucket': bucket_name, 'Key': source_key}
        destination_key = destination_key

        # Copy the object to the new location
        print("Copying file from : "+bucket_name+"/"+source_key+" : To : "+bucket_name+"/"+destination_key)
        s3.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=destination_key)

        # Delete the original object
        s3.delete_object(Bucket=bucket_name, Key=source_key)
        splunk.log_message({'Message': f"Moved '{source_key}' to '{destination_key}'", "Status": "success"}, context)
    except ClientError as e:
        splunk.log_message({'Message': f"Error moving objects: {e}", "Status": "failed"}, context)
        raise e

def lambda_handler(event, context):
    
    if 'detail' in event:
        bucket = event['detail']['bucket']['name']
        key= event['detail']['object']['key']
    else:
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
    
    file_name = key.rsplit('/')[-1]

    save_path = key.rsplit('/', 2)[0] + '/' + 'save'+ '/' +file_name
    failed_path = key.rsplit('/', 2)[0] + '/' + 'failed'+ '/' +file_name
  
    splunk.log_message({"message": f"event triggered to load config file with key {key} and bucket {bucket}", "Status": "Info"}, context)

    try:
        
        # Instantiate the DynamoDB client
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(os.getenv('CONFIG_TABLE_DYNAMO'))

        # Retrieve the JSON file from S3
        response = s3.get_object(Bucket=bucket, Key=key)

        client_config_json_data = response['Body'].read().decode('utf-8')

        client_data_dict = json.loads(client_config_json_data)

        client_name = client_data_dict.get('clientName', None)

        splunk.log_message({"message": f"Loading config data for the client {client_name}", "Status": "Info"}, context)

        # Store the JSON data as a string with the key 'config_data'
        data = {
            'clientName': client_name,
            'clientConfig': client_config_json_data
        }

        # Load the data into DynamoDB
        table.put_item(Item=data)
        
        splunk.log_message({"message": f"Successfully loaded config data into for client {client_name}", "Status": "SUCCESS"}, context)

        move_objects_within_bucket(bucket,key,save_path,context) # moving the file to save directory    

        
        splunk.log_message({"message": f"moved the config file for {client_name} to save folder succesfully", "Status": "SUCCESS"}, context)
        
        
    except Exception as e:
        move_objects_within_bucket(bucket,key,failed_path,context) # moving the file to fail directory
        splunk.log_message(
            {"message": f"Failed to load config data for config file \"{file_name}\" with error {str(e)}", "Status": "ERROR"},
            context)
        raise e
