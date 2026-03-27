
import os
import boto3
import json
from dpm_splunk_logger_py import splunk
from botocore.exceptions import ClientError

s3 = boto3.client('s3')

# moving the folders to the {decommission_clients/}
def move_folder(source_bucket, source_folder, context):
    destination_folder = os.environ.get("DESTINATION")
 
    try:
        response = s3.list_objects_v2(Bucket=source_bucket, Prefix=source_folder)
        # Move each object to the destination folder
        for obj in response.get('Contents', []):
            key = obj['Key']
            new_key = key.replace(source_folder, destination_folder, 1)  # Replace source_folder with destination_folder once
            copy_source = {'Bucket': source_bucket, 'Key': key}
    
            # Copy object to the destination folder
            s3.copy_object(CopySource=copy_source, Bucket=source_bucket, Key=new_key)
            # Delete the object from the source folder
            s3.delete_object(Bucket=source_bucket, Key=key)
        splunk.log_message({'Message': f"Moved '{source_folder}' to '{destination_folder}'", "Status": "success"}, context)
    except ClientError as e :
        splunk.log_message({'Message': f"Failed to move '{source_folder}' to '{destination_folder}'", "Status": "failed"}, context)
        raise e
        
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
        s3.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=destination_key)

        # Delete the original object
        s3.delete_object(Bucket=bucket_name, Key=source_key)
        splunk.log_message({'Message': f"Moved '{source_key}' to '{destination_key}'", "Status": "success"}, context)
    except ClientError as e:
        splunk.log_message({'Message': f"Error moving objects: {e}", "Status": "failed"}, context)
        raise e



def check_s3_folder_existence(bucket_name, folder_path, context):
    """
    Check if a folder exists in an S3 bucket.

    :param bucket_name: Name of the S3 bucket
    :param folder_path: Path of the folder to check
    :return: True if the folder exists, False otherwise
    """
    try:
        # List all objects with the specified prefix
        objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_path).get('Contents', [])

        # If any object with the specified prefix exists, consider the folder as existing
        return len(objects) > 0

    except ClientError as e:
        splunk.log_message({'Message': f"Error checking folder existence: {e}", "Status": "failed"}, context)
        return False


def create_folder(bucket_name, folder_path, context):
    """
    Create a folder in an S3 bucket.

    :param bucket_name: Name of the S3 bucket
    :param folder_path: Path of the folder to create
    """
    try:
        s3.put_object(Bucket=bucket_name, Key=folder_path)
        splunk.log_message({'Message': f"Folder '{folder_path}' created successfully", "Status": "success"}, context)
    except ClientError as e:
        splunk.log_message({'Message': f"Error creating folder: {e}", "Status": "failed"}, context)



def delete_folder(bucket_name, folder_path, context):
    """
    Delete a folder and its contents from an S3 bucket.

    :param bucket_name: Name of the S3 bucket
    :param folder_path: Path of the folder to delete
    """
    try:
        # List all objects in the folder
        objects_to_delete = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_path).get('Contents', [])

        # Delete each object in the folder
        for obj in objects_to_delete:
            s3.delete_object(Bucket=bucket_name, Key=obj['Key'])

    except ClientError as e:

        splunk.log_message({'Message': f"Error deleting folder: {e}", "Status": "failed"}, context)


def process_folders(event, context):
    """
    Process folders based on the provided event.

    :param event: Input event containing information about folder operations
    """
    bucket_name = event['bucketName']

    if event["isActive"]:
        try:
            folder_paths = event['pathsToBeCreated']['paths'].split(',')

            for folder_path in folder_paths:
                splunk.log_message(
                                       {'Message': f"{folder_paths}  if folder_path contains space' ' between the path name then replacing  with '_'", "Status": "success"},
                                   context)
                folder_path = folder_path.strip().replace(" " ,"_")

                if not check_s3_folder_existence(bucket_name, folder_path, context):
                    create_folder(bucket_name, folder_path, context)
                    splunk.log_message({'Message': f"{folder_path} folder created successfully", "Status": "success"},
                                       context)
                else :
                    splunk.log_message({'Message': f" Already folders processed ", "Status": "success"}, context)
                    continue
            splunk.log_message({'Message': f"processed folders successfully", "Status": "success"}, context)

        except ClientError as e:
            splunk.log_message({'Message': f"Error processing folders: {e}", "Status": "failed"}, context)
            raise e

    # invoked for destorying the paths when isActive is false

    else:
        splunk.log_message \
            ({'Message': f"the isActive Flag  the value is flase, hence distorying all dircetorie and sub dircetories  ", "Status": "success"}, context)
        try:
            folder_paths = event['pathsToBeCreated']['paths'].split(',')
            for folder_path in folder_paths:
                splunk.log_message(
                                       {'Message': f"{folder_paths}  if folder_path contains space' ' between the path name then replacing  with '_'", "Status": "success"},
                                   context)
                folder_path = folder_path.strip().replace(" " ,"_")
                if check_s3_folder_existence(bucket_name, folder_path, context):
                    move_folder(bucket_name, folder_path, context)
                    splunk.log_message(
                        {'Message': f"{folder_path} folder destoryed successfully", "Status": "success"}, context)

                else:
                    splunk.log_message(
                        {'Message': f"{folder_path} folder already destoryed successfully", "Status": "success"},
                        context)

        except ClientError as e:
            splunk.log_message({'Message': f"Error processing folders to destorying: {e}", "Status": "failed"}, context)
            raise e


def lambda_handler(event, context):
    """
    AWS Lambda function entry point.

    :param event: Input event containing information for processing
    :param context: AWS Lambda context object
    :return: Result of the Lambda function execution

    """
    try:
        
        if 'detail' in event:    
            bucket_name = event['detail']['bucket']['name']
            file_key= event['detail']['object']['key']
        else:
            bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
            file_key = event["Records"][0]["s3"]["object"]["key"]
            
        save_file_key = file_key.split('/', 2)[0]+'/save/'+file_key.split('/')[-1]
        failed_file_key = file_key.split('/', 2)[0]+'/failed/'+file_key.split('/')[-1]

        # Retrieve the JSON file content from S3
        try:
            response = s3.get_object(Bucket=bucket_name, Key=file_key)
            json_content = json.loads(response['Body'].read().decode('utf-8'))
            splunk.log_message({'Message': "config data loaded successfully"}, context)

            # Now, 'json_content' contains the content of the JSON file
            event = json_content
        except Exception as e:
            splunk.log_message({'Message': f"Failed to load the config data {e}", "Status": "failed"}, context)
            raise e

        if 'bucketName' not in event:
            event['bucketName'] = bucket_name

        process_folders(event, context)
      
        # once successeded the file process then moving the config file to save with
        move_objects_within_bucket(event["bucketName"], file_key, save_file_key, context)
    except Exception as e:
        move_objects_within_bucket(event["bucketName"], file_key, failed_file_key, context)
        splunk.log_message({'Message': str(e), "Status": "failed"}, context)
        raise e
    return {
        'statusCode': 200,
        'body': json.dumps('Client process is completed successfully')
    }
