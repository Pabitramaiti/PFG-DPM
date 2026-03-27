import json
import boto3
from dpm_splunk_logger_py import splunk

s3_client = boto3.client('s3')

def list_object(bucket_name, source_keys, prefix):
    all_files = []
    
    for source_key in source_keys:
        # Combine the source_key (folder) with the prefix to form the actual prefix for listing
        if prefix:
            full_prefix = f"{source_key}/{prefix}"
        else:
            full_prefix = source_key
        # List objects with the provided full prefix
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=full_prefix)
        
        # Filter the files based on the full prefix and exclude folders (keys that end with '/')
        files = [
            {'Key': item['Key']}
            for item in response.get('Contents', [])
            if item['Key'].startswith(full_prefix) and not item['Key'].endswith('/')
        ]
        
        # Add the found files to the result list
        all_files.extend(files)
    
    return {'statusCode': 200, 'Files': all_files}

def get_object(bucket_name, source_keys):
    results = []
    for source_key in source_keys:
        response = s3_client.get_object(Bucket=bucket_name, Key=source_key)
        body_content = response['Body'].read().decode('utf-8')
        print("body_content : ", body_content)
        
        # Handle different file types based on extension
        if source_key.endswith('.json'):
            content = json.loads(body_content)  # Parse JSON content
        elif source_key.endswith('.txt'):
            content = body_content  # Plain text, no parsing needed
        elif source_key.endswith('.csv'):
            content = body_content.splitlines()  # Split lines for CSV
        else:
            raise ValueError(f"Unsupported file type for {source_key}")

        results.append({
            'Key': source_key,
            'Content': content
        })
    return {'statusCode': 200, 'files': results}

def copy_object(bucket_name, source_files, destination_key):
    responses = []
    for source_key in source_files:
        file_name = source_key.rsplit('/', 1)[-1]
        copy_source = {'Bucket': bucket_name, 'Key': source_key}
        
        # Copy the object
        s3_client.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=f"{destination_key}{file_name}")
        # Delete the original object
        s3_client.delete_object(Bucket=bucket_name, Key=source_key)
        
        responses.append(f'{file_name} file processed successfully')
    
    return {
        'statusCode': 200,
        'body': json.dumps(responses)  # Return all success messages
    }

def lambda_handler(event, context):
    try:
        body = {}
        if event is not None:
            body = event if isinstance(event, dict) else json.loads(event)
        else:
            raise ValueError("body parameter is empty")

        # Extract parameters
        s3_operation_type = body.get('s3OperationType')
        bucket_name = body.get('bucketName')
        source_keys = body.get('sourceKeys', [])

        if isinstance(source_keys, str):  # If it's a single string
            source_keys = [source_keys]  # Convert to a list
        elif not isinstance(source_keys, list):  # If it's neither
            raise ValueError("sourceKeys must be a non-empty list or a single string")

        # Handle different S3 operations
        if s3_operation_type == "list_object":
            prefix = body.get('prefix')  # Get the prefix from the event
            if not prefix:
                prefix = ''
                #raise ValueError("Prefix is required and should be provided by the Step Function")

            return list_object(bucket_name, source_keys, prefix)
            
        elif s3_operation_type == "get_object":
            return get_object(bucket_name, source_keys)
            
        elif s3_operation_type == "copy_object":
            destination_key = body['destinationKey']
            return copy_object(bucket_name, source_keys, destination_key)
            
        elif s3_operation_type == "list_and_copy_object":
            # First, list the objects to get their keys
            prefix = body.get('prefix')  # Get the prefix from the event
            if not prefix:
                prefix = ''

            list_response = list_object(bucket_name, source_keys, prefix)
            if list_response['statusCode'] != 200:
                return list_response
            
            # Extracting the 'Key' values into a new list
            keys_list = [item['Key'] for item in list_response['Files']]
            
            # Now call copy_object with the listed files
            destination_key = body['destinationKey']  # Assuming destination is the same for all files
            return copy_object(bucket_name, keys_list, destination_key)
        else:
            raise ValueError("Invalid s3OperationType")

    except Exception as e:
        error_message = str(e)
        splunk.log_message({"Message": error_message, "Status": "Failed"}, context)
        return {
            'statusCode': 500,
            'body': json.dumps({"error": error_message})
        }
