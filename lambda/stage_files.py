import json
import boto3
from datetime import datetime
from dpm_splunk_logger_py import splunk
import re
from pathlib import Path

# Custom file rename options along with default join options (.)
def custom_file_rename(file_pattern, file_name,position_removal,position_insertion,context,join_symbol='.'):
    splunk.log_message({'Message': f"file rename is processing for file: {file_name} pattern is : {file_pattern}"},context)
    try:
        match = re.match(file_pattern,file_name)
        parts = list(match.groups())
        #Remove name value based on group positions
        if position_removal:
            for pos in sorted(position_removal,reverse=True):
                if 0 <= int(pos)-1 < len(parts):
                    del parts[int(pos)-1]
        #Add name based on the position and value like timestamp, TRIGGER
        if position_insertion:
            for pos_str,value in sorted(position_insertion.items(),key = lambda x:int(x[0])):
                pos = int(pos_str)-1
                if isinstance(value,dict) and "timestamp" in value:
                    ts_value = datetime.now().strftime(value["timestamp"])
                    insert_value = str(ts_value)
                elif isinstance(value,dict) and "date" in value:
                    dt_value = datetime.now().strftime(value["date"])
                    insert_value = str(dt_value)
                else:
                    insert_value = str(value)

                if pos > len(parts):
                    pos = len(parts)
                parts.insert(pos,insert_value)

        new_filename = join_symbol.join(parts)
        return new_filename
    except Exception as e:
        error_message = f"Failed to rename file {file_name} pattern is {file_pattern} Error: {str(e)}"
        splunk.log_message({'Message': f"Failed to rename file {file_name} pattern is {file_pattern}", 'Error': str(e)},context)
        raise ValueError(error_message) 

def lambda_handler(event, context):
    # TODO implement
    splunk.log_message({'Message': "Stage Files Lambda triggered successfully"}, context)
    destinationPath = event.get('destinationPath', '')
    
    try:

        s3Bucket = event.get('bucket', '')
        sourceFilePath = event.get('file_path', '')
        sourcekey = event.get('file_key', '')
        move_flag = event.get('move_flag', False)
        sourceFilePattern = event.get('file_pattern', '')
        include_timestamp = event.get('include_timestamp', True)
                
        s3 = boto3.client('s3')

        rename_flag = event.get('rename_flag', False)
        position_insertion = event.get('position_insertion', {})
        position_removal = event.get('position_removal', [])
        # If sourceFilePath and sourceFilePattern are a single space, move/copy the exact sourcekey
        use_source_key_only = sourceFilePath == " " and sourceFilePattern == " "

        if use_source_key_only:
            if not sourcekey:
                return {'statusCode': 400, 'body': 'sourcekey is required when sourceFilePath and sourceFilePattern are blank.'}
            keys_to_process = [sourcekey]
            splunk.log_message({'Message': f"Using sourcekey directly: {sourcekey}"}, context)
        else:
            # List all objects in the folder
            response = s3.list_objects_v2(Bucket=s3Bucket, Prefix=sourceFilePath)
        
            splunk.log_message({'Message': f"S3 list objects response: {str(response)}"}, context)

            if 'Contents' not in response:
                return {'statusCode': 404, 'body': 'No files found in folder.'}
            keys_to_process = [obj['Key'] for obj in response['Contents']]

        # Generate timestamp only if needed
        if include_timestamp:
            now = datetime.now()
            formatted_date = now.strftime("%m%d%Y%H%M%S")

        for key in keys_to_process:
            splunk.log_message({'Message': f"Processing key: {key}"}, context)
            if key.endswith('/'):  # Skip folder itself
                continue
            splunk.log_message({'Message': f"Source key pattern: {sourceFilePattern}"}, context)

            path = Path(key)
            
            # Apply timestamp only if include_timestamp is True
            if include_timestamp:
                input_file_tokens = path.name.split('.')
                input_file_tokens.insert(-1, formatted_date)
                output_file = '.'.join(input_file_tokens)
            else:
                output_file = path.name

            if use_source_key_only or re.search(sourceFilePattern , path.name):
                if rename_flag and not use_source_key_only:
                    output_file = custom_file_rename(sourceFilePattern, path.name, position_removal, position_insertion,context)
                dest_file = destinationPath+output_file
                dest_key = key.replace(key, dest_file, 1)
                splunk.log_message({'Message': f"Destination key: {dest_key}"}, context)
                copy_source = {'Bucket': s3Bucket, 'Key': key}
                s3.copy_object(CopySource=copy_source, Bucket=s3Bucket, Key=dest_key)
                
                # If move_flag is True, delete the source file after copying
                if move_flag:
                    splunk.log_message({'Message': f"Moving file: deleting source key {key}"}, context)
                    s3.delete_object(Bucket=s3Bucket, Key=key)


        return {
            'statusCode': 200,
            'body': json.dumps(f'File Stage Completed - Files {"moved" if move_flag else "copied"}')
        }
    except Exception as e:
        operation = "move" if move_flag else "copy"
        error_message = f"Failed to {operation} files to destination"
        splunk.log_message({'Message': f"Failed to {operation} files to destination: {destinationPath}", 'Error': str(e)}, context)
        raise ValueError(error_message)    