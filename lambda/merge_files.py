import json
import os
import boto3
from datetime import datetime
from dpm_splunk_logger_py import splunk
import re
from pathlib import Path
import sys
s3 = boto3.client('s3')

def merge_file_pattern(product,s3Bucket,s3Folder,mergeFilePattern, mergedFileName,MergeFilePrefix,context):
    try:
        now = datetime.now()
        # Custom format
        formatted_date = now.strftime("%m%d%Y%H%M%S")

        if product == 'jhi_misc':
            merged_file_tokens = mergedFileName.split('.')
            merged_file_tokens.insert(-1, formatted_date)
            mergedFile = '.'.join(merged_file_tokens)
            print("Merged file name is " + mergedFile)
        else :
            mergedFile = mergedFileName

        # List all objects in the folder
        response = s3.list_objects_v2(Bucket=s3Bucket, Prefix=s3Folder)
    
        print("Response is " + str(response))

        if 'Contents' not in response:
            return {'statusCode': 404, 'body': 'No files found in folder.'}

        merged_content = ""

        archiveDetails = checkForArchival(s3Folder)      

        for obj in response['Contents']:
            key = obj['Key']
            if key.endswith('/'):  # Skip folder itself
                continue
        
            path = Path(key)
            # file_content = ""
            if re.match(mergeFilePattern, path.name):
                print("File name is " + key)
                file_obj = s3.get_object(Bucket=s3Bucket, Key=key)
                file_content = file_obj['Body'].read().decode('utf-8')   
                
                isArchiveRequired = archiveDetails.get('archiveRequired', False)

                if isArchiveRequired:         
                    copy_source = {
                        'Bucket': s3Bucket,
                        'Key': key
                    }

                    # Copy the object
                    s3.copy_object(CopySource=copy_source, Bucket=s3Bucket, Key=archiveDetails.get('archival_folder', ''))

                    # Delete the original object
                    s3.delete_object(Bucket=s3Bucket, Key=key)
                    print("File " + key + " archived successfully")
                    splunk.log_message({'Message': "Files archived successfully", "Status": "success" , 'statusCode': 200}, context)
            
                # You can process each file here
                merged_content += file_content + "\n"
    
        # Optionally write merged content back to S3
        remove_newline = lambda s: s.rstrip('\n')
        merged_content = remove_newline(merged_content)    
       
        if merged_content == "":
            splunk.log_message({'Message': "No files found in folder.", "Status": "success" , 'statusCode': 404}, context)
            return {'statusCode': 404, 'body': 'No files found in folder.'}
        
        s3Key = mergedFile
        if MergeFilePrefix and mergedFile:
            s3Key = MergeFilePrefix + mergedFile
        print(f"s3Key is: {s3Key}")
        s3.put_object(Bucket=s3Bucket, Key=s3Key , Body=merged_content.encode('utf-8'))
        splunk.log_message({'Message': "Files merged successfully", "Status": "success" , 'statusCode': 200}, context)

        return {
            'statusCode': 200,
            'body': json.dumps('Files merged successfully')
        }

        
    except Exception as e:
        error_message = f"Failed to merge files: {str(e)}"
        splunk.log_message({'Message': error_message, "Status": "failed"}, context)
        raise ValueError(error_message)

def lambda_handler(event, context):
    # TODO implement
    splunk.log_message({'Message': "Merge Lambda triggered successfully"}, context)
    try:
        s3Bucket = event.get('s3Bucket', '')      
        s3Folder = event.get('s3Folder', '')
        mergeFilePattern = event.get('mergeFilePattern', '')
        mergedFileName = event.get('mergedFileName', '')
        product = event.get('product', '')
        multiplePatternMerge = event.get('multiplePatternMerge', [])
        MergeFilePrefix = event.get('MergeFilePrefix', '')
        print(f"s3Bucket : {s3Bucket} | s3Folder: {s3Folder} | mergeFilePattern: {mergeFilePattern} \
         | mergedFileName: {mergedFileName} | product:{product} | multiplePatternMerge:{multiplePatternMerge}")
        if multiplePatternMerge:
            for pattern in multiplePatternMerge:
                mergeFilePattern = pattern.get('mergeFilePattern', '')
                mergedFileName = pattern.get('mergedFileName', '')
                MergeFilePrefix = pattern.get('MergeFilePrefix', '')
                merge_file_pattern(product,s3Bucket,s3Folder,mergeFilePattern, mergedFileName,MergeFilePrefix,context)
            return {
                    'statusCode': 200,
                    'body': json.dumps('Files merged successfully')
                }
        else:
            return merge_file_pattern(product,s3Bucket,s3Folder,mergeFilePattern, mergedFileName,MergeFilePrefix,context)
    except Exception as e:
        error_message = f"Failed Lambda Handler: {str(e)}"
        splunk.log_message({'Message': error_message, "Status": "failed"}, context)
        raise ValueError(error_message)


def checkForArchival(s3Folder):
    archival_folder = ''
    archivalData = ''
    if 'wip' in s3Folder:
        archivalData = {
            'archival_folder' : archival_folder,
            'archiveRequired' : False
        }
    else:
        s3FolderList = s3Folder.split("/")
        s3FolderList[-1] = "save"
        archival_folder = '/'.join(s3FolderList)
        print("Archival folder is " + archival_folder)
        archivalData = {
            'archival_folder' : archival_folder,
            'archiveRequired' : True
        }
    return archivalData