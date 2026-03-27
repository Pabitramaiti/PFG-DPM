import boto3
import re
import sys
import os
import splunk
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from xml.etree.ElementTree import Element, tostring, fromstring, ElementTree

# Initialize boto3 S3 client
s3 = boto3.client('s3')

def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:merge_multiple_xml : " + account_id

def log_message(status, message):
    splunk.log_message({'FileName': inputFileName, 'Status': status, 'Message': message}, get_run_id())

def set_job_params_as_env_vars():
    # Loop through all command-line arguments starting from the second argument (skip the script name)
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]  # Remove the leading '--'
            value = sys.argv[i + 1]
            os.environ[key] = value

# Function to merge XML files
def merge_xml_files(file_contents):
    # Parse each XML file content
    parsed_files = [fromstring(content) for content in file_contents]
    
    # Parse XML content
    tree = ElementTree(fromstring(file_contents[0]))  # Parse directly from string
    root = tree.getroot()

    # Assuming the structure of each XML file is the same and they have <documents> tags
    root = Element(root.tag)

    # Use the first file's root and <documents> as the base
    base_root = parsed_files[0]
    base_documents = base_root.find("documents")
    if base_documents is None:
        log_message('failed', f"The <documents> tag is missing in the first XML file.")
        raise ValueError("The <documents> tag is missing in the first XML file.")
    
    
    # Add documents from all files and update sequenceNum
    fileDocumentCountFlag,documentCountFlag,fileImageCountFlag=False,False,False
               
    for field in parsed_files[0]:
        if (field.tag not in ["documents","fileDocumentCount","documentCount","fileImageCount"]):
            element = base_root.find(field.tag)
            root.append(element)
        if (field.tag == "fileDocumentCount"):
            fileDocumentCountFlag=True
        elif (field.tag == "documentCount"):
            documentCountFlag=True
        elif (field.tag == "fileImageCount"):
            fileImageCountFlag=True
        
    # Add the base documents to the new root
    documents = Element("documents")
    root.append(documents)

    # Initialize sequenceNum counter and fileImageCount sum
    sequence_counter = 1
    total_pdf_image_count = 0

    # Add documents from all files and update sequenceNum
    for parsed_file in parsed_files:
        additional_documents = parsed_file.find("documents")
        if additional_documents is not None:
            for doc in additional_documents:
                sequence_num_elem = doc.find("sequenceNum")
                if sequence_num_elem is not None:
                    sequence_num_elem.text = str(sequence_counter)
                    sequence_counter += 1
                documents.append(doc)

                # Sum up pdfImageCount for each document
                pdf_image_count_elem = doc.find("pdfImageCount")
                if pdf_image_count_elem is not None:
                    total_pdf_image_count += int(pdf_image_count_elem.text)

    # Replace fileDocumentCount with sequence_counter
    if fileDocumentCountFlag:
        file_document_count = Element("fileDocumentCount")
        file_document_count.text = str(sequence_counter - 1)
        root.append(file_document_count)
    if documentCountFlag:
        file_document_count = Element("documentCount")
        file_document_count.text = str(sequence_counter - 1)
        root.append(file_document_count)
    
    # Set fileImageCount to the sum of all pdfImageCount values
    if fileImageCountFlag:
        file_image_count = Element("fileImageCount")
        file_image_count.text = str(total_pdf_image_count)
        root.append(file_image_count)

    return tostring(root, encoding='utf-8', xml_declaration=True)

# Search for files matching the regex pattern in the specified path
def find_matching_files(bucket, path, regex):
    """Get a list of files in an S3 prefix with a specific suffix using paginator."""
    file_list = []
    pattern = re.compile(regex)
    global filename
    try:
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=path):
            if 'Contents' not in page:
                continue
            for obj in page['Contents']:
                print(obj['Key'])
                if pattern.search(obj['Key'].split('/')[-1]):
                    file_list.append(obj['Key'])
                    filename = obj['Key'].split('/')[-1]
                    
    except (NoCredentialsError, PartialCredentialsError):
        log_message('failed', f"Credentials not available.")
        raise ValueError(f"Credentials not available.")
    except Exception as e:
        log_message('failed', f"Error finding matching files: {e}")
        raise ValueError(f"Error finding matching files: {e}")
    return file_list

# Download matching files and merge them
def download_files(bucket, file_keys):
    file_contents = []
    for file_key in file_keys:
        try:
            response = s3.get_object(Bucket=bucket, Key=file_key)
            file_contents.append(response['Body'].read())
        except Exception as e:
            log_message('failed', f"Error downloading {file_key}: {e}")
            raise ValueError(f"Error downloading {file_key}: {e}")
    return file_contents

# Main function to execute the merging process
def main():
    
    set_job_params_as_env_vars()
    global inputFileName
    inputFileName=os.getenv('inputFileName')
    bucket=os.getenv('bucket')
    path=os.getenv('path')
    regex_pattern=os.getenv('regex_pattern')
    
    matching_files = find_matching_files(bucket, path, regex_pattern)
    if not matching_files:
        log_message('failed', f"No matching files found.")
        raise ValueError("No matching files found.")
        

    file_contents = download_files(bucket, matching_files)
    merged_xml = merge_xml_files(file_contents)

    # Save the merged XML back to S3
    keyList = filename.split(".")
    key = f"{keyList[0]}.{keyList[1]}.{keyList[2]}.{keyList[3]}.{keyList[-1]}"
    output_key = f'{path}{key}'
    
    try:
        s3.put_object(Bucket=bucket, Key=output_key, Body=merged_xml)
        log_message('success', f"Merged XML file saved to s3://{bucket}/{output_key}")
    except Exception as e:
        log_message('failed', f"Error saving merged file: {e}")
        raise ValueError(f"Error saving merged file: {e}")

filename = ''
inputFileName=''

# Run the main function
if __name__ == "__main__":
    main()
