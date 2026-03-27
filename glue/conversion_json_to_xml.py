import boto3
import json
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta  
import re
from awsglue.utils import getResolvedOptions
import os
from botocore.exceptions import ClientError
import sys
import PyPDF2
from io import BytesIO
import splunk

s3_client = boto3.client('s3')

# Get a unique run identifier based on AWS account ID
def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:br_icsdev_dpmtest_conversionJsonToXml:" + account_id

# Log messages to Splunk with status and message
def log_message(status, message):
    splunk.log_message({'FileName':inputFileName,'Status': status, 'Message': message}, get_run_id())

# Read a JSON file from S3 and return its contents
def read_s3_file(bucket, key):
    response = s3_client.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')
    return json.loads(content)

# Recursively get nested key value from dictionary using dot-separated path
def get_nested_value(data, nav):
    if not nav:  # If nav is empty, return the original dictionary
        return data

    keys = nav.split('.')  # Split the string into a list of keys

    current = data
    for key in keys:
        if key in current:
            current = current[key]  # Traverse deeper
        else:
            return "Key not found"

    return current

# Converts JSON to XML for one input file with single/multiple documents
def json_to_xml_n_1(json_data, key_mapping,iparm):
    
    urXmlType=key_mapping["urXmlType"]
    fileId, fileProductId, fileType , additionalTLE= get_fileDetails(json_data, key_mapping,iparm)
    root = ET.Element('file')
    sequenceNum=0
    totalPage=0
    
    # Logic based on urXmlType
    if urXmlType=="urxmlloader":
        populate_file_header_info_html(fileId, fileProductId, root)
        documents = ET.SubElement(root, 'documents')
        sequenceNum, totalPage = populate_documents(json_data, key_mapping, iparm, documents, additionalTLE, sequenceNum, totalPage)
        ET.SubElement(root, 'documentCount').text = str(sequenceNum)
    elif urXmlType=="urpdfloader":
        populate_file_header_info_pdf(fileId, fileProductId, root)
        documents = ET.SubElement(root, 'documents')
        sequenceNum, totalPage = populate_documents(json_data, key_mapping, iparm, documents, additionalTLE, sequenceNum, totalPage)
        ET.SubElement(root, 'fileDocumentCount').text = str(sequenceNum)
        if  "pdfPageCountFlag" in iparm["payloadFileMapping"] and iparm["payloadFileMapping"]["pdfPageCountFlag"]=="True":
            ET.SubElement(root, 'fileImageCount').text = str(totalPage)
        
    return ET.tostring(root, encoding='unicode')

# Converts JSON to XML for multiple input documents grouped by key
def json_to_xml_n_m(json_data, key_mapping, iparm):
    urXmlType=key_mapping["urXmlType"]
    fileId, fileProductId, fileType , additionalTLE= get_fileDetails(json_data, key_mapping,iparm)
    data={}
    
    keyNav=additionalTLE["keyNav"]
    for meta in json_data:
        ArchiveFile=get_nested_value(meta, keyNav)
        print(f"ArchiveFile : {ArchiveFile}")
        if ArchiveFile not in data:
            data[ArchiveFile] = []
        data[ArchiveFile].append(meta)
         
    root = ET.Element('file')
    sequenceNum=0
    totalPage=0
    get_payloadFileName(json_data, key_mapping,iparm)
    # Build XML structure based on type
    if urXmlType=="urxmlloader":
        populate_file_header_info_html(fileId, fileProductId, root)
        documents = ET.SubElement(root, 'documents')
        for ArchiveFile, metas in data.items():
            sequenceNum, totalPage = populate_documents(metas, key_mapping, iparm, documents, additionalTLE, sequenceNum, totalPage)
        # sequenceNum, totalPage = populate_documents(json_data, key_mapping, iparm, root, additionalTLE, sequenceNum, totalPage)
        ET.SubElement(root, 'documentCount').text = str(sequenceNum)
    elif urXmlType=="urpdfloader":
        populate_file_header_info_pdf(fileId, fileProductId, root)
        documents = ET.SubElement(root, 'documents')
        sequenceNum, totalPage = populate_documents(json_data, key_mapping, iparm, documents, additionalTLE, sequenceNum, totalPage)
        ET.SubElement(root, 'fileDocumentCount').text = str(sequenceNum)
        if  "pdfPageCountFlag" in iparm["payloadFileMapping"] and iparm["payloadFileMapping"]["pdfPageCountFlag"]=="True":
            ET.SubElement(root, 'fileImageCount').text = str(totalPage)
        
    return ET.tostring(root, encoding='unicode')

# Converts JSON to XML assuming 1-to-1 mapping (similar to n_m)
def json_to_xml_1_1(json_data, key_mapping, iparm):
    print("data stroped")
    urXmlType=key_mapping["urXmlType"]
    fileId, fileProductId, fileType , additionalTLE = get_fileDetails(json_data, key_mapping, iparm)
    data={}
    keyNav=additionalTLE["keyNav"]
    for meta in json_data:
        ArchiveFile=get_nested_value(meta, keyNav)
        print(f"ArchiveFile : {ArchiveFile}")
        if ArchiveFile not in data:
            data[ArchiveFile] = []
        data[ArchiveFile].append(meta)
            
    root = ET.Element('file')
    sequenceNum=0
    totalPage=0
    get_payloadFileName(json_data, key_mapping,iparm)
    if urXmlType=="urxmlloader":
        populate_file_header_info_html(fileId, fileProductId, root)
        documents = ET.SubElement(root, 'documents')
        for ArchiveFile, metas in data.items():
            sequenceNum, totalPage = populate_documents(metas, key_mapping, iparm, documents, additionalTLE, sequenceNum, totalPage)

        ET.SubElement(root, 'documentCount').text = str(sequenceNum)
    elif urXmlType=="urpdfloader":
        populate_file_header_info_pdf(fileId, fileProductId, root)
        documents = ET.SubElement(root, 'documents')
        sequenceNum, totalPage = populate_documents(json_data, key_mapping, iparm, documents, additionalTLE, sequenceNum, totalPage)
        ET.SubElement(root, 'fileDocumentCount').text = str(sequenceNum)
        if  "pdfPageCountFlag" in iparm["payloadFileMapping"] and iparm["payloadFileMapping"]["pdfPageCountFlag"]=="True":
            ET.SubElement(root, 'fileImageCount').text = str(totalPage)
        
    return ET.tostring(root, encoding='unicode')

# Populate <file-urattributes> section for HTML loader
def populate_file_header_info_html(fileId, fileProductId, root):
    document_urattributes = ET.SubElement(root, 'file-urattributes')
    urattribute = ET.SubElement(document_urattributes, 'urattribute')
    ET.SubElement(urattribute, 'name').text = "UR_PRODUCT_ID"
    ET.SubElement(urattribute, 'value').text = str(fileProductId)
    
# Populate <file> header for PDF loader
def populate_file_header_info_pdf(fileId, fileProductId, root):
    ET.SubElement(root, 'fileId').text = str(fileId)
    ET.SubElement(root, 'fileDate').text = str(datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'))
    ET.SubElement(root, 'fileProductId').text = str(fileProductId)
    
# Populate <document> elements from JSON data
def populate_documents(json_data, key_mapping, iparm, documents, additionalTLE, sequenceNum, totalPage):
    sequenceNum=sequenceNum+1
    
    # codes for <document> tag
    accountNumber=get_accountNumber(json_data, key_mapping, iparm)
    document = ET.SubElement(documents, 'document')
    ET.SubElement(document, 'sequenceNum').text = str(sequenceNum)
        
    # If PDF page count is required, add metadata

    if  "pdfPageCountFlag" in iparm["payloadFileMapping"] and iparm["payloadFileMapping"]["pdfPageCountFlag"]=="True":
        ET.SubElement(document, 'accountNumber').text = str(accountNumber)
        pdfFileName=get_payloadFileName(json_data, key_mapping,iparm)
        ET.SubElement(document, 'pdfFileName').text = str(pdfFileName)
        page=get_pdf_page_count(iparm,pdfFileName)
        totalPage=totalPage+page
        ET.SubElement(document, 'pdfImageCount').text = str(page)
    document_urattributes = ET.SubElement(document, 'document-urattributes')
    count = 0
    key_mapping=key_mapping["Mapping"]
        

    # Extract and map JSON fields to XML <urattribute>
    for key, value in json_data[0].items():
        if isinstance(value, dict):
            for nested_key, nested_value in value.items():
                if key in key_mapping and nested_key in key_mapping[key]:
                    mapping = key_mapping[key][nested_key]
                    urattribute = ET.SubElement(document_urattributes, 'urattribute')
                    ET.SubElement(urattribute, 'name').text = str(mapping['xmlTag'])
                    ET.SubElement(urattribute, 'value').text = str(nested_value)
                    
        elif key in key_mapping:
            mapping = key_mapping[key]
            urattribute = ET.SubElement(document_urattributes, 'urattribute')
            ET.SubElement(urattribute, 'name').text = str(mapping['xmlTag'])
            ET.SubElement(urattribute, 'value').text = str(value)
    
    # Handle additionalTLE attributes (static or custom logic)
    for tleType, tleMap in additionalTLE.items():
        if tleType =="document-urattributes":
            for key,value in tleMap.items():
                if value[0]=="STATIC":
                    urattribute = ET.SubElement(document_urattributes, 'urattribute')
                    ET.SubElement(urattribute, 'name').text = str(key)
                    ET.SubElement(urattribute, 'value').text = str(value[1])
                elif value[0]=="CUSTOM":
                    if value[1]=="TIMESTAMP":
                        if value[2]=="CURRENT":
                            # Generate current timestamp in custom format
                            current_datetime_utc = datetime.utcnow()  
                            current_datetime_est = current_datetime_utc - timedelta(hours=5) 
                            formatted_datetime = current_datetime_est.strftime(f"{value[3]}")  
                            urattribute = ET.SubElement(document_urattributes, 'urattribute')
                            ET.SubElement(urattribute, 'name').text = str(key)
                            ET.SubElement(urattribute, 'value').text = str(formatted_datetime)

                            print(f"formatted_datetime :: {formatted_datetime}")
                        elif value[2]=="COPY":                           
                            # Copy existing timestamp and reformat
                            if document_urattributes is not None:
                                for urattribute in document_urattributes.findall("urattribute"):
                                    name = urattribute.find("name")
                                    tleValue = urattribute.find("value")
                                    if name is not None and tleValue is not None and name.text == value[3]:
                                        original_date = tleValue.text
                                        parsed_date = datetime.strptime(original_date, value[4])
                                        formatted_date = parsed_date.strftime(value[5])
                                        urattribute = ET.SubElement(document_urattributes, 'urattribute')
                                        ET.SubElement(urattribute, 'name').text = str(key)
                                        ET.SubElement(urattribute, 'value').text = str(formatted_date)
                                        
        elif tleType =="document":
            for key,value in tleMap.items():
                if value[0]=="STATIC":
                    ET.SubElement(document, key).text = str(value[1])
                elif value[0]=="CUSTOM":
                    if value[1] == "JSON":
                        ET.SubElement(document, key).text = str(extract_value_from_json(value[2]["checkFilesDataType"], value[2]["checkFilesKey"], json_data))
        
        

    composite_index = []
    count = 0
    for item in json_data:
        if count == 0:
            count = count + 1
            continue
        index_item = {}
        for key, value in item.items():
            if isinstance(value, dict):
                for nested_key, nested_value in value.items():
                    if key in key_mapping and nested_key in key_mapping[key]:
                        mapping = key_mapping[key][nested_key]
                        if 'compositeIndex' in mapping and mapping['compositeIndex']:
                            index_item[mapping['xmlTag']] = nested_value
            elif key in key_mapping:
                mapping = key_mapping[key]
                if 'compositeIndex' in mapping and mapping['compositeIndex']:
                    index_item[mapping['xmlTag']] = value
        composite_index.append(index_item)
        urattribute = ET.SubElement(document_urattributes, 'urattribute')
        ET.SubElement(urattribute, 'name').text = 'UC_SUBDOCUMENT_COMPOSITE_INDEX'
        ET.SubElement(urattribute, 'value').text = json.dumps(composite_index)
        # Add closing urattribute tag
        # document_urattributes.append(urattribute)
        composite_index = []
        count = count + 1
        
    return sequenceNum, totalPage

# Function to extract value from JSON based on a list of data types and keys
def extract_value_from_json(checkFilesDataType, checkFilesKey, input_json_data):
    # Handle single key-value extraction for JSON
    if len(checkFilesKey) == 1:

        if checkFilesDataType[0] == 'json':
            # If the type is 'json', return the value corresponding to the key
            return input_json_data[checkFilesKey[0]]
        elif checkFilesDataType[0] == 'list':
            # If the type is 'list', iterate over the list of keys and return value from JSON
            key = checkFilesKey[0]
            if len(key) != 0:
                for index in key:
                    return input_json_data[index]
            else:
                for value in input_json_data:
                    return value
    else:
        # Process multiple keys in a recursive manner based on data type
        data_type = checkFilesDataType[0]
        key = checkFilesKey[0]
        if data_type == 'list':
            if len(key) != 0:
                # Recursively extract values from a list if data type is 'list'
                for index in key:
                    return extract_value_from_json(checkFilesDataType[1:], checkFilesKey[1:], input_json_data[index])
            else:
                for value in input_json_data:
                    return extract_value_from_json(checkFilesDataType[1:], checkFilesKey[1:], value)
        elif data_type == 'json':
            # If data type is 'json', recursively access the JSON data by key
            return extract_value_from_json(checkFilesDataType[1:], checkFilesKey[1:], input_json_data[key])
        else:
            # Handle unsupported data types
            log_message('failed',f"Unsupported data type: {data_type}")
            raise ValueError(f"Unsupported data type: {data_type}")

# Function to extract file details from the JSON data based on a key mapping
def get_fileDetails(json_data, key_mapping, iparm):
    # Retrieve and extract file-specific details from the client and product ID mappings
    checkFilesDataType = iparm["clientIdMapping"]['checkFilesDataType']
    checkFilesKey = iparm["clientIdMapping"]['checkFilesKey']
    fileIdKey= extract_value_from_json( checkFilesDataType, checkFilesKey,json_data)
    fileId=key_mapping["additionalAttributes"]["clientIdMapping"][fileIdKey]
    
    checkFilesDataType = iparm["productIdMapping"]['checkFilesDataType']
    checkFilesKey = iparm["productIdMapping"]['checkFilesKey']
    fileProductIdKey= extract_value_from_json( checkFilesDataType, checkFilesKey,json_data)
    fileProductId=key_mapping["additionalAttributes"]["fileProductIdMapping"][fileId][fileProductIdKey]
    additionalTLE=key_mapping["additionalAttributes"]["fileProductIdMapping"][fileId]["additionalTLE"][fileProductId]
    return fileId, fileProductId, fileProductIdKey, additionalTLE

# Function to get account number from the JSON data
def get_accountNumber(json_data, key_mapping, iparm):
    # Extract account number using the key mapping provided
    checkFilesDataType = iparm["clientIdMapping"]['checkFilesDataType']
    checkFilesKey = iparm["clientIdMapping"]['checkFilesKey']
    accountNumber= extract_value_from_json( checkFilesDataType, checkFilesKey,json_data)
    # log_message('success',f"accountNumber : {accountNumber}")
    return accountNumber

# Function to get the PDF file name from the JSON data
def get_payloadFileName(json_data, key_mapping, iparm):
    # Extract PDF file name using the key mapping provided
    checkFilesDataType = iparm["payloadFileMapping"]['checkFilesDataType']
    checkFilesKey = iparm["payloadFileMapping"]['checkFilesKey']
    pdfFileName= extract_value_from_json( checkFilesDataType, checkFilesKey,json_data)
    # log_message('success',f"pdfFileName : {pdfFileName}")
    return pdfFileName

# Function to get the page count of a PDF file from S3
def get_pdf_page_count(iparm, pdfFileName):
    # Fetch the PDF file from S3 and count the number of pages
    bucket=iparm["s3_bucket_input"]
    key=iparm["payloadPath"]+pdfFileName
    response = s3_client.get_object(Bucket=bucket, Key=key)
    pdf_stream = BytesIO(response['Body'].read())
    reader = PyPDF2.PdfReader(pdf_stream)
    num_pages=len(reader.pages)
    log_message('success',f"pdf_page_count: {num_pages}")
    return num_pages

# Function to get matching S3 keys based on prefix and pattern
def get_matching_s3_keys(bucket, prefix='', pattern=''):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    keys = []
    
    # Paginate through S3 objects and match keys with the provided pattern
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = os.path.basename(obj['Key'])
            if re.match(pattern, key):
                keys.append(obj['Key'])
    
    return keys[0]

# Function to set environment variables from command-line arguments
def set_job_params_as_env_vars():
    # Loop through all command-line arguments starting from the second argument (skip the script name)
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]  # Remove the leading '--'
            value = sys.argv[i + 1]
            os.environ[key] = value
            # print(f'Set environment variable {key} to {value}')

# Main function to execute the Glue job
def main():
    set_job_params_as_env_vars()  # Set the environment variables from arguments
    iparm={}
    # Store the environment variables into iparm for further use
    for key, value in os.environ.items():
        iparm[key] = value
    
    # Initialize file name and paths
    global inputFileName
    inputFileName = iparm['inputFileName']
    json_bucket = iparm["s3_bucket_input"]
    mapping_bucket = iparm["s3_bucket_input"]
    output_bucket = iparm["s3_bucket_input"]
    jsonPath = iparm["jsonPath"]
    jsonPattern = iparm["jsonPattern"]
    jsonKey = iparm["jsonKey"]
    xmlConfigPath = iparm["xmlConfigPath"]
    xmlConfigPattern = iparm["xmlConfigPattern"]
    xmlConfigKey = iparm["xmlConfigKey"]
    
    # Convert batch configuration and mappings from JSON format
    iparm["batching"]=json.loads(iparm["batching"])
    iparm["clientIdMapping"]=json.loads(iparm["clientIdMapping"])
    iparm["productIdMapping"]=json.loads(iparm["productIdMapping"])
    iparm["payloadFileMapping"]=json.loads(iparm["payloadFileMapping"])
    
    log_message('logging', f"conversion Json To UR Xml Glue Job called.")
    
    try:
        json_key = jsonKey if jsonKey !=" " else  get_matching_s3_keys(json_bucket, jsonPath, jsonPattern)
        mapping_key = xmlConfigKey if xmlConfigKey !=" " else  get_matching_s3_keys(mapping_bucket, xmlConfigPath, xmlConfigPattern)

        # Read JSON data and key mappings from S3
        json_data = read_s3_file(json_bucket, json_key)
        key_mapping = read_s3_file(mapping_bucket, mapping_key) 
        rootNav= key_mapping.get("rootNav", "")
        print(f"key_mapping rootNav : {rootNav}")   
        json_data=get_nested_value(json_data, rootNav) 
        fileId, fileProductId, fileType, additionalTLE = get_fileDetails(json_data, key_mapping,iparm)
                       
        # Initialize iparm JSON with file details
        iparm["json_key"]=json_key 
        iparm["fileId"]=fileId
        iparm["fileProductId"]=fileProductId
        xml_data=""
        
        # Convert JSON data to UR XML based on mapping type
        if(iparm["mappingType"]=="N-1"):
            xml_data = json_to_xml_n_1(json_data, key_mapping,iparm)    
        elif(iparm["mappingType"]=="N-M"):
            xml_data = json_to_xml_n_m(json_data, key_mapping,iparm)    
        elif(iparm["mappingType"]=="1-1"):
            xml_data = json_to_xml_1_1(json_data, key_mapping,iparm)    
        
        xmlFileName= f"{iparm['xmlDestinationPrefix']}.cbu.{fileId}.{fileProductId.split('/')[-1]}"
        if iparm["batching"]:
            # Handle batching logic and append timestamp/transaction info to the XML filename
            payloadFileName=get_payloadFileName(json_data, key_mapping,iparm)
            # Get the current timestamp in the format YYYYMMDDHHMISS
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            if 'timestampFlag' in iparm["batching"] and iparm["batching"]["timestampFlag"]:
                xmlFileName=xmlFileName+"."+timestamp
            if 'transactionFlag' in iparm["batching"] and iparm["batching"]["transactionFlag"]:
                xmlFileName=xmlFileName+"."+ iparm["batching"]["transactionId"]
            if 'fileNameFlag' in iparm["batching"] and iparm["batching"]["fileNameFlag"]:
                xmlFileName=xmlFileName+"."+ payloadFileName.split('/')[-1].split('.')[0]
                
            batchingPath=f'{iparm["batching"]["batchingPath"]}{fileId}-{fileProductId.split("/")[-1]}-{fileType}/'
            batchingPath = batchingPath.replace(" ", "_")
            xmlFileName=xmlFileName+".xml"
            batchingXmlKey=batchingPath+xmlFileName
            # Upload XML to S3
            s3_client.put_object(Bucket=output_bucket, Key=batchingXmlKey, Body=xml_data)
            s3_client.copy_object(
                Bucket=output_bucket,
                CopySource={'Bucket': output_bucket, 'Key': f"{iparm['payloadPath']}{payloadFileName}"},
                Key=f"{batchingPath}{payloadFileName}"
            )

        # Final upload to the output S3 bucket
        output_key= iparm["xmlDestination"]+xmlFileName
        if output_key.rsplit('.', 1)[-1]!="xml":
            output_key=output_key+".xml"
        
        s3_client.put_object(Bucket=output_bucket, Key=output_key, Body=xml_data)
        log_message('success',f"XML file successfully written to s3://{output_bucket}/{output_key}")
    except Exception as e:
        log_message('failed', f"Unexpected error during JSON to UR xml conversion : {str(e)}")
        raise ValueError(f"Unexpected error during JSON UR xml conversion : {str(e)}")

inputFileName=''
if __name__ == '__main__':
    main()

