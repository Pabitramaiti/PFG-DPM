import sys
import boto3
import traceback
from awsglue.utils import getResolvedOptions
import splunk
import re
import os
from s3fs import S3FileSystem
import xml.etree.ElementTree as ET
from lxml import etree

#s3 object
s3_client = boto3.client('s3')

# simplefilestorage to get data in s3 object in bytes
sfs = S3FileSystem()

# Custom exception module
#class EmptyS3FileError(Exception):
    #pass

def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:validate-xml-file:" + account_id

def get_configs():
    args = getResolvedOptions(sys.argv, ['s3_bucket_input', 's3_key_input_xml', 's3_key_input_xsd', 's3_xsd_pattern', 's3_destination_key'])
    return args

def open_input_file(input_bucket, input_key):
    
    #get file information
    file_info = sfs.info(f'{input_bucket}/{input_key}')

    if file_info['size'] == 0:
        #raise EmptyS3FileError(f"File is empty")
        raise ValueError(f"file open from s3 bucket failed. The file {input_key} in bucket {input_bucket} is empty.")
    else:
        # Open input file
        input_file_uri = "s3://" + input_bucket + "/" + input_key
        input_file_contents = sfs.open(input_file_uri, 'rb')
        
        return input_file_contents
    
def secure_parse(xml_data):
    """Securely parse XML data with DTD processing disabled."""
    parser = etree.XMLParser(load_dtd=False, no_network=True, resolve_entities=False)
    return etree.parse(xml_data, parser)

# def validate_xml(input_bucket, xml_key_data, xsd_key_data):
#     # Validate xml against xsd
#     xmlschema = etree.XMLSchema(etree.parse(xsd_key_data))
#     xmlparser = etree.XMLParser(schema=xmlschema)

#     etree.parse(xml_key_data, xmlparser)


def validate_xml(input_bucket, xml_key_data, xsd_key_data):
  # Parse the XSD schema
    xsd_tree = secure_parse(xsd_key_data)
    xmlschema = etree.XMLSchema(xsd_tree)

    # Parse and validate the XML data
    xml_tree = secure_parse(xml_key_data)
    xmlschema.assertValid(xml_tree)
    
def find_matching_file(bucket_name, path, file_name, pattern):
    '''
    function to extract matching file from the specified path
    '''
    
    # Ensure the path ends with a '/' to list contents inside it
    if not path.endswith('/'):
        path += '/'

    # Get the list of objects in the S3 bucket at the specified path
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=path)
    
    # Check if 'Contents' is in the response (it's not if the path is empty)
    if 'Contents' not in response:
        return None

    # Loop through all files in the specified path
    for obj in response['Contents']:
        file = obj['Key'].split('/')[-1]
        match = re.search(pattern, file)
        if match:
            group1 = match.group(1)
            if group1 in file_name:
                return obj['Key']
        # file = obj['Key']
        # if file.endswith('.xsd') and base_name in file:
        #     return file

    return None

def validate_configs(iparm):
    # Check if the input is not empty
    for key, value in iparm.items():
        if not value:
            raise ValueError(f"{key} should not be empty")

    # Check if the input is in the expected format
    # For example, if s3_bucket_input should be a string
    if not isinstance(iparm['s3_bucket_input'], str):
        raise ValueError("s3_bucket_input should be a string")

    # Check if the input is within the expected range
    # For example, if s3_key_input_xml should not exceed a certain length
    if len(iparm['s3_key_input_xml']) > 100:
        raise ValueError("s3_key_input_xml should not exceed 100 characters")

    return True

# main:
def main():
    try:
        iparm = get_configs()
        validate_configs(iparm)
        input_bucket = iparm['s3_bucket_input']
        xml_key = iparm['s3_key_input_xml']
        xsd_key_path = iparm['s3_key_input_xsd']
        s3_destination_key = iparm['s3_destination_key']
        s3_xsd_pattern = iparm['s3_xsd_pattern']
    
    except Exception as e:
        splunk.log_message({'status': 'failed', 'message': f'failed to retrieve input parameters from step function due to {str(e)}'}, get_run_id())
        raise Exception(f"input parameters retrieval failed: {str(e)}")

    try:
        # Split xsd file name and its current extension
        xsd_file_name, xsd_file_extension = os.path.splitext(xsd_key_path)

        if xsd_file_extension == ".xsd":
            xsd_key = xsd_key_path
        # else:
        #     # Extract the base name without the timestamp and extension from the XML file
        #     base_name_pattern = r'\d+_(.+)\.xml'
        #     match = re.search(base_name_pattern, xml_key)
            
        #     if not match:
        #         message = f"input xml filename {xml_key} pattern match with base name pattern failed"
        #         raise ValueError(message)

        #     xml_name_without_timestamp_and_ext = match.group(1)
        #     xsd_key = xsd_key_path + "/" + xml_name_without_timestamp_and_ext + ".xsd"
        else:
            xml_name = xml_key.split('/')[-1]
            xsd_key = find_matching_file(input_bucket, xsd_key_path, xml_name, s3_xsd_pattern)
            if xsd_key is None:
                raise KeyError(f"xsd not found for the file: {xml_name}")

    except Exception as e:
        message = f"{str(e)}"
        splunk.log_message({'status': 'failed', 'message': message}, get_run_id())
        raise Exception(message)
        raise e

    try:
        xml_key_data = open_input_file(input_bucket, xml_key)
        message = "file open from s3 bucket is SUCCESSFUL"
        splunk.log_message({'Status': 'success', 'InputFileName': xml_key, 'Message': message}, get_run_id())

    except ValueError as e:
        message = f"{str(e)}"
        splunk.log_message({'Status': 'failed', 'InputFileName': xml_key, 'Message': message}, get_run_id())
        raise Exception(message)

    except FileNotFoundError as e:
        message = f"file open from s3 bucket failed. The file {xml_key} in bucket {input_bucket} is not found."
        splunk.log_message({'Status': 'failed', 'InputFileName': xml_key, 'Message': message}, get_run_id())
        raise Exception(message)

    except OSError as e:
        message = f"file open from s3 bucket failed. {str(e)}"
        splunk.log_message({'Status': 'failed', 'InputFileName': xml_key, 'Message': message}, get_run_id())
        raise Exception(message)

    except Exception as e:
        message = f"file open from s3 bucket failed. {str(e)}"
        splunk.log_message({'Status': 'failed', 'InputFileName': xsd_key, 'Message': message}, get_run_id())
        raise Exception(message)

    try:
        xsd_key_data = open_input_file(input_bucket, xsd_key)
        message = "file open from s3 bucket is SUCCESSFUL"
        splunk.log_message({'Status': 'success', 'InputFileName': xsd_key, 'Message': message}, get_run_id())

    except ValueError as e:
        message = f"{str(e)}"
        splunk.log_message({'Status': 'failed', 'InputFileName': xsd_key, 'Message': message}, get_run_id())
        raise Exception(message)

    except FileNotFoundError as e:
        message = f"file open from s3 bucket failed. The file {xsd_key} in bucket {input_bucket} is not found."
        splunk.log_message({'Status': 'failed', 'InputFileName': xsd_key, 'Message': message}, get_run_id())
        raise Exception(message)

    except OSError as e:
        message = f"file open from s3 bucket failed. {str(e)}"
        splunk.log_message({'Status': 'failed', 'InputFileName': xsd_key, 'Message': message}, get_run_id())
        raise Exception(message)

    except Exception as e:
        message = f"file open from s3 bucket failed. {str(e)}"
        splunk.log_message({'Status': 'failed', 'InputFileName': xsd_key, 'Message': message}, get_run_id())
        raise Exception(message)

    try:
        result = validate_xml(input_bucket, xml_key_data, xsd_key_data)

        message = "xml file validation against xsd is SUCCESSFUL"
        splunk.log_message({'Status': 'success', 'InputFileName': xml_key, 'Message': message}, get_run_id())
        
        s3_client = boto3.client('s3')
        copy_source = {'Bucket': input_bucket, 'Key': xml_key}
        
        s3_client.copy_object(CopySource=copy_source, Bucket=input_bucket, Key=s3_destination_key)

    except etree.XMLSchemaError as e:
        message = f"xml file validation against xsd failed due to XML Schema error: {str(e)}"
        splunk.log_message({'Status': 'failed', 'InputFileName': xml_key, 'Message': message}, get_run_id())
        raise Exception(message)

    except etree.DocumentInvalid as e:
        message = f"xml file validation against xsd failed due to document invalid error: {str(e)}"
        splunk.log_message({'Status': 'failed', 'InputFileName': xml_key, 'Message': message}, get_run_id())
        raise Exception(message)

    except etree.XMLSyntaxError as e:
        message = f"xml file validation against xsd failed due to XML Syntax error: {str(e)}"
        #print(f"Error on line {e.lineno}: {e.msg}")
        splunk.log_message({'Status': 'failed', 'InputFileName': xml_key, 'Message': message}, get_run_id())
        raise Exception(message)

    except etree.XPathEvalError as e:
        message = f"xml file validation against xsd failed due to XPath evaluation error: {str(e)}"
        splunk.log_message({'Status': 'failed', 'InputFileName': xml_key, 'Message': message}, get_run_id())
        raise Exception(message)

    except etree.XSLTError as e:
        message = f"xml file validation against xsd failed due to XSLT transformation error: {str(e)}"
        splunk.log_message({'Status': 'failed', 'InputFileName': xml_key, 'Message': message}, get_run_id())
        raise Exception(message)
        
    except Exception as e:
        message = f"xml file validation against xsd failed due to : {str(e)}"
        splunk.log_message({'Status': 'failed', 'InputFileName': xml_key, 'Message': message}, get_run_id())
        raise Exception(message)

if __name__ == '__main__':
    main()