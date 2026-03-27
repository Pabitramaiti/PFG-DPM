import boto3
import re
from collections import defaultdict
import os
import sys
import codecs
import ebcdic
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
import splunk

s3_client = boto3.client('s3')

def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:ebcdic_validation:" + account_id

def log_message(status, message):
    splunk.log_message({'FileName':fileName,'Status': status, 'Message': message}, get_run_id())
    
    
def parse_config_data(data):
    config = defaultdict(lambda: defaultdict(list))
    lines = data.decode('utf-8').splitlines()
    section = None
    for line in lines:
        line = line.strip()
        if line.startswith('[') and line.endswith(']'):
            section = line[1:-1]
        elif section and '|' in line:
            key, value = line.split('|', 1)
            config[section][key].append(value)
    return config

def extract_section_identifier(file_name):
    match = re.search(r'[A-Z]EXT(\d{4})', file_name)
    if match:
        return f"E{match.group(1)}"
    raise ValueError(f"Could not extract section identifier from file name: {file_name}")

def read_s3_file(bucket, key):
    # s3 = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        data = response['Body'].read()
        if not data:  # Check if the file is empty
            log_message('failed', f"The file {key} in bucket {bucket} is empty.")
            raise ValueError(f"The file {key} in bucket {bucket} is empty.")
        return data
    except (NoCredentialsError, PartialCredentialsError):
        raise RuntimeError("Credentials for AWS S3 are not configured properly.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            log_message('failed',f" The file {key} does not exist in bucket {bucket}.")
            raise FileNotFoundError(f"The file {key} does not exist in bucket {bucket}.")
        log_message('failed',f" Failed to access file {key} in bucket {bucket}: {e}")
        raise RuntimeError(f"Failed to access file {key} in bucket {bucket}: {e}")
        
def decode_ebcdic_to_ascii(data):
    try:
        # Ensure data is in bytes
        if isinstance(data, str):
            data = data.encode('latin-1')  # Convert to bytes if it's a string

        # Decode EBCDIC data to ASCII using CP1047
        decoded_data = codecs.decode(data, 'cp1047')
        log_message("success", f"Ebcdic file decoded successfully")
        return decoded_data
        
    except Exception as e:
        log_message('failed', f"Decoding error: {str(e)}")
        raise ValueError(f"Decoding error: {e}")

def check_control_characters(data):
    CONTROL_CHARACTERS = {
        0: "NUL", 1: "SOH", 2: "STX", 3: "ETX", 5: "ENQ", 6: "ACK", 7: "BEL", 8: "BS",
        9: "HT",  10: "LF", 11: "VT", 12: "FF", 13: "CR", 14: "SO", 15: "SI", 16: "DLE",
        17: "DC1", 18: "DC2", 19: "DC3", 20: "DC4", 21: "NAK", 22: "SYN", 23: "ETB", 24: "CAN",
        25: "EM", 26: "SUB", 27: "ESC", 28: "FS", 29: "GS", 30: "RS", 31: "US", 55: "ETB",
        45: "SUB", 46: "ESC", 47: "FS", 37: "EM", 61: "GS", 50: "RS", 63: "US"
    }

    control_chars_found = {byte: CONTROL_CHARACTERS[byte] for byte in data if byte in CONTROL_CHARACTERS}

    if control_chars_found:
        for byte, name in control_chars_found.items():
            log_message('logging',f" Control Character: {name}  | Decimal: {byte}")
    else:
        log_message('logging',f" No control characters found.")

def validate_ebcdic_file(data, config, section):
    try:
        section_config = config.get(section)
        if not section_config:
            log_message('failed',f" Section {section} not found in the configuration.")
            raise ValueError(f"Section {section} not found in the configuration.")

        record_length_list = section_config.get('RECORD_LENGTH')
        if not record_length_list or not isinstance(record_length_list, list):
            log_message('failed',f" RECORD_LENGTH not found or is not a list for section {section}.")
            raise ValueError(f"RECORD_LENGTH not found or is not a list for section {section}.")
    
        record_length = int(record_length_list[0])
        data_length = len(data)
        block_size = (data_length // record_length) * record_length
        total_records = data_length // record_length
        log_message('logging',f" Data length: {data_length}, Record length: {record_length}, Block size: {block_size}, Total records found: {total_records}") 
        if block_size == 0:
            log_message('logging',f" Block size calculation failed for section {section}.")
            raise ValueError(f"Block size calculation failed for section {section}.")
    
        if data_length > block_size:
            log_message('logging', f"Data length exceeds the expected block size. Extra bytes: {data_length - block_size}")
            raise ValueError(f"Data length exceeds the expected block size. Extra bytes: {data_length - block_size}")
       
        decoded_data = decode_ebcdic_to_ascii(data)
        check_control_characters(data)
        log_message("success", f"Ebcdic file validated successfully")
    except Exception as e:
        error_message = 'EBCDIC Validation failed'
        raise ValueError(error_message)
        
def set_job_params_as_env_vars():
    # Loop through all command-line arguments starting from the second argument (skip the script name)
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]  # Remove the leading '--'
            value = sys.argv[i + 1]
            os.environ[key] = value
            # log_message('logging',f" Set environment variable {key} to {value}")

def getFileNames(bucket_name, path, pattern):
    regex = re.compile(pattern)
    try:
        def filter_keys_by_pattern(objects, pattern):
            return [obj['Key'] for obj in objects.get('Contents', []) if pattern.search(obj['Key'].split('/')[-1])]
        
        objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=path)
        files = filter_keys_by_pattern(objects, regex)
        
        if not files:
            error_message = 'No schema files found matching the pattern'
            log_message('failed',error_message)
            raise ValueError(error_message)
        
        return files[0]
    except Exception as e:
        log_message('failed',f" failed to get fileNames:{str(e)}")
        raise e

def main():
    try:
        set_job_params_as_env_vars()
        
        global fileName
        fileName= os.getenv('fileName')
        log_message('logging',f" EBCDIC Validation")
        s3_bucket = os.getenv('s3_bucket')
        ebcdicPath= os.getenv('ebcdicPath')
        ebcdicPattern= os.getenv('ebcdicPattern')
        destination_path= os.getenv('destination_path')
        copy_flag= os.getenv('copy_flag')
        ebcdicKey = os.getenv('ebcdicKey')
        config_file_key = os.getenv('config_file_key')  # config file path/key
        filetype= os.getenv('filetype')

        ebcdicKey = ebcdicKey if ebcdicKey != " " else getFileNames(s3_bucket, ebcdicPath, ebcdicPattern)
        # Extract section identifier
        # section = extract_section_identifier(ebcdicKey.split('/')[-1])
        section=filetype
        log_message('logging',f" Section Identifier: {section}")

        # Read the configuration file directly from S3 without saving locally
        try:
            config_data = read_s3_file(s3_bucket, config_file_key)
            config = parse_config_data(config_data)
        except FileNotFoundError as e:
            log_message('failed',f" Configuration file error: {e}")
            raise RuntimeError(f"Configuration file error: {e}")
        except Exception as e:
            log_message('failed',f" Configuration file error: {e}")
            raise e

        # log_message('logging',f" Parsed Configuration: {dict(config)}")
        

        # Read the EBCDIC file from S3
        try:
            ebcdic_data = read_s3_file(s3_bucket, ebcdicKey)
        except FileNotFoundError as e:
            log_message('failed',f" EBCDIC file error: {e}")
            raise RuntimeError(f"EBCDIC file error: {e}")
        except Exception as e:
            log_message('failed',f" EBCDIC file error: {e}")
            raise e

        # Validate the file
        validate_ebcdic_file(ebcdic_data, config, section)
        if copy_flag=="True":
            source_key = ebcdicKey
            destination_key = destination_path + ebcdicKey.rsplit("/",1)[-1]
            copy_source = {'Bucket': s3_bucket, 'Key': source_key}
            s3_client.copy_object(CopySource=copy_source, Bucket=s3_bucket, Key=destination_key)
    except Exception as e:
        log_message('failed',f" Error occured. {str(e)}")
        raise e

fileName=''
if __name__ == "__main__":
    main()
