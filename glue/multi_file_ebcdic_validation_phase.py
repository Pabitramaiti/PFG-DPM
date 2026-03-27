import sys
import boto3
import re
from collections import defaultdict
import os
import sys
import codecs
import ebcdic
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
import splunk
import zipfile
import io
import json

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
            if allow_empty_file == "True":
                log_message('logging', f"The file {key} in bucket {bucket} is empty, allowing the file")
                return bytes()
            else:
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

def validate_ebcdic_file(data, config, section,expected_count = None):
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
        print('logging',f" Data length: {data_length}, Record length: {record_length}, Block size: {block_size}, Total records found: {total_records} | expected_count: {expected_count}")
        if expected_count is None:
            pass
        elif expected_count and int(expected_count) == total_records:
            log_message("logging", f"Warehouse File Count Validation Successfull, total_records: {total_records} | expected_count: {int(expected_count)}")
        else:
            log_message("logging", f"Warehouse File Count Mismatch")
            raise ValueError(f"Warehouse File Count Mismatch")
        if block_size == 0:
            if allow_empty_file != "True":
                log_message('logging',f" Block size calculation failed for section {section}.")
                raise ValueError(f"Block size calculation failed for section {section}.")
    
        if data_length > block_size:
            log_message('logging', f"Record Length Exceeds the Layout Length. Extra bytes: {data_length - block_size}")
            raise ValueError(f"Record Length Exceeds the Layout Length. Extra bytes: {data_length - block_size}")
       
        decoded_data = decode_ebcdic_to_ascii(data)
        check_control_characters(data)
        log_message("success", f"Ebcdic file validated successfully")
    except Exception as e:
        error_message = f'EBCDIC Validation failed {str(e)}'
        if "Warehouse" in str(e):
            error_message = str(e)
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
    log_message("logging", f"starting in getFileNames bucket_name: {bucket_name} | path: {path} |pattern: {pattern}")
    regex = re.compile(pattern)
    try:
        def filter_keys_by_pattern(objects, pattern):
            return [obj['Key'] for obj in objects.get('Contents', []) if pattern.search(obj['Key'].split('/')[-1])]

        objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=path)
        files = filter_keys_by_pattern(objects, regex)
        log_message('logging', f'files list : {files}')
        if not files:
            error_message = 'No schema files found matching the pattern'
            log_message('failed', error_message)
            raise ValueError(error_message)
        log_message("logging", f"files in getFileNames files: {files}")
        return files[0]
    except Exception as e:
        log_message('failed', f" failed to get fileNames:{str(e)}")
        raise e

def read_data_preparation_file(load_validation_json,ebcdicPath,s3_bucket):
    global data_prep_file_name
    try:
        data_preparation = load_validation_json.get("data_preparation",{})
        data_prep_file_name = data_preparation.get("output_file_name","")
        data_prep_file_key = ebcdicPath + data_prep_file_name
        read_data_prep_file = read_s3_file(s3_bucket,data_prep_file_key).decode('utf-8')
        load_data_prep_file = json.loads(read_data_prep_file)
        log_message("logging", "reading data preparation file successfull")
        return load_data_prep_file
    except Exception as e:
        log_message('failed',f" unable to read data preparation file : {e}")
        raise e

def get_warehouse_count_details(data_prep_json_data,ebcdicPattern,s3_bucket,ebcdicPath,data_prep_file_name):
    #getting expected count of file
    try:
        warehouse_count_details = data_prep_json_data.get("extract_warehouse_count_files","")
        expected_count = None
        if warehouse_count_details:
            for file_info in warehouse_count_details:
                file_name = file_info.get("file_name")
                if re.search(ebcdicPattern,file_name.strip()):
                    expected_count = file_info.get("file_count")
            return expected_count
        else:
            error_message = f"Warehouse File Count Mismatch, please check {ebcdicPath + data_prep_file_name} file"
            log_message('failed', error_message)
            raise ValueError(error_message)
    except Exception as e:
        error_message = f"Warehouse File Count Mismatch, error : {str(e)}"
        log_message('failed', error_message)
        raise ValueError(error_message)
def main():
    try:
        set_job_params_as_env_vars()
        
        global fileName,allow_empty_file
        fileName= os.getenv('fileName')
        log_message('logging',f" EBCDIC Validation")
        s3_bucket = os.getenv('s3_bucket')
        ebcdicPath= os.getenv('ebcdicPath')
        ebcdicPattern= os.getenv('ebcdicPattern')
        destination_path= os.getenv('destination_path')
        copy_flag= os.getenv('copy_flag')
        ebcdicKey = os.getenv('ebcdicKey')
        config_file_key = os.getenv('config_file_key')  # config file path/key
        config_file_validation = os.getenv('validation_config') # validation config file
        filetype= os.getenv('filetype')
        sdlc_env = os.getenv('sdlc_env')
        allow_empty_file = os.getenv('allow_empty_file')
        skip_wh_validation = os.getenv('skip_wh_validation',"False")
        
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

        if config_file_validation:
            # read validations from validation config
            read_validation_config = read_s3_file(s3_bucket,config_file_validation).decode('utf-8')
            load_validation_json = json.loads(read_validation_config)
            log_message("logging",f"loaded json data {load_validation_json}")
            data_validation = load_validation_json.get("data_validation",{})
            active_validations = data_validation.get("active_validations",{})
            data_preparation = load_validation_json.get("data_preparation", {})
            log_message("logging", f"data_preparation - {data_preparation}")
            active_tasks = data_preparation.get("active_tasks", {})
            log_message("logging", f"active_tasks - {active_tasks}")
            file_patterns = data_preparation.get("file_pattern",{})
            modified_active_tasks = {}
            # read data preparation file
            data_prep_json_data = read_data_preparation_file(load_validation_json,ebcdicPath,s3_bucket)
            
            for validation,flag in active_validations.items():
                if flag and validation == "warehouse_count" and skip_wh_validation !="True":
                    try:
                        # getting warehouse count details
                        expected_count = get_warehouse_count_details(data_prep_json_data,ebcdicPattern,s3_bucket,ebcdicPath,data_prep_file_name)
                        log_message("logging", f"expected_count: {expected_count}")
                        if expected_count is not None and expected_count:
                            validate_ebcdic_file(ebcdic_data, config, section,expected_count)
                        else:
                            error_message = f"Warehouse File Count Mismatch"
                            log_message("failed",error_message)
                            raise ValueError(error_message)
                    except Exception as e:
                        error_message = f"{str(e)}"
                        log_message("failed",error_message)
                        raise ValueError(error_message)
    except Exception as e:
        log_message('failed',f" Error occured. {str(e)}")
        raise e

fileName=''
if __name__ == "__main__":
    main()
