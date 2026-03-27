import json
import boto3
import os
import codecs
import ebcdic
import sys
import re
import splunk

# Initialize the S3 client
s3 = boto3.client('s3')

def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:ascii_to_json:" + account_id

def log_message(status, message):
    splunk.log_message({'FileName':inputFileName,'Status': status, 'Message': message}, get_run_id())

# Function to download file from S3 to a local path
def download_file_from_s3(bucket_name,s3_path, local_path):
    try:
        s3.download_file(bucket_name, s3_path, local_path)
        log_message('logging',f"Downloaded {s3_path} to {local_path}")
    except Exception as e:
        log_message('failed',f" failed to download file:{str(e)}")
        raise e

# Load ASCII fields JSON from S3
def load_ascii_fields(bucket_name,s3_path):
    try:
        local_ascii_file = '/tmp/ascii_fields.json'  # Temporary local path
        download_file_from_s3(bucket_name,s3_path, local_ascii_file)
        with open(local_ascii_file, 'r') as f:
            return json.load(f)
        log_message('success',f" ASCII fields loaded successfully")
    except Exception as e:
        log_message('failed',f" Error loading ASCII fields from {s3_path}: {str(e)}")
        raise e


# Load EBCDIC config JSON from S3
def load_ebcdic_config(bucket_name,s3_path):
    try:
        local_ebcdic_file = '/tmp/ebcdic_config.json'  # Temporary local path
        download_file_from_s3(bucket_name,s3_path, local_ebcdic_file)
        with open(local_ebcdic_file, 'r') as f:
            return json.load(f)
    except Exception as e:
        error_message = 'Error loading EBCDIC config'
        log_message('failed',f" Error loading EBCDIC config from {s3_path}: {str(e)}")
        raise ValueError(error_message)


def load_recsections_content(bucket_name,s3_path):
    try:
        local_recsections_file = '/tmp/recSectionsFile.cfg'
        download_file_from_s3(bucket_name,s3_path,local_recsections_file)
        with open(local_recsections_file,'r') as f:
            content = f.read()
            s3.delete_object(Bucket=bucket_name, Key=s3_path)
            return content
    except Exception as e:
        log_message('failed',f" Error loading recsections file from {s3_path} : {str(e)}")
        raise e
        
def picture_length(picture,ext_length):
    length=0
    try:
        if "X" in picture:
            if "(" not in picture and ")" not in picture:
                length=1
            else:
                length=int(picture[picture.index("(")+1:picture.index(")")])
        if "COMP" in picture:
            if "(" not in picture and ")" not in picture:
                if "S" in picture:
                    length=2
                elif "V" in picture:
                    length=2
                elif "S" not in picture and "V" not in picture:
                    length=1
            else:
                idx2 = picture.index("(")+1
                idx1 = picture.index(")")
                if (picture[0] == "S"and "V" not in picture):
                    length = int(picture[idx2:idx1]) + 1
                elif ("SV" not in picture and ("S" in picture and "V" in picture)):
                    length = int(picture[idx2:idx1])+4
                elif "SV" in picture:
                    length = int(picture[idx2:idx1]) + 4
                else:
                    length = int(picture[2:idx1])
        elif picture[0] == "S":
            length = ext_length+1
        elif "V" in picture:
            length=ext_length+3
        elif picture[0] == "S" and "V" in picture:
            length = ext_length + 4
        else:
            length=ext_length
        return length
    except Exception as e:
        log_message('failed',f" Error during calculating field length: {str(e)}")
        raise e

    
# Your existing process function
def process_ascii_file(bucket_name,output_ascii_file_key, ascii_fields, ebcdic_config, delimiter, output_json_file_key, delimeter_flag, input_ascii_flag,recsections_content):
    try:
        recsections_content=recsections_content.split('\n')
        result = []
        field_config_map = {}
        count=0
        for config in ebcdic_config:
            field_key = (config['rec_type'], config['field_name'])
            field_config_map[field_key] = config

        # Download the ASCII file from S3 to a temporary local path
        local_ascii_file = '/tmp/local_ascii_file.asc'
        
        download_file_from_s3(bucket_name,output_ascii_file_key, local_ascii_file)
  
        # Open the ASCII file for reading
        with codecs.open(local_ascii_file, 'r') as f:
            for line in f:
                if delimiter:
                    fields = line.strip().split(delimiter)
                
                # Assuming 'REC-TYPE' and 'REC-CODE' are important identifiers
                rec_type = recsections_content[count]
                count=count+1
                if input_ascii_flag=="True":
                    ascii_rec = next((r for r in ascii_fields if r['rec_type'] == rec_type), None)
                    if not ascii_rec:
                        continue  # Skip if no matching record type is found
                    pos = 0
                    for idx, field_name in enumerate(ascii_rec['fields']):
                        field_key = (rec_type, field_name)
                        field_config = field_config_map.get(field_key)
                        if delimeter_flag=="True":
                            if field_name:
                                field_value = fields[idx]
                        else:
                            length = int(field_config.get('length'))
                            field_value = line[pos:pos+length]
                            pos = pos + length
                        
                        if field_config:
                            field_details = {
                                "field": field_name,
                                "value": field_value,
                                "rec_type": field_config.get('rec_type'),
                                "start": field_config.get('start'),
                                "end": field_config.get('end'),
                                "datatype": field_config.get('data_type'),
                                "length": field_config.get('length'),
                                "picture": field_config.get('picture'),
                                "precision": field_config.get('precision'),
                                "sign": field_config.get('sign')
                            }
                            result.append(field_details)
                    
                else:

                    ebcdic_rec = [r for r in ebcdic_config if r['rec_type'] == rec_type]
                    
                    if not ebcdic_rec:
                        continue  # Skip if no matching record type is found
                    pos = 0

                    for idx, field_config in enumerate(ebcdic_rec):
                        if delimeter_flag=="True":
                            if field_config:
                                field_value = fields[idx]
                        else:
                                
                            picture=field_config.get('picture')
                            if "YR" in picture and "MM" in picture and "DA" in picture:
                                yr_picture=picture[3:picture.index("M")].strip()
                                mm_picture=picture[picture.index("M")+2:picture.index("D")].strip()
                                da_picture=picture[picture.index("D")+3:].strip()
                                length=picture_length(yr_picture,field_config.get('length'))+picture_length(mm_picture,field_config.get('length'))+picture_length(da_picture,field_config.get('length'))
                                
                            elif "YR" in picture and "MM" in picture and "DA" not in picture:
                                yr_picture=picture[3:picture.index("M")].strip()
                                mm_picture=picture[picture.index("M")+2:].strip()
                                length=picture_length(yr_picture,field_config.get('length'))+picture_length(mm_picture,field_config.get('length'))
                                
                            else:
                                length=picture_length(picture,field_config.get('length'))
                            field_value = line[pos:pos+length]
                            pos = pos + length

                        if field_config:
                            field_details = {
                                "field": field_config.get('field_name'),
                                "value": field_value,
                                "rec_type": field_config.get('rec_type'),
                                "start": field_config.get('start'),
                                "end": field_config.get('end'),
                                "datatype": field_config.get('data_type'),
                                "length": field_config.get('length'),
                                "picture": field_config.get('picture'),
                                "precision": field_config.get('precision'),
                                "sign": field_config.get('sign')
                            }
                            result.append(field_details)

        try:
            local_output_file = '/tmp/output.json'
            with open(local_output_file, 'w') as json_file:
                json.dump(result, json_file, indent=4)
            log_message('success',f"Successfully wrote result to {local_output_file}")
        except Exception as e:
            log_message('failed',f" Error writing result to file: {str(e)}")
            raise

        # Upload the JSON result to the output S3 location
        try:
            # bucket_name, s3_key = output_json_file_key.split('/', 1)
            s3.upload_file(local_output_file, bucket_name, output_json_file_key)
            log_message('success',f"Successfully uploaded {local_output_file} to {bucket_name}")
        except Exception as e:
            log_message('failed',f" Error uploading file to S3 {s3_path}: {str(e)}")
            raise
        log_message('success',f" Ascii file processed Successfully")
    except Exception as e:
        log_message('failed',f" Error processing ASCII file: {str(e)}")
        raise e
    


def get_matching_s3_keys(bucket, prefix='', pattern=''):
    try:
        s3 = boto3.client('s3')
        paginator = s3.get_paginator('list_objects_v2')
        keys = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = os.path.basename(obj['Key'])
                if re.match(pattern, key):
                    keys.append(obj['Key'])
        return keys[0]
    except Exception as e:
        log_message('failed',f" Failed to get matching s3 keys.: {str(e)}")
        raise e

def set_job_params_as_env_vars():
    try:
        # Loop through all command-line arguments starting from the second argument (skip the script name)
        for i in range(1, len(sys.argv), 2):
            if sys.argv[i].startswith('--'):
                key = sys.argv[i][2:]  # Remove the leading '--'
                value = sys.argv[i + 1]
                os.environ[key] = value
    except Exception as e:
        log_message('failed',f" failed to set environment variables.:{str(e)}")
        raise

# Main function with try-except blocks
def main():
    try:
        set_job_params_as_env_vars()
        
        global inputFileName
        inputFileName = os.getenv('inputFileName')
        log_message('logging',f" ASCII to JSON conversion begins")

        bucket_name = os.getenv('bucketName')
        delimeter_flag = os.getenv('delimeter_flag')
        ASCII_Mapping_Key = os.getenv('ASCII_Mapping_Key')
        input_ascii_flag= os.getenv('input_ascii_flag')
        out_ASCII_path = os.getenv('out_ASCII_path')
        EBCDIC_Mapping_Key = os.getenv('EBCDIC_Mapping_Key')
        out_ASCII_pattern = os.getenv('out_ASCII_pattern')
        output_ascii_file_key = os.getenv('output_ascii_file_key')
        output_ascii_file_key = output_ascii_file_key if output_ascii_file_key !=" " else  get_matching_s3_keys(bucket_name, out_ASCII_path, out_ASCII_pattern)
        output_json_file_key = output_ascii_file_key.replace('.asc', '.json')
        delimiter = "~" if delimeter_flag=="True" else ""
        recsections_mapping_key=out_ASCII_path[:-7]+'recSectionsFile.cfg'
        
        
        # Load ASCII and EBCDIC configurations
        if input_ascii_flag=="True":
            ascii_fields = load_ascii_fields(bucket_name,ASCII_Mapping_Key)
        else: 
            ascii_fields=" "
        ebcdic_config = load_ebcdic_config(bucket_name,EBCDIC_Mapping_Key)
        recsections_content = load_recsections_content(bucket_name,recsections_mapping_key)

        # Process ASCII file
        process_ascii_file(bucket_name,output_ascii_file_key, ascii_fields, ebcdic_config, delimiter, output_json_file_key, delimeter_flag, input_ascii_flag,recsections_content)
        log_message('success',f" ASCII to JSON converted successfully")
    except Exception as e:
        log_message('failed',f" ASCII to JSON conversion failed")
        raise e
inputFileName=''
# Run the main function
if __name__ == "__main__":
    main()
