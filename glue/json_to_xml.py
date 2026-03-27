import sys
import boto3
import json
from awsglue.utils import getResolvedOptions
import splunk
import xmltodict
from s3fs import S3FileSystem

# simplefilestorage to get data in s3 object in bytes
sfs = S3FileSystem()

# Custom exception module
# class EmptyS3FileError(Exception):
# pass

def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:json-to-xml-conversion:" + account_id

def get_configs():
    args = getResolvedOptions(sys.argv, ['s3_bucket_input', 's3_input_file', 's3_bucket_output', 's3_output_file'])
    return args

def open_input_file(input_bucket, input_key):
    # get file information
    file_info = sfs.info(f'{input_bucket}/{input_key}')

    if file_info['size'] == 0:
        # raise EmptyS3FileError(f"File is empty")
        raise ValueError(f"file open from s3 bucket failed with error: Input file {input_key} in s3 bucket {input_bucket} is empty.")
    else:
        # Open input file
        input_file_uri = "s3://" + input_bucket + "/" + input_key
        input_file_contents = sfs.open(input_file_uri, 'rb')

        return input_file_contents

def convert_json_to_xml(input_file_contents):
    data_dict = json.load(input_file_contents)
    converted_contents = xmltodict.unparse(data_dict, pretty=True)

    return converted_contents

def write_output_file(converted_data_contents, output_bucket, output_file):
    output_file_uri = "s3://" + output_bucket + "/" + output_file
    if not converted_data_contents:
        raise ValueError(f"write operation to s3 bucket {output_bucket} failed with error: Contents to output file {output_file} is empty.")
    else:
        with sfs.open(output_file_uri, 'w') as out_file:
            out_file.write(converted_data_contents)

# main:
def main():
    try:
        iparm = get_configs()

        input_bucket = iparm['s3_bucket_input']
        input_file = iparm['s3_input_file']

        output_bucket = iparm['s3_bucket_output']
        output_file = iparm['s3_output_file']

    except Exception as e:
        splunk.log_message(
            {'status': 'failed', 'message': f'failed to retrieve input parameters from step function due to {str(e)}'},
            get_run_id())
        raise Exception(f"input parameters retrieval failed: {str(e)}")

    try:
        input_key_data = open_input_file(input_bucket, input_file)
        message = "file open from s3 bucket is SUCCESSFUL"
        splunk.log_message({'Status': 'success', 'InputFileName': input_file, 'Message': message}, get_run_id())

    except ValueError as e:
        message = f"{str(e)}"
        splunk.log_message({'Status': 'failed', 'InputFileName': input_file, 'Message': message}, get_run_id())
        raise Exception(message)

    except FileNotFoundError as e:
        message = f"file open from s3 bucket failed with error: Input file {input_file} in s3 bucket {input_bucket} is not found."
        splunk.log_message({'Status': 'failed', 'InputFileName': input_file, 'Message': message}, get_run_id())
        raise Exception(message)

    except OSError as e:
        message = f"file open from s3 bucket failed with error: {str(e)}"
        splunk.log_message({'Status': 'failed', 'InputFileName': input_file, 'Message': message}, get_run_id())
        raise Exception(message)

    try:
        converted_data = convert_json_to_xml(input_key_data)

        message = "json to xml file conversion is SUCCESSFUL"
        splunk.log_message({'Status': 'success', 'InputFileName': input_file, 'Message': message}, get_run_id())

    except json.JSONDecodeError as e:
        message = f"json to xml file conversion failed due to json decode error: {str(e)}"
        #tag_name = str(e).split()[-1]
        #print(f"Problematic tag causing issue is {tag_name}")

        splunk.log_message({'Status': 'failed', 'InputFileName': input_file, 'Message': message}, get_run_id())
        raise Exception(message)

    except ValueError as e:
        message = f"json to xml file conversion failed due to value error: {str(e)}"
        splunk.log_message({'Status': 'failed', 'InputFileName': input_file, 'Message': message}, get_run_id())
        raise Exception(message)

    except KeyError as e:
        message = f"json to xml file conversion failed due to key error: {str(e)}"
        splunk.log_message({'Status': 'failed', 'InputFileName': input_file, 'Message': message}, get_run_id())
        raise Exception(message)

    except IndexError as e:
        message = f"json to xml file conversion failed due to index error: {str(e)}"
        splunk.log_message({'Status': 'failed', 'InputFileName': input_file, 'Message': message}, get_run_id())
        raise Exception(message)

    except (NameError, TypeError, AttributeError) as e:
        message = f"json to xml file conversion failed due to other error: {str(e)}"
        splunk.log_message({'Status': 'failed', 'InputFileName': input_file, 'Message': message}, get_run_id())
        raise Exception(message)

    except Exception as e:
        message = f"json to xml file conversion failed due to exception: {str(e)}"
        splunk.log_message({'Status': 'failed', 'InputFileName': input_file, 'Message': message}, get_run_id())
        raise Exception(message)

    try:
        write_output_file(converted_data, output_bucket, output_file)

        message = "converted xml file write operation to s3 bucket is SUCCESSFUL"
        splunk.log_message({'Status': 'success', 'InputFileName': input_file, 'Message': message}, get_run_id())

    except FileNotFoundError as e:
        message = f"Write operation for the file {output_file} failed with s3 bucket {output_bucket} not found error."
        splunk.log_message({'Status': 'failed', 'InputFileName': input_file, 'Message': message}, get_run_id())
        raise Exception(message)

    except OSError as e:
        message = f"Write operation for the file {output_file} in s3 bucket {output_bucket} failed with error: {str(e)}"
        splunk.log_message({'Status': 'failed', 'InputFileName': input_file, 'Message': message}, get_run_id())
        raise Exception(message)

if __name__ == '__main__':
    main()