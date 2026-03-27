import sys
import boto3
import json
import os
from s3fs import S3FileSystem
from awsglue.utils import getResolvedOptions
import splunk


# simplefilestorage to get data in s3 object in bytes
sfs = S3FileSystem()


def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return f"arn:dpm:glue:B228 Header Prep running with account id:{account_id}"


def set_job_params_as_env_vars():
    try:
        for i in range(1, len(sys.argv), 2):
            if sys.argv[i].startswith('--'):
                key = sys.argv[i][2:]  # Remove the leading '--'
                value = sys.argv[i + 1]
                os.environ[key] = value
                print(f'Set environment variable {key} to {value}')
    except IndexError as e:
        message = "Error in parsing command line arguments: Missing value for a parameter."
        splunk.log_message({'Status': 'Failed', 'Message': message}, get_run_id())
        print(message)
        raise ValueError("Invalid command line arguments format.") from e


def get_json(json_str, input_file_name):
    """
    Read the string into a JSON object
    Args:
        json_str : a string representation of the json object
        input_file_name : name of the input data file
    Returns:
        json object that the string represents
    """
    try:
        json_content = json.loads(json_str)
        for key in json_content.keys():
            if key in input_file_name:
                config = json_content.get(key)
                break
        if config is None:
            raise ValueError(f"No matching key found in input_file_name: {input_file_name}")

        return config
    except Exception as e:
        message = f"Conversion of configuration string into json failed : {str(e)}"
        splunk.log_message({'Status': 'failed', 'json_str': json_str, 'Message': message}, get_run_id())
        raise Exception(message)


def write_output_file(output_bucket, output_file, report_json):
    """
    Write the text to the output file
    Args:
        output_bucket : bucket where the file needs to be written
        output_file : name of the file that needs to be written
        report_json : text to be written to the file
    Returns:
        the field matching the name
    """
    output_file_uri = "s3://" + output_bucket + "/" + output_file
    with sfs.open(output_file_uri, 'w') as out_file:
        out_file.write(json.dumps(report_json))
        out_file.close()


def main():
    message = "glue_b228_header_prep processing Started"
    splunk.log_message({'Status': 'success', 'Message': message}, get_run_id())
    
    file_name = " "
    header_output_json = " "
    bucket_name = " "
    return_report_config_str = " "   
    data = " "
    output_data = " "
    client_header_hierarchy = " "

    try:
        set_job_params_as_env_vars()
    except Exception as e:
        message = f"Error setting environment variables: {e}"
        print(message)
        splunk.log_message({'Status': 'Failed', 'Message': message}, get_run_id())
        raise ValueError(message)

    try:
        # Getting Header Input file
        header_input_json = os.environ['header_input_json']
        file_name = header_input_json[header_input_json.rfind('/') + 1:len(header_input_json)]

        # Getting Header Output file
        header_output_json = os.environ['header_output_json']
        # Getting Bucket
        bucket_name = os.environ['bucket_name']
        # Getting Bucket
        return_report_config_str = os.environ['config']
        print("return_report_config_str - 1")
        print(return_report_config_str)
        

    except Exception as e:
        splunk.log_message(
            {'status': 'failed', 'message': f'failed to retrieve input parameters from step function due to {str(e)}'},
            get_run_id())
        raise Exception(f"input parameters retrieval failed: {str(e)}")

    try:
        splunk.log_message({'Status': 'info', 'Message': "get the json"}, get_run_id())
        print("return_report_config_str - 2")
        print(return_report_config_str)
    
        config = get_json(return_report_config_str, file_name)
    except Exception as e:
        message = f"Error getting JSON config: {str(e)}"
        splunk.log_message({'Status': 'failed', 'Message': message}, get_run_id())
        raise Exception(message)

    try:
        input_file_uri = "s3://" + bucket_name + "/" + header_input_json
        print("input_file_uri ->" + input_file_uri)
        with sfs.open(input_file_uri, 'r') as file:
            data = json.load(file)
    except OSError as e:
        message = f"File open from S3 bucket failed with error: {str(e)}"
        splunk.log_message({'Status': 'failed', 'InputFileName': header_input_json, 'Message': message}, get_run_id())
        raise Exception(message)
    except ValueError as e:
        message = f"Error loading JSON data: {str(e)}"
        splunk.log_message({'Status': 'failed', 'InputFileName': header_input_json, 'Message': message}, get_run_id())
        raise Exception(message)

    try:
        client_header_hierarchy = config.get("client_header_hierarchy")

    except KeyError as e:
        message = f"Key error in client header hierarchy: {str(e)}"
        splunk.log_message({'Status': 'failed', 'Message': message}, get_run_id())
        raise Exception(message)
    except Exception as e:
        message = f"Error navigating JSON data: {str(e)}"
        splunk.log_message({'Status': 'failed', 'Message': message}, get_run_id())
        raise Exception(message)

    try:
        client_header_field = config.get("client_header_field")
        client_header_hierarchy_fix = eval(client_header_hierarchy)
        json_data = client_header_hierarchy_fix.get(eval(client_header_field), [])

        output_data = {
            "json_data": json_data
        }

        splunk.log_message({'Status': 'info', 'Message': "write output file"}, get_run_id())
        write_output_file(bucket_name, header_output_json, output_data)
    except KeyError as e:
        message = f"Key error in client header field: {str(e)}"
        splunk.log_message({'Status': 'failed', 'Message': message}, get_run_id())
        raise Exception(message)
    except Exception as e:
        message = f"Error processing or writing output file: {str(e)}"
        splunk.log_message({'Status': 'failed', 'Message': message}, get_run_id())
        raise Exception(message)

if __name__ == "__main__":
    main()
