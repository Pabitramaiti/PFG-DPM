import sys
import json
import boto3
from awsglue.utils import getResolvedOptions
import splunk
from s3fs import S3FileSystem
import pandas as pd
import xml.etree.ElementTree as ET
import io
import os

# simplefilestorage to get data in s3 object in bytes
sfs = S3FileSystem()

def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:xml-to-json-conversion:" + account_id




def open_input_file(input_bucket, input_key):
    # Construct the file URI
    input_file_uri = f"s3://{input_bucket}/{input_key}"

    # Open and read the entire file content
    with sfs.open(input_file_uri, 'rb') as file:
        file_content = file.read()
    # Process the file content
    # Split into lines and strip white spaces
    lines = file_content.split(b'\n')
    input_file_contents = [line.strip() for line in lines if line.strip()] # Ensure no empty lines
    return input_file_contents


def preprocess_csv_header(header_line,file_delimiter,replace_Pattern):
    # Remove spaces from each field name in the header
    for item in replace_Pattern:
        header_line = header_line.replace(item[0], item[1])

    header_list = [field.strip() for field in header_line.split(file_delimiter)]
    # Join the modified header list back into a single line
    modified_header_line = file_delimiter.join(header_list)
    return modified_header_line

def convert_csv_to_xml(input_file_contents, conversion_parameters):
    root_name = conversion_parameters.get('rootTag')
    additional_tags = conversion_parameters.get('additionalTagsRootTop')
    package_name = conversion_parameters.get('packageTag')
    account_name = conversion_parameters.get('accountTag')
    record_limit = conversion_parameters.get('recordLimit')
    file_delimiter = conversion_parameters.get('fileDelimiter')
    replace_Pattern = conversion_parameters.get('replacePattern')

    # Read the file and modify the header line
    header_line = input_file_contents[0].decode().strip()  # Decode byte string to string
    modified_header_line = preprocess_csv_header(header_line, file_delimiter, replace_Pattern)

    root = ET.Element(root_name)

    if additional_tags:
        for key, value in additional_tags.items():
            ET.SubElement(root, key).text = value

    package = root if not package_name else ET.SubElement(root, package_name)

    # Iterate over the lines after the header
    count=1
    for line in input_file_contents[1:]:
        print(count)
        count=count+1
        # Convert byte string to string and then to a file-like object for pandas
        byte_io = io.BytesIO(line)
        df = pd.read_csv(byte_io, delimiter=file_delimiter, na_filter=False, names=modified_header_line.split(file_delimiter), dtype=str)

        for _, row in df.iterrows():
            item = ET.SubElement(package, account_name)
            for col, cell in row.items():
                val = str(cell)
                if val.strip():
                    ET.SubElement(item, col).text = val.strip()

    ET.indent(root)
    converted_data = ET.tostring(root, encoding="UTF-8", xml_declaration=True, method='xml').decode("UTF-8")
    converted_data = converted_data.replace(" />", "/>")
    return converted_data


def write_output_file(converted_data_contents, output_bucket, output_file):
    output_file_uri = "s3://" + output_bucket + "/" + output_file
    if not converted_data_contents:
        raise ValueError(f"write operation to s3 bucket {output_bucket} failed with error: Contents to output file {output_file} is empty.")
    else:
        with sfs.open(output_file_uri, 'w') as out_file:
            out_file.write(converted_data_contents)

def set_job_params_as_env_vars():
    # Loop through all command-line arguments starting from the second argument (skip the script name)
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]  # Remove the leading '--'
            value = sys.argv[i+1]
            os.environ[key] = value
            print(f'Set environment variable {key} to {value}')
def main():
    try:
        set_job_params_as_env_vars()
        s3 = boto3.client('s3')
        input_bucket = os.getenv('s3_bucket_input')
        input_file = os.getenv('s3_input_file')
        output_bucket = os.getenv('s3_bucket_output')
        output_file = os.getenv('s3_output_file')
        conversion_parameters = json.loads(os.getenv('conversion_parameters'))
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

    except Exception as e:
        message = f"file open from s3 bucket failed with error: {str(e)}"
        splunk.log_message({'Status': 'failed', 'InputFileName': input_file, 'Message': message}, get_run_id())
        raise Exception(message)

    try:
        converted_xml_data = convert_csv_to_xml(input_key_data, conversion_parameters)

        message = "delimited to xml file conversion is SUCCESSFUL"
        splunk.log_message({'Status': 'success', 'InputFileName': input_file, 'Message': message}, get_run_id())

    except (NameError, ValueError, TypeError, AttributeError) as e:
        if "Duplicate names are not allowed" in str(e):
            message = f"delimited to xml file conversion failed due to other error: {str(e)} It is possible that header is missing, please check the file to make sure header is present."
        else:
            message = f"delimited to xml file conversion failed due to other error: {str(e)}"
        splunk.log_message({'Status': 'failed', 'InputFileName': input_file, 'Message': message}, get_run_id())
        raise Exception(message)

    except Exception as e:
        message = f"delimited to xml file conversion failed due to: {str(e)}"
        splunk.log_message({'Status': 'failed', 'InputFileName': input_file, 'Message': message}, get_run_id())
        raise Exception(message)

    try:
        write_output_file(converted_xml_data, output_bucket, output_file)

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

    except Exception as e:
        message = f"Write operation for the file {output_file} in s3 bucket {output_bucket} failed with error: {str(e)}"
        splunk.log_message({'Status': 'failed', 'InputFileName': input_file, 'Message': message}, get_run_id())
        raise Exception(message)


if __name__ == '__main__':
    main()
