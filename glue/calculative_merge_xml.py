import boto3
import re
import xml.etree.ElementTree as ET
import xml.dom.minidom as minidom
import json
import os
import sys
import splunk

def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:calculative_merge_xml:" + account_id

def log_message(status, message):
    splunk.log_message({'FileName':inputFileName,'Status': status, 'Message': message}, get_run_id())

def get_matching_keys(s3_client, bucket, prefix, pattern):
    try:
        regex = re.compile(pattern)
        paginator = s3_client.get_paginator('list_objects_v2')
        matching_keys = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if regex.match(os.path.basename(key)):
                    matching_keys.append(key)
        return matching_keys
    except Exception as e:
        log_message('failed', f"Error getting matching keys: {str(e)}")
        raise

def download_s3_file(s3_client, bucket, key):
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return response['Body'].read()
    except Exception as e:
        log_message('failed', f"Error downloading S3 file {key}: {str(e)}")
        raise

def upload_s3_file(s3_client, bucket, key, content):
    try:
        s3_client.put_object(Bucket=bucket, Key=key, Body=content)
    except Exception as e:
        log_message('failed', f"Error uploading to S3 at {key}: {str(e)}")
        raise

def parse_path_to_element(root, path):
    try:
        parts = path.split('.')
        for part in parts:
            root = root.find(part)
            if root is None:
                return None
        return root
    except Exception as e:
        log_message('failed', f"Error parsing path '{path}': {str(e)}")
        return None

def merge_sum_fields(root1, root2, sum_fields):
    try:
        for path in sum_fields:
            el1 = parse_path_to_element(root1, path)
            el2 = parse_path_to_element(root2, path)
            if el1 is not None and el2 is not None:
                el1.text = str(int(el1.text) + int(el2.text))
    except Exception as e:
        log_message('failed', f"Error merging sum fields: {str(e)}")
        raise

def merge_append_fields(root1, root2, append_fields):
    try:
        for path in append_fields:
            if '.' in path:
                parent_path, tag = path.rsplit('.', 1)
                parent1 = parse_path_to_element(root1, parent_path)
                parent2 = parse_path_to_element(root2, parent_path)
                if parent1 is not None and parent2 is not None:
                    for elem in parent2.findall(tag):
                        parent1.append(elem)
            else:
                all_account_info = root1.findall(path) + root2.findall(path)
                for elem in root1.findall(path):
                    root1.remove(elem)
                insert_index = 0
                for i, child in enumerate(root1):
                    if child.tag != path:
                        insert_index = i
                        break
                else:
                    insert_index = len(root1)
                for elem in all_account_info:
                    root1.insert(insert_index, elem)
                    insert_index += 1
    except Exception as e:
        log_message('failed', f"Error merging append fields: {str(e)}")
        raise

def merge_corp_breakout(root1, root2, config):
    try:
        base_path = config['element_name']
        regex = re.compile(config['regex'])
        cb1 = parse_path_to_element(root1, base_path)
        cb2 = parse_path_to_element(root2, base_path)
        if cb1 is not None and cb2 is not None:
            for child2 in cb2:
                if regex.match(child2.tag):
                    child1 = cb1.find(child2.tag)
                    if child1 is None:
                        cb1.append(child2)
                    else:
                        for metric in child2:
                            m1 = child1.find(metric.tag)
                            m2 = child2.find(metric.tag)
                            if m1 is not None and m2 is not None:
                                m1.text = str(int(m1.text) + int(m2.text))
    except Exception as e:
        log_message('failed', f"Error merging corp breakout: {str(e)}")
        raise

def extract_data_files_from_done(done_json, field):
    """
    Extract data file names from the done file based on the specified field.
    """
    try:
        data_files = []
        for report_set in done_json.values():
            if 'files' in report_set:
                for entry in report_set['files']:
                    if field in entry:
                        data_files.append(entry[field])
        return data_files
    except Exception as e:
        log_message('failed', f"Error extracting data files from done file: {str(e)}")
        raise

def extract_report_metadata_from_done(done_json, metadata_config):
    """
    Extract report metadata from done file based on client configuration
    """
    try:
        metadata = {}
        if not metadata_config:
            return metadata
            
        # Get the first report set (assuming all have same metadata)
        for report_key, report_data in done_json.items():
            if isinstance(report_data, dict):
                # Extract fields based on client configuration
                for field in metadata_config:
                    metadata[field] = report_data.get(field, '')
                break
        return metadata
    except Exception as e:
        log_message('failed', f"Error extracting report metadata from done file: {str(e)}")
        raise

def insert_data_file_tag_at_top(root, data_files, tag_name='data_files'):
    try:
        data_file_element = ET.Element(tag_name)
        for file in data_files:
            file_element = ET.Element('file')
            file_element.text = file
            data_file_element.append(file_element)
        root.insert(0, data_file_element)
    except Exception as e:
        log_message('failed', f"Error inserting data_files tag: {str(e)}")
        raise

def populate_report_section(root, metadata):
    """
    Populate the report section in XML with metadata
    """
    try:
        # Find the file_info section
        file_info = root.find('file_info')
        if file_info is None:
            log_message('warning', "file_info section not found in XML")
            return
            
        # Find or create the report section
        report = file_info.find('report')
        if report is None:
            report = ET.SubElement(file_info, 'report')
            
        # Update report fields with metadata
        for field, value in metadata.items():
            element = report.find(field)
            if element is not None:
                element.text = str(value) if value else ''
            else:
                # Create the element if it doesn't exist
                new_element = ET.Element(field)
                new_element.text = str(value) if value else ''
                report.append(new_element)
                
    except Exception as e:
        log_message('failed', f"Error populating report section: {str(e)}")
        raise

# prettify the xml output
def prettify(elem: ET.Element) -> str:
    rough_string = ET.tostring(elem, encoding="utf-8")
    reparsed = minidom.parseString(rough_string)
    pretty_xml = reparsed.toprettyxml(indent="  ")
    return "\n".join([line for line in pretty_xml.splitlines() if line.strip()])

def merge_xml_reports(bucket, input_path, input_regex, output_path, config_key,
                      done_file_flag, done_file_key, done_file_field, metadata_config):
    try:
        s3 = boto3.client('s3')

        # Load merge config
        config_data = download_s3_file(s3, bucket, config_key)
        config = json.loads(config_data)

        # Optional data_files insertion
        data_file_names = []
        done_json = None
        if done_file_flag.lower() == "true":
            if not done_file_key or done_file_field == " ":
                raise ValueError("done_file_key and done_file_field are required when done_file_flag is True")
            done_data = download_s3_file(s3, bucket, done_file_key)
            done_json = json.loads(done_data)
            data_file_names = extract_data_files_from_done(done_json, done_file_field)

        # Get input XML files to merge
        matching_keys = get_matching_keys(s3, bucket, input_path, input_regex)
        if not matching_keys:
            print("No matching XML files found")
            return

        # Handle case where only one file is to be merged
        if len(matching_keys) == 1:
            single_file_key = matching_keys[0]
            single_file_data = download_s3_file(s3, bucket, single_file_key)
            tree = ET.ElementTree(ET.fromstring(single_file_data))
            root = tree.getroot()

            # Add data_files tag if flag is set
            if done_file_flag and data_file_names:
                insert_data_file_tag_at_top(root, data_file_names)

            # Extract and populate report metadata if done_file_flag is set
            if done_file_flag.lower() == "true" and done_json and metadata_config:
                metadata = extract_report_metadata_from_done(done_json, metadata_config)
                populate_report_section(root, metadata)

            # Generate pretty-printed XML and upload
            pretty_xml = prettify(root).encode('utf-8')
            base_name = os.path.basename(done_file_key)
            output_filename = base_name.replace('.done.json', '.xml')  # Adjusted to replace .done.json with .xml
            output_key = os.path.join(output_path, output_filename)
            upload_s3_file(s3, bucket, output_key, pretty_xml)
            log_message('success', f"Single file processed and treated as merged output: {output_key}")
            return

        merged_tree = None
        merged_root = None

        for i, key in enumerate(matching_keys):
            xml_data = download_s3_file(s3, bucket, key)
            tree = ET.ElementTree(ET.fromstring(xml_data))
            root = tree.getroot()

            if i == 0:
                merged_tree = tree
                merged_root = root
            else:
                merge_sum_fields(merged_root, root, config.get("sum_fields", []))
                merge_append_fields(merged_root, root, config.get("append_fields", []))
                if 'corp_breakout' in config:
                    merge_corp_breakout(merged_root, root, config['corp_breakout'])

        # Add data_files tag if flag is set
        if done_file_flag and data_file_names:
            insert_data_file_tag_at_top(merged_root, data_file_names)

        # Extract and populate report metadata if done_file_flag is set
        if done_file_flag.lower() == "true" and done_json and metadata_config:
            metadata = extract_report_metadata_from_done(done_json, metadata_config)
            populate_report_section(merged_root, metadata)

        # ✅ Generate pretty-printed XML and upload
        pretty_xml = prettify(merged_root).encode('utf-8')

        if done_file_key:
            base_name = os.path.basename(done_file_key)
            output_filename = base_name.replace('.done.json', '.xml')  # Adjusted to replace .done.json with .xml
        else:
            output_filename = 'merged_report.xml'

        output_key = os.path.join(output_path, output_filename)
        upload_s3_file(s3, bucket, output_key, pretty_xml)
        log_message('success',f"Merged report written to: {output_key}")
    except Exception as e:
        log_message('failed', f"Error during XML merge process: {str(e)}")
        raise

def set_job_params_as_env_vars():
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]
            value = sys.argv[i + 1]
            os.environ[key] = value
            
inputFileName=''
if __name__ == '__main__':
    try:
        set_job_params_as_env_vars()
        inputFileName = os.getenv('inputFileName')
        bucket = os.getenv('bucket')
        input_path = os.getenv('input_path')
        input_regex = os.getenv('input_regex')
        output_path = os.getenv('output_path')
        config_key = os.getenv('config_key')
        done_file_key = os.getenv('done_file_key')
        done_file_flag = os.getenv('done_file_flag')
        done_file_field = os.getenv('done_file_field')
        metadata_config = json.loads(os.getenv('metadata_config'))
    
        merge_xml_reports(bucket, input_path, input_regex, output_path, config_key,
                          done_file_flag, done_file_key, done_file_field,metadata_config)
        log_message('success',f" Files merged successfully")
    except Exception as e:
        log_message('failed', f"Error occurred: {e}")
