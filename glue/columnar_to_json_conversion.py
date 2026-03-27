import traceback
import boto3
import os
import json
import sys
import re
import splunk
import io


# S3 client
s3 = boto3.client('s3')

# Initialize global variable
inputFileName = None

def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:columnartojson:" + account_id

def log_message(status, message):
    splunk.log_message({'FileName': inputFileName if inputFileName else 'Unknown', 'Status': status, 'Message': message}, get_run_id())

DEFAULT_INPUT_ENCODING = os.getenv('INPUT_FILE_ENCODING', 'cp1252')


def load_copybook_config_from_s3(s3_client, bucket, key):
    """Load copybook config directly from S3"""
    config = []
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        
        for line in content.splitlines():
            if not line.startswith('#') and line.strip():
                parts = line.split('|')
                field_info = {
                    'field_name': parts[2].strip(),
                    'start': int(parts[3].strip()),
                    'length': int(parts[5].strip()),
                    'in_format': parts[6].strip(),
                    'out_format': parts[7].strip()
                }
                config.append(field_info)
    except Exception as e:
        log_message('error', f"Failed to load config from S3: {key}, {str(e)}")
        raise e
    
    return config

def build_copybook_config_map_from_s3(s3_client, bucket, folder, config_files):
    """Build config map by reading directly from S3"""
    config_map = {}
    configs_without_record_type = []
    
    for config_file in config_files:
        s3_key = f"{folder}/{config_file}" if folder else config_file
        
        field_definitions = load_copybook_config_from_s3(s3_client, bucket, s3_key)
        if not field_definitions:
            log_message('failed', f"Config file missing or empty: {s3_key}")
            raise Exception(f"Config file missing or empty: {s3_key}")

        record_type_match = re.search(r'(\d+)', config_file)
        if not record_type_match:
            log_message('warning', f"Unable to derive record type from {config_file}")
            configs_without_record_type.append(field_definitions)
            continue

        record_type = record_type_match.group(1)[-2:]
        config_map[record_type] = field_definitions

    # If only one config and no record types found, use it as default
    if len(config_files) == 1 and not config_map and configs_without_record_type:
        log_message('info', 'Single config without record type - using as default for all records')
        config_map['DEFAULT'] = configs_without_record_type[0]

    return config_map


def process_fixed_length_file_from_s3_streaming(s3_client, bucket, key, copybook_configs, input_encoding, record_type_start, record_type_end, skip_patterns):
    """Process file using streaming - memory efficient for large files in Python Shell"""
    if skip_patterns is None:
        skip_patterns = ['HDR', 'TRL']
    
    header_trailer_count = 0
    skipped_count = 0
    processed_count = 0
    
    # Check if using default config (no record type identifier)
    use_default_config = 'DEFAULT' in copybook_configs and len(copybook_configs) == 1
    
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        
        # Stream line by line without loading entire file into memory
        stream = response['Body']
        line_buffer = io.TextIOWrapper(stream, encoding=input_encoding, errors='replace')
        
        for line_number, line in enumerate(line_buffer, start=1):
            line = line.rstrip('\n\r')
            
            if not line.strip():  # Skip empty lines
                continue

            # Skip records based on patterns (Header/Trailer)
            should_skip = False
            for pattern in skip_patterns:
                if line.startswith(pattern.strip()):
                    header_trailer_count += 1
                    should_skip = True
                    break
            
            if should_skip:
                continue

            # Determine which config to use
            if use_default_config:
                config = copybook_configs['DEFAULT']
            else:
                if len(line) < record_type_end:
                    log_message('warning', f"Line {line_number} shorter than expected, skipping")
                    skipped_count += 1
                    continue
                
                record_type = line[record_type_start:record_type_end]
                config = copybook_configs.get(record_type)
                
                if not config:
                    skipped_count += 1
                    continue

            record = {}
            for field in config:
                start = field['start'] - 1
                end = start + field['length']
                if start >= len(line):
                    value = ''
                else:
                    segment = line[start:end]
                    value = segment.strip()
                record[field['field_name']] = value

            yield record
            processed_count += 1
            
            # Log progress every 50k records
            if processed_count % 50000 == 0:
                log_message('info', f"Processed {processed_count} records...")

        log_message('info', f"Processing complete: {processed_count} records processed, {header_trailer_count} header/trailer skipped, {skipped_count} other skipped")
        
    except Exception as e:
        log_message('error', f"Failed to process S3 file: {str(e)}")
        raise

def upload_json_to_s3_streaming(s3_client, bucket, key, records_generator):
    """Upload JSON using streaming with temp file"""
    import tempfile
    
    temp_file = tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False, suffix='.json')
    tmp_filename = temp_file.name
    
    try:
        # Write JSON structure with streaming
        temp_file.write('{"json_data": [\n')
        
        first_record = True
        record_count = 0
        
        for record in records_generator:
            if not first_record:
                temp_file.write(',\n')
            else:
                first_record = False
            
            # Write each record as JSON
            json.dump(record, temp_file)
            record_count += 1
            
            # Flush buffer periodically
            if record_count % 5000 == 0:
                temp_file.flush()
        
        temp_file.write('\n]}')
        temp_file.close()
        
        # Upload to S3
        log_message('info', f"Uploading {record_count} records to S3...")
        s3_client.upload_file(
            tmp_filename, 
            bucket, 
            key,
            ExtraArgs={'ContentType': 'application/json'}
        )
        log_message('success', f"Successfully uploaded {record_count} records")
        
        return record_count
        
    finally:
        # Clean up temp file
        if os.path.exists(tmp_filename):
            os.remove(tmp_filename)


def set_job_params_as_env_vars():
    messages = []
    for i in range(1, len(sys.argv), 2):
        if i + 1 < len(sys.argv) and sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]
            value = sys.argv[i+1]
            os.environ[key] = value
            messages.append(f"{key}={value}")
    
    if messages:
        log_message('info', f"Environment variables set: {', '.join(messages)}")


def main():
    try:
        set_job_params_as_env_vars()
        global inputFileName
        inputFileName = os.getenv('inputFileName')
        input_bucket = os.getenv('input_bucket')
        input_path = os.getenv('input_path')
        config_bucket = os.getenv('config_bucket', input_bucket)
        config_folder = os.getenv('config_folder', 'config')
        config_files = os.getenv('config_files', '')
        output_bucket = os.getenv('output_bucket', input_bucket)
        output_path = os.getenv('output_path')
        input_encoding = DEFAULT_INPUT_ENCODING
        record_type_start = int(os.getenv('record_type_start', '8'))
        record_type_end = int(os.getenv('record_type_end', '10'))
        skip_patterns = os.getenv('skip_record_patterns', 'HDR,TRL').split(',')

        log_message('info', f"Starting columnar to JSON conversion for: {inputFileName}")

        if not config_files:
            log_message('failed', 'No config files specified in environment variable')
            return

        config_files_list = [f.strip() for f in config_files.split(',')]

        # Build config map directly from S3
        copybook_configs = build_copybook_config_map_from_s3(s3, config_bucket, config_folder, config_files_list)
        if not copybook_configs:
            log_message('failed', 'No copybook configurations loaded. Nothing to process.')
            return

        # Process input file using streaming
        s3_input_key = f"{input_path}/{inputFileName}" if input_path else inputFileName
        log_message('info', f"Reading from s3://{input_bucket}/{s3_input_key}")
        
        records_generator = process_fixed_length_file_from_s3_streaming(
            s3, input_bucket, s3_input_key, copybook_configs, input_encoding,
            record_type_start, record_type_end, skip_patterns
        )

        # Output file
        base_filename = inputFileName.rsplit('.', 1)[0] if '.' in inputFileName else inputFileName
        output_filename = f"{base_filename}.json"
        s3_output_key = f"{output_path}/{output_filename}" if output_path else output_filename

        # Upload using streaming
        upload_json_to_s3_streaming(s3, output_bucket, s3_output_key, records_generator)
        
        log_message('success', f"File converted successfully to s3://{output_bucket}/{s3_output_key}")

    except Exception as e:
        error_message = f"Main function error: {str(e)}"
        print(f"MAIN ERROR: {error_message}")
        print(f"Stack trace: {traceback.format_exc()}")
        log_message('failed', error_message)
        raise

if __name__ == "__main__":
    main()