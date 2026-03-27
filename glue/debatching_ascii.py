import boto3
import os
import json
import sys
import re
import splunk
from datetime import datetime
from zoneinfo import ZoneInfo

# S3 client
s3 = boto3.client('s3')

def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:debatchingAscii:" + account_id

def log_message(status, message):
    splunk.log_message({'FileName':inputFileName,'Status': status, 'Message': message}, get_run_id())

def download_file_from_s3(bucket, key, local_path):
    s3.download_file(bucket, key, local_path)

def upload_file_to_s3(bucket, key, local_path):
    s3.upload_file(local_path, bucket, key)

def split_file(local_file_path, batch_size, start_record_type, end_record_type, one_record_per_row, header_skip_flag, header_rec, delimeter, rec_pos):
    try:
        # Enhanced logic triggers only when use_end_record_only=True
        use_end_record_only = os.getenv('use_end_record_only', 'False').lower() == 'true'
        use_greedy_assignment = os.getenv('use_greedy_assignment', 'False').lower() == 'true'
      #  use_greedy_assignment = os.getenv('use_greedy_assignment', 'True').lower() == 'true'
      #  group_key_positions = "2,3,4,5"
        group_key_positions = os.getenv('group_key_positions', '').strip()
      #  group_key_positions = os.getenv('group_key_positions', '2,3,4,5').strip()
        header_record_type = os.getenv('header_record_type', '').strip()
        tailer_record_type = os.getenv('tailer_record_type', '').strip()

        if use_end_record_only:
            log_message("info", "Running enhanced multi‑header and multi‑tailer mode.")

            batches = []
            batch = []
            current_details = []
            header_record = ""
            current_tailer = ""
            i=0

            with open(local_file_path, "r", encoding="utf-8-sig") as f:
                lines = [line.replace("\r", "") for line in f.readlines()]

            for line in lines:
                stripped_line = line.strip()
                parts = stripped_line.split(delimeter)
                if not parts or not parts[0]:
                    continue
                record_type = parts[int(rec_pos)]

                # Capture header for current section
                if record_type == header_record_type:
                    header_record = line
                    continue

                # When tailer appears → close groups under current header
                if record_type == tailer_record_type:
                    current_tailer = line
                    if current_details:
                        temp_group = []
                        for detail in current_details:
                            temp_group.append(detail)
                            # end_record_type closes logical record
                            if detail.strip().split(delimeter)[int(rec_pos)] == end_record_type:
                                # Create a complete logical record
                                logical_record = []
                                if header_record:
                                    logical_record.append(header_record)
                                logical_record.extend(temp_group)
                                logical_record.append(current_tailer)

                                # Add to current batch
                                batch.extend(logical_record)

                                i += 1
                                if i == batch_size:
                                    print(f"i : {i}")
                                    i = 0
                                    batches.append(batch)
                                    batch = []  # Clear batch for next set

                                temp_group = []  # Clear temp_group for next logical record
                        current_details = []
                    continue

                # Normal detail record
                current_details.append(line)

            # Add remaining batch if it has records
            if batch:
                batches.append(batch)

            # In case the file ends without final tailer
            if current_details:
                temp_group = []
                for detail in current_details:
                    temp_group.append(detail)
                    if detail.strip().split(delimeter)[int(rec_pos)] == end_record_type:
                        # Create a complete logical record
                        logical_record = []
                        if header_record:
                            logical_record.append(header_record)
                        logical_record.extend(temp_group)
                        if current_tailer:
                            logical_record.append(current_tailer)

                        # Add to current batch
                        batch.extend(logical_record)

                        i += 1
                        print(f"here i : {i}")

                        if i >= batch_size:
                            print(f"i : {i}")
                            i = 0
                            batches.append(batch)
                            batch = []  # Clear batch for next set

                        temp_group = []  # Clear temp_group for next logical record

                # Add any remaining batch
                if batch:
                    batches.append(batch)

            log_message("success", f"Enhanced split‑only‑end mode: {len(batches)} batches created using end_record_type = '{end_record_type}'.")
            return batches, []  # header_lines not used in this mode

        if one_record_per_row.lower() != "true" and start_record_type == " ":
            raise ValueError("Error: 'start_record_type' must be specified when 'one_record_per_row' is False.")
        if not header_rec or all(rec.strip() == "" for rec in header_rec):
            if header_skip_flag.lower() != "true":
                raise ValueError("Error: 'header_rec' is missing, but 'header_skip_flag' is set to False.")

        start_record_found = False
        end_record_found = False

        batches = []
        current_batch = []
        customer_records = []
        header_lines = []
        is_record_start = False

        with open(local_file_path, 'r') as file:
            for line in file:
                record_type = line.strip().split(delimeter)[int(rec_pos)]

                if record_type in header_rec:
                    if header_skip_flag.lower() == "true":
                        continue
                    header_lines.append(line)

                if record_type == start_record_type:
                    start_record_found = True
                if end_record_type != " " and record_type == end_record_type:
                    end_record_found = True

                if one_record_per_row.lower() == "true":
                    customer_records.append([line])
                    continue

                if record_type == start_record_type:
                    if is_record_start:
                        customer_records.append(current_batch)
                        current_batch = []
                    is_record_start = True

                if is_record_start:
                    current_batch.append(line)

                if end_record_type != " " and record_type == end_record_type and is_record_start:
                    customer_records.append(current_batch)
                    current_batch = []
                    is_record_start = False

        if current_batch:
            customer_records.append(current_batch)

        if start_record_type != " " and not start_record_found:
            raise ValueError(f"Error: The start record type '{start_record_type}' is not present in the file.")
        if end_record_type != " " and not end_record_found:
            raise ValueError(f"Error: The end record type '{end_record_type}' is not present in the file.")

        # Apply greedy assignment if enabled
        if use_greedy_assignment:
            if not group_key_positions:
                raise ValueError("Error: 'group_key_positions' must be specified when 'use_greedy_assignment' is enabled.")
            log_message("info", "Applying greedy assignment with composite key grouping for balanced batching.")
            batches = greedy_batch_assignment_with_composite_key(customer_records, batch_size, delimeter, group_key_positions)
        else:
            # Standard sequential batching
            for i in range(0, len(customer_records), batch_size):
                batches.append(customer_records[i:i + batch_size])

        log_message("success", f"Split into {len(batches)} batches.")
        return batches, header_lines
    except Exception as e:
        log_message('failed',f" failed to split the files:{str(e)}")
        raise ValueError(f"failed to split the files:{str(e)}")

def greedy_batch_assignment_with_composite_key(customer_records, batch_size, delimiter, group_key_positions):
    """
    Groups records by a composite key (combination of multiple columns),
    then distributes groups across batches using greedy assignment.

    Args:
        customer_records: List of logical records
        batch_size: Target batch size
        delimiter: Field delimiter
        group_key_positions: Comma-separated column positions (e.g., "2,3,4,5" for 3rd, 4th, 5th, 6th columns)
    """
    # Parse the column positions
    try:
        key_positions = [int(pos.strip()) for pos in group_key_positions.split(',')]
    except ValueError:
        raise ValueError(f"Invalid group_key_positions format: '{group_key_positions}'. Expected comma-separated integers.")

    # Step 1: Group records by composite key
    composite_key_groups = {}
    for record in customer_records:
        # Extract composite key from first line of the record
        first_line = record[0] if isinstance(record, list) else record
        parts = first_line.strip().split(delimiter)

        # Build composite key from specified positions
        try:
            key_values = []
            for pos in key_positions:
                if len(parts) > pos:
                    key_values.append(parts[pos].strip())
                else:
                    key_values.append("MISSING")
            composite_key = "~".join(key_values)
        except IndexError:
            composite_key = "INVALID_KEY"
            log_message("warning", f"Could not extract composite key from record. Using 'INVALID_KEY'.")

        if composite_key not in composite_key_groups:
            composite_key_groups[composite_key] = []
        composite_key_groups[composite_key].append(record)

    log_message("info", f"Grouped records into {len(composite_key_groups)} unique composite keys.")

    # Step 2: Calculate group sizes (total lines per composite key)
    group_info = []
    for composite_key, records in composite_key_groups.items():
        total_lines = sum(len(record) if isinstance(record, list) else 1 for record in records)
        group_info.append({
            'composite_key': composite_key,
            'records': records,
            'total_lines': total_lines,
            'record_count': len(records)
        })

    # Sort groups by size in descending order (largest first)
    group_info.sort(key=lambda x: x['total_lines'], reverse=True)

    # Step 3: Initialize batches
    estimated_batches = max(1, len(customer_records) // batch_size)
    batch_containers = [[] for _ in range(estimated_batches)]
    batch_record_counts = [0] * estimated_batches

    # Step 4: Greedy assignment - assign each group to the batch with minimum current record count
    for group in group_info:
        # Find batch with minimum record count
        min_batch_idx = batch_record_counts.index(min(batch_record_counts))

        # Check if adding this group would exceed batch_size significantly
        # If so, create a new batch
        if batch_record_counts[min_batch_idx] > 0 and batch_record_counts[min_batch_idx] + group['record_count'] > batch_size * 1.5:
            batch_containers.append([])
            batch_record_counts.append(0)
            min_batch_idx = len(batch_containers) - 1

        # Assign all records of this composite key to the selected batch
        batch_containers[min_batch_idx].extend(group['records'])
        batch_record_counts[min_batch_idx] += group['record_count']

  #      log_message("info", f"Assigned composite key '{group['composite_key']}' ({group['record_count']} records, {group['total_lines']} lines) to batch {min_batch_idx + 1}")

    # Remove empty batches
    batches = [batch for batch in batch_containers if batch]

    log_message("info", f"Greedy assignment with composite key completed: {len(batches)} batches with record counts: {batch_record_counts[:len(batches)]}")
    return batches

def save_batches_to_files(batches, output_dir, ascii_file_key, header_in_file_flag, header_lines, inputFileName):
    try:
        if not batches:
            return [], "", []

        file_info = []
        record_counts = []

        # Check if object key starts with "citizen" (case insensitive)
        if ascii_file_key.lower().startswith('citizen'):
            # Use original naming convention
            base_name = os.path.basename(ascii_file_key).rsplit('.', 1)[0]
        else:
            # Use input file name (zip file name)
            base_name = os.path.basename(inputFileName).rsplit('.', 1)[0]

        total_files = len(batches)

        header_file_name = ""
        if header_lines and header_in_file_flag.lower() != "true":
            header_file_name = f"{base_name}.header.asc"
            header_file_path = os.path.join(output_dir, header_file_name)
            with open(header_file_path, 'w') as header_file:
                header_file.writelines(header_lines)
            file_info.append({"header_file": header_file_name})

        for idx, batch in enumerate(batches):
            file_num = f"{idx + 1:05d}"
            debatched_file_name = f"{base_name}.{file_num}.{total_files:05d}.in.asc"
            debatched_file_path = os.path.join(output_dir, debatched_file_name)

            current_batch_record_count = len(batch)
            record_counts.append(current_batch_record_count)

            with open(debatched_file_path, 'w') as batch_file:
                if header_in_file_flag.lower() == "true" and header_lines:
                    batch_file.writelines(header_lines)
                for customer in batch:
                    # Ensure each line ends with a newline character
                    if isinstance(customer, str):
                        # In enhanced mode, customer is a single line string
                        if not customer.endswith('\n'):
                            batch_file.write(customer + '\n')
                        else:
                            batch_file.write(customer)
                    else:
                        # In regular mode, customer is a list of lines
                        for line in customer:
                            if not line.endswith('\n'):
                                batch_file.write(line + '\n')
                            else:
                                batch_file.write(line)

            file_info.append({
                "debatched_file": debatched_file_name,
                "data_file": f"{base_name}.{file_num}.{total_files:05d}.out.json",
                "report_file": f"{base_name}.{file_num}.{total_files:05d}.out.xml",
                "record_count": current_batch_record_count  # NEW: Add record count to file info
            })

        log_message("success", f"Saved {len(file_info)} debatched files with record counts: {record_counts}")
        return file_info, header_file_name, record_counts  # NEW: Return record_counts

    except Exception as e:
        log_message('failed', f"An error occurred in save_batches_to_files: {str(e)}")
        raise ValueError(f"An error occurred in save_batches_to_files: {str(e)}")



def write_done_file(inputFileName, bucket, file_info, ascii_file_key, output_dir, done_file_flag, supp_file_path, supp_file_regex, run_type, include_metadata):
    try:
        if not file_info:
            print("No debatched files created. Skipping done file creation.")
            return None, None

        # Check if object key starts with "citizen" (case insensitive)
        if ascii_file_key.lower().startswith('citizen'):
            # Use original naming convention
            base_name = os.path.basename(ascii_file_key).rsplit('.', 1)[0]
        else:
            # Use input file name (zip file name)
            base_name = os.path.basename(inputFileName).rsplit('.', 1)[0]

        done_file_name = f"{base_name}.done.json"
        done_file_path = os.path.join(output_dir, done_file_name)

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Try to retrieve the supplementary file
        try:
            supp_filename = get_latest_matching_s3_key(bucket, supp_file_path, supp_file_regex)
        except ValueError:
            supp_filename = None
            log_message("info", f"No supplementary file found in path '{supp_file_path}' with regex '{supp_file_regex}'.")

        start_execution_time = datetime.now(ZoneInfo("America/New_York")).strftime('%m/%d/%Y %H:%M:%S EST')
        base_name = os.path.basename(ascii_file_key).rsplit('.', 1)[0]
        # Construct the done file content
        done_file_content = {base_name: {}}

        # Include metadata if the flag is enabled
        if include_metadata.lower() == "true":
            done_file_content[base_name].update({
                "title": "CITIZENS MORTGAGE",
                "doc_type": "Citizens Mortgage Billing Recon Report",
                "input_file": f"{inputFileName}",
                "unzipped_filename": base_name,
                "loan_officer_file": os.path.basename(supp_filename) if supp_filename else "None",
                "environment": run_type,
                "processing_start": start_execution_time
            })

        # Add the files list after metadata
        done_file_content[base_name]["files"] = []
        for info in file_info:
            if 'debatched_file' in info:
                done_file_content[base_name]["files"].append({
                    "debatchedfilename": info["debatched_file"],
                    "dataFile": info["data_file"],
                    "reportFile": info["report_file"]
                })
            elif 'header_file' in info:
                done_file_content[base_name]["files"].append({
                    "header_file": info["header_file"]
                })

        with open(done_file_path, 'w') as done_file:
            json.dump(done_file_content, done_file, indent=4)
        log_message("success", f"Done file created successfully")
        return done_file_name, done_file_path
    except Exception as e:
        log_message('failed', f"An error occurred in write_done_file: {str(e)}")
        raise ValueError(f"An error occurred in write_done_file: {str(e)}")

def get_matching_s3_keys(bucket, prefix='', pattern=''):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = os.path.basename(obj['Key'])
            if re.match(pattern, key):
                keys.append(obj['Key'])
    return keys[0]

def get_latest_matching_s3_key(bucket, prefix='', pattern=''):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    matching_files = []

    # List all files in the specified S3 path
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = os.path.basename(obj['Key'])
            if re.match(pattern, key):
                matching_files.append(obj)

    if not matching_files:
        raise ValueError(f"No files found in S3 path '{prefix}' matching regex '{pattern}'.")

    # Sort files by LastModified timestamp and pick the latest
    latest_file = max(matching_files, key=lambda x: x['LastModified'])
    return latest_file['Key']

def set_job_params_as_env_vars():
    # Loop through all command-line arguments starting from the second argument (skip the script name)
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]  # Remove the leading '--'
            value = sys.argv[i + 1]
            os.environ[key] = value

def main():
    try:
        set_job_params_as_env_vars()
        global inputFileName
        inputFileName = os.getenv('inputFileName')
        bucket_name = os.getenv('s3_bucket_input')
        ascii_file_path = os.getenv('ascii_file_path')
        ascii_file_pattern = os.getenv('ascii_file_pattern')
        ascii_file_key = os.getenv('ascii_file_key')
        output_folder = os.getenv('output_folder')
        batch_size = os.getenv('batch_size')
        one_record_per_row = os.getenv('one_record_per_row')
        start_record_type = os.getenv('start_record_type')
        end_record_type = os.getenv('end_record_type')
        header_skip_flag = os.getenv('header_skip_flag')
        header_in_file_flag = os.getenv('header_in_file_flag')
        header_rec = os.getenv('header_rec')
        delimeter = os.getenv('delimeter')
        rec_pos = os.getenv('rec_pos')
        done_file_flag = os.getenv('done_file_flag')
        copy_folder_debatch = os.getenv('copy_folder_debatch')
        copy_folder_manifest = os.getenv('copy_folder_manifest')
        supp_file_regex = os.getenv('supp_file_regex')
        supp_file_path = os.getenv('supp_file_path')
        supp_file_key = os.getenv('supp_file_key')
        run_type = os.getenv('run_type')
        include_metadata = os.getenv('include_metadata')
        send_to_kafka_flag = os.getenv('send_to_kafka_flag')
        configFile = os.getenv('configFile')

        if header_rec != " ":
            header_rec = header_rec.split(",")

        batch_size = int(batch_size)

        local_file_path = '/tmp/input-file.txt'
        output_dir = '/tmp/split_batches'

        try:
            # Get file key if not provided
            ascii_file_key = ascii_file_key if ascii_file_key != " " else get_matching_s3_keys(bucket_name, ascii_file_path, ascii_file_pattern)
        except Exception as e:
            log_message('failed',f"Error fetching matching S3 keys: {str(e)}")
            raise ValueError(f"Error fetching matching S3 keys: {str(e)}")

        try:
            # Cleanup or create output directory
            if os.path.exists(output_dir):
                for file in os.listdir(output_dir):
                    os.remove(os.path.join(output_dir, file))
            else:
                os.makedirs(output_dir)
        except Exception as e:
            log_message('failed',f"Error managing output directory: {str(e)}")
            raise ValueError(f"Error managing output directory: {str(e)}")

        try:
            # Download file from S3
            download_file_from_s3(bucket_name, ascii_file_key, local_file_path)
        except Exception as e:
            log_message('failed',f"Error downloading file from S3: {str(e)}")
            raise ValueError(f"Error downloading file from S3: {str(e)}")

        try:
            # Split file into batches
            batches, header_lines = split_file(local_file_path, batch_size, start_record_type, end_record_type,
                                               one_record_per_row, header_skip_flag, header_rec, delimeter, rec_pos)
            if not batches:
                log_message('failed',f"No records found for splitting. Exiting.")
                return
        except Exception as e:
            log_message('failed',f"Error splitting file: {str(e)}")
            raise ValueError(f"Error splitting file: {str(e)}")

        try:
            # Save batches to local files
            file_info, header_file_name, record_counts = save_batches_to_files(batches, output_dir, ascii_file_key, header_in_file_flag, header_lines, inputFileName)
        except Exception as e:
            log_message('failed',f"Error saving batches to files: {str(e)}")
            raise ValueError(f"Error saving batches to files: {str(e)}")

        try:
            # Write and upload done file if enabled
            if done_file_flag == "True":
                done_file_name, done_file_path = write_done_file(inputFileName,bucket_name,file_info, ascii_file_key, output_dir, done_file_flag,supp_file_path,supp_file_regex,run_type,include_metadata)
                s3_key_done = f"{output_folder}/{done_file_name}"
                upload_file_to_s3(bucket_name, s3_key_done, done_file_path)
                s3_key_done_copy = f"{copy_folder_manifest}/{done_file_name}"
                upload_file_to_s3(bucket_name, s3_key_done_copy, done_file_path)
                log_message("success", f"Splitting and uploading completed.")
        except Exception as e:
            log_message('failed',f"Error creating or uploading done file: {str(e)}")
            raise ValueError(f"Error creating or uploading done file: {str(e)}")
        try:
            # Upload debatched files to S3
            s3_uris = []  # Collect S3 URIs
            total_debatched_files = 0  # Track total number of debatched files
            if batch_size==1:
                requiredSegregation = "false"
            else:
                requiredSegregation = "true"
            for info in file_info:
                if 'debatched_file' in info:
                    debatched_file_path = os.path.join(output_dir, info["debatched_file"])
                    s3_key = f"{output_folder}/{info['debatched_file']}"
                    upload_file_to_s3(bucket_name, s3_key, debatched_file_path)
                    s3_key_copy = f"{copy_folder_debatch}/{info['debatched_file']}"
                    upload_file_to_s3(bucket_name, s3_key_copy, debatched_file_path)
                    s3_uris.append(f"s3://{bucket_name}/{s3_key}")
                    print(f"Debatched file {info['debatched_file']} with {info['record_count']} records copied successfully")
        except Exception as e:
            log_message('failed',f"Error uploading debatched files to S3: {str(e)}")
            raise ValueError(f"Error uploading debatched files to S3: {str(e)}")

        try:
            # Write configuration file
            if done_file_flag.lower()=="true" and send_to_kafka_flag.lower()=="true":
                config_data = {
                    "s3_uris": s3_uris,
                    "record_counts": record_counts,
                    "batch_size": batch_size,
                    "total_files": len(s3_uris),
                    "total_records": sum(record_counts),
                    "done_file_name": done_file_name,
                    "requiredSegregation": requiredSegregation,
                    "configFile": configFile
                }
                config_file_name = "debatch_config.json"
                config_file_path = os.path.join(output_dir, config_file_name)
                with open(config_file_path, 'w') as config_file:
                    json.dump(config_data, config_file, indent=4)

                # Upload configuration file to S3
                s3_key_config = f"{output_folder}/{config_file_name}"
                upload_file_to_s3(bucket_name, s3_key_config, config_file_path)
                print("Configuration file uploaded successfully")
        except Exception as e:
            log_message('failed', f"Error creating or uploading configuration file: {str(e)}")
            raise ValueError(f"Error creating or uploading configuration file: {str(e)}")

        try:
            # Upload header file if present
            if header_file_name:
                header_file_path = os.path.join(output_dir, header_file_name)
                s3_key_header = f"{output_folder}/{header_file_name}"
                upload_file_to_s3(bucket_name, s3_key_header, header_file_path)
        except Exception as e:
            log_message('failed',f"Error uploading header file to S3: {str(e)}")
            raise ValueError(f"Error uploading header file to S3: {str(e)}")
        log_message("success", f"debatchingAscii completed successfully")
    except Exception as e:
        log_message('failed',f"debatchingAscii failed: {str(e)}")
        raise ValueError(f"debatchingAscii failed: {str(e)}")

inputFileName=''
if __name__ == "__main__":
    main()
