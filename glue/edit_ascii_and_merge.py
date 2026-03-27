import sys
import boto3
import json
import re
import os
import tempfile
import shutil
from pathlib import Path
from typing import Any
from botocore.exceptions import  ClientError
from awsglue.utils import  getResolvedOptions
import splunk



def extract_trade_date_s3(bucket_name: str, file_key: str):
    trade_date = None
    filename = file_key.split('/')[-1]
    
    # Only process RRSU files
    if not (filename.startswith('RRSU') and filename.endswith('.asc')):
        return None
    
    try:
        # Read file from S3
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        content = response['Body'].read().decode('utf-8', errors='ignore')
        lines = content.splitlines()
        
        header_found = False
        for line in lines:
            # Skip until we find the header line
            if not header_found:
                if ('ALPHA CODE TRADE DATE  DEALER NO' in line or 
                    'ALPHA CODE  TRADE DATE    DEALER-BRANCH' in line):
                    header_found = True
                continue
            
            # After header, look for first data line
            if header_found and '~' in line:
                # Extract the trade date from the data line
                parts = line.split()
                for part in parts:
                    if re.match(r'\d{2}/\d{2}/\d{4}', part):
                        trade_date = part
                        return trade_date
    except Exception as e:
        log_message('failed', f"Error extracting trade date from {file_key}: {str(e)}")
        return None
    
    return trade_date

def process_files_s3(bucket_name: str, input_path: str):
    results = {}
    
    # Process only RRSU files
      #     target_files = ['RRSU0001.TXT.asc', 'RRSU0224.TXT.asc']

    target_files = json.loads(os.getenv("driverKeyList", "[]"))
    for filename in target_files:
        file_key = s3_join(input_path, filename)
        try:
            # Check if file exists in S3
            s3.head_object(Bucket=bucket_name, Key=file_key)
            trade_date = extract_trade_date_s3(bucket_name, file_key)
            if trade_date:
                results[filename] = trade_date
        except ClientError:
            log_message('failed', f"File not found in S3: {file_key}")
            continue
    
    return results

def get_run_id():
      account_id = boto3.client("sts").get_caller_identity()["Account"]
      return "arn:dpm:glue:br_icsdev_dpmtest_json_validation_against_jsonschema:" + account_id

def log_message(status, message):
      splunk.log_message({'FileName': os.getenv('inputFileName'), 'Status': status, 'Message': message}, get_run_id())

s3 = boto3.client('s3')

def extract_check_delivery_pairs_from_s3(bucket_name: str, source_key: str) -> list[tuple[str, str]]:
      """Extract check delivery pairs from S3 file content"""
      log_message('logging', f"Extracting check delivery pairs from {source_key}")
     
      # Load regex patterns from config
      config_key = os.getenv("configKey")
      if not config_key:
            log_message('failed', "configKey environment variable is not set")
            raise ValueError("configKey environment variable is required")
      
      # Try to load config: first as JSON string, then as S3 file path
      config = {}
      try:
            # Try parsing as JSON string first (consistent with main() approach)
            parsed_config = json.loads(config_key)
            # Check if the parsed result is actually a dict (could be a string if double-encoded)
            if isinstance(parsed_config, dict):
                  config = parsed_config
                  log_message('logging', "Loaded config from configKey as JSON string")
            elif isinstance(parsed_config, str):
                  # Double-encoded JSON string or S3 path - try parsing again, if fails treat as S3 path
                  try:
                        # Try parsing the parsed string as JSON again (double-encoded)
                        double_parsed = json.loads(parsed_config)
                        if isinstance(double_parsed, dict):
                              config = double_parsed
                              log_message('logging', "Loaded config from configKey as double-encoded JSON string")
                        else:
                              # Not a dict, treat as S3 file path
                              config = load_config_from_s3(bucket_name, parsed_config)
                              log_message('logging', f"Loaded config from S3: {parsed_config}")
                  except (json.JSONDecodeError, TypeError, ValueError):
                        # Not valid JSON, treat as S3 file path
                        config = load_config_from_s3(bucket_name, parsed_config)
                        log_message('logging', f"Loaded config from S3: {parsed_config}")
            else:
                  # Not a dict or string, treat original config_key as S3 path
                  config = load_config_from_s3(bucket_name, config_key)
                  log_message('logging', f"Loaded config from S3: {config_key}")
      except (json.JSONDecodeError, TypeError, ValueError) as e:
            # If not valid JSON, treat as S3 file path
            try:
                  config = load_config_from_s3(bucket_name, config_key)
                  log_message('logging', f"Loaded config from S3: {config_key}")
            except Exception as s3_error:
                  log_message('failed', f"Failed to load config from S3: {str(s3_error)}")
                  raise ValueError(f"Failed to load config from configKey: {str(s3_error)}")
      
      # Validate config is a dictionary (critical check)
      if not isinstance(config, dict):
            log_message('failed', f"Config is not a dictionary. Type: {type(config)}, Value: {config}")
            raise ValueError(f"Config must be a dictionary, got {type(config)}: {config}")
      
      # Check if we got a default error response from load_config_from_s3
      if "regex_patterns" not in config and "check_count" in config and "trailer_count" in config and len(config) == 2:
            # This is the default error response from load_config_from_s3
            log_message('failed', f"Config file not found or invalid at S3 path: {config_key}")
            raise ValueError(f"Config file not found or invalid at S3 path: {config_key}")
      
      # Extract regex patterns from config
      regex_patterns = config.get("regex_patterns", [])
      if not regex_patterns:
            log_message('failed', f"No regex_patterns found in config. Available keys: {list(config.keys())}")
            raise ValueError("regex_patterns not found in config file")
      
      # Find patterns by name - initialize as None to ensure they come from config
      check_line_pattern = None
      check_line_alt_pattern = None
      delivery_line_pattern = None
      delivery_line_flags = 0
      
      for pattern_obj in regex_patterns:
            pattern_name = pattern_obj.get("name")
            pattern_str = pattern_obj.get("pattern")
            if pattern_name == "check_line_re" and pattern_str:
                  check_line_pattern = pattern_str
            elif pattern_name == "check_line_re_alt" and pattern_str:
                  check_line_alt_pattern = pattern_str
            elif pattern_name == "delivery_line_re" and pattern_str:
                  delivery_line_pattern = pattern_str
                  flags_str = pattern_obj.get("flags", "")
                  delivery_line_flags = re.IGNORECASE if "IGNORECASE" in flags_str.upper() else 0
      
      # Validate that all required patterns were found
      if not check_line_pattern:
            log_message('failed', "check_line_re pattern not found in config")
            raise ValueError("check_line_re pattern is required in config")
      if not check_line_alt_pattern:
            log_message('failed', "check_line_re_alt pattern not found in config")
            raise ValueError("check_line_re_alt pattern is required in config")
      if not delivery_line_pattern:
            log_message('failed', "delivery_line_re pattern not found in config")
            raise ValueError("delivery_line_re pattern is required in config")
      
      # Compile regex patterns
      check_line_re = re.compile(check_line_pattern)
      check_line_re_alt = re.compile(check_line_alt_pattern)
      delivery_line_re = re.compile(delivery_line_pattern, delivery_line_flags)

      pairs: list[tuple[str, str]] =  []
      seen_for_check: set[tuple[str, str]] = set()
      current_check: str | None = None
      seen_checks: set[str] = set()

      try:
            response = s3.get_object(Bucket=bucket_name, Key=source_key)
            content = response['Body'].read().decode('utf-8', errors='ignore')
           
            for raw_line in content.split('\n'):
                  line = raw_line.rstrip('\n')

                  m_check = check_line_re.search(line)
                  if not m_check:
                        m_check = check_line_re_alt.search(line)
                  if m_check:
                        current_check = m_check.group(1)
                        seen_checks.add(current_check)
                        continue

                  m_delivery = delivery_line_re.search(line)
                  if m_delivery and current_check:
                        location = m_delivery.group(1)
                        # Strip trailing tildes or stray separators
                        location = location.replace('~', '').strip()
                        # if location=='B':
                        #       location='B~'
                        # elif location=='C':
                        #       location='C~'

                        key =  (current_check, location)
                        if key not in seen_for_check:
                              pairs.append(key)
                              seen_for_check.add(key)
      except Exception as e:
            log_message('failed', f"Error extracting check delivery pairs from {source_key}: {str(e)}")
            raise e

      return pairs

def load_config_from_s3(bucket_name: str, config_key: str) -> dict[str, Any]:
      """Load configuration from S3"""
      try:
            response = s3.get_object(Bucket=bucket_name, Key=config_key)
            content = response['Body'].read().decode('utf-8', errors='ignore')
            parsed = json.loads(content)
            # Ensure we return a dict, not a string or other type
            if not isinstance(parsed, dict):
                  log_message('failed', f"Config file {config_key} does not contain a JSON object. Got type: {type(parsed)}")
                  raise ValueError(f"Config file must contain a JSON object, got {type(parsed)}")
            return parsed
      except Exception as e:
            log_message('logging', f"Config file not found or error reading {config_key}: {str(e)}")
            return  {"check_count": [], "trailer_count": []}

def _find_files_by_regex_s3(bucket_name: str, path: str, pattern: str) -> list[str]:
      """Find files in S3 matching regex pattern"""
      compiled = re.compile(pattern)
      try:
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=path)
            files =  []
            for obj in response.get('Contents', []):
                  filename = obj['Key'].split('/')[-1]
                  if compiled.search(filename):
                        files.append(obj['Key'])
            return files
      except Exception as e:
            log_message('failed', f"Error finding files in S3: {str(e)}")
            return  []

def _read_text_from_s3(bucket_name: str, key: str) -> list[str]:
      """Read text content from S3"""
      try:
            response = s3.get_object(Bucket=bucket_name, Key=key)
            content = response['Body'].read().decode('utf-8', errors='ignore')
            return content.split('\n')
      except Exception as e:
            log_message('failed', f"Error reading text from S3 {key}: {str(e)}")
            return  []

def validate_check_counts_from_config_s3(bucket_name: str, cfg: dict[str, Any]) -> None:
      """Validate check counts from config using S3 files"""
      rules = cfg.get('check_count') or  []
      for rule in rules:
            try:
                  driver_files = _find_files_by_regex_s3(bucket_name, os.getenv('inputPath'), rule['driver_file_pattern'])
                  
                  register_files = _find_files_by_regex_s3(bucket_name, os.getenv('inputPath'), rule['register_file_pattern'])
                  issued_marker = rule['issued_marker']
                  issued_count_re = re.compile(rule['issued_count_regex'])
            except KeyError:
                  continue

            for drv in driver_files:
                  lines = _read_text_from_s3(bucket_name, drv)
                  issued_count: int | None = None
                  for line in lines:
                        if issued_marker in line:
                              m = issued_count_re.search(line)
                              if m:
                                    issued_count = int(m.group(1))
                                    break
                  if issued_count is not None:
                        log_message('logging', f"{drv}: extracted issued count = {issued_count}")

            # Register file handling is project-specific; we locate files here
            for reg in register_files:
                  log_message('logging', f"Found register file: {reg}")

def validate_trailer_counts_from_config_s3(bucket_name: str, cfg: dict[str, Any]) -> None:
      """Validate trailer counts from config using S3 files"""
      rules = cfg.get('trailer_count') or  []
      for rule in rules:
            try:
                  ascii_files = _find_files_by_regex_s3(bucket_name, os.getenv('inputPath'), rule['ascii_file_pattern'])
                  detail_id = rule['detail_record_identifier']
                  trailer_id = rule['trailer_record_identifier']
                  trailer_idx = int(rule['trailer_count_field_index'])
            except KeyError:
                  continue

            for asc in ascii_files:
                  lines = _read_text_from_s3(bucket_name, asc)
                  detail_count = sum(1 for ln in lines if ln.startswith(detail_id))
                  trailer_counts: list[int] =  []
                  for ln in lines:
                        if ln.startswith(trailer_id):
                              parts = ln.rstrip('\n').split('~')
                              if 0 <= trailer_idx < len(parts):
                                    try:
                                          trailer_counts.append(int(parts[trailer_idx]))
                                    except ValueError:
                                          pass
                  if trailer_counts:
                        log_message('logging', f"{asc}: detail={detail_count}, trailer_field(s)={trailer_counts}")

def merge_two_files_s3(bucket_name: str, output_key: str, first_key: str, second_key: str) -> bool:
      """Merge two S3 files into output file. Returns True if merged, else False."""
      try:
            # Read first file
            response1 = s3.get_object(Bucket=bucket_name, Key=first_key)
            content1 = response1['Body'].read().decode('utf-8', errors='ignore')
           
            # Read second file
            response2 = s3.get_object(Bucket=bucket_name, Key=second_key)
            content2 = response2['Body'].read().decode('utf-8', errors='ignore')
           
            # Merge content
            merged_content = content1 + content2
           
            # Write merged content to output
            s3.put_object(Bucket=bucket_name, Key=output_key, Body=merged_content.encode('utf-8'))
            return True
      except Exception as e:
            log_message('failed', f"Error merging files {first_key}  and {second_key}: {str(e)}")
            return False

def merge_n_files_s3(bucket_name: str, files: list[str], temp_path: str) -> bool:
      """Merge n files dynamically in S3. Returns True if all files merged successfully, else False."""
      if len(files) < 2:
            return False
     
      # Check if all files exist
      for file_key in files:
            try:
                  s3.head_object(Bucket=bucket_name, Key=file_key)
            except  ClientError:
                  log_message('failed', f"File not found: {file_key}")
                  return False
     
      # Get the first filename for naming convention
      first_filename = files[0].split('/')[-1].split('.')[0]       # Gets filename without extension
      log_message('logging', f"Merge function - first_filename: {first_filename}")
     
      # Start with merging first two files
      current_merged = f"{temp_path}{first_filename}_edit.asc"
      if not merge_two_files_s3(bucket_name, current_merged, files[0], files[1]):
            return False
     
      # Iteratively merge remaining files with the most recently generated merged file
      for i in range(2, len(files)):
            next_merged = f"{temp_path}{first_filename}_edit_{i-1}.asc"
            if not merge_two_files_s3(bucket_name, next_merged, current_merged, files[i]):
                  return False
            # Update current_merged to the newly created file for next iteration
            current_merged = next_merged
     
      # Create final output file
      final_output = f"{temp_path}{first_filename}_output.asc"
      log_message('logging', f"Merge function - creating final output: {final_output}")
      try:
            # Copy the final merged content to the output file
            response = s3.get_object(Bucket=bucket_name, Key=current_merged)
            content = response['Body'].read()
            s3.put_object(Bucket=bucket_name, Key=final_output, Body=content)
            log_message('logging', f"Created merged output file: {final_output}")
            return True
      except Exception as e:
            log_message('failed', f"Error creating final output: {str(e)}")
            return False


def add_blank_column_to_s3(bucket_name: str, file_key: str, list_change_value: list, num_blank: int = 0, add_blank: bool = False):
      """Add blank column to S3 file content"""
      log_message('logging', f"Adding blank column to {file_key}")

      # Get the input path from environment variable
      input_path = os.getenv('inputPath')
      if not input_path:
            raise ValueError("inputPath environment variable is not set")
            
      try:
            # Read the source file and get trade dates
            response = s3.get_object(Bucket=bucket_name, Key=file_key)
            content = response['Body'].read().decode('utf-8')
            lines = content.splitlines()
            
            # Get trade dates from RRSU files
            trade_dates = process_files_s3(bucket_name, input_path)
            print("Trade dates extracted:", trade_dates)

            # Format trade date if available
            trade_date_fmt = None
            if trade_dates:
                  # Get the first available trade date
                  trade_date = next(iter(trade_dates.values()))
                  try:
                        # Convert YYYY year to YY format
                        from datetime import datetime
                        dt = datetime.strptime(trade_date, '%m/%d/%Y')
                        trade_date_fmt = dt.strftime('%m/%d/%y')
                        print('----->', trade_date_fmt)
                  except Exception:
                        # If parsing fails, use original value
                        trade_date_fmt = trade_date
            
            # Process the file content
            new_lines = []
            check_to_desc = {check_num: desc for check_num, desc in list_change_value}
            
            for line in lines:
                  raw = line.rstrip('\n')
                  placed = False

                  for check_num, desc in check_to_desc.items():
                        if check_num and check_num in raw:
                              field_text = desc[:num_blank] if desc else ""
                              if raw.endswith(field_text) or raw.endswith('~' + field_text):
                                    new_lines.append(raw)
                              else:
                                    suffix = ' ~'+trade_date_fmt +'~' if not desc else field_text + '~' +trade_date_fmt +'~'
                                    new_line = raw + suffix
                                    new_lines.append(new_line)
                              placed = True
                              break
                        
                  if not placed:
                        new_line = raw + ' ~'+trade_date_fmt +'~'
                        new_lines.append(new_line)

            # Write the updated content back to S3
            updated_content = '\n'.join(new_lines) + '\n'
            s3.put_object(Bucket=bucket_name, Key=file_key, Body=updated_content.encode('utf-8'))
            log_message('success', f"Successfully updated {file_key}")
            
      except Exception as e:
            log_message('failed', f"Error processing file {file_key}: {str(e)}")
            
def s3_join(*parts):
    """Safely join S3 key parts ensuring single slashes"""
    return "/".join(p.strip("/") for p in parts if p)

def copy_files_with_postfix_s3(bucket_name: str, source_keys: list[str], dest_path: str, add_edited: bool = False) -> list[str]:
    """Copy files in S3 with optional postfix"""
    copied_files = []
    for source_key in source_keys:
        try:
            print(f"🔹 Reading from S3: {bucket_name}/{source_key}")
            response = s3.get_object(Bucket=bucket_name, Key=source_key)
            content = response['Body'].read()
            print(f"✅ File read OK: {source_key} (size={len(content)} bytes)")

            # Extract name and extension safely
            filename = source_key.split("/")[-1]
            name, ext = filename.rsplit(".", 1) if "." in filename else (filename, "")
            edited_postfix = os.getenv("editedFilePostfix", "_edited")

            # Add postfix if required
            new_name = name + (edited_postfix if add_edited and edited_postfix not in name else "") + ('.' + ext if ext else '')

            # Build destination key safely
            dest_key = s3_join(dest_path, new_name)
            print(f"📦 Writing to S3: {bucket_name}/{dest_key}")

            # Write to destination
            s3.put_object(Bucket=bucket_name, Key=dest_key, Body=content)
            copied_files.append(dest_key)
            log_message('success', f"Copied {source_key} to {dest_key}")
        except Exception as e:
            log_message('failed', f"Error copying file {source_key}: {str(e)}")
            print(f"❌ Error copying {source_key}: {e}")
            continue

    print(f"✅ Total copied files: {len(copied_files)}")
    return copied_files

def main():
      print("🚀 Starting file processing Glue Job...")
      log_message('logging', "File processing Glue Job started.")
      set_job_params_as_env_vars()
      print(set_job_params_as_env_vars())
      try:
            # Step 1: Initialize configuration from environment variables
            print("📋 Step 1: Initializing configuration from environment variables...")
            bucket_name = os.getenv("bucket")
            input_path = os.getenv("inputPath")
            edited_path = os.getenv("editedFilePath")
            output_path = os.getenv("outputPath")
            input_key_list = json.loads(os.getenv("inputKeyList", "[]"))
            driver_key_list = json.loads(os.getenv("driverKeyList", "[]"))
            config_json = json.loads(os.getenv("configKey", "{}"))
            print(f"✅ Configuration initialized - Bucket: {bucket_name}")

            print("input_path",input_path)
            print("edited_path",edited_path)
            print("output_path",output_path)
            print("input_key_list",input_key_list)
            print("driver_key_list",driver_key_list)
           
            # Step 2: Define source files
            print("📁 Step 2: Defining source files...")
            source_main_files =  [input_path + f for f in input_key_list]
            source_default_inputs =  [input_path + f for f in driver_key_list]
            print(f"✅ Main files defined: {source_main_files}")
            print(f"✅ Driver files defined: {source_default_inputs}")
           
            log_message('logging', f"Processing main files: {source_main_files}")
            log_message('logging', f"Processing driver files: {source_default_inputs}")
           
            # Step 3: Copy main files with _edited postfix
            print("📋 Step 3: Copying main files with _edited postfix...")
            try:
                  print(' bucket_name : ',bucket_name ,"source_main_files : ",source_main_files," edited_path ",edited_path)
                  copied_main_files = copy_files_with_postfix_s3(bucket_name, source_main_files, edited_path, add_edited=True)
                  print(f"✅ Successfully copied {len(copied_main_files)}  main files: {copied_main_files}")
            except Exception as e:
                  print(f"❌ Failed to copy main files: {str(e)}")
                  log_message('failed', f"Error copying main files: {str(e)}")
                  return
           
            # Step 4: Copy default input files with _edited postfix
            print("📋 Step 4: Copying default input files with _edited postfix...")
            try:
                  copied_default_inputs = copy_files_with_postfix_s3(bucket_name, source_default_inputs, edited_path, add_edited=True)
                  print(f"✅ Successfully copied {len(copied_default_inputs)}  default input files: {copied_default_inputs}")
            except Exception as e:
                  print(f"❌ Failed to copy default input files: {str(e)}")
                  log_message('failed', f"Error copying default input files: {str(e)}")
                  return
           
            log_message('logging', f"Copied main files: {copied_main_files}")
            log_message('logging', f"Copied default inputs: {copied_default_inputs}")
           
            # Step 5: Validate file counts match
            print("🔍 Step 5: Validating file counts...")
            if len(copied_main_files) != len(copied_default_inputs):
                  print(f"❌ File count mismatch - Main files: {len(copied_main_files)}, Default inputs: {len(copied_default_inputs)}")
                  log_message('failed', "Number of main files and default input files do not match!")
                  return
            print(f"✅ File counts match - {len(copied_main_files)}  files each")
           
            # Step 6: Process each main file with its corresponding default input
            print("⚙️ Step 6: Processing main files with delivery location data...")
            successful_updates = 0
            for i, main_file in enumerate(copied_main_files):
                  default_input = copied_default_inputs[i]
                  print(f"🔄 Processing file {i+1}/{len(copied_main_files)}: {main_file.split('/')[-1]}")
                 
                  try:
                        # Extract check → delivery pairs from the default input file
                        print(f"    📊 Extracting check-delivery pairs from {default_input.split('/')[-1]}...")
                        pairs = extract_check_delivery_pairs_from_s3(bucket_name, default_input)
                        print(f"    ✅ Found {len(pairs)}  check-delivery pairs")
                       
                        # Append delivery location column to main file only if missing
                        print(f"    📝 Adding delivery location column to {main_file.split('/')[-1]}...")
                        add_blank_column_to_s3(bucket_name, main_file, pairs, num_blank=40, add_blank=False)
                        print(f"    ✅ Successfully updated {main_file.split('/')[-1]}")
                        log_message('success', f"Updated {main_file}  successfully.")
                        successful_updates += 1
                  except Exception as e:
                        print(f"    ❌ Failed to process {main_file.split('/')[-1]}: {str(e)}")
                        log_message('failed', f"Error processing {main_file}: {str(e)}")
                        continue
           
            print(f"✅ Successfully processed {successful_updates}/{len(copied_main_files)}  files")
           
            # Step 7: Load and validate configuration if available
            print("⚙️ Step 7: Loading and validating configuration...")
            # try:
            #       config = load_config_from_s3(bucket_name, os.getenv("configKey"))
            #       print("✅ Configuration loaded successfully")
            #       validate_check_counts_from_config_s3(bucket_name, config)
            #       print("✅ Check counts validated")
            #       validate_trailer_counts_from_config_s3(bucket_name, config)
            #       print("✅ Trailer counts validated")
            # except Exception as e:
            #       print(f"⚠️ Config validation skipped: {str(e)}")
            #       log_message('logging', f"Config validation skipped: {str(e)}")
           
                  # Step 8: Merge MUSD files
            print("🔗 Step 8: Merging MUSD files...")
            if len(copied_main_files) >= 2:
                  print(f"📁 Found {len(copied_main_files)} files to merge")
                  temp_path = edited_path + "temp/"
                  try:
                        print("🔄 Starting file merge process...")
                        if merge_n_files_s3(bucket_name, copied_main_files, temp_path):
                              print("✅ Files merged successfully")
                              # Move the output file to the output directory with correct naming
                              edited_postfix = os.getenv('editedFilePostfix', '_edited')
                              first_filename = copied_main_files[0].split('/')[-1].replace(edited_postfix, '')
                              first_filename = first_filename.split('.')[0]  # Remove extension
                              output_postfix = os.getenv('outputPostfix', '_output')
                              # Use s3_join to properly handle slashes in the path
                                    # Set the final output file name explicitly to MUSD9078_output.asc
                              # output_file = s3_join(output_path.strip('"'), "MUSD9078_output.asc")
                              # output_file = s3_join(output_path.strip('"'), f"{first_filename}{output_postfix}.asc")
                              # Sanitize filename and path by removing all double quotes
                              clean_first_filename = first_filename.replace('"', '')
                              clean_output_path = output_path.replace('"', '')
                              output_postfix=output_postfix.replace('"', '')
                              path_output=clean_first_filename+output_postfix+'.asc'
                              #f"{clean_first_filename}{output_postfix}.asc"
                              # Construct the final output file path safely
                              output_file = s3_join(clean_output_path, path_output)
                              print('----------------------------------------------------<>-------------------------------')
                              print(output_file,clean_first_filename,clean_output_path)
                              print('---------------------------------------------<>------------------------------------')
                              # The merge function creates: {first_filename}_output.asc
                              temp_output_file = temp_path + f"{first_filename}_output.asc"
                              print(f"🔍 Main function - first_filename: {first_filename}")
                              print(f"🔍 Main function - temp_output_file: {temp_output_file}")
                              try:
                                    print(f"📁 Moving merged file to final output: {output_file}")
                                    print(f"🔍 Looking for temporary file: {temp_output_file}")
                                    # Copy temp output to final output
                                    response = s3.get_object(Bucket=bucket_name, Key=temp_output_file)
                                    content = response['Body'].read()
                                            # First write the merged content to the output file
                                    s3.put_object(Bucket=bucket_name, Key=output_file, Body=content)
                                    log_message('success', f"Final content with trade dates written to {output_file}")
                                    # Clean up temporary files
                                    try:
                                          print(f"🧹 Cleaning up temporary file: {temp_output_file}")
                                          s3.delete_object(Bucket=bucket_name, Key=temp_output_file)
                                          print("✅ Temporary file cleaned up successfully")
                                          log_message('logging', f"Cleaned up temporary file: {temp_output_file}")
                                    except Exception as e:
                                          print(f"⚠️ Warning: Error cleaning up temp file: {str(e)}")
                                          log_message('logging', f"Error cleaning up temp file: {str(e)}")
                              except Exception as e:
                                    print(f"❌ CRITICAL ERROR: Failed to move output file: {str(e)}")
                                    log_message('failed', f"CRITICAL ERROR: Failed to move output file: {str(e)}")
                                    print("💥 Job failed due to critical merge output error!")
                                    raise Exception(f"Critical error: Failed to move merged output file - {str(e)}")
                        else:
                              print("❌ CRITICAL ERROR: File merge failed")
                              log_message('failed', "CRITICAL ERROR: File merge failed - missing input files")
                              print("💥 Job failed due to critical merge failure!")
                              raise Exception("Critical error: File merge operation failed - missing input files")
                  except Exception as e:
                        print(f"❌ CRITICAL ERROR during merge process: {str(e)}")
                        log_message('failed', f"CRITICAL ERROR during merge process: {str(e)}")
                        print("💥 Job failed due to critical merge process error!")
                        raise Exception(f"Critical error during merge process: {str(e)}")
            else:
                  print("⚠️ Not enough files to merge (need at least 2)")
                  log_message('failed', "Skipped MUSD merge; missing input files")

            print("🎉 File processing Glue Job completed successfully!")
            log_message('success', "File processing Glue Job completed successfully.")

                  # --- Custom Post-Merge Attachment Logic (S3 compatible) ---
                  # Attach RRSU0224.TXT.asc and RRSU0001.TXT.asc based on matching TRADE DATE using S3 APIs
            
           
      except Exception as e:
            print(f"💥 Critical error in main process: {str(e)}")
            log_message('failed', f"Critical error in main process: {str(e)}")
            raise

def set_job_params_as_env_vars():
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]
            value = sys.argv[i + 1]
            os.environ[key] = value

if __name__ == "__main__":
      main()