import json
import csv
import boto3
import re
from datetime import datetime
from typing import Dict, List, Optional
import tempfile
import os

class SimpleReportUpdater:
    def __init__(self):
        self.s3_client = boto3.client('s3')
    
    def download_s3_file(self, bucket: str, key: str) -> str:
        """Download S3 file to temporary location"""
        try:
            # Check if object exists first
            self.s3_client.head_object(Bucket=bucket, Key=key)
            
            tmp_fd, tmp_path = tempfile.mkstemp(suffix=os.path.basename(key))
            os.close(tmp_fd)
            
            self.s3_client.download_file(bucket, key, tmp_path)
            return tmp_path
        except self.s3_client.exceptions.NoSuchKey:
            raise FileNotFoundError(f"S3 object not found: s3://{bucket}/{key}")
        except Exception as e:
            raise Exception(f"Error downloading {bucket}/{key}: {e}")
    
    def upload_s3_file(self, local_path: str, bucket: str, key: str):
        """Upload file to S3"""
        try:
            self.s3_client.upload_file(local_path, bucket, key)
        except Exception as e:
            raise Exception(f"Error uploading to {bucket}/{key}: {e}")
    
    def extract_csv_value(self, csv_path: str, column_name: str, delimiter: str = "\t") -> Optional[str]:
        """Extract first value from specified CSV column or Excel file"""
        try:
            # Check if it's an Excel file (.xlsx or .xls)
            if csv_path.lower().endswith(('.xlsx', '.xls')):
                import pandas as pd
                
                df = pd.read_excel(csv_path, dtype=str, engine='openpyxl' if csv_path.endswith('.xlsx') else None)
                df.columns = df.columns.str.strip()
                
                if column_name not in df.columns:
                    raise ValueError(f"Column '{column_name}' not found in Excel file. Available columns: {list(df.columns)}")
                
                # Get first non-empty value from the specified column
                for idx in range(len(df)):
                    cell_value = df.iloc[idx][column_name]
                    if pd.notna(cell_value):
                        value_str = str(cell_value).strip()
                        if value_str and value_str.lower() not in ['nan', 'none', '']:
                            return value_str
                
                raise ValueError(f"No non-empty values found in column '{column_name}'")
            
            else:
                # Process as CSV/text file with delimiters
                with open(csv_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    f.seek(0)
                    
                    # Try to detect delimiter automatically
                    sample = content[:1024]
                    tab_count = sample.count('\t')
                    comma_count = sample.count(',')
                    pipe_count = sample.count('|')
                    
                    # Choose best delimiter
                    if tab_count > comma_count and tab_count > pipe_count:
                        delimiter = '\t'
                    elif comma_count > tab_count and comma_count > pipe_count:
                        delimiter = ','
                    elif pipe_count > 0:
                        delimiter = '|'
                    
                    reader = csv.DictReader(f, delimiter=delimiter)
                    
                    # Check for column name variations
                    possible_columns = [column_name]
                    if column_name == 'MAIL_DATE':
                        possible_columns.extend(['Mail_Date', 'mail_date', 'MAIL DATE', 'Mail Date'])
                    
                    found_column = None
                    for col in possible_columns:
                        if col in (reader.fieldnames or []):
                            found_column = col
                            break
                    
                    if not found_column:
                        raise ValueError(f"Column {column_name} not found. Available columns: {reader.fieldnames}")
                    
                    for row in reader:
                        value = (row.get(found_column) or '').strip()
                        if value:
                            return value
                    
                    raise ValueError(f"No value found for {found_column}")
                
        except Exception as e:
            raise Exception(f"Error reading file: {e}")

    def format_date(self, date_str: str, input_format: str = "auto", output_format: str = "%Y%m%d") -> str:
        """Format date string"""
        try:
            if input_format == "auto":
                # Auto-detect date format
                if '/' in date_str:
                    date_part = date_str.split()[0]  # Remove time part
                    if len(date_part.split('/')) == 3:
                        month, day, year = date_part.split('/')
                        return f"{year}{month.zfill(2)}{day.zfill(2)}"
                elif '-' in date_str:
                    date_part = date_str.split()[0]  # Remove time part
                    if len(date_part.split('-')) == 3:
                        month, day, year = date_part.split('-')
                        return f"{year}{month.zfill(2)}{day.zfill(2)}"
                elif len(date_str) == 8 and date_str.isdigit():
                    return date_str
            else:
                # Use specified input format
                dt = datetime.strptime(date_str, input_format)
                return dt.strftime(output_format)
            
            raise ValueError(f"Unable to parse date '{date_str}' with format '{input_format}'")
        except Exception as e:
            raise Exception(f"Error formatting date '{date_str}': {e}")

    def replace_text_in_file(self, input_path: str, output_path: str, replacements: List[Dict]) -> int:
        """Replace text in file based on replacement rules"""
        try:
            replacements_made = 0
            
            # Read entire file content
            with open(input_path, 'rb') as f:
                raw_content = f.read()
            
            # Try to decode as UTF-8
            try:
                content = raw_content.decode('utf-8')
            except UnicodeDecodeError:
                try:
                    content = raw_content.decode('latin-1')
                except:
                    content = raw_content.decode('utf-8', errors='replace')
            
            # Count placeholders before replacement
            initial_placeholder_count = content.count('fulfilled_date_placeholder')
            
            # Check if file appears to already be processed
            if initial_placeholder_count == 0:
                # Check if replacement values already exist
                for replacement in replacements:
                    replace_text = replacement.get('replace_text', '')
                    if replace_text and content.count(replace_text) > 0:
                        # Write the original content to output (no changes needed)
                        with open(output_path, 'w', encoding='utf-8') as f:
                            f.write(content)
                        return content.count(replace_text)
            
            # Apply each replacement rule
            for replacement in replacements:
                find_text = replacement.get('find_text', '')
                replace_text = replacement.get('replace_text', '')
                
                if find_text and replace_text:
                    initial_count = content.count(find_text)
                    
                    if initial_count > 0:
                        content = content.replace(find_text, replace_text)
                        final_count = content.count(find_text)
                        
                        if final_count == 0:
                            replacements_made += initial_count
            
            # Write updated content to output file
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            return replacements_made
            
        except Exception as e:
            raise Exception(f"Error replacing text in file: {e}")

    def extract_filename_components(self, filename: str, csv_patterns: List[str], report_patterns: List[str], report_rundate_format: str = None) -> Dict[str, str]:
        """Extract corp, cycle, and rundate from filename using patterns from config"""
        try:
            components = {}
            # Try CSV patterns (assume YYYYMMDD)
            for pat in csv_patterns:
                m = re.search(pat, filename)
                if m:
                    components['corp'] = m.group(1)
                    components['rundate'] = m.group(2)
                    components['cycle'] = m.group(3)
                    return components
            # Try report patterns
            for pat in report_patterns:
                m = re.search(pat, filename)
                if m:
                    components['corp'] = m.group(1)
                    components['cycle'] = m.group(2)
                    rundate = m.group(3)
                    # Convert MMDDYYYY to YYYYMMDD if specified
                    if report_rundate_format == "MMDDYYYY" and len(rundate) == 8:
                        # MMDDYYYY -> YYYYMMDD
                        rundate = rundate[4:] + rundate[:2].zfill(2) + rundate[2:4].zfill(2)
                    components['rundate'] = rundate
                    return components
            return {}
        except Exception:
            return {}

    def find_matching_s3_file(self, bucket: str, path: str, pattern: str, match_components: Dict[str, str] = None, csv_patterns: List[str] = None, report_patterns: List[str] = None, report_rundate_format: str = None) -> str:
        """Find S3 file matching regex pattern and optionally matching filename components"""
        try:
            # List objects in the specified path
            response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=path)
            
            if 'Contents' not in response:
                raise ValueError(f"No files found in s3://{bucket}/{path}")
            
            # Compile regex pattern
            regex_pattern = re.compile(pattern)
            
            pattern_matches = []
            component_matches = []
            
            for obj in response['Contents']:
                file_key = obj['Key']
                filename = os.path.basename(file_key)
                
                # First check if filename matches the regex pattern
                if regex_pattern.search(filename):
                    pattern_matches.append((file_key, filename))
                    
                    # If we need to match components, check them too
                    if match_components:
                        file_components = self.extract_filename_components(filename, csv_patterns, report_patterns, report_rundate_format)
                        
                        # Check if all required components match
                        components_match = True
                        for key, value in match_components.items():
                            if key not in file_components or file_components[key] != value:
                                components_match = False
                                break
                        
                        if components_match:
                            component_matches.append((file_key, filename, file_components))
            
            # Determine which files to use
            if match_components:
                matching_files = [item[0] for item in component_matches]
                if not matching_files:
                    available_files = [item[1] for item in pattern_matches]
                    # Extract components from all pattern-matching files for debugging
                    debug_info = []
                    for _, filename in pattern_matches:
                        file_components = self.extract_filename_components(filename, csv_patterns, report_patterns, report_rundate_format)
                        debug_info.append(f"{filename}: {file_components}")
                    
                    raise ValueError(f"No files matching pattern '{pattern}' AND components {match_components} found. Files matching pattern only: {available_files}. Component details: {debug_info}")
            else:
                matching_files = [item[0] for item in pattern_matches]
                if not matching_files:
                    available_files = [os.path.basename(obj['Key']) for obj in response['Contents']]
                    raise ValueError(f"No files matching pattern '{pattern}' found. Available files: {available_files}")
            
            if len(matching_files) > 1:
                # Sort by last modified date and take the most recent
                matching_objects = [obj for obj in response['Contents'] if obj['Key'] in matching_files]
                most_recent = max(matching_objects, key=lambda x: x['LastModified'])
                return most_recent['Key']
            else:
                return matching_files[0]
            
        except Exception as e:
            raise Exception(f"Error finding matching file: {e}")

    def process_report_update(self, config: Dict, csv_key: str, transaction_id: str = None):
        """Main processing function with direct CSV key"""
        try:
            # Extract configurations
            bucket = config.get('bucket')
            report_config = config.get('report_file', {})
            output_config = config.get('output_file', {})
            
            # Require CSV patterns from config
            if 'source_file' not in config or 'pattern' not in config['source_file']:
                raise ValueError("source_file.pattern is required in config")
            csv_patterns = config['source_file']['pattern']
            if not isinstance(csv_patterns, list):
                csv_patterns = [csv_patterns]
            # Require report patterns from config
            if 'report_file' not in config or 'pattern' not in config['report_file']:
                raise ValueError("report_file.pattern is required in config")
            report_patterns = config['report_file']['pattern']
            if not isinstance(report_patterns, list):
                report_patterns = [report_patterns]
            # Extract components from the provided CSV filename using config patterns
            csv_filename = os.path.basename(csv_key)
            report_rundate_format = config.get('report_file', {}).get('rundate_format')
            csv_components = self.extract_filename_components(csv_filename, csv_patterns, report_patterns, report_rundate_format)
            if not csv_components:
                raise ValueError(f"Could not extract components from CSV filename: {csv_filename}")
            
            # Find the report file that matches the CSV components
            report_path = report_config.get('path')
            report_pattern = report_config.get('pattern')
            if not report_path or not report_pattern:
                raise ValueError("report_file must have both 'path' and 'pattern'")
            
            # Add transaction ID to search path if provided AND path contains "wip"
            search_path = report_path
            if transaction_id and "wip" in report_path.lower():
                if not search_path.endswith('/'):
                    search_path += '/'
                if transaction_id not in search_path:
                    search_path = f"{search_path.rstrip('/')}/{transaction_id}/"
            
            # Pass patterns to find_matching_s3_file
            report_key = self.find_matching_s3_file(
                bucket, search_path, report_pattern, csv_components, csv_patterns, report_patterns, report_rundate_format
            )
            
            # Get output path configuration
            output_base_path = output_config.get('path', '')
            
            # Add transaction ID to output path ONLY if provided AND output path contains "wip"
            output_path = output_base_path
            if transaction_id and output_base_path and "wip" in output_base_path.lower():
                if not output_path.endswith('/'):
                    output_path += '/'
                if transaction_id not in output_path:
                    output_path = f"{output_path.rstrip('/')}/{transaction_id}/"
            
            if not all([bucket, csv_key, report_key]):
                raise ValueError("bucket, csv_key and report_file configurations are required")
            
            # Extract filename from report_key for output
            report_filename = os.path.basename(report_key)
            
            # Construct output key
            if output_path:
                output_key = f"{output_path.rstrip('/')}/{report_filename}"
            else:
                # If no output path specified, use same location as report
                output_key = report_key
            
            # Download files
            csv_local_path = self.download_s3_file(bucket, csv_key)
            report_local_path = self.download_s3_file(bucket, report_key)
            
            # Process replacements
            csv_config = config.get('csv_config', {})
            delimiter = csv_config.get('delimiter', '\t')
            
            replacement_rules = []
            for replacement in config.get('replacements', []):
                find_text = replacement.get('find_text')
                csv_column = replacement.get('replace_with_csv_column')
                date_format_config = replacement.get('date_format', {})
                
                if find_text and csv_column:
                    csv_value = self.extract_csv_value(csv_local_path, csv_column, delimiter)
                    
                    if not csv_value:
                        raise ValueError(f"Failed to extract value from column '{csv_column}'")
                    
                    # Apply date formatting if specified
                    if date_format_config:
                        input_fmt = date_format_config.get('input_format', 'auto')
                        output_fmt = date_format_config.get('output_format', '%Y%m%d')
                        formatted_value = self.format_date(csv_value, input_fmt, output_fmt)
                    else:
                        formatted_value = csv_value
                    
                    replacement_rules.append({
                        'find_text': find_text,
                        'replace_text': formatted_value
                    })
            
            if not replacement_rules:
                raise ValueError("No valid replacement rules generated from configuration")
            
            # Create temporary output file
            output_local_path = tempfile.mktemp(suffix='_updated.txt')
            
            # Apply replacements
            updates_made = self.replace_text_in_file(report_local_path, output_local_path, replacement_rules)
            
            # Upload updated report to output location
            self.upload_s3_file(output_local_path, bucket, output_key)
            
            # Copy to WIP path if provided and not empty
            wip_output_key = None
            wip_path = config.get('wip_path', '').strip()
            if wip_path and wip_path not in ['', ' ']:
                if not wip_path.endswith('/'):
                    wip_path += '/'
                wip_output_key = f"{wip_path}{report_filename}"
                
                try:
                    self.s3_client.copy_object(
                        Bucket=bucket,
                        CopySource={'Bucket': bucket, 'Key': output_key},
                        Key=wip_output_key
                    )
                except Exception as copy_error:
                    print(f"Warning: Failed to copy to WIP path: {copy_error}")
            
            # Cleanup temporary files
            try:
                os.unlink(csv_local_path)
                os.unlink(report_local_path)
                os.unlink(output_local_path)
            except Exception:
                pass  # Ignore cleanup errors
            
            status_message = "successfully updated" if updates_made > 0 else "already processed"
            
            result = {
                'status': 'success',
                'updates_made': updates_made,
                'csv_key': csv_key,
                'report_key': report_key,
                'output_key': output_key,
                'message': f'Report file {status_message} and saved to s3://{bucket}/{output_key}'
            }
            
            if wip_output_key:
                result['wip_output_key'] = wip_output_key
                result['message'] += f' and copied to s3://{bucket}/{wip_output_key}'
            
            return result
            
        except Exception as e:
            # Cleanup temporary files if they exist
            try:
                if 'csv_local_path' in locals() and os.path.exists(csv_local_path):
                    os.unlink(csv_local_path)
                if 'report_local_path' in locals() and os.path.exists(report_local_path):
                    os.unlink(report_local_path)
                if 'output_local_path' in locals() and os.path.exists(output_local_path):
                    os.unlink(output_local_path)
            except Exception:
                pass
            raise Exception(f"Error in process_report_update: {e}")

    def load_config_from_s3(self, config_bucket: str, config_key: str) -> Dict:
        """Load configuration from S3 JSON file"""
        try:
            # Check if config file exists first
            self.s3_client.head_object(Bucket=config_bucket, Key=config_key)
            
            config_local_path = self.download_s3_file(config_bucket, config_key)
            
            with open(config_local_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # Cleanup temp file
            os.unlink(config_local_path)
            
            return config
            
        except Exception as e:
            raise Exception(f"Error loading configuration from S3: {e}")

def lambda_handler(event, context):
    """AWS Lambda handler function"""
    try:
        updater = SimpleReportUpdater()
        
        config_bucket = event.get('config_bucket')
        transaction_id = event.get("transactionId")
        config_filename = event.get("config_filename")
        config_path = event.get('config_path')
        csv_key = event.get('csv_key')
        wip_path = event.get("wip_path")
        # Validate required parameters
        if not config_bucket:
            raise ValueError("config_bucket is required in event")
        if not config_filename:
            raise ValueError("config_filename is required in event")
        if not config_path:
            raise ValueError("config_path is required in event")
        
        config_key = f"{config_path.rstrip('/')}/{config_filename}"
        
        # Load configuration from S3
        config = updater.load_config_from_s3(config_bucket, config_key)
        
        # Add wip_path to config if provided in event
        if wip_path:
            config['wip_path'] = wip_path
        
        # Process the report update with provided CSV key and transaction ID
        result = updater.process_report_update(config, csv_key, transaction_id)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Report updated successfully',
                'result': result,
                'transaction_id': transaction_id
            })
        }
    except Exception as e:
        raise 

# Example usage for testing
if __name__ == "__main__":
    payload = {
        "detail": {
            "bucket": {
                "name": "config_bucket"
            },
            "object": {
                "key": "jhi/confirms/config/lambda_config_s3.json"
            }
        }
    }
    
    result = lambda_handler(payload, None)
    print(f"Result: {json.dumps(result, indent=2)}")
