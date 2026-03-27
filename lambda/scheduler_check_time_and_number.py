import boto3
import json
import re
import calendar
import os

from datetime import datetime, timedelta, timezone
from dpm_splunk_logger_py import splunk
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import NoCredentialsError, ClientError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

# Initialize AWS clients at module level
dynamodb = boto3.resource('dynamodb')
ses_client = boto3.client('ses')
s3_client = boto3.client('s3')

s3 = boto3.client('s3')
lambda_client = boto3.client('lambda')
timestamp=''
def lambda_handler(event, context):
    print("*****************************************")
    est_time = convert_to_est(datetime.utcnow())
    current_hour = est_time.hour

    for e in event['detail']:
        bucket_name = e['bucket']['name']
        config_key = e['object']['key']
    
        try:
            config_json, config_bucket_name, lambda_function_name = get_config(bucket_name, config_key, context)
            config_bucket_name = config_bucket_name.replace("{env}",os.getenv('SDLC_ENV'))
            lambda_function_name = lambda_function_name.replace("{env}",os.getenv('SDLC_ENV'))
            for config in config_json:
                if 'UR_BATCHING' in config['functionalityName']:
                    print(f"running UR_BATCHING")
                    try:
                        config_data=config['functionalityToDo']['object']
                        for config in config_data:
                            path = config.get('path')
                            if not path:
                                # print("No path specified in the configuration.")
                                continue

                            number_of_files = config['numberofFile']
                            xml_regex = config['xmlRegex']
                            interim_sweep_duration = int(config['interimSweepduration'])
                            final_sweep_hours = [int(hour) for hour in config['finalSweepHour']]

                            # Call count_matching_files once and store results
                            count, keyList, valueList = count_matching_files(bucket_name, path, xml_regex)
                            
                            if current_hour in final_sweep_hours or len(keyList) >= number_of_files:
                                handle_cleanup(config_bucket_name, path, lambda_function_name, keyList, valueList,context)
                            elif current_hour % interim_sweep_duration == 0:
                                handle_hourly_check(config_bucket_name, path, xml_regex, number_of_files, lambda_function_name, keyList, valueList, context)
                    except Exception as e:
                        message = f"Error processing UR_BATCHING functionality: {e}"
                        splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)
                        # raise ValueError(message)
                    
                elif 'CALC_BATCHING' in config['functionalityName']:
                    print(f"running CALC_BATCHING")
                    try:
                        config_data=config['functionalityToDo']['object']
                        for config in config_data:
                            
                            # Validate and process done files
                            process_done_files(config_bucket_name, config, lambda_function_name,context)
                    except Exception as e:
                        message = f"Error processing CALC_BATCHING functionality: {e}"
                        splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)
                        # raise ValueError(message)

                elif 'FILE_MONITORING' in config['functionalityName']:
                    print(f"running FILE_MONITORING")
                    try:
                        config_data=config['functionalityToDo']['object']
                        for config in config_data:
                            
                            # Validate and process done files
                            file_monitor(config_bucket_name, config, lambda_function_name,context)
                    except Exception as e:
                        message = f"Error processing FILE_MONITORING functionality: {e}"
                        splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)
                        # raise ValueError(message)
                elif 'CRON_BASED_FILE_TRIGGER' in config['functionalityName']:
                    print(f"running CRON_BASED_FILE_TRIGGER")
                    try:
                        config_data=config['functionalityToDo']['object']
                        for config in config_data:
                            
                            # Validate and process done files
                            cron_based_file_trigger(config_bucket_name, config, lambda_function_name,context)
                    except Exception as e:
                        message = f"Error processing CRON_BASED_FILE_TRIGGER functionality: {e}"
                        splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)
                        # raise ValueError(message)
                

        except Exception as e:
            message=(f"Error getting config: {e}")
            splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)
            # raise ValueError(message)
    print("*****************************************")


def get_config(bucket_name, config_key, context):
    try:
        response = s3.get_object(Bucket=bucket_name, Key=config_key)
        config_content = response['Body'].read().decode('utf-8')
        config_json = json.loads(config_content)
        config_bucket_name = config_json['bucket']
        lambda_function_name = config_json['lambdaToCall']
        config_json=config_json['cronProcess']
        return config_json, config_bucket_name, lambda_function_name
    except Exception as e:
        message =f"Error retrieving S3 object: {e}"
        splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)
        raise ValueError(message)

def handle_hourly_check(config_bucket_name, path, xml_regex, number_of_files, lambda_function_name, keyList, valueList, context):
    try:
        if len(keyList) != 0:
            if len(keyList) >= number_of_files:
                handle_cleanup(config_bucket_name, path, lambda_function_name, keyList, valueList, context)
    except Exception as e:
        message = f"Error during hourly check: {e}"
        splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)
        raise ValueError(message)

def handle_cleanup(bucket_name, path, lambda_function_name, keyList, valueList,context):
    try:
        if len(keyList) != 0:
            fileCreated,manifest_file_name = manifestFileCreation(bucket_name, path, keyList, valueList, context)
            if fileCreated:
                invoke_lambda(bucket_name, "/".join(path.split("/")[:2]) + "/", lambda_function_name, manifest_file_name, context)
    except Exception as e:
        message = f"Error during cleanup: {e}"
        splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)
        raise ValueError(message)

def count_matching_files(bucket_name, path, xml_regex):
    keyList = []
    valueList = []
    try:
        paginator = s3.get_paginator('list_objects_v2')
        regex_pattern = re.compile(xml_regex)
        count = 0
        for page in paginator.paginate(Bucket=bucket_name, Prefix=path):
            if 'Contents' not in page:
                continue
            for obj in page['Contents']:
                count += 1
                filename = obj['Key'].split('/')[-1]
                if regex_pattern.search(filename):
                    keyList.append(filename)
                else:
                    if filename:
                        valueList.append(filename)
        
        return len(keyList), keyList, valueList
    except Exception as e:
        # print(f"Error counting matching files: {e}")
        return 0, keyList, valueList

def manifestFileCreation(bucket_name, path, keyList, valueList, context):
    manifest_data = {}
    pathList=path.rsplit('/',2)
    for value in valueList:
        base_name = value.rsplit('.', 1)[0]
        matching_key = next((key for key in keyList if base_name in key), None)
        if matching_key:
            manifest_data[matching_key] = value

    global timestamp
    timestamp = convert_to_est(datetime.utcnow()).strftime("%m%d%Y%H%M%S")
    manifest_file_name = f"{pathList[1]}.manifest.{timestamp}.json"
    manifest_file_name = manifest_file_name.replace(" ", "_")
    manifest_file_key = f"{path}{manifest_file_name}"
    fileCreated=False
    
    
    try:
        if manifest_data:  # Only proceed if manifest_data is non-empty
            s3.put_object(Bucket=bucket_name, Key=manifest_file_key, Body=json.dumps(manifest_data))
            fileCreated=True
            # print(f"Manifest file created at: {manifest_file_key}")
        else:
            # print("manifest_data is empty, skipping manifest file creation.")
            message="manifest_data is empty, skipping manifest file creation."
            splunk.log_message({'Message': message, "FileName": "cron", "Status": "logging"}, context)
            
    except Exception as e:
        message = f"Error creating manifest file: {e}"
        splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)
        raise ValueError(message)
        
    # Destination bucket and key (where you want to copy the JSON file)    
    
    try:
        if fileCreated:
            destination_key=f"{pathList[0]}/{manifest_file_name}"
            # Copy the file
            copy_source = {
                'Bucket': bucket_name,
                'Key': manifest_file_key
            }
            s3.copy(copy_source, bucket_name, destination_key)
            # print(f'destination_key : {destination_key}')
    except Exception as e:
        message = f"Error occurred while copying the file: {str(e)}"
        splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)
        raise ValueError(message)
    return fileCreated,manifest_file_name

def invoke_lambda(bucket_name, path, lambda_function_name,manifest_file_name, context):
    try:
        payload = {
            "detail": {
                "bucket": {
                    "name": bucket_name
                },
                "object": {
                    "key": f"{path}{manifest_file_name}"
                }
            }
        }
        
        lambda_client.invoke(
            FunctionName=lambda_function_name,
            InvocationType='Event',
            Payload=json.dumps(payload)
        )
        message=f'cron job triggered {path}{manifest_file_name} sucessfully'
        splunk.log_message({'Message': message, "FileName": "cron", "Status": "sucess"}, context)
    except Exception as e:
        message=f"Error invoking Lambda: {e}"
        splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)
        raise ValueError(message)

def convert_to_est(utc_dt):
    est_offset = timedelta(hours=-5)
    est_time = utc_dt.replace(tzinfo=timezone.utc) + est_offset
    return est_time

def process_done_files(bucket, config, lambda_function_name, context):
    """Validate done files and trigger Lambda if all report files are present."""
    
    # Fetch matching done files
    all_done_files = get_s3_file_list(bucket, config["done_file_path"], config["done_file_regex"])
    for done_file in all_done_files:
        #print(f"Processing done file: {done_file}")
        try:
            validate_and_trigger_lambda(bucket, done_file, config, lambda_function_name,context)
        except Exception as e:
            message = f"Error processing done file {done_file}: {e}"
            splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)

def get_s3_file_list(bucket, path, regex):
    """Get a list of files in an S3 prefix with a specific suffix using paginator."""
    file_list = []
    pattern = re.compile(regex)
    try:
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=path):
            if 'Contents' not in page:
                continue
            for obj in page['Contents']:
                if pattern.search(obj['Key'].split('/')[-1]):
                    file_list.append(obj['Key'])
    except ClientError as e:
        error_message = f"Error fetching files from S3: {e}"
        raise Exception(error_message)
    except Exception as e:
        raise e
        
    return file_list

def validate_and_trigger_lambda(bucket, done_file_key, config, lambda_function_name, context):
    """Validate report files and trigger the associated Lambda."""
    try:
        # Fetch and parse the done file
        response = s3.get_object(Bucket=bucket, Key=done_file_key)
        done_data = json.loads(response['Body'].read().decode('utf-8'))

        # Get the list of report files from the specified path
        all_report_files = get_s3_file_list(bucket, config["batch_file_path"], config["batch_file_regex"])
        #print(f"All report files found in path '{batch_file_path}': {all_report_files}")

        # Extract just the file names from the keys for easier comparison
        all_report_file_names = [os.path.basename(file) for file in all_report_files]
        #print(f"Report file names extracted: {all_report_file_names}")

        missing_files_tracker = {}  # Tracker for missing files

        for record_key, records in done_data.items():
            missing_files = []
            for record in records["files"]:
                # Extract reportFile from each record
                report_file = record.get(config["batch_config_key"])
                if not report_file:
                    continue

                #print(f"Checking presence of report file: {report_file}")

                # Validate the presence of the report file
                if report_file not in all_report_file_names:
                    #print(f"Report file missing: {report_file}")
                    missing_files.append(report_file)
                else:
                    pass    # No action needed
                    #print(f"Report file found: {report_file}")

            if missing_files:
                # Add '_missing_files' to the done_file_key for the tracker
                missing_files_tracker[f"{done_file_key}_missing_files"] = missing_files

        if missing_files_tracker:
            #print(f"Missing files for {done_file_key}: {missing_files_tracker}")
            handle_missing_files(bucket, done_file_key, missing_files_tracker,config)
        else:
            #print(f"All report files are present for {done_file_key}. Triggering Lambda...")
            hold_file_key = f"{config['done_destination_path']}{os.path.basename(done_file_key)}"
        
            # Copy the done file to hold directory
            s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': done_file_key}, Key=hold_file_key)
            print(f"Copied {done_file_key} to {hold_file_key}")
            path, filename = hold_file_key.rsplit("/", 1)
            path = path + "/"
            invoke_lambda(bucket, path, lambda_function_name,filename, context)

    except ClientError as e:
        error_message=f"Error fetching or processing done file {done_file_key}: {e}"
        raise Exception(error_message)
    except json.JSONDecodeError as e:
        error_message=f"Error decoding JSON from done file {done_file_key}: {e}"
        raise Exception(error_message)
    except Exception as e:
        raise e

def handle_missing_files(bucket, done_file_key, missing_files_tracker,config):
    """Handle missing files by creating a tracker and moving the done file if necessary."""
    # Extract the directory path from the done file key
    done_file_dir = os.path.dirname(done_file_key)
    missing_files_key = f"{os.path.splitext(os.path.basename(done_file_key))[0]}.missing_report.json"

    try:
        # Save missing files tracker to the same directory as the done file
        missing_files_tracker_content = {os.path.basename(done_file_key): missing_files_tracker.get(f"{done_file_key}_missing_files", [])}
        missing_files_path = f"{done_file_dir}/{missing_files_key}"
        
        s3.put_object(Bucket=bucket, Key=missing_files_path, Body=json.dumps(missing_files_tracker_content))
        #print(f"Missing files tracker saved to: {missing_files_path}")

        # Check if the missing report file has been there for more than 24 hours
        check_for_done_file_threshold_hours(bucket, missing_files_path, done_file_key,config)

    except ClientError as e:
        error_message=f"Error saving missing files tracker: {e}"
        raise Exception(error_message)
    except ClientError as e:
        error_message=f"Error saving missing files tracker: {e}"
        raise Exception(error_message)
    except Exception as e:
        raise e

def check_for_done_file_threshold_hours(bucket,missing_files_path, done_file_key,config):
    """Check if the done file was modified more than 24 hours ago."""
    try:
        # Get the metadata of the done file
        response = s3.head_object(Bucket=bucket, Key=done_file_key)
        last_modified = response['LastModified']
        
        # Ensure that current time is in the same timezone as last_modified
        current_time = datetime.now(last_modified.tzinfo)

        # Check if 24 hrs have passed since the done file was last modified
        expiry_time = last_modified + timedelta(hours=config["threshold_hours"])

        if current_time > expiry_time:
            #print(f"24 hours passed for {done_file_key}. Moving to failed directory.")
            move_to_failed_directory(bucket, done_file_key, missing_files_path)

    except ClientError as e:
        error_message=f"Error checking done file timestamp for {done_file_key}: {e}"
        raise Exception(error_message)
    except Exception as e:
        raise e

def move_to_failed_directory(bucket, done_file_key, missing_files_key=None):
    """Move the done file and missing files tracker to the failed directory."""
    try:
        FAILED_DIRECTORY=done_file_key.split('/')[0]+'/failed/'
        # Move the done file to the failed directory
        failed_done_file_key = f"{FAILED_DIRECTORY}{os.path.basename(done_file_key)}"
        s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': done_file_key}, Key=failed_done_file_key)
        s3.delete_object(Bucket=bucket, Key=done_file_key)

        if missing_files_key:
            #print("missing_files_key",missing_files_key)
            # Move the missing files tracker to the failed directory (same as done file)
            failed_missing_files_key = f"{FAILED_DIRECTORY}{os.path.basename(missing_files_key)}"
            #print("os.path.basename(missing_files_key",os.path.basename(missing_files_key))
            #print("failed_missing_files_key",failed_missing_files_key)
            s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': missing_files_key}, Key=failed_missing_files_key)
            s3.delete_object(Bucket=bucket, Key=missing_files_key)

        #print(f"Moved {done_file_key} and {missing_files_key if missing_files_key else ''} to failed directory.")

    except ClientError as e:
        error_message=f"Error moving files to failed directory: {e}"
        raise Exception(error_message)
    except Exception as e:
        raise e



def is_business_day(date):
    """Check if given date is a business day (Mon–Fri)."""
    return date.weekday() < 5

def get_nth_business_day_of_month(year, month, n, context):
    """Return the date of the nth business day of a given month."""
    try:
        
        if n <= 0:
            raise ValueError(f"n must be positive, got {n}")
            
        count = 0
        for day in range(1, 32):
            try:
                date = datetime(year, month, day)
            except ValueError:
                break
            if is_business_day(date):
                count += 1
                if count == n:
                    return date.date()
        return None
    except Exception as e:
        message=f"Error in get_nth_business_day_of_month: {str(e)}"
        splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)
        return None

def get_last_business_day_of_month(now, context):
    """Return the last business day of a given month."""
    try:
        if not isinstance(now, datetime):
            raise TypeError(f"Expected datetime object, got {type(now)}")
            
        if now.date().strftime('%d') == "01":
            now=now -timedelta(days=1)    
        year = now.year
        month = now.month
        last_day = calendar.monthrange(year, month)[1]
        
        for d in range(last_day, 0, -1):
            try:
                date = datetime(year, month, d)
                if is_business_day(date):
                    return date.date()
            except ValueError as ve:
                message = f"Error creating date for {year}-{month}-{d}: {ve}"
                splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)
                continue
        
        # If no business day found (shouldn't happen), return None
        message = f"Warning: No business day found in month {month}/{year}"
        splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)
        return None
        
    except Exception as e:
        message = f"Error in get_last_business_day_of_month: {str(e)}"
        splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)
        return None

def send_email_with_attachment(
    sender_email,
    recipient_emails,
    cc_emails,
    subject,
    body_text,
    attachment_file_path,
    bucket_name,
    object_key,
    attachment_flag,
    report_attachment_flag="",
    context=None
):
    """Send email with attachment using SES."""
    try:
        # Validate input parameters
        if not sender_email or not recipient_emails:
            raise ValueError("sender_email and recipient_emails are required")
        
        if not isinstance(recipient_emails, list):
            raise TypeError("recipient_emails must be a list")
            
        # Create an SES client
        ses_client = boto3.client('ses')
        message = "Preparing email with attachment..."
        splunk.log_message({'Message': message, "FileName": "cron", "Status": "success"}, context)

        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = sender_email
        msg['To'] = ', '.join(recipient_emails)
        msg['Cc'] = ', '.join(cc_emails) if cc_emails else None

        # Attach body text
        msg.attach(MIMEText(body_text, 'plain'))

        # Handle attachment if flag is True
        if attachment_flag:
            if object_key:
                try:
                    # Download from S3
                    download_file_from_s3(bucket_name, object_key, attachment_file_path, context)

                    # Correctly extract full file name with extension
                    if report_attachment_flag:
                        file_name = object_key.split('/')[-1]
                    else:
                        file_name = object_key.split('/')[-1].split('.')[-2]

                    # Attach the file
                    with open(attachment_file_path, 'rb') as file:
                        part = MIMEApplication(file.read(), Name=file_name)
                        part['Content-Disposition'] = f'attachment; filename="{file_name}"'
                        msg.attach(part)
                except FileNotFoundError:
                    message = f"[ERROR] Attachment file not found: {attachment_file_path}"
                    splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)
                    return "Failed to attach file - file not found"
                except Exception as attach_error:
                    message = f"[ERROR] Failed to attach file: {str(attach_error)}"
                    splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)
                    return f"Failed to attach file: {str(attach_error)}"
            else:
                # Log a warning but continue sending email
                message = "[WARNING] Attachment flag is True but no object_key was provided — sending without attachment"
                splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)

        # Send the email
        try:
            response = ses_client.send_raw_email(
                Source=sender_email,
                Destinations=recipient_emails + (cc_emails if cc_emails else []),
                RawMessage={'Data': msg.as_string()}
            )
            message = f"Email sent successfully! Message ID: {response['MessageId']}"
            splunk.log_message({'Message': message, "FileName": "cron", "Status": "success"}, context)
            return f"Email sent! Message ID: {response['MessageId']}"
        except NoCredentialsError:
            error_msg = "AWS credentials not available"
            splunk.log_message({'Message': f"[ERROR] {error_msg}", "FileName": "cron", "Status": "failed"}, context)
            return error_msg
        except ClientError as ce:
            error_msg = f"AWS SES error: {str(ce)}"
            splunk.log_message({'Message': f"[ERROR] {error_msg}", "FileName": "cron", "Status": "failed"}, context)
            return error_msg
            
    except Exception as e:
        error_msg = f"Error in send_email_with_attachment: {str(e)}"
        splunk.log_message({'Message': f"[ERROR] {error_msg}", "FileName": "cron", "Status": "failed"}, context)
        return error_msg

def download_file_from_s3(bucket_name, object_key, destination_path, context):
    """Download file from S3 with error handling."""
    try:
        if not all([bucket_name, object_key, destination_path]):
            raise ValueError("bucket_name, object_key, and destination_path are required")
            
        s3_client.download_file(bucket_name, object_key, destination_path)
        message = f"File downloaded from S3 bucket: {object_key}"
        splunk.log_message({'Message': message, "FileName": "cron", "Status": "success"}, context)
        return True
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchBucket':
            message = f"[ERROR] S3 bucket does not exist: {bucket_name}"
            splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)
        elif error_code == 'NoSuchKey':
            message = f"[ERROR] S3 object does not exist: {object_key}"
            splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)
        elif error_code == 'AccessDenied':
            message = f"[ERROR] Access denied to S3 object: {object_key}"
            splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)
        else:
            message = f"[ERROR] S3 ClientError: {str(e)}"
            splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)
        return False
    except FileNotFoundError:
        message = f"[ERROR] Destination directory does not exist: {destination_path}"
        splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)
        return False
    except Exception as e:
        message = f"[ERROR] Failed to download file from S3: {str(e)}"
        splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)
        return False

def check_files_received(now, expected_filename_prefix, table, current_date, emailConfig, context):
    """Check for files received with comprehensive error handling."""
    try:
        message = "Checking for received files..."
        print(message)
        print(f"current_date : {current_date}")
        splunk.log_message({'Message': message, "FileName": "cron", "Status": "success"}, context)
        
        # Validate input parameters
        if not all([now, expected_filename_prefix, table, current_date]):
            raise ValueError("Missing required parameters for file checking")
        
        if not isinstance(now, datetime):
            raise TypeError(f"Expected datetime object for 'now', got {type(now)}")

        # Add timeout and pagination for DynamoDB scan
        try:
            current_time_est = now
            # Scan for files with prefix and today's date, also check timestamp if available
            response = table.scan(
                FilterExpression=Attr('fileName').contains(expected_filename_prefix)
            )
            message = f"DynamoDB scan completed. Items found: {response.get('Count', 0)}"
            splunk.log_message({'Message': message, "FileName": "cron", "Status": "success"}, context)
            
        except ClientError as ce:
            error_msg = f"DynamoDB ClientError: {str(ce)}"
            print(error_msg)
            splunk.log_message({'Message': f"[ERROR] {error_msg}", "FileName": "cron", "Status": "failed"}, context)
            raise ce
        except Exception as db_error:
            error_msg = f"DynamoDB scan failed: {str(db_error)}"
            print(error_msg)
            splunk.log_message({'Message': f"[ERROR] {error_msg}", "FileName": "cron", "Status": "failed"}, context)
            raise db_error
        
        # Check if any files received from 00:00 AM today till now
        files_received_for_date = []
        files_received_today = []
        
        try:
            current_date_str = current_date.strftime('%Y-%m-%d')
            message = f"Looking for files with date: {current_date_str}"
            splunk.log_message({'Message': message, "FileName": "cron", "Status": "success"}, context)
            print(message)
            
            for item in response.get('Items', []):
                try:
                    file_name = item.get('fileName', '')
                    start_date_time = item.get('startDateTime', '')
                    
                    # Check if filename matches pattern for today and startDateTime matches current date
                    start_date_matches = start_date_time.startswith(current_date_str) if start_date_time else False
                    
                    if expected_filename_prefix in file_name and start_date_matches:
                        files_received_for_date.append(item)
                       
                        try:
                            files_received_today.append(item)
                            message = f"  ✅ File added to today's list: {file_name}"
                            splunk.log_message({'Message': message, "FileName": "cron", "Status": "success"}, context)
                           
                        except (ValueError, TypeError) as ts_error:
                            # If timestamp parsing fails, still count the file as received today based on filename
                            files_received_today.append(item)
                            message = f"[WARNING] Found file with invalid timestamp: {file_name}, error: {str(ts_error)}"
                            splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)
                            print(message)
                        except Exception as file_error:
                            message = f"[ERROR] Error processing file {file_name}: {str(file_error)}"
                            splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)
                            print(file_error)
                            
                except Exception as item_error:
                    message = f"[ERROR] Error processing DynamoDB item: {str(item_error)}"
                    splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)
                    print(message)
                    continue

            print(f"files_received_today: {files_received_today}")
        except Exception as processing_error:
            message = f"[ERROR] Error processing DynamoDB response: {str(processing_error)}"
            splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)
            print(message)
            return []

        # List down the file names
        try:
            received_file_names = [item.get('fileName', '') for item in files_received_today]
            for name in received_file_names:
                message = f"  • {name}"
                splunk.log_message({'Message': message, "FileName": "cron", "Status": "success"}, context)

        except Exception as summary_error:
            message = f"[ERROR] Error creating file summary: {str(summary_error)}"
            splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)
            print(message)            
            received_file_names = []
        
        # Handle email notification if no files found
        try:
            if not received_file_names:
                message = "⚠️  No files received today - checking email configuration..."
                splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)
                
                if emailConfig:
                    message = "📧 Sending notification email..."
                    splunk.log_message({'Message': message, "FileName": "cron", "Status": "success"}, context)

                    sender_email= emailConfig.get("sender_email", "dpm_no_replay@broadridge.com")
                    recipient_emails=emailConfig.get("recipient_emails")
                    cc_emails=emailConfig.get("cc_emails")
                    subject=emailConfig.get("subject").replace("{expected_filename_prefix}",expected_filename_prefix).replace("{current_date}",current_date_str)
                    print(f"subject :{subject}")
                    body_text=emailConfig.get("body_text").replace("{expected_filename_prefix}",expected_filename_prefix).replace("{current_date}",current_date_str)
                    print(f"body_text :{body_text}")
                    attachment_file_path="/tmp/downloaded_attachment"
                    bucket_name=emailConfig.get("cc_emails")
                    object_key=emailConfig.get("object_key")
                    attachment_flag = emailConfig.get("attachment_flag")
                    report_attachment_flag=emailConfig.get("report_attachment_flag")

                    email_response = send_email_with_attachment(
                        sender_email, recipient_emails, cc_emails, subject, body_text,
                        attachment_file_path, bucket_name, object_key,
                        attachment_flag, report_attachment_flag, context
                    )
                    message = f"📧 Email response: {email_response}"
                    splunk.log_message({'Message': message, "FileName": "cron", "Status": "success"}, context)
                else:
                    message = "📧 Email notification disabled or not configured"
                    splunk.log_message({'Message': message, "FileName": "cron", "Status": "success"}, context)
            else:
                message = "✅ Files received today, no notification email needed."
                splunk.log_message({'Message': message, "FileName": "cron", "Status": "success"}, context)
                print(message)
                
        except Exception as email_error:
            message = f"[ERROR] Error in email notification process: {str(email_error)}"
            splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)
            print(message)
        
        return received_file_names
        
    except Exception as e:
        error_msg = f"Critical error in check_files_received: {str(e)}"
        splunk.log_message({'Message': f"[ERROR] {error_msg}", "FileName": "cron", "Status": "failed"}, context)
        print(error_msg)
        return []

def file_monitor(config_bucket_name, config, lambda_function_name, context):
    """Main file monitoring function with comprehensive error handling."""
    try:
        
        message = "🚀 Starting File Monitoring Lambda"
        splunk.log_message({'Message': message, "FileName": "cron", "Status": "success"}, context)
        print(f"file_monitor.......")
        
        # Check AWS clients initialization
        if dynamodb is None or ses_client is None or s3_client is None:
            error_msg = "AWS clients not properly initialized"
            splunk.log_message({'Message': f"[ERROR] {error_msg}", "FileName": "cron", "Status": "failed"}, context)
            return {"statusCode": 500, "error": error_msg}
                
        # Configuration with error handling
        try:
            timezoneName=config.get("timezoneName", "America/New_York")
            expected_filename_prefix=config.get("expected_filename_prefix")
            # expected_filename_prefix="JSC.SUPPPP."
            table_name=config.get("table_name").replace("{env}",os.getenv('SDLC_ENV'))
            
        except Exception as config_error:
            message = f"[ERROR] Configuration error: {str(config_error)}"
            splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)
            return {"statusCode": 500, "error": "Configuration error"}
        
        # Initialize AWS resources with error handling
        try:
            table = dynamodb.Table(table_name)
            message = f"✅ DynamoDB table connection established: {table_name}"
            splunk.log_message({'Message': message, "FileName": "cron", "Status": "success"}, context)
        except Exception as table_error:
            message = f"[ERROR] Failed to connect to DynamoDB table: {str(table_error)}"
            splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)
            return {"statusCode": 500, "error": "Database connection failed"}
        
        # Date and time calculations with error handling
        try:
            # now = datetime.now(ZoneInfo(timezoneName))+ timedelta(days=11)
            now = datetime.now(ZoneInfo(timezoneName))
            print(f"Now : {now}")
            current_date = now.date()
            current_time = now.strftime("%H:%M")
            current_hour = now.strftime("%H")
            current_day = now.strftime("%A") 
            
        except Exception as time_error:
            message = f"[ERROR] Time calculation error: {str(time_error)}"
            splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)
            return {"statusCode": 500, "error": "Time calculation failed"}
        
        print(f"size config: {len(config)}")
        for file_monitoring_config in config["file_monitoring_config"]:
            fileMonitoringCode=file_monitoring_config.get("file_monitoring_code")
            print(f"#### file_monitoring_code for loop:{fileMonitoringCode }")

            # Business day calculations with error handling
            try:     
                last_business_day = get_last_business_day_of_month(now, context)
            except Exception as business_day_error:
                message = f"[ERROR] Business day calculation error: {str(business_day_error)}"
                splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)
               
            # File monitoring logic with error handling
            received_file_names = []
            
            try:
                if fileMonitoringCode == "D1":
                    day_range=file_monitoring_config.get("day_range")
                    fileMonitoringHour=file_monitoring_config.get("fileMonitoringHour","23")
                    emailConfig=file_monitoring_config.get("emailConfig")   
                    print(f"fileMonitoringCode : {fileMonitoringCode}, day_range: {day_range}, fileMonitoringHour: {fileMonitoringHour}, current_day : {current_day}")  
                    
                    if current_hour == fileMonitoringHour and current_day in day_range:
                        message = f"✅ D1 trigger: Daily monitoring at {fileMonitoringHour}:00 on {current_day}"
                        print(f"message : {message}")
                        splunk.log_message({'Message': message, "FileName": "cron", "Status": "success"}, context)
                        received_file_names = check_files_received(now, expected_filename_prefix, table, current_date, emailConfig, context)
                
                elif fileMonitoringCode == "NW1":
                    nth_business_day_of_week=file_monitoring_config.get("nth_business_day_of_week")
                    fileMonitoringHour=file_monitoring_config.get("fileMonitoringHour","23")
                    emailConfig=file_monitoring_config.get("emailConfig")
                    
                    nth_business_day_of_week_var = None
                    # For simplicity, assume Monday is week start
                    monday_of_week = now - timedelta(days=now.weekday())
                    business_days = [monday_of_week + timedelta(days=i) for i in range(7) if is_business_day(monday_of_week + timedelta(days=i))]
                    if len(business_days) >= nth_business_day_of_week:
                        nth_business_day_of_week_var = business_days[nth_business_day_of_week - 1].date()
                        message = f"  • {nth_business_day_of_week}th business day of week: {nth_business_day_of_week_var}"
                        splunk.log_message({'Message': message, "FileName": "cron", "Status": "success"}, context)
                        print(message)
                    else:
                        message=f"[WARNING] Not enough business days in week for {nth_business_day_of_week}th business day"
                        splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)
                        raise ValueError(message)
                    print(f"fileMonitoringCode : {fileMonitoringCode}, nth_business_day_of_week: {nth_business_day_of_week}, fileMonitoringHour: {fileMonitoringHour}, nth_business_day_of_week_var : {nth_business_day_of_week_var}")
                    if current_hour == fileMonitoringHour and current_date == nth_business_day_of_week_var:
                        message = f"✅ NW1 trigger: {nth_business_day_of_week}th business day of week monitoring"
                        print(f"message : {message}")
                        splunk.log_message({'Message': message, "FileName": "cron", "Status": "success"}, context)
                        received_file_names = check_files_received(now, expected_filename_prefix, table, current_date, emailConfig, context)
                        
                elif fileMonitoringCode == "NM1" :
                    nth_business_day_of_month=file_monitoring_config.get("nth_business_day_of_month")
                    fileMonitoringHour=file_monitoring_config.get("fileMonitoringHour","23")
                    emailConfig=file_monitoring_config.get("emailConfig")
                    
                    nth_business_day_of_month_var = get_nth_business_day_of_month(now.year, now.month, nth_business_day_of_month, context)
                    print(f"fileMonitoringCode : {fileMonitoringCode}   , nth_business_day_of_month: {nth_business_day_of_month}, fileMonitoringHour: {fileMonitoringHour}, nth_business_day_of_month_var: {nth_business_day_of_month_var}")
                    
                    if current_hour == fileMonitoringHour and current_date == nth_business_day_of_month_var:
                        message = f"✅ NM1 trigger: {nth_business_day_of_month}th business day of month monitoring"
                        print(f"message : {message}")
                        splunk.log_message({'Message': message, "FileName": "cron", "Status": "success"}, context)
                        received_file_names = check_files_received(now, expected_filename_prefix, table, current_date, emailConfig, context)
                
                elif fileMonitoringCode == "L1" :
                    fileMonitoringHour=file_monitoring_config.get("fileMonitoringHour","23")
                    emailConfig=file_monitoring_config.get("emailConfig") 
                    print(f"fileMonitoringCode : {fileMonitoringCode} , fileMonitoringHour: {fileMonitoringHour}, last_business_day : {last_business_day}")
                    if current_hour == fileMonitoringHour and current_date == last_business_day:
                        message = f"✅ L1 trigger: Last business day monitoring"
                        print(f"message : {message}")
                        splunk.log_message({'Message': message, "FileName": "cron", "Status": "success"}, context)
                        received_file_names = check_files_received(now, expected_filename_prefix, table, current_date, emailConfig, context)
            
                elif fileMonitoringCode == "L2" :
                    fileMonitoringHour=file_monitoring_config.get("fileMonitoringHour","23")
                    emailConfig=file_monitoring_config.get("emailConfig") 
                    
                    if last_business_day is None:
                        message = "[WARNING] Could not calculate last business day"
                        splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)
                        day_after_last_business_day = None
                        continue
                    else:
                        day_after_last_business_day = last_business_day + timedelta(days=1)
                    print(f"fileMonitoringCode : {fileMonitoringCode} , fileMonitoringHour: {fileMonitoringHour}, day_after_last_business_day : {day_after_last_business_day}")
                
                    if current_hour == fileMonitoringHour and current_date == day_after_last_business_day:
                        message = f"✅ L2 trigger: Day after last business day monitoring"
                        print(f"message : {message}")
                        splunk.log_message({'Message': message, "FileName": "cron", "Status": "success"}, context)
                        received_file_names = check_files_received(now, expected_filename_prefix, table, current_date, emailConfig, context)
                else:
                    message = f"⏸️  No monitoring trigger matched:"
                    splunk.log_message({'Message': message, "FileName": "cron", "Status": "success"}, context)
                    message = f"   • Code: {fileMonitoringCode}, Hour: {current_hour}, Day: {current_day}"
                    splunk.log_message({'Message': message, "FileName": "cron", "Status": "success"}, context)
                    message = f"   • Expected hour: {fileMonitoringHour}"
                    splunk.log_message({'Message': message, "FileName": "cron", "Status": "success"}, context)
                       
                # received_file_names = check_files_received(now, expected_filename_prefix, table, current_date, emailConfig, context)
            
            except Exception as monitoring_error:
                message = f"[ERROR] Monitoring logic error: {str(monitoring_error)}"
                splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)
                received_file_names = []
            
    except Exception as e:
        error_msg = f"Critical error in file_monitor: {str(e)}"
        splunk.log_message({'Message': f"[ERROR] {error_msg}", "FileName": "cron", "Status": "failed"}, context)
        return {
            "statusCode": 500,
            "error": "File monitoring failed",
            "details": str(e)
        }

def cron_based_file_trigger(config_bucket_name, config, lambda_function_name,context):
    
    try:
        
        message = "🚀 Starting cron based file trigger"
        splunk.log_message({'Message': message, "FileName": "cron", "Status": "success"}, context)
        print(f"cron_based_file_trigger.......")
               
        # Configuration with error handling
        try:
            timezoneName=config.get("timezoneName", "America/New_York")
            now = datetime.now(ZoneInfo(timezoneName))
            current_date = now.date()
            current_hour = now.strftime("%H")
            current_day = now.strftime("%A") 
            
        except Exception as config_error:
            message = f"[ERROR] Configuration error: {str(config_error)}"
            splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)
            return {"statusCode": 500, "error": "Configuration error"}
        
        for file_monitoring_config in config["file_monitoring_config"]:
            fileMonitoringCode=file_monitoring_config.get("file_monitoring_code")

            # File monitoring logic with error handling
            received_file_names = []
            
            try:
                if fileMonitoringCode == "D1":
                    day_range = file_monitoring_config.get("day_range")
                    fileMonitoringHour = file_monitoring_config.get("fileMonitoringHour", "23")
                    # print(f"*fileMonitoringCode : {fileMonitoringCode},current_hour {current_hour}, fileMonitoringHour: {fileMonitoringHour} :: {type(fileMonitoringHour)}, current_day : {current_day}, day_range: {day_range}")
                    done_file = ""                    

                    if current_hour == fileMonitoringHour and current_day in day_range:
                        # print(f"in : {config_bucket_name}, {file_monitoring_config['file_path']}, {file_monitoring_config['file_pattern']}")
                        # print(f"config_bucket_name : {config_bucket_name}")
                        # print(f"file_monitoring_config['file_path'] : {file_monitoring_config['file_path']}")
                        # print(f"file_monitoring_config['file_pattern'] : {file_monitoring_config['file_pattern']}")
                        all_data_files = get_s3_file_list(config_bucket_name, file_monitoring_config["file_path"], file_monitoring_config["file_pattern"])
                        # print(f"all_data_files : {all_data_files}")
                        if all_data_files:
                            for done_file_custom in file_monitoring_config["done_file_custom"]:
                                if "STATIC" in done_file_custom["nameAction"]:
                                    done_file = done_file + done_file_custom.get("namePattern")
                                elif "DATE" in done_file_custom["nameAction"]:
                                    date_str = now.strftime(done_file_custom.get("namePattern"))
                                    done_file = done_file + date_str

                        print(f"done_file : {done_file}")
                        s3.put_object(Bucket=config_bucket_name, Key=f"{file_monitoring_config['done_file_path']}{done_file}", Body="")

            except Exception as monitoring_error:
                message = f"[ERROR] Monitoring logic error: {str(monitoring_error)}"
                splunk.log_message({'Message': message, "FileName": "cron", "Status": "failed"}, context)
            
    except Exception as e:
        error_msg = f"Critical error in file_monitor: {str(e)}"
        splunk.log_message({'Message': f"[ERROR] {error_msg}", "FileName": "cron", "Status": "failed"}, context)
        return {
            "statusCode": 500,
            "error": "File monitoring failed",
            "details": str(e)
        }
