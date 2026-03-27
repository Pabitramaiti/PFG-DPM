import boto3
import os
import re
import json
from typing import List, Dict, Optional
from botocore.exceptions import ClientError, NoCredentialsError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from dpm_splunk_logger_py import splunk


# AWS clients
s3_client = boto3.client('s3')
ses_client = boto3.client('ses')


def log_message(status: str, message: str, context=None, event=None):
    """
    Log messages with consistent formatting and Splunk integration.
    
    Args:
        status: Log level (INFO, SUCCESS, WARNING, ERROR, Failed)
        message: Log message
        context: Lambda context (optional)
        event: Lambda event (optional)
    """
    timestamp = __import__('datetime').datetime.utcnow().isoformat()
    log_entry = {
        'timestamp': timestamp,
        'status': status,
        'message': message
    }
    
    if context:
        log_entry['request_id'] = getattr(context, 'aws_request_id', 'N/A')
        log_entry['function_name'] = getattr(context, 'function_name', 'N/A')
    
    print(f"[{status}] {message}")
    
    if context:
        splunk.log_message(log_entry, context)


def download_file_from_s3(bucket_name: str, object_key: str, destination_path: str):

    try:
        s3_client.download_file(bucket_name, object_key, destination_path)
        log_message('INFO', f'File downloaded from S3: {bucket_name}/{object_key}')
    except ClientError as e:
        log_message('ERROR', f'Failed to download file from S3: {str(e)}')
        raise


def find_files_by_pattern(bucket_name: str, file_path: str, file_regex: str, context=None) -> List[str]:
    try:
        log_message('INFO', f'Searching for files in s3://{bucket_name}/{file_path} with pattern: {file_regex}', context)
        
        prefix = file_path.rstrip('/') + '/' if file_path and not file_path.endswith('/') else file_path
        
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
        if 'Contents' not in response:
            log_message('WARNING', f'No files found in path: {prefix}', context)
            return []
        
        matched_keys = []
        pattern = re.compile(file_regex)
        
        for obj in response.get('Contents', []):
            key = obj['Key']
            filename = key.split('/')[-1]
            
            if pattern.match(filename):
                matched_keys.append(key)
                log_message('INFO', f'Matched file: {key}', context)
        
        log_message('INFO', f'Found {len(matched_keys)} file(s) matching pattern', context)
        return matched_keys
        
    except Exception as e:
        log_message('ERROR', f'Failed to search files by pattern: {str(e)}', context)
        return []


def process_attachment_files(attachment_files: List[Dict], bucket_name: str, context=None) -> List[Dict]:

    processed_attachments = []
    
    if not attachment_files:
        log_message('INFO', 'No attachment_files provided - email will be sent without attachments', context)
        return processed_attachments
    
    log_message('INFO', f'Processing {len(attachment_files)} attachment configuration(s)', context)
    
    for idx, attachment_config in enumerate(attachment_files):
        file_key = attachment_config.get('file_key', '').strip()
        file_path = attachment_config.get('file_path', '').strip()
        file_regex = attachment_config.get('file_regex', '').strip()
        
        log_message('INFO', f'Processing attachment {idx + 1}: file_key="{file_key}", file_path="{file_path}", file_regex="{file_regex}"', context)
        
        if file_key:
            log_message('INFO', f'Using direct file_key: {file_key}', context)
            
            try:
                s3_client.head_object(Bucket=bucket_name, Key=file_key)
                filename = file_key.split('/')[-1]
                
                processed_attachments.append({
                    'bucket': bucket_name,
                    's3_key': file_key,
                    'filename': filename
                })
            except ClientError as e:
                error_msg = f'File not found: s3://{bucket_name}/{file_key}'
                log_message('ERROR', error_msg, context)
                raise FileNotFoundError(error_msg)
        
        elif file_path and file_regex:
            log_message('INFO', f'Searching for files in path: {file_path} with regex: {file_regex}', context)
            
            matched_keys = find_files_by_pattern(bucket_name, file_path, file_regex, context)
            
            if not matched_keys:
                error_msg = f'No files matched pattern in {file_path} with regex {file_regex}'
                log_message('ERROR', error_msg, context)
                raise FileNotFoundError(error_msg)
            else:
                for matched_key in matched_keys:
                    filename = matched_key.split('/')[-1]
                    
                    processed_attachments.append({
                        'bucket': bucket_name,
                        's3_key': matched_key,
                        'filename': filename
                    })
        
        else:
            error_msg = f'Invalid attachment configuration at index {idx}: must provide either file_key OR (file_path + file_regex)'
            log_message('ERROR', error_msg, context)
            raise ValueError(error_msg)
    
    log_message('INFO', f'Processed {len(processed_attachments)} attachment(s) to attach', context)
    return processed_attachments


def send_email_with_attachment(
    sender_email: str,
    recipient_emails: List[str],
    cc_emails: Optional[List[str]],
    subject: str,
    body_text: str,
    bucket_name: str,
    attachment_files: Optional[List[Dict]] = None,
    context=None
) -> str:

    # Log email configuration
    attachment_count = len(attachment_files) if attachment_files else 0
    log_message('INFO', f'Email configuration - Attachment configs: {attachment_count}', context)
    log_message('INFO', f'Email details - From: {sender_email}, To: {recipient_emails}, Subject: {subject}', context)
    log_message('INFO', f'Using S3 bucket: {bucket_name}', context)

    # Create MIME message
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = ', '.join(recipient_emails)
    msg['Cc'] = ', '.join(cc_emails) if cc_emails else None

    msg.attach(MIMEText(body_text, 'plain'))

    if attachment_files:
        log_message('INFO', f'Processing attachment_files configuration with {len(attachment_files)} item(s)', context)
        
        processed_attachments = process_attachment_files(attachment_files, bucket_name, context)
        
        if not processed_attachments:
            log_message('WARNING', 'No attachments found after processing attachment_files configuration', context)
        else:
            log_message('INFO', f'Attaching {len(processed_attachments)} file(s) to email', context)
            
            for idx, attachment in enumerate(processed_attachments):
                try:
                    att_bucket = attachment['bucket']
                    att_key = attachment['s3_key']
                    att_filename = attachment['filename']
                    
                    local_path = f'/tmp/attachment_{idx}_{att_filename}'
                    log_message('INFO', f'Downloading attachment {idx + 1}/{len(processed_attachments)}: s3://{att_bucket}/{att_key}', context)
                    download_file_from_s3(att_bucket, att_key, local_path)
                    
                    with open(local_path, 'rb') as file:
                        part = MIMEApplication(file.read(), Name=att_filename)
                        part['Content-Disposition'] = f'attachment; filename="{att_filename}"'
                        msg.attach(part)
                    
                    log_message('SUCCESS', f'Attachment {idx + 1} added: {att_filename}', context)
                    
                    try:
                        os.remove(local_path)
                    except Exception as cleanup_error:
                        log_message('WARNING', f'Failed to clean up temp file: {str(cleanup_error)}', context)
                
                except Exception as e:
                    error_msg = f'Failed to attach file {idx + 1}: {str(e)}'
                    log_message('ERROR', error_msg, context)
                    raise Exception(error_msg)
            
            log_message('INFO', f'Attachment processing completed - {len(processed_attachments)} file(s) attached', context)
    else:
        log_message('INFO', 'No attachment_files provided - sending email without attachments', context)

    try:
        log_message('INFO', f'Attempting to send email via SES - From: {sender_email}, To: {recipient_emails}', context)
        if cc_emails:
            log_message('INFO', f'CC recipients: {cc_emails}', context)
        
        response = ses_client.send_raw_email(
            Source=sender_email,
            Destinations=recipient_emails + cc_emails if cc_emails else recipient_emails,
            RawMessage={'Data': msg.as_string()}
        )
        
        message_id = response['MessageId']
        log_message('SUCCESS', f'Email sent successfully! Message ID: {message_id}', context)
        log_message('SUCCESS', f'Email delivery completed - Recipients: {recipient_emails}', context)
        
        return f"Email sent! Message ID: {message_id}"
    
    except NoCredentialsError:
        error_msg = "AWS credentials not available for SES"
        log_message('ERROR', error_msg, context)
        raise NoCredentialsError(error_msg)
    
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        
        if error_code == 'AccessDenied':
            error_msg = "SES Access Denied - IAM role lacks ses:SendRawEmail permission"
            log_message('ERROR', error_msg, context)
            log_message('ERROR', "Please add SES permissions to the Lambda execution role", context)
            log_message('ERROR', "Required permission: ses:SendRawEmail on resource arn:aws:ses:*:*:identity/*", context)
            raise Exception(error_msg)
        
        elif error_code == 'MessageRejected':
            error_msg = f"Email message rejected by SES: {error_message}"
            log_message('ERROR', error_msg, context)
            raise Exception(error_msg)
        
        else:
            error_msg = f"SES error ({error_code}): {error_message}"
            log_message('ERROR', error_msg, context)
            raise Exception(error_msg)
    
    except Exception as e:
        error_msg = f"Unexpected error sending email: {str(e)}"
        log_message('Failed', error_msg, context)
        raise Exception(error_msg)


def lambda_handler(event: Dict, context) -> Dict:

    try:
        sender_email = event.get('sender_email')
        if not sender_email:
            raise ValueError("sender_email is required in event payload")
        
        recipient_emails = event.get('recipient_emails')
        if not recipient_emails or not isinstance(recipient_emails, list):
            raise ValueError("recipient_emails must be a list of email addresses")
        
        subject = event.get('subject', 'No Subject')
        body_text = event.get('body_text', '')
        cc_emails = event.get('cc_emails', [])
        
        add_timestamp_in_subject = event.get('add_timestamp_in_subject', False)
        add_timestamp_in_body = event.get('add_timestamp_in_body', False)
        
        date_str = __import__('datetime').datetime.utcnow().strftime('%m/%d/%Y')
        timestamp_str = __import__('datetime').datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
        
        if add_timestamp_in_subject:
            subject = f"{subject} - {date_str}"
            log_message('INFO', f'Added date to subject: {subject}', context)
        
        if add_timestamp_in_body:
            body_text = f"{body_text}\n\nTimestamp: {timestamp_str}"
            log_message('INFO', 'Added timestamp to email body', context)
        
        bucket_name = event.get('bucket_name', '')
        attachment_files = event.get('attachment_files', [])

        # Process files with add_content_to_mail flag
        if attachment_files and bucket_name:
            for attachment_config in attachment_files:
                add_content = attachment_config.get('add_content_to_mail', False)
                
                if add_content:
                    file_key = attachment_config.get('file_key', '').strip()
                    file_path = attachment_config.get('file_path', '').strip()
                    file_regex = attachment_config.get('file_regex', '').strip()
                    
                    # Determine which files to process
                    files_to_process = []
                    
                    if file_key:
                        files_to_process.append(file_key)
                    elif file_path and file_regex:
                        matched_keys = find_files_by_pattern(bucket_name, file_path, file_regex, context)
                        files_to_process.extend(matched_keys)
                    
                    # Process each file and add content to email body
                    for s3_key in files_to_process:
                        filename = s3_key.split('/')[-1]
                        file_extension = filename.lower().split('.')[-1]
                        
                        # Only process JSON and TXT files
                        if file_extension in ['json', 'txt']:
                            try:
                                log_message('INFO', f'Adding content from {filename} to email body', context)
                                
                                # Download file to temp location
                                local_path = f'/tmp/content_{filename}'
                                download_file_from_s3(bucket_name, s3_key, local_path)
                                
                                # Read file content
                                with open(local_path, 'r', encoding='utf-8') as file:
                                    file_content = file.read()
                                
                                # Add content to email body
                                body_text += f"\n\n{file_content}"
                                log_message('SUCCESS', f'Added content from {filename} to email body', context)
                                
                                # Clean up temp file
                                try:
                                    os.remove(local_path)
                                except Exception as cleanup_error:
                                    log_message('WARNING', f'Failed to clean up temp file: {str(cleanup_error)}', context)
                                    
                            except Exception as e:
                                log_message('ERROR', f'Failed to add content from {filename}: {str(e)}', context)
        
        if not isinstance(attachment_files, list):
            log_message('ERROR', 'attachment_files must be a list', context)
            attachment_files = []
        
        if attachment_files:
            log_message('INFO', f'Loaded {len(attachment_files)} attachment configuration(s) from event payload', context)
            if not bucket_name:
                log_message('WARNING', 'attachment_files provided but bucket_name is missing', context)
        else:
            log_message('INFO', 'No attachment_files in event - email will be sent without attachments', context)
        
        result = send_email_with_attachment(
            sender_email=sender_email,
            recipient_emails=recipient_emails,
            cc_emails=cc_emails,
            subject=subject,
            body_text=body_text,
            bucket_name=bucket_name,
            attachment_files=attachment_files,
            context=context
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': result,
                'timestamp': __import__('datetime').datetime.utcnow().isoformat()
            })
        }
    
    except (FileNotFoundError, ValueError) as e:
        error_msg = f"Validation error: {str(e)}"
        log_message('ERROR', error_msg, context)
        return {
            'statusCode': 400,
            'body': json.dumps({
                'error': str(e),
                'timestamp': __import__('datetime').datetime.utcnow().isoformat()
            })
        }
    
    except Exception as e:
        error_msg = f"Lambda handler error: {str(e)}"
        log_message('Failed', error_msg, context)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'timestamp': __import__('datetime').datetime.utcnow().isoformat()
            })
        }
