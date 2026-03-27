import boto3
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from boto3.dynamodb.conditions import Attr
from dpm_splunk_logger_py import splunk

# Initialize AWS clients at module level
dynamodb = boto3.resource('dynamodb')
ses_client = boto3.client('ses')
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    try:
        splunk.log_message({'Status': 'info', 'Message': 'Starting file notification check process'}, context)
        
        # Get configuration from S3 based on event input
        splunk.log_message({'Status': 'info', 'Message': 'Starting config retrieval from S3'}, context)
        config = get_config_from_s3(event, context)
        splunk.log_message({'Status': 'info', 'Message': 'Config retrieved successfully'}, context)
        
        # Get current time in Eastern timezone
        eastern = ZoneInfo('America/New_York')
        current_time_est = datetime.now(eastern)
        
        splunk.log_message({'Status': 'info', 'Message': f'Current EST time: {current_time_est.strftime("%Y-%m-%d %H:%M:%S")}'}, context)
        
        # Check if current time is past configured notification time
        cutoff_time = current_time_est.replace(
            hour=config['notificationTimeEst'], 
            minute=0, 
            second=0, 
            microsecond=0
        )
        
        if current_time_est < cutoff_time:
            splunk.log_message({'Status': 'info', 'Message': f'Not yet time to check - current: {current_time_est.strftime("%H:%M")}, cutoff: {cutoff_time.strftime("%H:%M")}'}, context)
            return {
                'statusCode': 200,
                'body': json.dumps('Not yet time to check for files')
            }
        
        # Determine target date - check for today's files
        splunk.log_message({'Status': 'info', 'Message': 'Calculating target date'}, context)
        target_date = current_time_est

        target_date_str = target_date.strftime('%Y%m%d')
        expected_filename_prefix = f"{config['filePrefix']}.{target_date_str}"
        
        splunk.log_message({'Status': 'info', 'Message': f'Checking for files from: {target_date.strftime("%Y-%m-%d")} ({target_date.strftime("%A")})', 'target_date': target_date_str, 'expected_prefix': expected_filename_prefix}, context)
        
        # Query DynamoDB using configured table name
        splunk.log_message({'Status': 'info', 'Message': f'Starting DynamoDB scan on table: {config["tableName"]}'}, context)
        table = dynamodb.Table(config['tableName'])
        
        # Add timeout and pagination for DynamoDB scan
        try:
            # Direct scan for target date files instead of two separate scans
            response = table.scan(
                FilterExpression=Attr('fileName').contains(f"{config['filePrefix']}.{target_date_str}")
            )
            splunk.log_message({'Status': 'info', 'Message': f'DynamoDB scan for target date completed. Items found: {response["Count"]}'}, context)
            
        except Exception as db_error:
            splunk.log_message({'Status': 'failed', 'Message': f'DynamoDB scan failed: {str(db_error)}'}, context)
            raise db_error
        
        # Check if any files received for target date
        files_received_for_date = []
        for item in response['Items']:
            file_name = item.get('fileName', '')
            if f"{config['filePrefix']}.{target_date_str}." in file_name:
                files_received_for_date.append(item)
                splunk.log_message({'Status': 'info', 'Message': f'Found matching file: {file_name}'}, context)

        # If no files received for target date, send notification
        if not files_received_for_date:
            splunk.log_message({'Status': 'warning', 'Message': f'No files received for {target_date_str}, sending notification email'}, context)
            
            send_notification_email(
                config['emailConfig']['fromEmail'],
                [config['emailConfig']['jhiEmail'], config['emailConfig']['brClientEmail']], 
                config['emailConfig']['subject'],
                expected_filename_prefix,
                context
            )
            
            splunk.log_message({'Status': 'success', 'Message': f'Notification sent - No files received for {target_date_str}'}, context)
            
            return {
                'statusCode': 200,
                'body': json.dumps(f'Notification sent - No files received for {target_date_str}')
            }
        else:
            splunk.log_message({'Status': 'success', 'Message': f'Files found for {target_date_str}: {len(files_received_for_date)} files'}, context)
            return {
                'statusCode': 200,
                'body': json.dumps(f'Files found for {target_date_str}: {len(files_received_for_date)} files')
            }
            
    except Exception as e:
        error_msg = f"Error in lambda_handler: {str(e)}"
        splunk.log_message({'Status': 'failed', 'Message': error_msg}, context)
        print(f"Error: {str(e)}")
        raise Exception(error_msg)

def get_config_from_s3(event, context):
    """Get configuration from S3 based on event structure and extract from cronProcess"""
    try:
        splunk.log_message({'Status': 'info', 'Message': 'Loading configuration from S3', 'event': json.dumps(event, default=str)}, context)
        
        if 'detail' in event and len(event['detail']) > 0:
            detail = event['detail'][0]
            bucket_name = detail['bucket']['name']
            object_key = detail['object']['key']
        else:
            # Fallback for direct config in event
            bucket_name = event.get('bucket', {}).get('name')
            object_key = event.get('object', {}).get('key')
        
        if not bucket_name or not object_key:
            raise ValueError("No S3 bucket or object key found in event")
        
        splunk.log_message({'Status': 'info', 'Message': f'Reading config from S3: s3://{bucket_name}/{object_key}'}, context)
        
        # Add timeout for S3 operation
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
            cron_config = json.loads(response['Body'].read().decode('utf-8'))
            splunk.log_message({'Status': 'info', 'Message': 'S3 config read successful'}, context)
        except Exception as s3_error:
            splunk.log_message({'Status': 'failed', 'Message': f'S3 read failed: {str(s3_error)}'}, context)
            raise s3_error
        
        # Extract FILE_STATUS_MONITORING config from cronProcess structure
        file_monitoring_config = None
        for process in cron_config.get('cronProcess', []):
            if process.get('functionalityName') == 'FILE_STATUS_MONITORING':
                file_monitoring_config = process['functionalityToDo']['object'][0]
                break
        
        if not file_monitoring_config:
            raise ValueError("FILE_STATUS_MONITORING configuration not found in cronProcess")
        
        splunk.log_message({'Status': 'success', 'Message': 'Successfully extracted FILE_STATUS_MONITORING config'}, context)
        
        return file_monitoring_config
        
    except Exception as e:
        error_msg = f"Error reading config from S3: {str(e)}"
        splunk.log_message({'Status': 'failed', 'Message': error_msg}, context)
        raise e

def send_notification_email(from_email, to_emails, subject, expected_filename, context):
    """Send notification email using SES"""
    try:
        splunk.log_message({'Status': 'info', 'Message': 'Starting email send process'}, context)
        
        # Use Eastern timezone for consistency
        eastern = ZoneInfo('America/New_York')
        current_time_est = datetime.now(eastern)
        
        body_html = f"""
        <html>
        <body>
        <p>Expected file pattern: {expected_filename}*</p>
        <p>Time checked: {current_time_est.strftime('%Y-%m-%d %H:%M:%S')} EST</p>
        </body>
        </html>
        """
        
        response = ses_client.send_email(
            Destination={'ToAddresses': to_emails},
            Message={
                'Body': {
                    'Html': {'Charset': 'UTF-8', 'Data': body_html},
                    'Text': {'Charset': 'UTF-8', 'Data': ''},
                },
                'Subject': {'Charset': 'UTF-8', 'Data': subject},
            },
            Source=from_email,
        )
        
        splunk.log_message({'Status': 'success', 'Message': f'Email sent successfully. MessageId: {response["MessageId"]}'}, context)
        
    except Exception as e:
        error_msg = f"Error sending email: {str(e)}"
        splunk.log_message({'Status': 'failed', 'Message': error_msg}, context)
        raise e