import boto3
import os
import re
import json
from botocore.exceptions import NoCredentialsError, ClientError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from dpm_splunk_logger_py import splunk
from jinja2 import Environment, FileSystemLoader, DictLoader
import csv
import io
from datetime import datetime

s3_client = boto3.client('s3')

def get_email_body_from_template(bucket_name, template_key, data):
    if not template_key:
        raise ValueError("Template key cannot be empty! Got: {}".format(template_key))
   
    print(f"Fetching template from s3://{bucket_name}/{template_key}")
 
    response = s3_client.get_object(Bucket=bucket_name, Key=template_key)
    html_template = response['Body'].read().decode('utf-8')
 
    env = Environment(loader=DictLoader({'template.html': html_template}))
    template = env.get_template('template.html')
 
    headers = data.get("headers",[])
    rows = data.get("rows",[])
    raw_data = data.get("raw_data",{})
    error_details = data.get("errorDetails", "")
    run_date = data.get("run_date", "")
    rendered_html = template.render(headers=headers, rows=rows, raw_data=raw_data, error_details=error_details,run_date=run_date)
    return rendered_html

def fetch_matching_keys_in_folder(bucket_name, folder_prefix, pattern,context,multi_file = False):
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)
        if multi_file:
            matched_keys =[]
            if not response:
                print(F"No matching keys found in the specified folder.{folder_prefix}")
                return matched_keys

            for obj in response.get('Contents', []):
                key = obj['Key']
                file = key.split('/')[-1]
                if key.startswith(folder_prefix) and re.match(pattern, file):
                    matched_keys.append(key)
            return matched_keys
        else:
            for obj in response.get('Contents', []):
                key = obj['Key']
                file = key.split('/')[-1]
                if key.startswith(folder_prefix) and re.match(pattern, file):
                    return key
            return ""
    except Exception as e:
        error_details = f"Failed to read data from s3 bucket check folder and patters {str(e)}"
        splunk.log_message({"Status": "Failed", "Message": "Failed to read data from s3 bucket check folder and patters "}, context)
        raise Exception(f"[ERROR] No files found in the bucket/ folder : {error_details}")

def download_file_from_s3(bucket_name, object_key, destination_path):
    try:
        s3_client.download_file(bucket_name, object_key, destination_path)
        print(f"File downloaded from S3 bucket: {object_key}")
    except ClientError as e:
        print(f"Failed to download file from S3: {e}")

def extract_data_from_error(error_details,context):
    # Extracting the error details
    splunk.log_message({"Status": "log", "Message": f"extract filename from error details {error_details}"}, context)
    try:
        start_index = error_details.find('{')
        if start_index == -1:
            return None
        json_data = json.loads(error_details[start_index:].strip())
        return json_data['report_files']
    except Exception as e:
        splunk.log_message({"Status": "Failed", "Message": "Failed to read data from error details"}, context)
        raise Exception(f"[ERROR] No data found in details error details : {error_details}")
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
    report_attachment_flag=""
):
    # Create an SES client
    ses_client = boto3.client('ses')

    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = ', '.join(recipient_emails)
    msg['Cc'] = ', '.join(cc_emails) if cc_emails else None

    # Attach body text
    msg.attach(MIMEText(body_text, 'html'))

    # Handle attachment if flag is True
    if attachment_flag:
        if object_key and isinstance(object_key,list):
            # Get each matched file from bucket and attach to mail
            for key in object_key:
                if key:
                    #print(bucket_name," key ",key)
                    obj = s3_client.get_object(Bucket=bucket_name, Key=key)
                    file_name = key.split('/')[-1]
                    part = MIMEApplication(obj['Body'].read(), Name=file_name)
                    part.add_header('Content-Disposition', 'attachment', filename=file_name)
                    msg.attach(part)
                    print(f"Attachment added: {key}")
                else:
                    # Log a warning but continue sending email
                    print("[WARNING] Attachment flag is True but no object_key was provided — sending without attachment")
        elif object_key:
            # Download from S3
            download_file_from_s3(bucket_name, object_key, attachment_file_path)

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
        else:
            # Log a warning but continue sending email
            print("[WARNING] Attachment flag is True but no object_key was provided — sending without attachment")

    # Send the email
    try:
        response = ses_client.send_raw_email(
            Source=sender_email,
            Destinations=recipient_emails + cc_emails if cc_emails else recipient_emails,
            RawMessage={'Data': msg.as_string()}
        )
        return f"Email sent! Message ID: {response['MessageId']}"
    except NoCredentialsError:
        return "Credentials not available"

def format_email_body(input_format, input_bucket, email_input_file, output_format, output_delimiter, context):
    """ Compose the body of the email based on the configuration. """
    try:
        if input_format.lower() == 'json':
            email_json = read_file_to_json(input_bucket, email_input_file, context)
            message = f"File read from S3 bucket: {input_bucket}"
            splunk.log_message({"Status": "log", "Message": message}, context)

            if output_format.lower() == 'columnar':
                body = "<pre>"
                for key, value in email_json.items():
                    if key.lower().startswith('header'):
                        body += f"{value}\n"
                    else:
                        body += f"{key}{output_delimiter}{value}\n"
                body += "</pre>"
                return body
            else:
                message = f"Invalid Parameter Parameter name: output_format, Value : {output_format}"
                splunk.log_message({"Status": "failed", "Message": message}, context)
                raise ValueError(message)
        else:
            message = f"Invalid Parameter Parameter name: input_format, Value : {input_format}"
            splunk.log_message({"Status": "failed", "Message": message}, context)
            raise ValueError(message)
    except Exception as e:
        message = f"Error while formatting the email Body : {e}"
        splunk.log_message({"Status": "failed", "Message": message}, context)
        raise e

def read_csv_file(bucket_name, file_key, context):
    try:
        report_data = {}
        splunk.log_message({"Status": "log", "bucket_name": bucket_name, "file_key": file_key}, context)
        read_csv = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        csv_content = read_csv['Body'].read().decode('utf-8')
        csv_reader = csv.reader(io.StringIO(csv_content))
        report_rows = list(csv_reader)
        report_columns = report_rows[0]
        custom_details = report_rows[1]

        report_data.update({
            "raw_data": custom_details
            })
        report_rows = report_rows[2:]
        report_data.update(
            {
                "headers": report_columns,
                "rows": report_rows
            }
        )
        return report_data
    except Exception as e:
        message = f"{str(e)}"
        splunk.log_message({"Message": "Failed to read csv file", "Message": message, "Status": "FAILED"}, context)
        raise Exception("Failed to read csv file: " + message)

def read_file_to_json(bucket_name, file_key, context):
    """ Read the file into a JSON object """
    downloaded_file_path = file_key[file_key.rfind('/') + 1:len(file_key)]
    try:
        splunk.log_message({"Status": "log", "bucket_name": bucket_name, "file_key": file_key}, context)
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        json_content = json.loads(response['Body'].read().decode('utf-8'))
        splunk.log_message({"Status": "log", "json_content": json_content}, context)
        return json_content
    except Exception as e:
        message = f"{str(e)}"
        splunk.log_message({"Message": "Failed to read file", "Message": message, "Status": "FAILED"}, context)
        raise Exception("Failed to read file: " + message)

def parse_return_template_data(bucket_name,template_key,file_name,report_data_key,context,error_details=None):
    try:    
        if report_data_key:
            report_data = read_csv_file(bucket_name,report_data_key,context)
            raw_data = report_data.get("raw_data",{})
            date_part = datetime.strptime(raw_data[1],"%m%d%Y")
            subject = f"PASS: Post Reconciliation Report: JHI STMT {raw_data[0]} {date_part.strftime('%m/%d/%Y')}"
        else:
            report_data = {}
        if error_details:
            match = re.search(r'\b\d{8}\b', file_name)
            if match:
                rundate_raw = match.group(0)
                # Convert to MM/DD/YYYY format
                rundate = datetime.strptime(rundate_raw, "%Y%m%d").strftime("%m/%d/%Y")
            else:
                rundate = None
            subject = f"FAIL: Post Reconciliation Report: JHI STMT  {rundate}"
            report_data["errorDetails"] = error_details
            report_data["run_date"] = rundate
        body_text = get_email_body_from_template(bucket_name, template_key, report_data)
        return subject,body_text
    except Exception as e:
        message = f"{str(e)}"
        splunk.log_message({"Status": "failed", "failed at parse_return_template_data": message}, context)
        raise Exception("Failed at parse_return_template_data: " + message)

def lambda_handler(event, context):
    try:
        if not event["skip_email_send"] or (event.get("failureMailFlag",False) and event.get('duplicate_report_flag',False)):
            sender_email = os.getenv('sender_email')
            attachment_file_path = os.getenv('downloaded_attachment')
            recipient_emails = event['recipient_emails']
            cc_emails = event['cc_emails']
            subject = event['subject']
            body_text = event['body']
            bucket_name = event['bucket_name']
            folder_prefix = event['object_key']
            # This duplicate_report_flag, multiple_files for duplicate balance report validation
            multiple_files=event.get('multiple_files',False)
            duplicate_report_flag = event.get('duplicate_report_flag',False)
            pattern = event['pattern']
            attachment_flag = event['attachment_flag']
            return_template_flag = event.get('return_template_flag', False)
            template_key = event.get('template_key', None)
            report_data_key = event.get('report_data_key', None)
            if 'report_attachment_flag' in  event:
                report_attachment_flag = event['report_attachment_flag']
            else:
                report_attachment_flag = False
            object_key = ""
            input_file = event['input_file']
            if input_file.startswith('lpl/'):
                subject = "LPL STATEMENTS – ASSET ALLOCATION FILE SUCCESS NOTIFICATION -" + os.environ.get("SDLC_ENV").upper()
            else:
                subject = event['subject']
            transaction_id = event.get('transactionId', 'Unknown')

            # Handle failed status
            if event.get('sfStatus') == 'Failed':
                print(input_file)
                file_name = os.path.basename(input_file)
                client_name = input_file.split('/')[0]
                print(file_name)
                print(client_name)
                try:
                    error_details = json.loads(body_text)
                    if duplicate_report_flag:
                        report_files = extract_data_from_error(error_details.get('ErrorMessage'),context)
                        file_names_str = ",".join(report_files)
                        subject = f"{client_name.upper()} - Validation Failed : Report File {file_names_str} "
                        body_text = (
                            f"""
                                <p>Input File : {file_name}</p>
                                <p>Report File Name : {file_names_str}</p>
                                <p>Status : Failed </p>
                            """
                        )
                    else:
                        if return_template_flag:
                            subject,body_text = parse_return_template_data(bucket_name,template_key,file_name,"",context,error_details)
                        else:
                            subject = f"{client_name.upper()} - Validation Failed : Input File {file_name} Failed due to {subject}"
                            body_text = (
                                f"""
                                    <p>Transaction ID: {transaction_id}</p>
                                    <p>Input File : {file_name} </p>
                                    <p>Error Message : {error_details.get('ErrorMessage')}</p>
                                    <p>Job Name : {error_details.get('JobName')}</p>
                                """
                            )
                except Exception as e:
                    error_message = f"Failed to parse error details: {str(e)}"
                    splunk.log_message({"Status": "failed", "Message": error_message}, context)
                    raise e

            # Format body from input file if needed
            if 'email_input_file' in event:
                email_input_file = event['email_input_file']
                if email_input_file:  #only process if actually present
                    if ('email_input_format' in event and
                        'email_output_format' in event and
                        'email_output_delimiter' in event):

                        input_format = event['email_input_format']
                        output_format = event['email_output_format']
                        output_delimiter = event['email_output_delimiter']

                        body_text = format_email_body(
                            input_format, bucket_name, email_input_file,
                            output_format, output_delimiter, context
                        )
                    else:
                        message = "Parameters required for formatting the email body from the file are missing"
                        splunk.log_message({"Status": "failed", "Message": message}, context)
                        raise ValueError(message)
                else:
                    print("[DEBUG] Skipping body formatting since email_input_file is not provided")

            if attachment_flag:
                if duplicate_report_flag:
                    file_names_list  = extract_data_from_error(error_details.get('ErrorMessage'),context)
                    object_key = [os.path.join(folder_prefix,fname) for fname in file_names_list] 
                else:
                    object_key = fetch_matching_keys_in_folder(bucket_name, folder_prefix, pattern,context,multiple_files)
                if not object_key:
                    raise Exception(f"[ERROR] No matching attachment found in {bucket_name}/{folder_prefix} with pattern {pattern}")
            if return_template_flag and report_data_key:
                file_name = os.path.basename(input_file)
                subject,body_text = parse_return_template_data(bucket_name,template_key,file_name,report_data_key,context)

            response = send_email_with_attachment(
                sender_email, recipient_emails, cc_emails, subject, body_text,
                attachment_file_path, bucket_name, object_key,
                attachment_flag, report_attachment_flag
            )
            return {'statusCode': 200, 'body': response}
        else:
            splunk.log_message({"Status": "success", "Message": "The system skipped the email notification"}, context)
            return {'statusCode': 200, 'body': "The system skipped the email notification"}
    except Exception as e:
        raise e
