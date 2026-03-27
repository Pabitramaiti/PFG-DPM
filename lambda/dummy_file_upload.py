import boto3
import os
from datetime import datetime
from zoneinfo import ZoneInfo
from dpm_splunk_logger_py import splunk
s3 = boto3.client("s3")

def timezone_map():
    timezone_map = {
        "UTC": "UTC",
        "IST": "Asia/Kolkata",
        "EST": "US/Eastern",
        "CST": "US/Central",
        "MST": "US/Mountain",
        "PST": "US/Pacific"
        }
    return timezone_map

def upload_dummy_file(bucket_name, prefix, file_details, context):
    try:
        file_format = file_details.get("file_format", "DUMMY.{date}.{time}")
        date_format = file_details.get("date_format", "%Y%m%d")
        time_format = file_details.get("time_format", "%H%M%S")
        timezone = file_details.get("timezone", "UTC").upper()
        tz_name = timezone_map().get(timezone, "UTC")

        tz_code = timezone_map().get(timezone, "UTC")
        try:
            tz = ZoneInfo(tz_name)
        except Exception:
            tz = ZoneInfo("UTC")
            tz_code = "UTC"
        
        if not bucket_name:
            raise ValueError("bucket_name is required")
            
        now = datetime.now(tz)
        date_str = now.strftime(date_format)
        time_str = now.strftime(time_format)

        file_name = file_format
        if "date" in file_format:
            file_name = file_format.replace("{date}", date_str)
        if "time" in file_format:
            file_name = file_name.replace("{time}", time_str)

        if prefix:
            if not prefix.endswith("/"):
                prefix += "/"
            file_name = f"{prefix}{file_name}"

        # Create dummy file content
        dummy_content = (
            f"This is a dummy file.\n"
            f"Generated at {now.isoformat()} in {tz_code} timezone.\n"
            f"Date format: {date_format}, Time format: {time_format}"
        )

        # Upload file to S3
        s3.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=dummy_content.encode("utf-8"),
            ContentType="text/plain"
        )

        return {
            "statusCode": 200,
            "bucket": bucket_name,
            "file_name": file_name,
            "timezone_used": tz_code,
            "message": f"File uploaded successfully to {bucket_name}"
        }
    except Exception as e:
        error_message = f"Error uploading dummy file: {str(e)}"
        splunk.log_message({'Message': error_message, "Status": "failed"}, context)
        raise ValueError(error_message)

def lambda_handler(event, context):
    try:
        for event in event:
            bucket_name = event.get("bucket_name", "")
            prefix = event.get("prefix", "")
            file_details = event.get("file_details", {})
            splunk.log_message({'Message': f"bucket_name : {bucket_name} | prefix : {prefix} | file_details : {file_details}", "Status": "logging"}, context)
            upload_dummy_file(bucket_name, prefix, file_details, context)
    except Exception as e:
        error_message = f"[ERROR] Failed Lambda Handler: {str(e)}"
        splunk.log_message({'Message': error_message, "Status": "failed"}, context)
        raise ValueError(error_message)