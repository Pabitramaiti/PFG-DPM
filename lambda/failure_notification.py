import json
import boto3
from dpm_splunk_logger_py import splunk
import traceback
from datetime import datetime, timezone

AWS_ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]
MAX_SPLUNK_SIZE = 6 * 1024

def get_context_info(context):
    return {
        "aws_request_id": context.aws_request_id,
        "log_group_name": context.log_group_name,
        "log_stream_name": context.log_stream_name,
        "function_name": context.function_name,
        "function_version": context.aws_request_id,
        "invoked_function_arn": context.aws_request_id
    }

def truncate_if_needed(obj, max_chars=MAX_SPLUNK_SIZE):
    """Serialize and truncate to ensure payload is under limit. Always returns a dict."""
    text = json.dumps(obj, default=str)
    if len(text) > max_chars:
        print(f"[WARN] Payload too large ({len(text)} chars). Truncating to {max_chars} chars.")
        text = text[:max_chars] + f'...__TRUNCATED__({len(text)} chars total)'
        text = ensure_dict(text)
        return text
    else:
        return obj

def ensure_dict(payload):
    if isinstance(payload, dict):
        return payload

    if isinstance(payload, (bytes, bytearray)):
        payload = payload.decode("utf-8", errors="replace")

    if isinstance(payload, str):
        try:
            return json.loads(payload)
        except json.JSONDecodeError:
            return {"truncated_event_text": payload}
    else:
        return {"truncated_event_text": str(payload)}

def lambda_handler(event, context):
    try:
        if not event:
            raise ValueError("Event data is missing")
        
        print("Received event:", json.dumps(event))
        print("Context:", json.dumps(vars(context), default=str))
        event["context"] = get_context_info(context)

        # Truncate event safely before pushing to Splunk
        payload = truncate_if_needed(event, MAX_SPLUNK_SIZE)

        try:
            safe_event = dict(payload) if isinstance(payload, dict) else json.loads(payload)
            print(f"[TEST] Payload JSON decoded successfully.")
        except json.JSONDecodeError:
            print(f"[WARN] Payload JSON decode error after truncation.")
            safe_event = {"truncated_event_text": payload}

        try:
            safe_event["event_time"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            safe_event["event_type"] =  "FAILURE_NOTIFICATION"

            safe_event["job_name"] =  context.function_name
            safe_event["aws_account_id"] = AWS_ACCOUNT_ID
            safe_event["Status"] = "failed"
            print("Final safe_event:", json.dumps(safe_event))
            splunk.log_message(safe_event, context)
        except Exception as e:
            print(f"[ERROR] lmbd_failure_notification : log_event : Failed to write log to Splunk: {str(e)}")

        return {
            "statusCode": 200,
            "body": json.dumps({"FAILURE_NOTIFICATION": "Logged successfully"})
        }
    except Exception as e:
        error_message = f"Exception in lmbd_failure_notification: {str(e)}:: Traceback: {traceback.format_exc()}"
        print(error_message)
        return {
            'statusCode': 500,
            'body': json.dumps({"error": str(e)})
        }