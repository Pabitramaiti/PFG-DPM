import boto3
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dpm_splunk_logger_py import splunk

s3_client = boto3.client("s3")
dynamodb = boto3.resource(
    "dynamodb",
    region_name=os.environ.get("AWS_REGION", "us-east-1")
)

def log_to_splunk(status, message, context):
    """Helper to send log message to Splunk in the right format"""
    splunk.log_message(
        {'Status': status, 'Message': message},
        context
    )

# Ensure DynamoDB table exists
def ensure_table_exists(table_name, partition_key, context):
    client = dynamodb.meta.client
    try:
        client.describe_table(TableName=table_name)
        log_to_splunk("success", f"Table {table_name} already exists", context)
    except client.exceptions.ResourceNotFoundException:
        log_to_splunk("warn", f"Table {table_name} not found. Creating...", context)
        client.create_table(
            TableName=table_name,
            AttributeDefinitions=[{"AttributeName": partition_key, "AttributeType": "S"}],
            KeySchema=[{"AttributeName": partition_key, "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST"
        )
        waiter = client.get_waiter("table_exists")
        waiter.wait(TableName=table_name)
        log_to_splunk("success", f"Table {table_name} created successfully", context)

# Perform a single operation
def process_operation(table, operation, context):
    op_type = operation.get("operation")

    try:
        if op_type == "CREATE":
            results = []
            if "item" in operation:  # single insert
                table.put_item(Item=operation["item"])
                log_to_splunk("success", f"CREATE: {operation['item']}", context)
                results.append(operation["item"])
            elif "items" in operation:  # multiple inserts
                for item in operation["items"]:
                    table.put_item(Item=item)
                    log_to_splunk("success", f"CREATE: {item}", context)
                    results.append(item)
            return {"status": "success", "op": "CREATE", "items": results}

        elif op_type == "READ":
            response = table.get_item(Key=operation["key"])
            result = response.get("Item")
            log_to_splunk("success", f"READ: {result}", context)
            return {"status": "success", "op": "READ", "result": result}

        elif op_type == "UPDATE":
            key = operation["key"]
            updates = operation["updates"]
            update_expr = "SET " + ", ".join(f"#{k}=:{k}" for k in updates.keys())
            expr_attr_names = {f"#{k}": k for k in updates.keys()}
            expr_attr_values = {f":{k}": v for k, v in updates.items()}
            table.update_item(
                Key=key,
                UpdateExpression=update_expr,
                ExpressionAttributeNames=expr_attr_names,
                ExpressionAttributeValues=expr_attr_values,
            )
            log_to_splunk("success", f"UPDATE on {key}: {updates}", context)
            return {"status": "success", "op": "UPDATE", "updates": updates}

        elif op_type == "DELETE":
            table.delete_item(Key=operation["key"])
            log_to_splunk("success", f"DELETE: {operation['key']}", context)
            return {"status": "success", "op": "DELETE", "key": operation["key"]}

        else:
            log_to_splunk("error", f"Unsupported operation {op_type}", context)
            return {"status": "failed", "reason": f"Unsupported op {op_type}"}

    except Exception as e:
        log_to_splunk("error", f"{op_type} failed: {str(e)}", context)
        return {"status": "failed", "op": op_type, "error": str(e)}

# Lambda entry point
def lambda_handler(event, context):
    try:
        # Get S3 bucket + object
        bucket = event["Records"][0]["s3"]["bucket"]["name"]
        key = event["Records"][0]["s3"]["object"]["key"]
        log_to_splunk("info", f"Triggered by file s3://{bucket}/{key}", context)

        # Read file from S3
        s3_obj = s3_client.get_object(Bucket=bucket, Key=key)
        file_content = s3_obj["Body"].read().decode("utf-8")
        data = json.loads(file_content)

        table_name = data.get("table_name")
        partition_key = data.get("partition_key")
        operations = data.get("operations", [])

        if not table_name or not partition_key:
            raise ValueError("JSON must include 'table_name' and 'partition_key'")

        # Ensure table exists
        ensure_table_exists(table_name, partition_key, context)
        table = dynamodb.Table(table_name)

        results = []
        # Run ops in parallel
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(process_operation, table, op, context) for op in operations]
            for future in as_completed(futures):
                results.append(future.result())

        log_to_splunk("success", f"All operations completed on {table_name}", context)
        return {"table": table_name, "results": results}

    except Exception as e:
        log_to_splunk("error", f"Lambda failed: {str(e)}", context)
        raise
