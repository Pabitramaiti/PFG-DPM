import json
import boto3
import logging
import uuid
from psycopg2 import pool
from datetime import datetime
from psycopg2 import sql


# Logging Setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS Clients
secrets = boto3.client("secretsmanager")
s3_client = boto3.client("s3")

# Global DB Pool
db_pool = None

# Exceptions
class ConfigError(Exception):
    pass
class DatabaseError(Exception):
    pass

# Extract and Resolve the Transaction Id if needed.
def resolve_output_config(output_config: dict, transaction_id: str | None):
    if not transaction_id:
        return output_config
    resolved = {}
    for key, value in output_config.items():
        if isinstance(value, str):
            resolved[key] = value.replace("{transactionId}", transaction_id)
        else:
            resolved[key] = value
    return resolved

def write_final_output_files(brx_result, extraction_result, output_config):
    bucket = output_config["s3_bucket"]
    completed_prefix = output_config["completed_prefix"].rstrip("/")
    error_prefix = output_config["error_prefix"].rstrip("/")

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    files_created = {}

    def upload_json_file(account_list, key_name):
        if not account_list:
            return None

        body = json.dumps(
            {"accountnumbers": account_list},
            indent=2
        )

        s3_client.put_object(
            Bucket=bucket,
            Key=key_name,
            Body=body.encode("utf-8"),
            ContentType="application/json"
        )

        return f"s3://{bucket}/{key_name}"

    # BRX FILES
    brx_completed = brx_result.get("brx_success_accounts", [])
    brx_failed = brx_result.get("brx_failed_accounts", [])

    key = f"{completed_prefix}/brx_completed_accounts_{timestamp}.json"
    file_path = upload_json_file(brx_completed, key)
    if file_path:
        files_created["brx_completed_file"] = file_path

    key = f"{error_prefix}/brx_error_accounts_{timestamp}.json"
    file_path = upload_json_file(brx_failed, key)
    if file_path:
        files_created["brx_error_file"] = file_path

    # EXTRACTION FILES
    extraction_completed = extraction_result.get("extraction_success_accounts", [])
    extraction_failed = extraction_result.get("extraction_failed_accounts", [])

    key = f"{completed_prefix}/extraction_completed_accounts_{timestamp}.json"
    file_path = upload_json_file(extraction_completed, key)
    if file_path:
        files_created["extraction_completed_file"] = file_path

    key = f"{error_prefix}/extraction_error_accounts_{timestamp}.json"
    file_path = upload_json_file(extraction_failed, key)
    if file_path:
        files_created["extraction_error_file"] = file_path

    return files_created

# Contract Builder
def build_polling_response(state_payload: dict, should_continue=None):
    return {
        "persistToState": state_payload or {},
        "shouldContinue": should_continue
    }

def summarize_result_counts(result: dict | None):
    if not result:
        return None

    summarized = dict(result)

    list_keys = [
        "brx_success_accounts",
        "brx_failed_accounts",
        "extraction_success_accounts",
        "extraction_failed_accounts"
    ]

    for key in list_keys:
        if key in summarized and isinstance(summarized[key], list):
            summarized[f"{key}_count"] = len(summarized[key])
            del summarized[key]

    return summarized

# Parameter Extraction
def extract_parameters(event):
    lambda_config = event.get("LambdaConfig", {})

    if "tableInfo" not in lambda_config:
        raise ConfigError("Missing tableInfo in LambdaConfig")

    tableinfo = lambda_config["tableInfo"]

    required_keys = [
        "dbkey",
        "dbname",
        "region",
        "schema",
        "account_table",
        "trid_table"
    ]

    for key in required_keys:
        if key not in tableinfo:
            raise ConfigError(f"Missing tableInfo key: {key}")

    return {
        "pollingAttempt": event.get("PollingParams", {}).get("pollingAttempt", 0),
        "persistedState": event.get("PersistedState", {}),
        "tableInfo": tableinfo,
        "outputConfig": lambda_config.get("outputConfig", {})
    }



# DB Helpers
def get_db_credentials(ssm_param_name, region):
    ssm = boto3.client("ssm", region_name=region)
    param = ssm.get_parameter(Name=ssm_param_name, WithDecryption=True)
    secret_name = param["Parameter"]["Value"]

    secret = secrets.get_secret_value(SecretId=secret_name)
    return json.loads(secret["SecretString"])


def initialize_db_pool(ssm_param_name, dbname, region):
    global db_pool

    if db_pool:
        return db_pool

    creds = get_db_credentials(ssm_param_name, region)

    db_pool = pool.SimpleConnectionPool(
        minconn=1,
        maxconn=5,
        host=creds["host"],
        port=creds["port"],
        user=creds["username"],
        password=creds["password"],
        dbname=dbname
    )

    return db_pool

# def check_extraction_complete(cur, schema, account_table):
#     cur.execute(
#         "SELECT anurag_dev.recon_check_extraction_complete(%s, %s);",
#         (schema, account_table)
#     )
#     return cur.fetchone()[0]

def check_extraction_complete(cur, schema, account_table):
    query = sql.SQL(
        "SELECT {}.recon_check_extraction_complete(%s, %s);"
    ).format(sql.Identifier(schema))

    cur.execute(query, (schema, account_table))
    return cur.fetchone()[0]


# def check_brx_complete(cur, schema, account_table):
#     cur.execute(
#         "SELECT anurag_dev.recon_check_brx_complete(%s, %s);",
#         (schema, account_table)
#     )
#     return cur.fetchone()[0]

def check_brx_complete(cur, schema, account_table):
    query = sql.SQL(
        "SELECT {}.recon_check_brx_complete(%s, %s);"
    ).format(sql.Identifier(schema))

    cur.execute(query, (schema, account_table))
    return cur.fetchone()[0]


def generate_recon_report(brx_result, extraction_result, output_config):
    bucket = output_config["s3_bucket"]
    recon_prefix = output_config["recon_prefix"].rstrip("/")
    recon_filename = output_config["recon_filename"]

    client_name = output_config.get("clientName")
    run_type = output_config.get("runType")
    env = output_config.get("env")
    batch_uuid = str(uuid.uuid4())
    created_timestamp = datetime.utcnow().isoformat()

    processed_single = extraction_result.get("processed_individual_count", 0)
    master_count = extraction_result.get("processed_master_count", 0)
    sister_count = extraction_result.get("processed_sister_count", 0)

    report_body = {
        "clientName": client_name,
        "status": "COMPLETED",
        "uuid": batch_uuid,
        "runType": run_type,
        "createdTimestamp": created_timestamp,
        "env": env,
        "batchCounts": {
            "processedCounts": {
                "single": str(processed_single),
                "csg": {
                    "master": str(master_count),
                    "sister": str(sister_count),
                    "householdSummary": str(master_count)
                }
            }
        }
    }

    key = f"{recon_prefix}/{recon_filename}"

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(report_body, indent=2).encode("utf-8"),
        ContentType="text/plain"
    )

    return f"s3://{bucket}/{key}"


def evaluate_stages(cur, schema, account_table, output_config):
    brx_result = check_brx_complete(cur, schema, account_table)

    if not brx_result.get("brx_complete", False):
        return True,{
            "stage": "BRX",
            "brx": summarize_result_counts(brx_result),
            "extraction": None
        }

    extraction_result = check_extraction_complete(cur, schema, account_table)

    if not extraction_result.get("extraction_complete", False):
        return True, {
            "stage": "EXTRACTION",
            "brx": summarize_result_counts(brx_result),
            "extraction": summarize_result_counts(extraction_result)
        }

    files_created = write_final_output_files(
        brx_result,
        extraction_result,
        output_config
    )

    recon_file_path = generate_recon_report(
        brx_result,
        extraction_result,
        output_config
    )

    files_created["recon_report_file"] = recon_file_path

    return False, {
        "stage": "COMPLETE",
        "brx": summarize_result_counts(brx_result),
        "extraction": summarize_result_counts(extraction_result),
        "files_created": files_created
    }


# Lambda Handler
def lambda_handler(event, context):
    conn = None
    cur = None
    pool_instance = None

    try:
        lambda_config = event.get("LambdaConfig", {})
        tableinfo = lambda_config["tableInfo"]

        ssm_param_name = tableinfo["dbkey"]
        dbname = tableinfo["dbname"]
        region = tableinfo["region"]
        schema = tableinfo["schema"]
        account_table = tableinfo["account_table"]

        pool_instance = initialize_db_pool(ssm_param_name, dbname, region)
        conn = pool_instance.getconn()
        cur = conn.cursor()

        transaction_id = event.get("transactionId")
        output_config = lambda_config.get("outputConfig", {})
        output_config = resolve_output_config(output_config, transaction_id)

        should_continue, state_payload = evaluate_stages(
            cur,
            schema,
            account_table,
            output_config
        )

        conn.commit()

        return build_polling_response(
            should_continue=should_continue,
            state_payload=state_payload
        )

    except Exception as e:
        logger.exception("Lambda execution failed")

        return build_polling_response(
            should_continue=False,
            state_payload={
                "error": str(e),
                "errorType": type(e).__name__
            }
        )

    finally:
        if cur:
            cur.close()
        if conn and pool_instance:
            pool_instance.putconn(conn)