import json
import boto3
import logging
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

# Custom Exceptions
class ConfigError(Exception):
    pass
class DatabaseError(Exception):
    pass
class S3WriteError(Exception):
    pass

# Step Function Contract Builder
def build_polling_response(should_continue: bool, state_payload: dict = None):
    """
        Enforces Step Function polling contract.

        Always returns:
        {
            "persistToState": {...},
            "shouldContinue": bool
        }
    """
    return {
        "persistToState": state_payload or {},
        "shouldContinue": bool(should_continue)
    }



# Configuration Validation
def validate_event(config):
    if "tableInfo" not in config:
        raise ConfigError("Missing required key: tableInfo")

    required_tableinfo_keys = [
        "dbkey",
        "dbname",
        "region",
        "schema",
        "account_table",
        "trid_table"
    ]

    for key in required_tableinfo_keys:
        if key not in config["tableInfo"]:
            raise ConfigError(f"Missing tableInfo key: {key}")



# Database Credential Helpers
def get_db_credentials(ssm_param_name, region):
    try:
        ssm = boto3.client("ssm", region_name=region)
        param = ssm.get_parameter(Name=ssm_param_name, WithDecryption=True)
        secret_name = param["Parameter"]["Value"]

        secret = secrets.get_secret_value(SecretId=secret_name)
        return json.loads(secret["SecretString"])
    except Exception as e:
        logger.exception("Failed to retrieve database credentials")
        raise DatabaseError("Unable to retrieve database credentials") from e


def initialize_db_pool(ssm_param_name, dbname, region):
    global db_pool

    if db_pool:
        return db_pool

    try:
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
    except Exception as e:
        logger.exception("Failed to initialize DB connection pool")
        raise DatabaseError("Database pool initialization failed") from e

def run_prerun_queries(cur, schema, account_table, trid_table):
    query = sql.SQL("SELECT {}.recon_prerun_queries(%s, %s, %s);").format(
        sql.Identifier(schema)
    )

    cur.execute(query, (schema, account_table, trid_table))
    return cur.fetchone()[0]

def should_lambda_continue(cur, schema, account_table):
    query = sql.SQL("SELECT {}.recon_should_continue(%s, %s);").format(
        sql.Identifier(schema)
    )

    cur.execute(query, (schema, account_table))
    return cur.fetchone()[0]


def run_orchestrator(
    cur,
    schema,
    account_table,
    trid_table,
    individual_batch_size,
    all_accounts_batch_size
):
    query = sql.SQL("""
        SELECT {}.recon_orchestrator(
            %s, %s, %s, %s, %s
        );
    """).format(sql.Identifier(schema))

    cur.execute(
        query,
        (
            schema,
            account_table,
            trid_table,
            individual_batch_size,
            all_accounts_batch_size
        )
    )

    return cur.fetchone()[0]

# Helper Utilities
def _safe_get(config, key, default=None):
    value = config.get(key)
    return value if value not in (None, "") else default


def _append_timestamp(filename, file_type):
    ts = datetime.utcnow().strftime("%Y-%m-%dT%H%M%S")
    if "." in filename:
        name = filename.rsplit(".", 1)[0]
    else:
        name = filename
    if file_type == "completed":
        new_ext = "trigger"
    elif file_type == "error":
        new_ext = "error"
    else:
        new_ext = "txt"
    return f"{name}_{ts}.{new_ext}"

# S3 Writer
def write_account_files_to_s3(
    completed_accounts,
    error_accounts,
    output_config
):
    result = {
        "completed_count": 0,
        "error_count": 0,
        "completed_file": None,
        "error_file": None
    }

    if "s3_bucket" not in output_config:
        raise ConfigError("Missing required config: s3_bucket")

    bucket = output_config["s3_bucket"]

    completed_prefix = _safe_get(output_config, "completed_prefix", "recon/completed")
    error_prefix = _safe_get(output_config, "error_prefix", "recon/error")

    completed_filename = _safe_get(
        output_config,
        "completed_filename",
        "completed_accounts.txt"
    )

    error_filename = _safe_get(
        output_config,
        "error_filename",
        "error_accounts.txt"
    )

    completed_accounts = completed_accounts or []
    error_accounts = error_accounts or []

    completed_filename = _append_timestamp(completed_filename, "completed")
    error_filename = _append_timestamp(error_filename, "error")

    try:
        if completed_accounts:
            key = f"{completed_prefix.rstrip('/')}/{completed_filename}"
            # body = "\n".join(completed_accounts)
            body = json.dumps(
                {"accountnumbers": completed_accounts}
            )
            s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=body.encode("utf-8")
            )
            result["completed_count"] = len(completed_accounts)
            result["completed_file"] = f"s3://{bucket}/{key}"

        if error_accounts:
            key = f"{error_prefix.rstrip('/')}/{error_filename}"
            body = "\n".join(error_accounts)
            s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=body.encode("utf-8")
            )
            result["error_count"] = len(error_accounts)
            result["error_file"] = f"s3://{bucket}/{key}"

        return result

    except Exception as e:
        logger.exception("Failed while writing account files to S3")
        raise S3WriteError("Error occurred during S3 file write") from e

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

# Lambda Handler
def lambda_handler(event, context):
    conn = None
    cur = None
    pool_instance = None

    try:
        lambda_config = event.get("LambdaConfig", {})
        validate_event(lambda_config)
        polling_attempt = event.get("PollingParams", {}).get("pollingAttempt", 0)

        tableinfo = lambda_config["tableInfo"]
        transaction_id = event.get("transactionId")
        output_config = lambda_config.get("outputConfig", {})
        output_config = resolve_output_config(output_config, transaction_id)

        processing_config = lambda_config.get("processingConfig", {})

        individual_batch_size = processing_config.get(
            "individual_batch_size", 5000
        )
        all_accounts_batch_size = processing_config.get(
            "all_accounts_batch_size", 5000
        )

        ssm_param_name = tableinfo["dbkey"]
        dbname = tableinfo["dbname"]
        region = tableinfo["region"]
        schema = tableinfo["schema"]

        account_table = tableinfo["account_table"]
        trid_table = tableinfo["trid_table"]

        pool_instance = initialize_db_pool(ssm_param_name, dbname, region)
        conn = pool_instance.getconn()
        cur = conn.cursor()

        if polling_attempt == 0:
            run_prerun_queries(cur, schema, account_table, trid_table)
            conn.commit()

        should_continue = should_lambda_continue(cur, schema, account_table)

        if not should_continue:
            conn.commit()
            return build_polling_response(
                should_continue=False,
                state_payload={
                    "state": "NONE",
                    "reason": "All accounts processed"
                }
            )

        orchestration_result = run_orchestrator(
            cur,
            schema,
            account_table,
            trid_table,
            individual_batch_size,
            all_accounts_batch_size
        )
        conn.commit()

        result_payload = orchestration_result.get("result", {})
        completed_accounts = result_payload.get("completed_accounts", [])
        error_accounts = result_payload.get("error_accounts", [])

        details_summary = {
            "completed_accounts_count": len(completed_accounts),
            "error_accounts_count": len(error_accounts)
        }

        s3_output = write_account_files_to_s3(
            completed_accounts,
            error_accounts,
            output_config
        )

        return build_polling_response(
            should_continue=True,
            state_payload={
                "state": orchestration_result.get("state"),
                "details": details_summary,
                "batch_config": {
                    "individual_batch_size": individual_batch_size,
                    "all_accounts_batch_size": all_accounts_batch_size
                },
                "s3_output": s3_output
            }
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