import json
import boto3
from psycopg2 import pool
from psycopg2.extras import Json

# AWS clients
secrets = boto3.client("secretsmanager")

# Global DB pool (reused across warm invocations)
db_pool = None


def parse_json_field(value, field_name):
    try:
        return json.loads(value)
    except Exception as e:
        raise ValueError(f"Invalid JSON in field '{field_name}': {e}")


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
        maxconn=10,
        host=creds["host"],
        port=creds["port"],
        user=creds["username"],
        password=creds["password"],
        dbname=dbname
    )

    return db_pool


def lambda_handler(event, context):
    conn = None
    cur = None
    pool_instance = None

    try:
        # Parse inputs
        party_cfg = parse_json_field(event["partyids_list"], "partyids_list")
        party_account_and_clientid = party_cfg["partyidentifiervalue_list"]
        historical_table_name = party_cfg.get("historical_table_name", "")
        staging_table_name = party_cfg.get("staging_table_name", "")
        recon_status = event["recon_status"]

        sql_queries = parse_json_field(event["sql_queries"], "sql_queries")
        update_recon_table_query = sql_queries["update_recon_table"]
        delete_historical_records_query = sql_queries.get("delete_historical_records", "")
        delete_staging_records_query = sql_queries.get("delete_staging_records", "")
        clientID = sql_queries.get("clientID", "")

        tableinfo = parse_json_field(event["tableInfo"], "tableInfo")
        ssm_param_name = tableinfo["dbkey"]
        dbname = tableinfo["dbname"]
        region = tableinfo["region"]
        schema = tableinfo["schema"]

        # Initialize pool and get connection
        pool_instance = initialize_db_pool(ssm_param_name, dbname, region)
        conn = pool_instance.getconn()
        cur = conn.cursor()

        # Secure search_path
        cur.execute(f"SET search_path TO {schema}")

        # Build account info list
        account_info_list = []
        for value in party_account_and_clientid:
            try:
                record = {
                    "account_number": "",
                    "client_id": "",
                    "extraction_status": recon_status
                }
                
                if "-" in value:
                    account_number, client_id = value.split("-")
                    record["account_number"] = account_number
                    record["client_id"] = client_id
                else:
                    account_number = value
                    record["account_number"] = account_number
                    if clientID:
                        record["client_id"] = clientID
                    else:
                        record["client_id"] = "000"
                
                account_info_list.append(record)
            except ValueError:
                print(f"Invalid party identifier format: {value}")

        if not account_info_list:
            return {"status": "No valid accounts to update"}

        print("account_info_list:", json.dumps(account_info_list, indent=2))

        if recon_status.upper() == "FAILED":
            # ---- DELETE historical records ----
            if delete_historical_records_query:
                delete_historical_sql = delete_historical_records_query.format(historicaltablename=historical_table_name)
                cur.execute(delete_historical_sql)

            # ---- DELETE staging records ----
            if delete_staging_records_query:
                delete_staging_sql = delete_staging_records_query.format(stagingtablename=staging_table_name)
                cur.execute(delete_staging_sql)
        
        # Execute DB update
        cur.execute(
            update_recon_table_query,
            {"accountinfo": Json(account_info_list)}
        )
        conn.commit()

        return {
            "status": "RECON is updated"
        }

    except Exception as e:
        if conn:
            conn.rollback()
        return {
            "status": "FAILED",
            "error": str(e)
        }

    finally:
        if cur:
            cur.close()
        if conn and pool_instance:
            pool_instance.putconn(conn)
