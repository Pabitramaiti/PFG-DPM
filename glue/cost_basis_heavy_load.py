import time
import json
import math
from decimal import Decimal

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
import sys
import boto3
import os
import splunk
from datetime import datetime
from collections import defaultdict

import psycopg2
import psycopg2.extras
from psycopg2 import sql


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

DB_KEY = None
REGION = None

job = Job(glueContext)
job.init("holding_processing_job", {})
NUM_PARTITIONS = 128
INSERT_BATCH_SIZE = 50

# CORRECT way in Glue Spark 3.5
sc = spark.sparkContext

processed_acc = sc.accumulator(0)
start_time = time.time()

LOG_EVERY = max(1, math.ceil(1))

# Main execution

header_record = {}

SCHEMA = "public"
DEST_TABLE_NAME=None

def get_db_connection(ssmdbkey, region):
    ssm = boto3.client('ssm', region_name=region)

    secret_name = ssm.get_parameter(Name=ssmdbkey, WithDecryption=True)
    secret_name = secret_name['Parameter']['Value']

    host = get_secret_values(secret_name)
    if os.getenv("schema"):
        SCHEMA = os.getenv("schema")
    os.environ["host"]=host['host']
    os.environ["port"]=host['port']
    os.environ["db_name"]=host['dbname']
    os.environ["username"]=host['username']
    os.environ["password"]=host['password']
    return psycopg2.connect(
        host=host['host'],
        port=host['port'],
        dbname=host['dbname'],
        user=host['username'],
        password=host['password'],
        connect_timeout=30
    )




def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:dpmdev-di-glue-load_stage:" + account_id

def get_jdbc_props():
    props = {
        "user": os.getenv("username"),
        "password": os.getenv("password"),
        "driver": "org.postgresql.Driver"
    }
    return props


def drop_table(conn,table_name):
    with conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS {SCHEMA}."{table_name}"')
    conn.commit()


def table_exists(conn, table_name):
    sql = """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_name = %s
        );
    """
    with conn.cursor() as cur:
        cur.execute(sql, (SCHEMA, table_name))
        return cur.fetchone()[0]

def process_holding_data(src_table):
    print("Processing holding data - started")
    predicates = [
        f"""
        MOD(ABS(HASHTEXT(key || tle_gl_sec_no)), {NUM_PARTITIONS}) = {i}
        AND "tle_gl_r_u_sw" != 'R'
        AND trid = '5075'
        """
        for i in range(NUM_PARTITIONS)
    ]

    df = spark.read.jdbc(
        url=f'jdbc:postgresql://{os.environ["host"]}/{os.environ["db_name"]}',
        table=f'{SCHEMA}."{src_table}"',
        predicates=predicates,
        properties=get_jdbc_props()
    )

    grouped_df = (
        df.groupBy("key", "tle_gl_sec_no")
        .agg(F.collect_list("value").alias("values"))
    )

    global LOG_EVERY
    # For calculating completion percentage

    grouped_df.foreachPartition(process_holding_partition)

    total_time = time.time() - start_time
    print(
        f"JOB COMPLETED in "
        f"{int(total_time // 3600):02d}:"
        f"{int((total_time % 3600) // 60):02d}:"
        f"{int(total_time % 60):02d}"
    )
    job.commit()

def convert_date(date_str):
    if not date_str or len(date_str) != 9:
        return None
    year = date_str[1:5]
    month = date_str[5:7]
    day = date_str[7:]
    try:
        return f"{year}-{month}-{day}"
    except:
        return None


def to_float(val):
    try:
        val = val.strip()
        if '.' in val:
            parts = val.split('.')
            whole = parts[0].lstrip('0') or '0'
            decimals = parts[1]
            decimal_len = len(decimals)
            combined = float(whole + decimals)
            return combined / (10 ** decimal_len)
        else:
            return float(val)
    except Exception:
        return 0.0

def get_5075_common_data(item):
    invalid_calc = False

    if (item.get("TLE-GL-R-U-SW") == "R" and (item.get("TLE-GL-L-S-SW") in ('L','S'))) and \
            item.get("TLE-GL-CCH-CD") == "Y" or \
            item.get("TLE-GL-LM-BUY-DATE-SW") == "1" or \
            item.get("TLE-GL-LM-COST-SW") == "1" or \
            item.get("TLE-GL-LM-MKTV-SW") == "1" or \
            item.get("TLE-GL-LM-GNAMT-SW") in ("1", "2"):
        invalid_calc = True

    common_data = {
        "purchaseDate": convert_date(item.get("TLE-GL-DATE-BUY")),
        "saleDate": convert_date(item.get("TLE-GL-DATE-SOLD")),
        "settlementDate": convert_date(item.get("TLE-GL-DATE-SETT")),
        "costBasis": "N/A" if invalid_calc else to_float(item.get("TLE-GL-COST")),
        "proceeds": to_float(item.get("TLE-GL-NET-PRCDS")),
        "realizedGainLoss": "N/A" if invalid_calc else to_float(item.get("TLE-GL-REALIZ-GL")),
        "quantity": to_float(item.get("TLE-GL-QUANTITY")),
        "realizedUnrealizedInd": item.get("TLE-GL-R-U-SW", ""),
        "longShortTermInd": item.get("TLE-GL-L-S-SW", ""),
        "gainLossInd": item.get("TLE-GL-G-L-SW", ""),
        "tleGLCchCd": item.get("TLE-GL-CCH-CD"),
        "tleGLPndgCd": item.get("TLE-GL-PNDG-CD"),
        "tleGLTrueZeroInd":item.get("TLE-GL-TRUE-ZERO-IND"),
        "tleGLReiInd": item.get("TLE-GL-REI-IND"),
        "tleTRID": item.get("XBASE-TLE-TRID-HI", "") + item.get("XBASE-TLE-TRID-LO", "")
    }
    return common_data

ACH_BKT_CODE_MAP={
    "RA":"totalRealizedGainLossYTD",
    "RD":"shortTermRealizedGainLossYTD",
    "RG":"longTermRealizedGainLossYTD"
}

def process_realised_gain_loss_summary_data(src_table):
    print("Processing realised gain/loss summary data - started")
    predicates = [
        f"""
        MOD(ABS(HASHTEXT(key)), {NUM_PARTITIONS}) = {i}
        AND trid = '1101' 
        """
        for i in range(NUM_PARTITIONS)
    ]

    df = spark.read.jdbc(
        url=f'jdbc:postgresql://{os.environ["host"]}/{os.environ["db_name"]}',
        table=f'{SCHEMA}."{src_table}"',
        predicates=predicates,
        properties=get_jdbc_props()
    )



    global LOG_EVERY
    # For calculating completion percentage

    df.foreachPartition(process_realised_gain_loss_summary_partition)

    total_time = time.time() - start_time
    print(
        f"JOB COMPLETED in "
        f"{int(total_time // 3600):02d}:"
        f"{int((total_time % 3600) // 60):02d}:"
        f"{int(total_time % 60):02d}"
    )
    job.commit()

def process_realised_gain_loss_summary_partition(rows):
    batch = []
    conn = get_db_connection(DB_KEY, REGION)
    local_count = 0
    try:
        for row in rows:
            transformed = transform_realized_gain_loss_summary_record(
                row["value"], row["key"]
            )

            batch.append({
                "key": transformed["key"],
                "value": json.dumps({"gainLossSummary": transformed["gainLossSummary"]})
            })

            if len(batch) >= INSERT_BATCH_SIZE:
                insert_key_value(conn,batch)
                batch.clear()

            # ---- progress tracking ----
            processed_acc.add(1)
            local_count += 1
            if local_count % 100 == 0:
                print(f"Executor processed {local_count} records in this partition")

        if batch:
            insert_key_value(conn,batch)

    except Exception as e:
        message = f"Error processing realized gain loss summary partition: {str(e)}"
        splunk.log_message({'Status': 'ERROR', 'Message': message}, get_run_id())
        raise Exception(message)

def transform_realized_gain_loss_summary_record(raw_json, key):
    dest_table_name = os.getenv("dest_table_name", "")
    global aggregatedTable
    record = {"recordId": "R001",
              "tleTRID": "1101"
              }
    records = []
    add_record = False
    json_item = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
    for item in json_item.get("TLE-ACH-BKT"):
        ach_bkt_code = f"{item.get('TLE-ACH-BKT-CODE1')}{item.get('TLE-ACH-BKT-CODE2')}"
        val = f"{item.get('TLE-ACH-BKT-VAL')}"
        if ach_bkt_code in ACH_BKT_CODE_MAP:
            record.update(
                {
                    ACH_BKT_CODE_MAP.get(ach_bkt_code): val,
                    "recordId": "R001",
                    "tleTRID": "1101"
                })
            add_record = True
    if add_record:
        records.append(record)
    return {"gainLossSummary": [
        {
            "totalRecords": len(records),
            "fileSource": "TLE",
            "records": records,

        },

    ],
        "key": f"{key}|GainLossSummary"
    }
def build_alternativeInvestments_desc(item):
    tle_ai_array = item.get("TLE-AI", [])
    desc_lines = []

    for desc_obj in tle_ai_array:
        line = desc_obj.get("TLE-AI-DESC", "").replace("*", "").strip()
        if line:
            desc_lines.append(line)

    # Join with space or line break based on your preference
    return desc_lines
def transform_alternate_investment_record(json_array, key):
    grouped = defaultdict(list)
    dest_table_name = os.getenv("dest_table_name","")
    global aggregatedTable
    set_key=False
    for raw_item in json_array:
        item = json.loads(raw_item) if isinstance(raw_item, str) else raw_item
        if not set_key:
            key = f"{key}|AlternativeInvestments"
            set_key = True
        sec_id = item.get("TLE-AI-SEC-NO", "")
        grouped[sec_id].append(item)

    result = []
    recordId = 0
    for sec_id, items in grouped.items():
        # Sort by XBASE-TLE-RECSEQ numerically
        items_sorted = sorted(items, key=lambda x: int(x.get("XBASE-TLE-RECSEQ", 0)))

        total_quantity = sum(to_float(x.get("TLE-AI-QUANTITY", "0")) for x in items_sorted)
        total_market_value = sum(to_float(x.get("TLE-AI-MKT-VAL", "0")) for x in items_sorted)

        last_item = items_sorted[-1]

        symbol = last_item.get("TLE-AI-SYMBOL", "")
        cusip = last_item.get("TLE-AI-CUSIP", "")
        price = to_float(last_item.get("TLE-AI-PRICE", "0"))
        no_of_desc = int(last_item.get("TLE-AI-DESC-NO", "0"))
        trid = last_item.get("XBASE-TRID","")
        descs = build_alternativeInvestments_desc(last_item)
        recordId=+1
        grouped_obj = {
            "securityId": sec_id,
            "symbol": symbol,
            "cusip": cusip,
            "noOfDesc": no_of_desc,
            "descLines": descs,
            "price": price,
            "quantity": total_quantity,
            "marketValue": total_market_value,
            "fileSource": "TLE",
            "tleTRID":trid,
            "recordId":f"R{str(recordId).zfill(3)}"
        }
        result.append(grouped_obj)

    return {
        "alternativeInvestments":{
        "fileSource": "TLE",
        "totalRecords": len(result),
        "records": result,
        },
        "key": key
    }
def process_alternate_investment_partition(rows):
    batch = []
    conn = get_db_connection(DB_KEY, REGION)
    local_count = 0
    try:
        for row in rows:
            transformed = transform_alternate_investment_record(
                row["values"], row["key"]
            )

            batch.append({
                "key": transformed["key"],
                "value": json.dumps({"alternativeInvestments": transformed["alternativeInvestments"]})
            })

            if len(batch) >= INSERT_BATCH_SIZE:
                insert_key_value(conn,batch)
                batch.clear()

            # ---- progress tracking ----
            processed_acc.add(1)
            local_count += 1
            if local_count % 100 == 0:
                print(f"Executor processed {local_count} records in this partition")

        if batch:
            insert_key_value(conn,batch)

    except Exception as e:
        message = f"Error processing alternate investment partition: {str(e)}"
        splunk.log_message({'Status': 'ERROR', 'Message': message}, get_run_id())
        raise Exception(message)
def process_alternate_investment_data(src_table):
    print("Processing alternate investment data - started")
    predicates = [
        f"""
        MOD(ABS(HASHTEXT(key)), {NUM_PARTITIONS}) = {i}
        AND trid = '5077'
        """
        for i in range(NUM_PARTITIONS)
    ]

    df = spark.read.jdbc(
        url=f'jdbc:postgresql://{os.environ["host"]}/{os.environ["db_name"]}',
        table=f'{SCHEMA}."{src_table}"',
        predicates=predicates,
        properties=get_jdbc_props()
    )

    grouped_df = (
        df.groupBy("key")
        .agg(F.collect_list("value").alias("values"))
    )

    global LOG_EVERY
    # For calculating completion percentage

    grouped_df.foreachPartition(process_alternate_investment_partition)

    total_time = time.time() - start_time
    print(
        f"JOB COMPLETED in "
        f"{int(total_time // 3600):02d}:"
        f"{int((total_time % 3600) // 60):02d}:"
        f"{int(total_time % 60):02d}"
    )
    job.commit()

def transform_grouped_holdings_record(json_array,key):
    lots=[]
    lot_id_counter = 0
    stop_further_lots = False
    set_key=False
    unrealizedShortTermGain= Decimal(0)
    unrealizedShortTermLoss= Decimal(0)
    unrealizedLongTermGain=Decimal(0)
    unrealizedLongTermLoss=Decimal(0)
    print("Grouped json array",json_array)
    for raw_item  in json_array:
        item = json.loads(raw_item) if isinstance(raw_item, str) else raw_item
        if not set_key:
            key = f"{key}|{item.get('TLE-GL-SEC-NO')}|Holdings"
            set_key = True
        if stop_further_lots:
            break
        common_data=get_5075_common_data(item)
        lots.append({"lotId": f"L{str(lot_id_counter).zfill(3)}", "fileId": f"L{str(lot_id_counter).zfill(3)}",
                     **common_data})
        lot_id_counter += 1
        if item.get("TLE-GL-CCH-CD") == 'Y' or item.get("TLE-GL-PNDG-CD") == 'Y':
            stop_further_lots = True
        if item.get("TLE-GL-COST") == '0' and item.get("TLE-GL-TRUE-ZERO-IND") == ' ':
            stop_further_lots = True

        ls = item.get("TLE-GL-L-S-SW", "").strip()
        gl = item.get("TLE-GL-G-L-SW", "").strip()
        gain_loss = Decimal(item.get("TLE-GL-REALIZ-GL", 0) or 0)
        if ls == 'S' and gl == 'G':
            unrealizedShortTermGain += gain_loss
        elif ls == 'S' and gl == 'L':
            unrealizedShortTermLoss += gain_loss
        elif ls == 'L' and gl == 'G':
            unrealizedLongTermGain += gain_loss
        elif ls == 'L' and gl == 'L':
            unrealizedLongTermLoss += gain_loss
    return {"holdings": [
                {
                    "totalLots": 999999999 if stop_further_lots else len(lots),
                    "fileSource": "TLE",
                    "lots": lots,
                    "unrealizedShortTermGain":float(unrealizedShortTermGain),
                    "unrealizedShortTermLoss":float(unrealizedShortTermLoss),
                    "unrealizedLongTermGain":float(unrealizedLongTermGain),
                    "unrealizedLongTermLoss":float(unrealizedLongTermLoss)
                },

            ],
        "key":key
    }

# Insert transformed batch

def insert_key_value(conn,batch):
    query = sql.SQL("""
        INSERT INTO {schema}.{table} (key, value)
        VALUES (%(key)s, %(value)s)
    """).format(
        schema=sql.Identifier(SCHEMA),
        table=sql.Identifier(DEST_TABLE_NAME),
    )
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(
            cur,
            query,
            batch,
            page_size=len(batch)
        )
    conn.commit()


def create_table_from_query(conn, table_name, query):
    """
    Create table if it does not exist using query metadata (no data).
    Equivalent to SQLAlchemy CREATE TABLE AS ... LIMIT 0
    """

    ddl = f'''
        CREATE TABLE IF NOT EXISTS {SCHEMA}."{table_name}" AS
        {query}
        LIMIT 0
    '''

    print(f"Ensuring destination table exists:\n{ddl}")

    with conn.cursor() as cur:
        cur.execute(ddl)

    conn.commit()
    print(f"Destination table ready: {SCHEMA}.{table_name}")

def copy_from_another_table(conn, target_table, query):
    """
    Insert data into target_table using a SELECT query.
    Equivalent to SQLAlchemy INSERT INTO table <query>
    """

    insert_sql = f'''
        INSERT INTO {SCHEMA}."{target_table}"
        {query}
    '''

    print(f"Inserting data using query:\n{insert_sql}")

    with conn.cursor() as cur:
        cur.execute(insert_sql)

    conn.commit()
    print(f"Data inserted into {SCHEMA}.{target_table}")


def process_holding_partition(rows):
    batch = []
    conn = get_db_connection(DB_KEY, REGION)
    local_count = 0
    try:
        for row in rows:
            transformed = transform_grouped_holdings_record(
                row["values"], row["key"]
            )

            batch.append({
                "key": transformed["key"],
                "value": json.dumps({"holdings": transformed["holdings"]})
            })

            if len(batch) >= INSERT_BATCH_SIZE:
                insert_key_value(conn,batch)
                batch.clear()

            # ---- progress tracking ----
            processed_acc.add(1)
            local_count += 1
            if local_count % 100 == 0:
                print(f"Executor processed {local_count} records in this partition")

        if batch:
            insert_key_value(conn,batch)

    except Exception as e:
        message = f"Error processing holding partition: {str(e)}"
        splunk.log_message({'Status': 'ERROR', 'Message': message}, get_run_id())
        raise Exception(message)

def get_secret_values(secret_name):
    try:
        client = boto3.client("secretsmanager")
        response = client.get_secret_value(SecretId=secret_name)
        secret_value = response['SecretString']
        json_secret_value = json.loads(secret_value)
        splunk.log_message({'Message': f"Secret values retrieved successfully for {secret_name}.", "Status": "SUCCESS"},
                           get_run_id())

        return json_secret_value
    except Exception as e:
        splunk.log_message({'Message': f"Error retrieving secret values for {secret_name}: {e}", "Status": "ERROR"},
                           get_run_id())
        raise e

def set_job_params_as_env_vars():
    print("sys.argv", sys.argv)
    # Loop through all command-line arguments starting from the second argument (skip the script name)
    i = 1
    while i < len(sys.argv):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]  # Remove the leading '--'
            if i + 1 < len(sys.argv) and not sys.argv[i + 1].startswith('--'):
                value = sys.argv[i + 1]
                print(f"key:{key}")
                print(f"value:{value}")
                os.environ[key] = value
                i += 2  # Move to the next key-value pair
            else:
                # Handle cases where the key doesn't have a value
                # print(f"Key '{key}' does not have a value.")
                i += 1  # Move to the next key
        else:
            i += 1  # Move to the next argument
def fetch_header_trid(conn,src_table):
    query = f'''
        SELECT key, value
        FROM {SCHEMA}."{src_table}"
        WHERE trid = %s
    '''

    with conn.cursor() as cur:
        cur.execute(query, ('0000',))
        return cur.fetchall()

def load_header_trid(src_table):
    global header_record
    print("loading header trid")
    conn = get_db_connection(DB_KEY, REGION)
    header_trid = fetch_header_trid(conn,src_table)
    if len(header_trid) < 1:
        raise Exception("Header TRID cannot be empty.")
    for key, value in header_trid:
        header_record = value
        print("header trid is loaded", header_record)
        break

def create_tables(conn):
    move_table=os.getenv("move_table_if_exists","false")
    dest_table_name=os.getenv("dest_table_name")
    if table_exists(conn,dest_table_name) and move_table.lower()=="true":
        print("Table already exist. So creating a temp table and moving the current data")
        now = datetime.now()
        formatted_date = now.strftime("%d%m%y%H")
        temp_table_name = f"{dest_table_name}_{formatted_date}"
        select_source_query = f"SELECT * from \"{dest_table_name}\""
        drop_table(conn,temp_table_name)
        print("Creating temp table")
        create_table_from_query(conn,temp_table_name,select_source_query)
        copy_from_another_table(conn,temp_table_name,select_source_query)
        print(f"Moved data from {dest_table_name} to {temp_table_name}")

    drop_table(conn,dest_table_name)
    ddl = f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}."{dest_table_name}" (
            key   TEXT PRIMARY KEY,
            value JSONB
        );
    """
    print(f"Ensuring table exists:\n{ddl}")
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    print(f"Table ready: {SCHEMA}.{dest_table_name}")
    print("Table created successfully")
    return dest_table_name

def create_aggregated_table(conn):
    print("Creating aggregated table")
    global DEST_TABLE_NAME
    create_tables(conn)

def fetch_holding_json_one_row(conn,src_table):
    query = f'''
        SELECT key, value AS value
        FROM {SCHEMA}."{src_table}"
        WHERE "tle_gl_r_u_sw" != 'R'
          AND trid = '5075'
        LIMIT 1 OFFSET 0
    '''

    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()

def create_cost_basis_table(src_table):
    conn = get_db_connection(DB_KEY, REGION)
    create_aggregated_table(conn)

def getDescSet(descJsonList):
    descSet=[]
    for des_val in descJsonList:
        descSet.append(des_val.get("TLE-GL-DESC").replace("*", ""))
    return descSet
def map_indicator(val, valid_set):
    return 'Y' if val in valid_set else 'N'
def transform_grouped_realized_gain_loss_record(json_array, key):
    records = []
    record_id_counter = 1
    set_key=False
    dest_table_name = os.getenv("dest_table_name","")
    for raw_item in json_array:
        item = json.loads(raw_item) if isinstance(raw_item, str) else raw_item
        if not set_key:
            key = f"{key}|RealizedGainLoss"
            set_key=True
        if item.get("TLE-GL-LM-BUY-DATE-SW") == "1" or \
           item.get("TLE-GL-LM-COST-SW") == "1" or item.get("TLE-GL-LM-MKTV-SW") == "1" or \
           item.get("TLE-GL-LM-GNAMT-SW") in ("1", "2"):
            if item.get("TLE-GL-L-S-SW", "").strip() == "":
                item["TLE-GL-L-S-SW"] = "S"

        common_data = get_5075_common_data(item)
        record = {
            "recordId": f"R{str(record_id_counter).zfill(3)}",
            "securityId": item.get("TLE-GL-SEC-NO", ""),
            "cusip": item.get("TLE-GL-CUSIP", ""),
            "symbol": (
                item.get("TLE-GL-SYMBOL", "").replace(".", " ")[1:]
                if item.get("TLE-GL-MSD-TYPE") == "OPT" and item.get("TLE-GL-SYMBOL", "").startswith(("X", "Q"))
                else item.get("TLE-GL-SYMBOL", "").replace(".", " ")
            ),
            "noOfDesc": int(item.get("TLE-GL-DESC-NO", 0)),
            "descLines": getDescSet(item.get("TLE-GL", [])),
            "gainLossVSPInd": map_indicator(item.get("TLE-VSP-PRCHS-IND"), {"V", "H", "O", "I", "B", "C", "D"}),
            "gainLossErrCCH": map_indicator(item.get("TLE-GL-CCH-CD"), {"Y"}),
            "gainLossErrDB": map_indicator(item.get("TLE-GL-LM-BUY-DATE-SW"), {"1"}),
            "gainLossErrCost": map_indicator(item.get("TLE-GL-LM-COST-SW"), {"1"}),
            "gainLossErrNP": map_indicator(item.get("TLE-GL-LM-GNAMT-SW"), {"1", "2"}),
            "gainLossErrGI": map_indicator(item.get("TLE-GL-GIFT-IND"), {"G"}),
            "gainLossErrII": map_indicator(item.get("TLE-GL-INHRTN-IND"), {"I"}),
            **common_data
        }
        records.append(record)
        record_id_counter += 1
    return {
        "realizedGainLoss": [
            {
                "fileSource": "TLE",
                "totalRecords": len(records),
                "records": records
            },
        ],
        "key":key
    }

def process_realized_gain_loss_partition(rows):
    batch = []
    conn = get_db_connection(DB_KEY, REGION)
    local_count = 0
    try:
        for row in rows:
            transformed = transform_grouped_realized_gain_loss_record(
                row["values"], row["key"]
            )

            batch.append({
                "key": transformed["key"],
                "value": json.dumps({"realizedGainLoss": transformed["realizedGainLoss"]})
            })

            if len(batch) >= INSERT_BATCH_SIZE:
                insert_key_value(conn,batch)
                batch.clear()

            # ---- progress tracking ----
            processed_acc.add(1)
            local_count += 1
            if local_count % 100 == 0:
                print(f"Executor processed {local_count} records in this partition")

        if batch:
            insert_key_value(conn,batch)

    except Exception as e:
        message = f"Error processing realized gain loss partition: {str(e)}"
        splunk.log_message({'Status': 'ERROR', 'Message': message}, get_run_id())
        raise Exception(message)

def process_realized_gain_loss_data(src_table):
    print("Processing realized gain/loss data - started")
    predicates = [
        f"""
        MOD(ABS(HASHTEXT(key)), {NUM_PARTITIONS}) = {i}
        AND "tle_gl_r_u_sw" = 'R'
        AND trid = '5075'
        """
        for i in range(NUM_PARTITIONS)
    ]

    df = spark.read.jdbc(
        url=f'jdbc:postgresql://{os.environ["host"]}/{os.environ["db_name"]}',
        table=f'{SCHEMA}."{src_table}"',
        predicates=predicates,
        properties=get_jdbc_props()
    )

    grouped_df = (
        df.groupBy("key")
        .agg(F.collect_list("value").alias("values"))
    )

    global LOG_EVERY
    # For calculating completion percentage

    grouped_df.foreachPartition(process_realized_gain_loss_partition)

    total_time = time.time() - start_time
    print(
        f"JOB COMPLETED in "
        f"{int(total_time // 3600):02d}:"
        f"{int((total_time % 3600) // 60):02d}:"
        f"{int(total_time % 60):02d}"
    )
    job.commit()

if __name__ == "__main__":
    try:
        print("Setting params")
        set_job_params_as_env_vars()
        print("params execute successfully")

    except Exception as e:
        message = f"failed to retrieve input parameters. {str(e)}"
        splunk.log_message({'Status': 'failed', 'InputFileName': 'Test', 'Message': message}, get_run_id())
        raise Exception(message)

    src_table = os.getenv("src_table")
    batch_size = os.getenv("batch_size")
    start_time = time.time()
    SCHEMA=os.getenv("schema")
    DB_KEY = os.getenv("dbkey")
    REGION = os.getenv("region")
    type_of_data = os.getenv("type_of_data")
    DEST_TABLE_NAME = os.getenv("dest_table_name")
    delete_source_table = os.getenv("delete_source_table")
    if batch_size is not None and batch_size != "":
        INSERT_BATCH_SIZE = int(batch_size)
    load_header_trid(src_table)
    print("Processing holding data")
    if type_of_data == "holdings":
        # Creating table
        create_cost_basis_table(src_table)
        process_holding_data(src_table)
    if type_of_data == "realized_gain_loss":
        time.sleep(2 * 60)
        process_realized_gain_loss_data(src_table)
    if type_of_data  == "realized_gain_loss_summary":
        time.sleep(2 * 60)
        process_realised_gain_loss_summary_data(src_table)
    if type_of_data  == "alternative_investments":
        time.sleep(2 * 60)
        process_alternate_investment_data(src_table)
    conn = get_db_connection(DB_KEY,  REGION)
    print("Dropping source table", src_table)
    if bool(delete_source_table):
        drop_table(conn, src_table)
    job.commit()
