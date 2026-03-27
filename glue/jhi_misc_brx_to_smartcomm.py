import sys, os, json, boto3, datetime, re, time
from multiprocessing.connection import Client
from asyncio.log import logger
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from awsglue.utils import getResolvedOptions

s3 = boto3.client('s3')
ecs_client = boto3.client("ecs")

POLL_INTERVAL = 30  # seconds

def get_environment(bucket_name):
    """Determine environment from bucket name"""
    bucket_lower = bucket_name.lower()
    if 'icsprod' in bucket_lower or 'icsprd' in bucket_lower:
        return "dat"
    else:
        return "tst"

def _strip(v):
    if v is None:
        return ""
    return str(v).strip()

def _comm_lookup(comm_list, key, default=""):
    if not isinstance(comm_list, list):
        return default
    for d in comm_list:
        if d.get("CommunicationCustomKey") == key:
            val = d.get("CommunicationCustomValue", "")
            if isinstance(val, str) and val.replace('"', '').strip() == "":
                return default
            return _strip(val)
    return default

def _comm_lookup_all(comm_list, key):
    """Get all values for a given key from CommunicationCustom (returns list)"""
    if not isinstance(comm_list, list):
        return []
    values = []
    for d in comm_list:
        if d.get("CommunicationCustomKey") == key:
            val = d.get("CommunicationCustomValue", "")
            if isinstance(val, str) and val.replace('"', '').strip() != "":
                values.append(_strip(val))
    return values

def _comm_date(comm_list, date_type):
    """Extract date value from CommunicationDate"""
    if not isinstance(comm_list, list):
        return ""
    for comm in comm_list:
        for date_entry in (comm.get("CommunicationDate") or []):
            if date_entry.get("CommunicationDateType") == date_type:
                return _strip(date_entry.get("CommunicationDateValue", ""))
    return ""

def _party_identifier(party_list, party_id_match=None, id_type=None):
    if not isinstance(party_list, list):
        return ""
    candidates = party_list
    if party_id_match is not None:
        candidates = [p for p in party_list if p.get("PartyId") == party_id_match]
    for p in candidates:
        for pid in (p.get("PartyIdentifier") or []):
            if id_type is None or pid.get("PartyIdentifierType") == id_type:
                return _strip(pid.get("PartyIdentifierValue", ""))
    return ""

def _party_date(party_list, date_type):
    if not isinstance(party_list, list):
        return ""
    for p in party_list:
        for d in (p.get("PartyDate") or []):
            if d.get("PartyDateType") == date_type:
                return _strip(d.get("PartyDateValue", ""))
    return ""

def _party_address_lines(party_list, address_type="SendAddress"):
    if not isinstance(party_list, list):
        return []
    for p in party_list:
        for addr in (p.get("PartyContactMailingAddress") or []):
            if addr.get("PartyContactAddressType") == address_type:
                lines = []
                for i in range(1, 8):
                    line = _strip(addr.get(f"PartyContactAddressLine{i}", ""))
                    if line:
                        lines.append(line)
                return lines
    return []

def _party_contact_employer(party_list):
    """Extract employer name from PartyContact"""
    if not isinstance(party_list, list):
        return ""
    for p in party_list:
        for contact in (p.get("PartyContact") or []):
            if contact.get("PartyContactType") == "PartyContact":
                return _strip(contact.get("PartyContactEmployerName", ""))
    return ""

def _transaction_field_for_instrument(trans_list, instrument_id, group, type_field, type_value, value_field):
    if not isinstance(trans_list, list):
        return ""
    tx = next((t for t in trans_list if t.get("TransactionId") == instrument_id), None)
    if not tx:
        return ""
    group_val = tx.get(group)
    if group == "TransactionPercentage":
        if isinstance(group_val, dict) and "item" in group_val:
            group_iter = [group_val["item"]]
        elif isinstance(group_val, list):
            group_iter = group_val
        else:
            group_iter = []
    else:
        group_iter = group_val if isinstance(group_val, list) else []
    for d in group_iter:
        if d.get(type_field) == type_value:
            return _strip(d.get(value_field, ""))
    return ""

def _extract_grp_inv_total(trans_list, instrument_ids):
    if not isinstance(trans_list, list):
        return ""
    for t in trans_list:
            # Check TransactionAmount array
            for amt in (t.get("TransactionAmount") or []):
                if amt.get("TransactionAmountType") == "TotalNetAmount":
                    value = amt.get("TransactionAmountValue", "")
                    return _strip(value)
    return ""

def _first_tx_classification_value(trans_list, type_value):
    if not isinstance(trans_list, list):
        return ""
    for t in trans_list:
        for c in (t.get("TransactionClassification") or []):
            if c.get("TransactionClassificationType") == type_value:
                return _strip(c.get("TransactionClassificationValue", ""))
    return ""

def get_matching_s3_keys(bucket, prefix='', pattern=''):
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    keys = []
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            full_key = obj['Key']
            file_name = os.path.basename(full_key)
            
            if '/transformed/' in full_key or file_name.endswith('.done.json'):
                continue
                
            if re.match(pattern, file_name):
                keys.append(full_key)
                print(f"Found matching file: {full_key}")
    
    return keys

# ====================inits====functionality=====================================
def wait_for_task_completion(ecs_client, cluster, task_arn, timeout=3600):
    """Poll ECS task status until completion."""
    start = time.time()
    while True:
        try:
            response = ecs_client.describe_tasks(cluster=cluster, tasks=[task_arn])

            failures = response.get("failures") or []
            if failures:
                raise RuntimeError(f"describe_tasks failures for {task_arn}: {failures}")

            tasks = response.get("tasks", [])
            if not tasks:
                raise RuntimeError(f"No task info found for {task_arn}")

            task = tasks[0]
            status = task.get("lastStatus")

            if status == "STOPPED":
                reason = task.get("stoppedReason", "Unknown")
                containers = task.get("containers", [])

                for container in containers:
                    exit_code = container.get("exitCode")  # don't default to 0
                    if exit_code not in (0,):
                        logger.error(f"Container failed: exitCode={exit_code}, reason={reason}")
                        raise RuntimeError(f"Task failed with exit code {exit_code}: {reason}")
                break

            if time.time() - start > timeout:
                raise TimeoutError(f"ECS task timeout after {timeout}s: {task_arn}")
            print("response :=>", response)
            time.sleep(POLL_INTERVAL)

        except Exception as e:
            logger.error(f"Error describing ECS task: {e}")
            raise



def ints_brx_adaptor(in_key):
    try:
        path_parts = in_key.split('/')
        transaction_id = path_parts[3]
    
        inputKey = in_key
        input_dir = os.path.dirname(in_key)
        outputKey = f"{input_dir}/transformed/{os.path.basename(inputKey)}"
        
        command = [
               "java", "-jar", "brxadaptor-0.0.1.jar",
               "-inputBucket", os.getenv("bucketName"),
               "-outputKey", outputKey,
               "-outputBucket", os.getenv("bucketName"),
               "-inputKey", inputKey,
               "-outputReportKey", os.getenv("output_report_key")
        ]
        
        print(f"ECS Command: {' '.join(command)}")
        
        container_name = os.getenv("container_name")
        cluster = os.getenv("cluster_name")
        task_def = os.getenv("task_definition")
        response = ecs_client.run_task(
            cluster=cluster,
            taskDefinition=task_def,
            launchType="EC2",
            overrides={
                "containerOverrides": [
                    {
                        "name": container_name,
                        "command": command
                    }
                ]
            },
            count=1,
        )
        failures = response.get("failures", [])
        if failures:
            logger.error(f"ECS run_task failures: {failures}")
            raise RuntimeError("ECS run_task failed.")

        task_arn = response["tasks"][0]["taskArn"]
        print(f"Task ARN: {task_arn}")
        wait_for_task_completion(ecs_client, cluster, task_arn)

    except Exception as e:
        raise RuntimeError(str(e))


def main():

    bucket = os.getenv('bucketName')
    input_path = os.getenv('input_key').strip('/')
    input_pattern = os.getenv('input_pattern')
    output_path = os.getenv('output_key').strip('/')
    transaction_id = os.getenv('transactionId')


    environment = get_environment(bucket)
    print(f"Environment detected: {environment}")
    print(f"Searching for files in s3://{bucket}/{input_path}/ with pattern: {input_pattern}")
    matching_files = get_matching_s3_keys(bucket, f"{input_path}/", input_pattern)

    if transaction_id:
        matching_files = [f for f in matching_files if f"/{transaction_id}/" in f]
        print(f"Filtered to {len(matching_files)} file(s) for transaction {transaction_id}")

    if not matching_files:
        print("No matching files found for this transaction")
        sys.exit(0)

    print(f"Found {len(matching_files)} file(s) to process")

    for in_key in matching_files:
        print(f"\nProcessing: s3://{bucket}/{in_key}")
        
        file_name = os.path.basename(in_key)
        file_dir = os.path.dirname(in_key)
        is_int_file = bool(re.match(r"^JSC\.INT", file_name))
        if is_int_file:
            ints_brx_adaptor(in_key)
            return

        if file_dir != input_path:
            relative_path = file_dir.replace(f"{input_path}/", "")
            if '/' not in relative_path:
                out_key = f"{file_dir}/transformed/{file_name}"
            else:
                out_key = f"{file_dir}/transformed/{file_name}"
        else:
            out_key = f"{output_path}/{file_name}"
        
        try:
            print(f"Loading input file: {in_key}")
            raw = s3.get_object(Bucket=bucket, Key=in_key)['Body'].read().decode('utf-8')
            data = json.loads(raw)
            records = data.get('root', data)
            if isinstance(records, dict):
                records = [records]
            
            out_docs = []
            
            for idx, rec in enumerate(records, start=1):
                cust = rec.get("customer", {}) or {}
                comm_full_list = cust.get("Communication", []) or []
                comm_list = (comm_full_list[0] if comm_full_list else {}).get("CommunicationCustom", []) or []
                party_list = cust.get("Party", []) or []
                trans_list = cust.get("Transaction", []) or []
                instr_list = cust.get("Instrument", []) or []
                
                product       = _comm_lookup(comm_list, "UC_DOCUMENT_TYPE")
                mailing_address_lines = _party_address_lines(party_list)
                ph_subdocument_indices = _comm_lookup_all(comm_list, "PH_SUBDOCUMENT_COMPOSITE_INDEX")
                group_name    = _party_identifier(party_list, party_id_match=1, id_type="AccountGroupName")
                group_number  = _party_identifier(party_list, party_id_match=1, id_type="AccountGroupIdentifier")
                last_trade_dt = _party_date(party_list, "LastTradeDate")
                payment       = _first_tx_classification_value(trans_list, "PaymentMethod")
                header_date= _comm_date(comm_full_list, "DocumentBusinessDate")
                contact_name = _party_contact_employer(party_list)
                
                return_address_lines = [
                    "PO BOX 219109",
                    "Kansas City, MO 64121-9109",
                    ""
                ]
                
                check_number  = ""
                for t in trans_list:
                    for d in (t.get("TransactionIdentifier") or []):
                        if d.get("TransactionIdentifierType") == "CheckNumber":
                            check_number = _strip(d.get("TransactionIdentifierValue", ""))
                            break
                    if check_number:
                        break
                
                shareholder_funds = {}
                
                for inst in instr_list:
                    instrument_id = inst.get("InstrumentId")
                    share_holder = ""
                    instrument_ref = ""
                    for idr in (inst.get("InstrumentIdentifier") or []):
                        if idr.get("InstrumentIdentifierType") == "UniqueReference":
                            share_holder = _strip(idr.get("InstrumentIdentifierNarrative", ""))
                            instrument_ref = _strip(idr.get("InstrumentIdentifierValue", ""))
                            break
                    
                    if not share_holder:
                        continue
                    
                    if share_holder not in shareholder_funds:
                        shareholder_funds[share_holder] = {
                            "fund_number": "",
                            "funds": [],
                            "instrument_ids": []
                        }
                    
                    shareholder_funds[share_holder]["instrument_ids"].append(instrument_id)
                    
                    inst_common = inst.get("InstrumentCommon", [])
                    if inst_common and len(inst_common) > 0:
                        fund_name = _strip(inst_common[0].get("InstrumentName", ""))
                        
                        # Party that matches BOTH name AND instrument reference
                        matching_party_id = None
                        fund_number = ""

                        for p in party_list:
                            # checking if Party matches the shareholder name
                            for contact in (p.get("PartyContact") or []):
                                if contact.get("PartyContactType") == "ActualOwner":
                                    party_shareholder = _strip(contact.get("PartyContactFullName", ""))
                                    if party_shareholder.strip() == share_holder.strip():
                                        # checking if PartyIdentifierValue starts with the instrument_ref
                                        for pid in (p.get("PartyIdentifier") or []):
                                            if pid.get("PartyIdentifierType") == "Internal":
                                                party_id_value = _strip(pid.get("PartyIdentifierValue", ""))
                                                if party_id_value.startswith(f"{instrument_ref}-"):
                                                    matching_party_id = p.get("PartyId")
                                                    fund_number = party_id_value
                                                    break
                                        if matching_party_id:
                                            break
                            if matching_party_id:
                                break
                        
                        year_cp = _transaction_field_for_instrument(
                            trans_list, instrument_id, "TransactionIdentifier",
                            "TransactionIdentifierType", "SpecialInstructionCode",
                            "TransactionIdentifierValue"
                        )
                        
                        percent_val = _transaction_field_for_instrument(
                            trans_list, instrument_id, "TransactionPercentage",
                            "TransactionPercentageType", "InvestmentPercentage",
                            "TransactionPercentageValue"
                        )
                        
                        if percent_val == '""' or percent_val == '\"\"':
                            percent_val = ""
                        
                        invested_amt = _transaction_field_for_instrument(
                            trans_list, instrument_id, "TransactionAmount",
                            "TransactionAmountType", "InvestmentAmount",
                            "TransactionAmountValue"
                        )
                        
                        shareholder_funds[share_holder]["funds"].append({
                            "FundName": fund_name,
                            "FundNumber": fund_number,
                            "YearCP": year_cp,
                            "Percent": percent_val,
                            "TotalAmount": "",
                            "InvestedAmount": _strip(invested_amt),
                        })
                
                funds_data = []
                for share_holder, data in shareholder_funds.items():
                    funds_data.append({
                        "ShareHolder": { "value": share_holder },
                        "FundsList": data["funds"]
                    })
                
                tle = {
                    "DOC_ACCOUNT_NUMBER": _comm_lookup(comm_list, "DOC_ACCOUNT_NUMBER"),
                    "DOC_PRODUCT_CODE": _comm_lookup(comm_list, "DOC_PRODUCT_CODE"),
                    "DOC_SPECIAL_HANDLING_CODE": _comm_lookup(comm_list, "DOC_SPECIAL_HANDLING_CODE"),
                    "DOC_SEND_ADDR_LINE1": _comm_lookup(comm_list, "DOC_SEND_ADDR_LINE1"),
                    "DOC_SEND_ADDR_LINE2": _comm_lookup(comm_list, "DOC_SEND_ADDR_LINE2"),
                    "DOC_SEND_ADDR_LINE3": _comm_lookup(comm_list, "DOC_SEND_ADDR_LINE3"),
                    "DOC_SEND_ADDR_LINE4": _comm_lookup(comm_list, "DOC_SEND_ADDR_LINE4"),
                    "DOC_SEND_ADDR_LINE5": _comm_lookup(comm_list, "DOC_SEND_ADDR_LINE5"),
                    "DOC_SEND_ADDR_LINE6": _comm_lookup(comm_list, "DOC_SEND_ADDR_LINE6"),
                    "DOC_SEND_ADDR_LINE7": _comm_lookup(comm_list, "DOC_SEND_ADDR_LINE7"),
                    "DOC_SEND_ADDR_ZIP_CODE": _comm_lookup(comm_list, "DOC_SEND_ADDR_ZIP_CODE"),
                    "UR_CLIENT_ID": _comm_lookup(comm_list, "UR_CLIENT_ID"),
                    "UR_PRODUCT_ID": _comm_lookup(comm_list, "UR_PRODUCT_ID"),
                    "DM_SOURCE_SYSTEM_ID": _comm_lookup(comm_list, "DM_SOURCE_SYSTEM_ID"),
                    "DM_SOURCE_SYSTEM_BRAND_ID": _comm_lookup(comm_list, "DM_SOURCE_SYSTEM_BRAND_ID"),
                    "UC_DOCUMENT_TYPE": product,
                    "UC_TAX_ID": _comm_lookup(comm_list, "UC_TAX_ID"),
                    "UC_ACCOUNT_NUMBER": _comm_lookup(comm_list, "UC_ACCOUNT_NUMBER"),
                    "UC_DOCUMENT_DATE": _comm_lookup(comm_list, "UC_DOCUMENT_DATE"),
                    "UC_CUSTOMER_NAME": _comm_lookup(comm_list, "UC_CUSTOMER_NAME"),
                    "UC_ADDRESS_01": _comm_lookup(comm_list, "UC_ADDRESS_01"),
                    "UC_ADDRESS_02": _comm_lookup(comm_list, "UC_ADDRESS_02"),
                    "UC_ADDRESS_03": _comm_lookup(comm_list, "UC_ADDRESS_03"),
                    "UC_ADDRESS_04": _comm_lookup(comm_list, "UC_ADDRESS_04"),
                    "UC_ADDRESS_05": _comm_lookup(comm_list, "UC_ADDRESS_05"),
                    "UC_ADDRESS_06": _comm_lookup(comm_list, "UC_ADDRESS_06"),
                    "UC_ADDRESS_07": _comm_lookup(comm_list, "UC_ADDRESS_07"),
                    "UC_DOCUMENT_CLASSIFICATION_01": _comm_lookup(comm_list, "UC_DOCUMENT_CLASSIFICATION_01"),
                    "UC_CUSTOMER_EMAIL_ADDRESS": _comm_lookup(comm_list, "UC_CUSTOMER_EMAIL_ADDRESS"),
                    "UC_DOCUMENT_DELIVERY_PREFERENCE": _comm_lookup(comm_list, "UC_DOCUMENT_DELIVERY_PREFERENCE"),
                    "PS_DOC_PRODUCT_CODE": _comm_lookup(comm_list, "PS_DOC_PRODUCT_CODE"),
                    "DOC_BUNDLE_KEY": _comm_lookup(comm_list, "DOC_BUNDLE_KEY"),
                    "DOC_SORT_KEY": _comm_lookup(comm_list, "DOC_SORT_KEY"),
                    "FUND_SPONCER_ID": _comm_lookup(comm_list, "FUND_SPONCER_ID"),
                    "UC_CORP": _comm_lookup(comm_list, "UC_CORP")
                }
                
                all_instrument_ids = []
                for data in shareholder_funds.values():
                    all_instrument_ids.extend(data["instrument_ids"])
                    
                grp_inv_total_value = _extract_grp_inv_total(trans_list, all_instrument_ids)
                
                out_docs.append({
                    "Customer": {
                        "CustomerID": str(idx),
                        "Product": product,
                        "AFPFileIndicator": "AFP",
                        "Environment": environment,
                        "HeaderDate": header_date,
                        "ReturnAddress": { "value": return_address_lines },
                        "MailingAddress": {
                            "value": mailing_address_lines  
                        },
                        "GroupName": { "value": group_name },
                        "GroupNumber": { "value": group_number },
                        "Contact": { "value": contact_name },
                        "PurchaseDate": { "value": last_trade_dt },
                        "Payment": { "value": payment },
                        "CheckNumber": { "value": check_number },
                        "FundsData": { "value": funds_data },
                        "GrpInvTotal": { "value": grp_inv_total_value },
                        "TLE": tle,
                        "MultiNOP": {
                            "PH_SUBDOCUMENT_COMPOSITE_INDEX": ph_subdocument_indices
                        }
                    }
                })
            
            final_payload = {"Miscdocs": out_docs}
            out_json = json.dumps(final_payload, indent=2)
            
            print(f"Writing output to: {out_key}")
            s3.put_object(Bucket=bucket, Key=out_key, Body=out_json, ContentType='application/json')
            print(f"Successfully transformed: {in_key} → {out_key}")
            
        except Exception as e:
            print(f"Error processing {in_key}: {str(e)}")
            continue

    print("\nTransformation complete for all files")


def set_job_params_as_env_vars():
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]
            os.environ[key] = sys.argv[i + 1]

if __name__ == "__main__":
    set_job_params_as_env_vars()
    main()
