import sys, os, boto3, io, json, re
import codecs
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
import splunk
from pyspark import SparkConf
from pyspark.sql import SparkSession

def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:glue_json_to_json: " + account_id
run_id = get_run_id()

def log_message(status, message):
    try:
        splunk.log_message({'FileName': inputFileName, 'Status': status, 'Message': message}, run_id)
    except Exception as e:
        print(f"Logging failed: {e} | Status: {status} | Message: {message}")

# Set environment variables from job arguments
def set_job_params_as_env_vars():
    try:
        i = 1
        while i < len(sys.argv):
            if sys.argv[i].startswith('--'):
                key = sys.argv[i][2:]
                # Check if next argument exists and is not another key
                if i + 1 < len(sys.argv) and not sys.argv[i + 1].startswith('--'):
                    value = sys.argv[i + 1]
                    os.environ[key] = value
                    i += 2
                else:
                    # No value for this key, skip to next
                    i += 1
            else:
                # Stray value, skip it
                i += 1
        log_message('success', "Job parameters set as environment variables.")
    except Exception as e:
        message = f"Exception in set_job_params_as_env_vars: {e}"
        log_message('failed', message)
        raise Exception(message)

# S3 utility for downloading/uploading
class FileUtility:
    def __init__(self):
        self.s3_client = boto3.client('s3')

    def download_from_s3(self, bucketName, key):
        response = self.s3_client.get_object(Bucket=bucketName, Key=key)
        return json.loads(response['Body'].read().decode('utf-8'))

    def upload_to_s3(self, bucketName, key, content):
        self.s3_client.put_object(
            Bucket=bucketName,
            Key=key,
            Body=json.dumps(content, indent=4),
            ContentType='application/json'
        )

def get_matching_s3_keys(bucket, prefix='', pattern=''):
    try:    
        s3 = boto3.client('s3')
        paginator = s3.get_paginator('list_objects_v2')
        keys = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = os.path.basename(obj['Key'])
                log_message('success', f"Checking S3 key: {key}")
                if re.match(pattern, key.rsplit('/', 1)[-1]):
                    keys.append(obj['Key'])
        log_message('success', f"Matching S3 keys found: {keys}")
        if not keys:
            raise Exception("No matching S3 keys found.")
        return keys[0]
    except Exception as e:
        message = f"Exception in get_matching_s3_keys: {e}"
        log_message('failed', message)
        raise Exception(message)

def collect_all_dicts(obj):
    """Yield all dictionaries from nested data structures (generator for memory efficiency)."""
    try:
        if isinstance(obj, dict):
            yield obj
            for v in obj.values():
                yield from collect_all_dicts(v)
        elif isinstance(obj, list):
            for v in obj:
                yield from collect_all_dicts(v)
    except Exception as e:
        message = f"Exception in collect_all_dicts: {e}"
        log_message('failed', message)
        raise Exception(message)

def get_by_path(obj, path):
    """Safely walk through obj by path tuple/list."""
    try:
        for key in path:
            if isinstance(obj, list):
                if not isinstance(key, int) or key >= len(obj):
                    return None
                obj = obj[key]
            elif isinstance(obj, dict):
                obj = obj.get(key)
            else:
                return None
            if obj is None:
                return None
        return obj
    except Exception as e:
        message = f"Exception in get_by_path: {e}"
        log_message('failed', message)
        raise Exception(message)

def collapse(val):
    """Collapse single-element lists into a scalar, leave others unchanged."""
    try:
        if isinstance(val, list) and len(val) == 1:
            return val[0]
        return val
    except Exception as e:
        message = f"Exception in collapse: {e}"
        log_message('failed', message)
        raise Exception(message)

def clean_empty(d):
    """
    Recursively remove empty lists, empty dicts, or None elements from a dictionary or list.
    """
    try:
        if isinstance(d, dict):
            return {
                k: v for k, v in ((k, clean_empty(v)) for k, v in d.items())
                if v is not None and v != [] and v != {}
            }
        elif isinstance(d, list):
            return [v for v in (clean_empty(v) for v in d) if v is not None and v != [] and v != {}]
        return d
    except Exception as e:
        message = f"Exception in clean_empty: {e}"
        log_message('failed', message)
        raise Exception(message)
    
def merge_data(source, rule):
    try:
        filter_list = rule["filter_list"]
        merge_attributes = rule["merge_attributes"]
        result = []
        for src in source:
            flg = False
            for rslt in result:
                if rslt[filter_list] == src[filter_list]:
                    for atr in merge_attributes:
                        rslt[atr].append(src[atr])
                    flg = True
                    break
            if not flg:
                result.append(src)
                for atr in merge_attributes:
                    result[-1][atr] = [src[atr]]
        return result
    except Exception as e:
        message = f"Exception in merge_data: {e}"
        log_message('failed', message)
        raise Exception(message)

def extract_value(source, rule, parent_data=None, PARENT_CTX = {}):
    try:
        if not PARENT_CTX:
            PARENT_CTX["root"] = source
        # ---- FOREACH ----
        if isinstance(rule, dict) and "foreach" in rule and "do" in rule:
            arr = get_by_path(source, rule["foreach"]["path"])
            if not isinstance(arr, list):
                source_key = rule["foreach"].get("key", "root")
                source = PARENT_CTX[source_key]
                arr = get_by_path(source, rule["foreach"]["path"])

            if not isinstance(arr, list):
                arr = [arr]
            
            # Optional filter
            filter_rule = rule.get("filter")
            if filter_rule:
                # Optional index based selection
                index_filter = filter_rule.get("indices")
                if index_filter:
                    filtered_arr = []
                    for i in index_filter:
                        if len(arr) <= i:
                            break
                        filtered_arr.append(arr[i])
                    arr = filtered_arr

                filtered_arr = []
                # Optional Condition based selection
                if filter_rule.get("filters"):
                    for f in filter_rule.get("filters"):
                        exclude = f.get("exclude", False)
                        if f.get("value") and f.get("value_from_parent"):
                            if parent_data.get(f["value_from_parent"]) != f["value"]:
                                return None
                    for item in arr:
                        # Flatten all dicts in the item
                        # narrative_arr = collect_all_dicts(item)
                        narrative_arr = list(collect_all_dicts(item))

                        if not isinstance(arr, list):
                            return None
                        # Check each filter independently
                        all_filters_match = True
                        for f in filter_rule.get("filters"):
                            if f.get("value") and f.get("value_from_parent"):
                                continue
                            val = f.get("value")
                            exclude = f.get("exclude", False)
                            if "value_from_parent" in f and parent_data:
                                val = parent_data.get(f["value_from_parent"])
                            # Try to find ANY dict that matches this filter
                            filter_matched = False
                            for n in narrative_arr:
                                v1 = str(n.get(f["field"], "")).strip()
                                if isinstance(val, list):
                                    v2s = [str(v).strip() for v in val]
                                    if v1 in v2s:
                                        filter_matched = True
                                        break
                                else:
                                    v2 = str(val).strip()
                                    if v1 == v2:
                                        filter_matched = True
                                        break
                            if exclude:
                                if filter_matched:
                                    all_filters_match = False
                                    break
                            elif not filter_matched:
                                all_filters_match = False
                                break

                        if all_filters_match:
                            filtered_arr.append(item)
                    arr = filtered_arr
            results = []
            if rule.get("collapse_result", False):
                for item in arr:
                    new_rule = rule["do"]["value"] if rule["do"].get("value",None) else rule["do"]
                    results.append(extract_value(item, new_rule,parent_data,PARENT_CTX=PARENT_CTX))
                    break
            elif rule.get("is_list", False)  and "value" in rule["do"].keys():
                for item in arr:
                    parent_key = rule["foreach"]["value"]
                    PARENT_CTX[parent_key] = item
                    for v in rule["do"].values():
                        results.append(extract_value(item, v,parent_data,PARENT_CTX=PARENT_CTX))
            else:
                for item in arr:
                    mapped = {}
                    for k, v in rule["do"].items():
                        parent_items=mapped|parent_data if parent_data else mapped
                        parent_key = rule["foreach"]["value"]
                        PARENT_CTX[parent_key] = item
                        if isinstance(v, dict) and "foreach" in v and "do" in v:
                            key = v["foreach"].get("key","root")
                            new_source = PARENT_CTX[key]

                            mapped[k] = extract_value(new_source, v, parent_items,PARENT_CTX=PARENT_CTX)
                            if v.get("add_to", False):
                                mapped[v["add_to"]].extend(mapped[k])
                                del(mapped[k])
                            del(PARENT_CTX[parent_key])
                        else:
                            mapped[k] = extract_value(item, v, parent_items,PARENT_CTX=PARENT_CTX)
                    if rule.get("is_list", False):
                        results.append(mapped)
                    else:
                        results.append({kk: collapse(vv) for kk, vv in mapped.items()})

            if isinstance(rule, dict) and "merge_data" in rule:
                results = merge_data(results, rule["merge_data"])

            if rule.get("temp_attribute", False):
                temp_attribute = rule["temp_attribute"]
                for item in results:
                    for temp in temp_attribute:
                        if temp in item:
                            del(item[temp])
            if rule.get("is_unique", False):
                unique_results = []
                for item in results:
                    if item not in unique_results:
                        unique_results.append(item)
                results = unique_results
            if rule.get("collapse_result", False) and results:
                return results[0]
            return results
    except Exception as e:
        message = f"Exception in extract_value: {e}"
        raise Exception(message)

    # ---- FILTERED extraction ----
    if isinstance(rule, dict) and "path" in rule and ("value_field" in rule or "value_fields" in rule or "value" in rule) and ("filter" in rule or "filters" in rule):
        default = rule.get("default", None)
        if rule.get('key'):
            source =  PARENT_CTX[ rule["key"]]
        arr = get_by_path(source, rule["path"]) 
        if isinstance(arr, dict):
            arr = [arr]
        elif not isinstance(arr, list) and default:
            return default
        elif not isinstance(arr, list):
            return None

        filters = rule.get("filters")
        if not filters and "filter" in rule:
            filters = [rule["filter"]]

        # Determine if we need OR logic instead of AND logic
        use_or_logic = rule.get("logic") == "OR"
        for item in arr:
            if not isinstance(item, dict):
                continue
            if use_or_logic:
                match = False
                for f in filters:
                    val = f.get("value")
                    if "value_from_parent" in f and parent_data:
                        val = parent_data.get(f["value_from_parent"])
                    if isinstance(val, list):
                        if item.get(f["field"]) in val:
                            match = True
                            break
                    else:
                        if item.get(f["field"]) == val:
                            match = True
                            break
            else:
                match = True
                for f in filters:
                    val = f.get("value")
                    if "value_from_parent" in f and parent_data:
                        val = parent_data.get(f["value_from_parent"])
                    if isinstance(val, list):
                        if item.get(f["field"]) not in val:
                            match = False
                            break
                    else:
                        if item.get(f["field"]) != val:
                            match = False
                            break
            if match:
                if rule.get("value_field"):
                    if isinstance(rule.get("value_field"), list):
                        return get_by_path(item, rule["value_field"])
                    return collapse(item.get(rule["value_field"]))
                if rule.get("value"):
                    return rule["value"]
                if rule.get("value_fields"):
                    results = []
                    for value_field in rule["value_fields"]:
                        if isinstance(value_field, list):
                            results.append(get_by_path(item, value_field))
                        else:
                            results.append(collapse(item.get(value_field)))
                    return results
        if default:
            return default
        return None

    # ---- Direct value update ----
    value = ""
    if "expression" in rule:
        if eval(rule["expression"]) and rule["true"]:
            value = eval(rule["true"][2:]) if rule["true"][:2] == "&." else rule["true"]
        elif  not eval(rule["expression"]) and rule.get("false"):
            value =  eval(rule["false"][2:]) if rule["false"][:2] == "&." else rule["false"]
    elif isinstance(rule, dict) and rule.get("value"):
        value =  eval(rule["value"][2:]) if rule["value"][:2] == "&." else rule["value"]
    if value:
        return value

    # ---- Simple path ----
    if isinstance(rule, (list, tuple)):
        got = get_by_path(source, rule)
        return got

    # ---- Nested mapping ----
    if isinstance(rule, dict):
        mapped = {}
        for k, v in rule.items():
            parent_items=mapped|parent_data if parent_data else mapped

            if isinstance(v, dict) and v.get("is_list"):
                mapped[k] = extract_value(source, v, parent_items,PARENT_CTX=PARENT_CTX)
            else:
                mapped[k] = collapse(extract_value(source, v, parent_items,PARENT_CTX=PARENT_CTX))

        # Check for required field
        required = rule.get("required", [])
        for req in required:
            if not mapped.get(req):
                return None
        return mapped

def map_output(source, mapping):
    try:
        spark = SparkSession.builder.appName("json_to_json").getOrCreate()
        rdd = spark.sparkContext.parallelize(source["root"])
        repartitioned_rdd = rdd.repartition(len(source["root"]))
        def process_record(source_part):
            return extract_value(source_part, mapping["mapping"],PARENT_CTX={"root":source_part})
        processed_rdd = repartitioned_rdd.map(process_record)
        results = processed_rdd.collect()
        result =[]
        for rslt in results:
            result.append(rslt)

        final_result = {}
        if "custom" in mapping and "addRootkey" in mapping["custom"]:
            if "additionalInfo" in mapping["custom"]:
                for key, value in mapping["custom"]["additionalInfo"].items():
                    final_result[key] = result if value == "rootData" else extract_value({}, value)
            if not final_result:
                final_result = result
            if "addRootkey" in mapping["custom"]:
                final_result = {mapping["custom"]["addRootkey"]: final_result}
        else:
            final_result = result
        # Clean empty values from the result
        return clean_empty(final_result)
    except Exception as e:
        message = f"Exception in map_output: {e}"
        log_message('failed', message)
        raise Exception(message)

def main():
    try:
        # sc = SparkContext()
        sc = SparkContext(
            conf=SparkConf().set("spark.driver.maxResultSize","2g")
        )
        glueContext = GlueContext(sc)
        set_job_params_as_env_vars()

        global inputFileName
        global file_name
        inputFileName = os.getenv("inputFileName")
        file_key = os.getenv("file_key")
        bucketName = os.getenv("bucketName")
        input_path = os.getenv("input_path")
        input_regex = os.getenv("input_regex")
        output_path = os.getenv("output_path")
        mapping_key = os.getenv("mapping_key") 
        output_file_postfix = os.getenv('output_file_postfix').strip()
        file_key = file_key if file_key !=" " else get_matching_s3_keys(bucketName, input_path, input_regex)
        log_message('success', f"File key determined: {file_key}")
        file_name = os.path.basename(file_key)
        output_key = f"{file_name.rsplit('.', 1)[0]}{output_file_postfix}.{file_name.rsplit('.', 1)[-1]}"
        output_file = f"{output_path}{output_key}"
        file_util = FileUtility()


        log_message('success', "Loading input JSON from S3...")
        input_data = file_util.download_from_s3(bucketName, file_key)
        log_message('success', "Input JSON loaded.")
        log_message('success', "Loading mapping JSON from S3...")
        mapping = file_util.download_from_s3(bucketName, mapping_key)
        log_message('success', "Mapping JSON loaded.")

        # for checks specific
        file_name = file_key.rsplit('/', 1)[1]
        log_message('success', "Transforming input data...")
        result = map_output(input_data, mapping)
        log_message('success', f"Transformation complete. RESULT : {json.dumps(result, indent=2)}")

        log_message('success', "Saving transformed JSON to S3...")
        file_util.upload_to_s3(bucketName, output_file, result)
        log_message('success', f"Transformed JSON successfully written to {output_file}")

        sys.stdout.flush()
    except Exception as e:
        import traceback
        error_msg = f"Error during output file upload: {str(e)}\n{traceback.format_exc()}"
        log_message('failed',error_msg)
        print(error_msg)
        sys.stdout.flush()
        raise Exception(error_msg)

inputFileName=""
if __name__ == "__main__":
    log_message('success', "JSON to JSON conversion started.")
    main()
