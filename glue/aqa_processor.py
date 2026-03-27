from pathlib import Path
import json
import copy
import importlib
import ijson
from decimal import Decimal
from asteval import Interpreter
import copy
from typing import Dict, Any
import json, sys, os, subprocess, uuid, traceback, inspect, logging
from datetime import datetime
from awsglue.utils import getResolvedOptions
import boto3
from boto3.s3.transfer import TransferConfig

account_number = ''
ip_number = []
first_entry = True


# ============================================================
# Singleton for Logger
# ============================================================
class SingletonMeta(type):
    _instances: Dict = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class LoggerManager(metaclass=SingletonMeta):
    def __init__(self):
        self.logger = logging.getLogger("GlueJobLogger")
        if not self.logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s",
                                    datefmt="%Y-%m-%d %H:%M:%S")
            handler.setFormatter(fmt)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

        self.cfg = None
        self.job_id = str(uuid.uuid4())

    def set_config_manager(self, cfg):
        self.cfg = cfg

    def _context_info(self):
        frame = inspect.stack()[2]
        return {
            "module": frame.frame.f_globals["__name__"],
            "function": frame.function,
            "line": frame.lineno
        }

    def _compose(self, level, message, extra=None, exc=None):
        payload = {
            "timestamp": datetime.utcnow().isoformat(),
            "job_id": self.job_id,
            "level": level,
            "message": message,
            **self._context_info()
        }
        if extra:
            payload.update(extra)
        if exc:
            payload["exception"] = {
                "type": type(exc).__name__,
                "message": str(exc),
                "traceback": traceback.format_exc()
            }
        return payload

    def _print(self, payload):
        lvl = getattr(logging, payload["level"].upper(), logging.INFO)
        self.logger.log(lvl, json.dumps(payload))

    def _send_splunk(self, payload):
        try:
            if self.cfg:
                run_id = self.cfg.get_run_id()
                splunk.log_message(payload, run_id)
            else:
                self.logger.warning("Splunk skipped: ConfigManager not set.")
        except Exception as e:
            self.logger.warning(f"Splunk send failed: {e}")

    def log(self, level, msg, extra=None, exc=None):
        payload = self._compose(level, msg, extra, exc)
        self._print(payload)
        self._send_splunk(payload)

    def info(self, msg, extra=None):
        self.log("INFO", msg, extra)

    def success(self, msg, extra=None):
        self.log("INFO", msg, extra)

    def warn(self, msg, extra=None):
        self.log("WARNING", msg, extra)

    def error(self, msg, extra=None, exc=None):
        self.log("ERROR", msg, extra, exc)

    def critical(self, msg, extra=None, exc=None):
        self.log("CRITICAL", msg, extra, exc)


# ============================================================
# Config Manager
# ============================================================
class ConfigManager(metaclass=SingletonMeta):
    def __init__(self):
        # ssm ( Systems Manager ) is a centralized operation hub, that provides visibility and control over your cloud and on premises infrastructure
        self.ssm = boto3.client("ssm")
        # secretsmanager is for storing and managing highly sensitive credentials, which is also encrypted at rest in there
        self.secrets = boto3.client("secretsmanager")
        # sts is security token service ( for short term )
        self.sts = boto3.client("sts")

    def get_ssm_parameter(self, name, region):
        try:
            client = boto3.client("ssm", region_name=region)
            return client.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]
        except Exception as e:
            LoggerManager().error("Failed to get SSM Parameter", {"param": name}, e)
            raise

    def get_secret_values(self, secret_name):
        try:
            resp = self.secrets.get_secret_value(SecretId=secret_name)
            return json.loads(resp["SecretString"])
        except Exception as e:
            LoggerManager().error("Failed to get secret values", {"secret": secret_name}, e)
            raise

    def get_run_id(self):
        return f"arn:dpm:glue:{self.sts.get_caller_identity()['Account']}:run-id"


# ============================================================
# Misc Services
# ============================================================
class AWSClientFactory:
    @staticmethod
    def get_client(service, region=None):
        try:
            return boto3.client(service, region_name=region) if region else boto3.client(service)
        except Exception as e:
            LoggerManager().error(f"Failed to init boto3 client: {service}", {"service": service}, e)
            raise


# ============================================================
# Glue Job Arguments
# ============================================================
class JobArguments:
    def __init__(self):
        self.args = getResolvedOptions(sys.argv, [
            "region", "dpm_to_wifcore_kafka_messages"
        ])

    def get(self, k): return self.args.get(k)

    def __getitem__(self, k): return self.args[k]


# ============================================================
# AqaRuleLoader ( fetching all input arguments and dpm json files and processing aqa rules)
# ============================================================
class AqaRuleLoader:
    def __init__(self, args: JobArguments):
        self.args = args
        self.logger = LoggerManager()
        self.config = ConfigManager()
        self.logger.set_config_manager(self.config)

        # AWSClientFactory is internal class here, defined above. and get_client is its static method
        self.s3 = AWSClientFactory.get_client("s3", args["region"])
        self.kafka_messages = args["dpm_to_wifcore_kafka_messages"]

        self.disk_dpm_json_file = f"/tmp/dpm_json_file.txt"
        self.disk_output_file = f"/tmp/aqa_output.txt"

    # ------------------------------------------------------------
    def _start_rule_processing(self):

        print("start rule processing")

        aqaReportGenerator = AqaReportGenerator(self.disk_dpm_json_file, self.disk_output_file)

        for kafka_message in json.loads(self.kafka_messages):

            clientname = None
            if kafka_message.get('fileLocation') and len(kafka_message.get('fileLocation')) > 0 and \
                    kafka_message.get('fileLocation')[0].get('fileName'):
                clientname = kafka_message.get('fileLocation')[0].get('fileName').split("/")[0]
            else:
                print("This kafka message do not have file details:" + str(kafka_message))
                return

            origin_identifier = self.get_origin_identifier(kafka_message)

            if kafka_message.get('accountType'):

                non_hh_rules = self.read_all_rules(kafka_message.get('bucketName'), f"{clientname}/config/aqa_rules",
                                                   'aqa_rules_non_hh_')

                hh_rules = None
                if not hh_rules and kafka_message.get('accountType') == 'HOUSEHOLD':
                    hh_rules = self.read_all_rules(kafka_message.get('bucketName'), f"{clientname}/config/aqa_rules",
                                                   'aqa_rules_hh_')

                self._process_single_message(kafka_message, copy.deepcopy(non_hh_rules), copy.deepcopy(hh_rules),
                                             aqaReportGenerator)

                s3_output_filepath = f"{clientname}/wip/aqa/acct_level_reports/{origin_identifier}/{kafka_message.get('accountNumber')}_aqa_output.txt"

                # ---- Upload main file to S3 ----
                self._upload_to_s3(self.disk_output_file, kafka_message.get('bucketName'), s3_output_filepath)

            else:
                # put start message and end message processing logic here
                if kafka_message.get('status') == 'COMPLETED':

                    # Remove output_file file if exists
                    try:
                        os.remove(self.disk_output_file)
                    except FileNotFoundError:
                        pass

                    with open(self.disk_output_file, 'a', encoding="utf-8") as merged_aqa_output:
                        # rundate = kafka_message.get('createdTimestamp')[0:10]
                        merged_file_header = """
                        {
                        "header": {
                        "rundate": """ + kafka_message.get('createdTimestamp')[0:10] + """ ,
                        "filename": "aqa_report.txt"
                        },
                        "details": [ """

                        merged_aqa_output.write(merged_file_header)

                        s3_input_folderpath = f"{clientname}/wip/aqa/acct_level_reports/{origin_identifier}"
                        response = self.s3.list_objects_v2(Bucket=kafka_message.get('bucketName'),
                                                           Prefix=s3_input_folderpath)
                        matched_files = [obj['Key'] for obj in response.get('Contents', []) if
                                         '_aqa_output' in obj['Key']]
                        first_merge_write = True
                        for file_path in matched_files:

                            aqa_file = self.s3.get_object(Bucket=kafka_message.get('bucketName'), Key=file_path)

                            aqa_file_json = json.loads("""[""" + aqa_file['Body'].read().decode('utf-8') + """]""")

                            aqa_file_json = self.merge_summary_errors_with_primary_errors(aqa_file_json)

                            for item in aqa_file_json:
                                if first_merge_write == False:
                                    merged_aqa_output.write(",")
                                merged_aqa_output.write(json.dumps(item))
                                first_merge_write = False

                        merged_aqa_output.write("] }")

                    s3_output_filepath = f"{clientname}/wip/aqa/origin_level_reports/{origin_identifier}_aqa_report.json"

                    # ---- Upload main file to S3 ----
                    self._upload_to_s3(self.disk_output_file, kafka_message.get('bucketName'), s3_output_filepath)

    def _process_single_message(self, kafka_message, non_hh_rules, hh_rules, aqaReportGenerator):

        total_files = len(kafka_message.get('fileLocation'))
        for index, file_location in enumerate(kafka_message.get('fileLocation'), start=1):
            file_location_type = ''
            if file_location.get('type'):
                file_location_type = file_location.get('type')
            else :
                file_location_type = kafka_message.get('accountType')
            self._copy_s3_file_to_local_disk(kafka_message.get('bucketName'), file_location.get('fileName'),
                                             self.disk_dpm_json_file)

            if file_location_type in ('SINGLE','MULTIACCOUNT'):
                rule_list = {'non_hh_rule': copy.deepcopy(non_hh_rules)}
                aqaReportGenerator.process_rules(rule_list, self.disk_dpm_json_file, index == total_files, index)
            if file_location_type in ('PRIMARY') :
                rule_list = {'non_hh_rule': copy.deepcopy(non_hh_rules), 'hh_rules': hh_rules}
                aqaReportGenerator.process_rules(rule_list, self.disk_dpm_json_file, index == total_files, index)
            if file_location_type in ('SECONDARY') :
                rule_list = {'non_hh_rule': copy.deepcopy(non_hh_rules), 'hh_rules': hh_rules}
                aqaReportGenerator.process_rules(rule_list, self.disk_dpm_json_file, index == total_files, index)
            if file_location_type in ('SUMMARY') :
                rule_list = {'hh_rules': hh_rules}
                aqaReportGenerator.process_rules(rule_list, self.disk_dpm_json_file, index == total_files, index)

    def read_all_rules(self, bucket_name, folder_path, filepattern):

        response = self.s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)

        matched_files = [obj['Key'] for obj in response.get('Contents', []) if filepattern in obj['Key']]

        all_rules = {'Rules': []}

        for file_path in matched_files:
            rules_file = self.s3.get_object(Bucket=bucket_name, Key=file_path)

            rules_load = json.loads(rules_file['Body'].read().decode('utf-8'))

            all_rules.get('Rules').extend(rules_load.get('Rules'))

        return all_rules

    def get_origin_identifier(self, kafka_message):
        origin_identifier = kafka_message.get('origin')
        if origin_identifier == None:
            origin_identifier = kafka_message.get('batchId')

        return origin_identifier

    def _copy_s3_file_to_local_disk(self, bucketName, s3_file, disk_outputfile):

        # Remove output_file file if exists
        try:
            os.remove(disk_outputfile)
        except FileNotFoundError:
            pass

        # Optional: Configure transfer settings to optimize for 5GB
        # multipart_threshold: use multipart for files > 200MB
        # max_concurrency: number of threads for parallel downloading
        config = TransferConfig(
            multipart_threshold=200 * 1024 * 1024,
            max_concurrency=10,
            use_threads=True
        )

        try:
            # Managed download (handles chunks automatically)
            self.s3.download_file(bucketName, s3_file, disk_outputfile, Config=config)

            print(f"File downloaded successfully to {disk_outputfile}")
        except Exception:
            print("[CRITICAL] Failed writing JSON")
            print(traceback.format_exc())
            return

    # ------------------------------------------------------------
    def _upload_to_s3(self, disk_outputfile, bucketName, s3_output_file):

        path = f"{bucketName}/{s3_output_file}"
        try:
            self.s3.upload_file(disk_outputfile, bucketName, s3_output_file)

            self.logger.success("Uploaded file to S3", {"path": path})

            # Remove output_file file if exists
            try:
                os.remove(disk_outputfile)
            except FileNotFoundError:
                pass

        except Exception as e:
            self.logger.error("S3 upload failed", {"record": path}, e)
            raise

    # ------------------------------------------------------------
    def merge_summary_errors_with_primary_errors(self, aqa_file_json):
        # logic needs to be added to merge primary and summary rule errors
        return aqa_file_json

        # ------------------------------------------------------------


class AqaReportGenerator:
    def __init__(self, disk_dpm_json_file, disk_output_file):
        self.dpm_json_file = disk_dpm_json_file
        self.output_file = disk_output_file

    def start_operation_processing(self, lhs_value, operation_type, rhs_value, module_item):

        if module_item.get(lhs_value) == None:
            return False
        lhs_value_copy = None
        rhs_value_copy = None
        if type(rhs_value) in (int,float):
            lhs_value_copy = Decimal(module_item.get(lhs_value))
            rhs_value_copy = Decimal(rhs_value)
        else:
            lhs_value_copy = module_item.get(lhs_value)
            rhs_value_copy = rhs_value

        if operation_type == '=':
            return lhs_value_copy == rhs_value_copy
        elif operation_type == '>':
            return lhs_value_copy > rhs_value_copy
        elif operation_type == '<':
            return lhs_value_copy < rhs_value_copy
        elif operation_type == '!=':
            return lhs_value_copy != rhs_value_copy

    def start_condition_processing(self, path, operations, value, module_element, module_name,
                                   value_array_temp_new_entry, rule_name):

        module_element_current_pos = module_element
        for path_element in path:
            if path_element is not None:
                module_element_current_pos = module_element_current_pos.get(path_element, 'Path not Found')
            if module_element_current_pos == 'Path not Found':
                return False

        operation_status = False
        if type(module_element_current_pos) == dict:
            module_element_current_pos = [module_element_current_pos]
        if type(module_element_current_pos) == list:
            for module_item in module_element_current_pos:
                for operation in operations:
                    try:
                        if len(operation) == 3:
                            lhs_value = operation[0]
                            operation_type = operation[1]
                            rhs_value = operation[2]
                            operation_status = self.start_operation_processing(lhs_value, operation_type, rhs_value,
                                                                               module_item)
                        elif len(operation) == 1:
                            if operation[0].find('AmountValue') != -1:
                                amount_type = module_item.get(module_name + "AmountType")
                                amount_value = Decimal(module_item.get(module_name + "AmountValue", 0))
                                amount_currency = module_item.get(module_name + "AmountCurrency")

                                value_array_temp_new_entry.update({'AmountType': amount_type, 'AmountValue': amount_value,
                                                                   'AmountCurrency': amount_currency})
                                operation_status = True
                            if operation[0].find('AmountCurrency') != -1:
                                amount_type = module_item.get(module_name + "AmountType")
                                amount_currency = module_item.get(module_name + "AmountCurrency")

                                value_array_temp_new_entry['AmountCurrency'] = amount_currency

                                operation_status = True
                            elif operation[0].find('Narrative') != -1:
                                amount_value = Decimal(module_item.get(module_name + "NarrativeValue", 0))
                                value_array_temp_new_entry['AmountValue'] = amount_value
                                operation_status = True
                            elif operation[0].find('Price') != -1:
                                amount_type = module_item.get(module_name + "PriceType")
                                amount_value = Decimal(module_item.get(module_name + "PriceValue", 0))
                                amount_currency = module_item.get(module_name + "PriceCurrency")

                                value_array_temp_new_entry.update({'AmountType': amount_type, 'AmountValue': amount_value,
                                                                   'AmountCurrency': amount_currency})
                                operation_status = True
                            elif operation[0].find('AccountIdentifierValue') != -1:
                                temp_account_number = module_item.get(module_name + "AccountIdentifierValue")
                                if temp_account_number is None:
                                    temp_account_number = account_number

                                value_array_temp_new_entry['AccountNumber'] = temp_account_number
                                operation_status = True
                    
                    except Exception as e:
                        # 'from e' preserves the original context in the traceback
                        raise RuntimeError(
                            f"Input details of failure. rule : {rule_name}, operation : {operation}"
                            f", current_input_entry : {module_element}") from e

                    if operation_status == False:
                        break

                if operation_status == True:
                    return True

        return operation_status

    def start_rule_processing_for_json_entry(self, values, module_element, module_name, rule_name):
        for value in values:
            value_array_temp_new_entry = {}
            for condition in value.get('Conditions', []):
                condition_status = self.start_condition_processing(condition.get("Path", []),
                                                                   condition.get('Operations', []),
                                                                   value, module_element, module_name,
                                                                   value_array_temp_new_entry, rule_name)
                if condition_status == False:
                    break

            if value_array_temp_new_entry != {}:

                entry_found = False
                for value_array_item in value.get('ValueArray', []):
                    if ((value_array_item.get('AmountCurrency') == value_array_temp_new_entry.get('AmountCurrency')) and
                            (value_array_item.get('AccountNumber') == value_array_temp_new_entry.get('AccountNumber'))):
                        value_array_item['AmountValue'] = value_array_item['AmountValue'] + value_array_temp_new_entry[
                            'AmountValue']
                        entry_found = True
                if entry_found == False:
                    value.get('ValueArray', []).append(value_array_temp_new_entry)

    def postoperation_processing(self, value_array_element, postoperations):
        for postoperation in postoperations:
            if len(postoperation) == 2:
                if postoperation[1] == 'multiply':
                    value_array_element['AmountValue'] = value_array_element['AmountValue'] * Decimal(postoperation[0])
            if len(postoperation) == 3:
                if postoperation[1] == 'assign_new_key':
                    value_array_element[(postoperation[0])] = (postoperation[2])

    def processing_intermediate_amt_arrays_in_rule_load(self, lhs_rhs_element, final_value_array):
        values = lhs_rhs_element.get('Values', [])

        for value in values:
            for value_array_element in value.get('ValueArray', []):
                self.postoperation_processing(value_array_element, value.get('PostOperations', []))

                entry_found = False
                for final_value_array_element in final_value_array:
                    if (final_value_array_element.get('AmountCurrency') == value_array_element.get('AmountCurrency') and
                            (final_value_array_element.get('AccountNumber') == value_array_element.get(
                                'AccountNumber')) and
                            final_value_array_element.get('group_key') == value_array_element.get('group_key')):
                        final_value_array_element['AmountValue'] = final_value_array_element['AmountValue'] + \
                                                                   value_array_element['AmountValue']
                        entry_found = True
                if entry_found == False:
                    new_entry = {'AmountValue': value_array_element['AmountValue'],
                                 'AmountCurrency': value_array_element['AmountCurrency'],
                                 'AccountNumber': value_array_element.get('AccountNumber'),
                                 'group_key': value_array_element.get('group_key')}
                    final_value_array.append(new_entry)

    def processing_final_amt_arrays_in_rule_load(self, final_operations, final_value_array):
        for finaloperation in final_operations:
            if len(finaloperation) == 2:
                if finaloperation[1] == 'multiply':
                    for value_array_element in final_value_array:
                        value_array_element['AmountValue'] = value_array_element['AmountValue'] * Decimal(
                            finaloperation[0])

            if len(finaloperation) == 3:
                if finaloperation[1] == 'formula':
                    # aeval = Interpreter()
                    currency_list = set()
                    new_final_value_array = []
                    for value_array_element in final_value_array:
                        currency_list.add(value_array_element['AmountCurrency'])

                    for amount_currency in currency_list:
                        aeval = Interpreter()
                        formula_var_list = copy.deepcopy(finaloperation[2])
                        for value_array_element in final_value_array:
                            if value_array_element['AmountCurrency'] == amount_currency:
                                aeval.symtable[value_array_element['group_key']] = value_array_element['AmountValue']
                                formula_var_list.remove(value_array_element['group_key'])

                        if (len(formula_var_list) == 0):
                            try:
                                result_amount = aeval(finaloperation[0])
                                if result_amount is None:
                                    result_amount=0
                            except ZeroDivisionError:
                                result_amount = 0

                            new_final_value_array.append({'AmountValue': result_amount,
                                                          'AmountCurrency': amount_currency})

                    final_value_array.clear()
                    final_value_array.extend(new_final_value_array)

    def writing_in_aqa_output_file(self, currency, ip, rule_name, lhs_amount, rhs_amount, diff, error_message,
                                   aqa_output):

        global account_number
        global ip_number
        global first_entry
        mismatch_json = {
            "cur": currency,
            "ip": ip_number,
            "error_code": rule_name,
            "statement_value": lhs_amount,
            "aqa_value": rhs_amount,
            "difference": diff,
            "statement_section": error_message
        }

        if first_entry == False:
            aqa_output.write(",\n")

        json.dump(mismatch_json, aqa_output, indent=4)

        first_entry = False

    def comparing_lhs_with_rhs(self, lhs_final_value_array, rhs_final_value_array, rule_name, error_message,
                               aqa_output):
        for lhs_final_array_element in lhs_final_value_array:
            for rhs_final_array_element in rhs_final_value_array:
                if ((lhs_final_array_element.get('AmountCurrency') == rhs_final_array_element.get('AmountCurrency') and
                     (lhs_final_array_element.get('AccountNumber') == rhs_final_array_element.get('AccountNumber')))
                        or rhs_final_array_element.get('AmountCurrency') == 'NoCurrency'):
                    if round(lhs_final_array_element.get('AmountValue'), 2) != round(
                            rhs_final_array_element.get('AmountValue'), 2):
                        currency = lhs_final_array_element.get('AmountCurrency')
                        ip = 'xx'
                        lhs_amount = str(round(lhs_final_array_element.get('AmountValue'), 2))
                        rhs_amount = str(round(rhs_final_array_element.get('AmountValue'), 2))
                        diff = str(
                            round(lhs_final_array_element.get('AmountValue', 0), 2) - round(rhs_final_array_element.get(
                                'AmountValue', 0), 2))
                        self.writing_in_aqa_output_file(currency, ip, rule_name, lhs_amount, rhs_amount
                                                        , diff, error_message, aqa_output)

                    lhs_final_array_element['processed'] = True
                    rhs_final_array_element['processed'] = True

        # Find and report LHS, who don't have RHS
        for lhs_final_array_element in lhs_final_value_array:
            if lhs_final_array_element.get('processed') == None and lhs_final_array_element.get('AmountValue') != 0:
                currency = lhs_final_array_element.get('AmountCurrency')
                ip = 'xx'
                lhs_amount = str(round(lhs_final_array_element.get('AmountValue'), 2))
                rhs_amount = 'Not Found'
                diff = 'N/A'
                self.writing_in_aqa_output_file(currency, ip, rule_name, lhs_amount, rhs_amount
                                                , diff, error_message, aqa_output)

        # Find and report RHS, who don't have LHS
        for rhs_final_array_element in rhs_final_value_array:
            if rhs_final_array_element.get('processed') == None and rhs_final_array_element.get('AmountValue') != 0:
                currency = rhs_final_array_element.get('AmountCurrency')
                ip = 'xx'
                lhs_amount = 'Not Found'
                rhs_amount = str(round(rhs_final_array_element.get('AmountValue'), 2))
                diff = 'N/A'
                self.writing_in_aqa_output_file(currency, ip, rule_name, lhs_amount, rhs_amount
                                                , diff, error_message, aqa_output)

    def processing_data_in_rules_file(self, rule, aqa_output):

        lhs_final_value_array = rule.get('LHS', {}).get('FinalValueArray', [])
        self.processing_intermediate_amt_arrays_in_rule_load(rule.get('LHS', {}), lhs_final_value_array)
        self.processing_final_amt_arrays_in_rule_load(rule.get('LHS', {}).get('FinalOperations', []),
                                                      lhs_final_value_array)

        rhs_final_value_array = rule.get('RHS', {}).get('FinalValueArray', [])
        self.processing_intermediate_amt_arrays_in_rule_load(rule.get('RHS', {}), rhs_final_value_array)
        self.processing_final_amt_arrays_in_rule_load(rule.get('RHS', {}).get('FinalOperations', []),
                                                      rhs_final_value_array)

        self.comparing_lhs_with_rhs(lhs_final_value_array, rhs_final_value_array, rule.get('RuleName', 'DefaultName'),
                                    rule.get('ErrorMessage', 'DefaultErrorMessage'), aqa_output)

    def get_party_details_from_file(self, dpm_json_file, module_name, index):
        global account_number
        global ip_number
        module_data_array = ijson.items(dpm_json_file, "Customer." + module_name + ".item")
        for module_element in module_data_array:
            party_classification = module_element.get('PartyClassification', [])
            for party_classification_type in party_classification:
                if party_classification_type.get('PartyClassificationValue') == 'Account':
                    for party_identifier_type in module_element.get('PartyIdentifier', []):
                        if party_identifier_type.get('PartyIdentifierType') == 'OtherExternal':
                            account_number = party_identifier_type.get('PartyIdentifierValue')
                if party_classification_type.get('PartyClassificationValue') == 'InterestedParty':
                    for party_identifier_type in module_element.get('PartyIdentifier', []):
                        if party_identifier_type.get('PartyIdentifierType') == 'OtherExternal':
                            ip_number.append(party_identifier_type.get('PartyIdentifierValue'))

        with open(self.output_file, 'a', encoding="utf-8") as aqa_output:

            if index > 1:
                aqa_output.write(",")

            aqa_output_prefix = """{
            "account_number": """ + account_number + """,
            "aqa_details": [\n"""
            aqa_output_suffix = """}\n ]"""
            aqa_output.write(aqa_output_prefix)

    def process_rules(self, rule_list, filepath, process_household, index):

        global account_number
        global ip_number
        global first_entry

        # Open the JSON file on disk
        with open(
                filepath, "rb"
        ) as dpm_json_file:  # note the 'rb' - ijson works with bytes

            self.get_party_details_from_file(dpm_json_file, "Party", index)

            module_name_list = ['Transaction', 'Position', 'Balance']
            for module_name in module_name_list:
                dpm_json_file.seek(0)
                module_data_array = ijson.items(dpm_json_file, "Customer." + module_name + ".item")

                for module_element in module_data_array:
                    # print(module_name)

                    with open(self.output_file, 'a', encoding="utf-8") as aqa_output:
                        for rule_list_item in rule_list.values():
                            for rule in rule_list_item.get('Rules'):
                                if rule.get('LHS', {}).get("ModuleName", {}) == module_name:
                                    self.start_rule_processing_for_json_entry(rule.get('LHS', {}).get('Values', {}),
                                                                              module_element,
                                                                              module_name, rule.get('RuleName'))

                                if rule.get('RHS', {}).get("ModuleName", {}) == module_name:
                                    self.start_rule_processing_for_json_entry(rule.get('RHS', {}).get('Values', {}),
                                                                              module_element,
                                                                              module_name, rule.get('RuleName'))

                                # print(rule.get('ComparisonType', 'Searching'))
                                if rule.get('ComparisonType', {}) == 'Detail':

                                    self.processing_data_in_rules_file(rule, aqa_output)

                                    for value_array_element in rule.get('LHS', {}).get('Values', []):
                                        value_array_element.get("ValueArray", []).clear()
                                    for value_array_element in rule.get('RHS', {}).get('Values', []):
                                        value_array_element.get("ValueArray", []).clear()

                                    rule.get('LHS', {}).get('FinalValueArray', []).clear()
                                    rule.get('RHS', {}).get('FinalValueArray', []).clear()

        # Second pass on rules json for accumulated amounts postprocessing, and aggregating them in LHS and RHS final amount
        # with final condition processing on them, if needed.

        with open(self.output_file, 'a', encoding="utf-8") as aqa_output:
            for rule_type, rule_list_value in rule_list.items():
                if rule_type != 'hh_rules' or process_household:
                    for rule in rule_list_value.get('Rules'):
                        self.processing_data_in_rules_file(rule, aqa_output)

        with open(self.output_file, 'a', encoding="utf-8") as aqa_output:
            aqa_output_suffix = """] }"""
            aqa_output.write(aqa_output_suffix)

        # resetting global variables before next call this code
        account_number = ''
        ip_number = []
        first_entry = True


# ============================================================
def main():
    log = LoggerManager()
    try:
        args = JobArguments()
        job = AqaRuleLoader(args)
        job._start_rule_processing()
    except Exception as e:
        log.critical("Glue job aborted", {}, e)
        raise


if __name__ == "__main__":
    main()
