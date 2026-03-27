import json, sys, os, re, subprocess, uuid, traceback, inspect, logging
from datetime import datetime
from typing import Dict, Any
from awsglue.utils import getResolvedOptions
import boto3
import psycopg2
import psycopg2.extras
import splunk  # available at runtime
import shutil
import traceback
from confluent_kafka import Producer


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

    def info(self, msg, extra=None): self.log("INFO", msg, extra)
    def success(self, msg, extra=None): self.log("INFO", msg, extra)
    def warn(self, msg, extra=None): self.log("WARNING", msg, extra)
    def error(self, msg, extra=None, exc=None): self.log("ERROR", msg, extra, exc)
    def critical(self, msg, extra=None, exc=None): self.log("CRITICAL", msg, extra, exc)


# ============================================================
# Config Manager
# ============================================================
class ConfigManager(metaclass=SingletonMeta):
    def __init__(self):
        self.ssm = boto3.client("ssm")
        self.secrets = boto3.client("secretsmanager")
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
# DIRECT psycopg2 Connection Manager (NO SQLAlchemy)
# ============================================================
class DBConnectionManager(metaclass=SingletonMeta):
    def __init__(self):
        self.conns = {}

    def get_conn(self, dbkey, dbname, region):
        log = LoggerManager()
        try:
            if dbkey in self.conns:
                return self.conns[dbkey]

            cfg = ConfigManager()
            param = cfg.get_ssm_parameter(dbkey, region)
            creds = cfg.get_secret_values(param)

            # CONNECT USING psycopg2
            conn = psycopg2.connect(
                host=creds["host"],
                port=creds["port"],
                user=creds["username"],
                password=creds["password"],
                dbname=dbname,
                cursor_factory=psycopg2.extras.RealDictCursor
            )
            conn.autocommit = False

            self.conns[dbkey] = conn
            log.success("PostgreSQL connection established", {"Database": dbname})
            return conn

        except Exception as e:
            log.error("Database connection failed", {"Database": dbname}, e)
            raise


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

class KafkaService:
    def __init__(self, brokers, username, password, topic):
        self.logger = LoggerManager()
        self.topic = topic

        try:
            conf = {
                "bootstrap.servers": brokers,
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "SCRAM-SHA-512",
                "sasl.username": username,
                "sasl.password": password,
                "enable.idempotence": True,
                "linger.ms": 5
            }
            self.producer = Producer(conf)
            self.logger.success("Kafka Producer initialized", {"topic": topic})

        except Exception as e:
            self.logger.error("Kafka initialization failed", {"topic": topic}, e)
            raise

    def send(self, key, message):
        try:
            payload = json.dumps(message).encode("utf-8")
            self.producer.produce(self.topic, key=key, value=payload)
            self.producer.flush()
            self.logger.success("Kafka message sent", {"topic": self.topic, "key": key})
        except Exception as e:
            self.logger.error("Kafka send failed", {"topic": self.topic, "key": key}, e)
            raise


# ============================================================
# Glue Job Arguments
# ============================================================
class JobArguments:
    def __init__(self):
        self.args = getResolvedOptions(sys.argv, [
            "region", "dbkey", "dbname", "schema", "batch_size", "summary_option",
            "schema_set_query", "function_call_query", "temp_account_details_query",
            "input_bucket", "input_folder", "input_filename", "single_party_account_type",
            "group_party_account_type", "hh_party_account_type","master_party_contact_type",
            "sister_party_contact_type", "summary_party_contact_type_kafka", "party_section",
            "balance_section", "hh_advisor_query", "query_dic", "summary_query", "hh_summary_query",
            "s3_wip", "clientName", "periodDateRange", "runType", "kafka_topic",
            "kafka_username", "kafka_password", "kafka_endpoints", "aqa_report_glue_job_name"
        ])

    def get(self, k): return self.args.get(k)
    def __getitem__(self, k): return self.args[k]


# ============================================================
# DataPostgres (NOW USING psycopg2 COMPLETELY)
# ============================================================
class DataPostgres:
    def __init__(self, args: JobArguments):
        self.args = args
        self.logger = LoggerManager()
        self.config = ConfigManager()
        self.logger.set_config_manager(self.config)

        # DB via psycopg2
        self.conn = DBConnectionManager().get_conn(args["dbkey"], args["dbname"], args["region"])

        self.s3 = AWSClientFactory.get_client("s3", args["region"])
        self.glue = AWSClientFactory.get_client("glue", args["region"])

        username = self.config.get_ssm_parameter(args["kafka_username"], args["region"])
        password = self.config.get_ssm_parameter(args["kafka_password"], args["region"])
        self.kafka = KafkaService(args["kafka_endpoints"], username, password, args["kafka_topic"])

        self.input_file = args["input_filename"]
        self.client_name = args["clientName"]
        self.schema_query = args["schema_set_query"]
        self.uuid_value = args["s3_wip"].split("/")[-1]
        self.bucket = args["input_bucket"]
        self.execution_time = datetime.now()
        self.output_file = ''
        self.kafka_metadata = []
        self.accountType = ''
        self.hh_advisor = None
        self.is_house_hold = False
        self.hh_party_account_type = args["hh_party_account_type"]
        self.master_party_contact_type = args["master_party_contact_type"]
        self.sister_party_contact_type = args["sister_party_contact_type"]
        self.summary_party_contact_type_kafka = args["summary_party_contact_type_kafka"]
        self.party_section = args["party_section"]
        self.balance_section = args["balance_section"]
        self.query_dic = json.loads(args["query_dic"])
        self.single_party_account_type = args["single_party_account_type"]
        self.group_party_account_type = args["group_party_account_type"]
        self.batch_size = int(self.args['batch_size'])
        self.hh_summary_output_file = ''
        self.region = args["region"]
        self.aqa_report_glue_job_name = args["aqa_report_glue_job_name"]

    # ------------------------------------------------------------
    def _parse_filename(self):
        try:
            pattern = (
                r"(?:CIT_TEST_)?PATHFINDERSTATEMENT_([A-Z0-9]{8})_([0-9]{3})_([A-Z0-9._]+)_"
                r"(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})(\d{3})\.trigger"
            )
            match = re.search(pattern, self.input_file)
            if not match:
                raise ValueError("Filename mismatch")

            self.account_number, self.client_id, self.origin = match.group(1, 2, 3)
            y, m, d, hh, mm, ss, ms = match.group(4, 5, 6, 7, 8, 9, 10)
            self.timestamp = f"{y}-{m}-{d}T{hh}:{mm}:{ss}.{ms}"

            self.logger.info("Parsed trigger filename", {"file": self.input_file})

        except Exception as e:
            self.logger.error("Trigger filename parse error", {"file": self.input_file}, e)
            raise

    # ------------------------------------------------------------
    def _create_db_json(self):
        try:
            self._parse_filename()
            with self.conn.cursor() as cur:
                # Set schema
                cur.execute(f"{self.schema_query} {self.args['schema']}")

                #Function call to create JSON in database
                cur.execute(self.args["function_call_query"], (self.account_number, self.client_id, self.origin, self.args["summary_option"]))
            self.conn.commit()

            # Pulling list of accounts from temp_account_details_query
            with self.conn.cursor() as cur:
                cur.execute(self.args['temp_account_details_query'])
                rows = cur.fetchall()

            master_account = ''

            for row in rows:
                party_id = row["partyid"]
                party_contact_type = row["partycontacttype"]
                party_account_type = row["accounttype"]

                #Check if party_contact_type is PRIMARY and party_account_type is HOUSEHOLD
                if (
                        party_contact_type == self.master_party_contact_type
                        and party_account_type == self.hh_party_account_type
                ):
                    self.is_house_hold = True

                    # Setting accountType for kafka message
                    self.accountType = self.hh_party_account_type

                    master_account = party_id

                    # Pull advisor record of PRIMARY to add in all the sisters
                    with self.conn.cursor() as cur_inner:
                        cur_inner.execute(
                            self.args['hh_advisor_query'],
                            (party_id,)
                        )
                        adv_row = cur_inner.fetchone()
                        if adv_row:
                            self.hh_advisor= self._clean_json(adv_row)
                        else:
                            self.hh_advisor = None

                # Setting accountType for SINGLE account in kafka message
                elif party_account_type == self.single_party_account_type:
                    self.accountType = self.single_party_account_type

                # Setting accountType for MULTI account in kafka message
                elif party_account_type == self.group_party_account_type:
                    self.accountType = self.group_party_account_type

                '''
                Call _create_json_in_disk to pull JSON records from databae and stream them to
                a temporary file in glue job's tmp location
                '''
                self._create_json_in_disk(party_id, party_contact_type)

            # If HOUSEHOLD, create house_hold summary JSON
            if self.is_house_hold:
                self._create_hh_summary_json_in_disk(master_account)

            # Call _publish_kafk to publish kafka message
            self._publish_kafka()

        except Exception as e:
            self.conn.rollback()
            self.logger.critical("Creating DB JSON failed", {"file": self.input_file}, e)
            raise

    def _create_json_in_disk(self, party_id, party_contact_type):

        self.output_file = f"/tmp/{party_id}.json"

        # Remove output_file file if exists
        try:
            os.remove(self.output_file)
        except FileNotFoundError:
            pass

        hh_summary_file = None

        try:
            with open(self.output_file, "w", encoding="utf-8") as f:

                # If master, open hh_summary_output_file and add party data of master into it
                if party_contact_type == self.master_party_contact_type:
                    self.hh_summary_output_file = "/tmp/hh_summary.json"
                    try:
                        os.remove(self.hh_summary_output_file)
                    except FileNotFoundError:
                        pass
                    hh_summary_file = open(self.hh_summary_output_file, "w", encoding="utf-8")
                    hh_summary_file.write('{"Customer": {"Party": [')

                f.write('{"Customer": {')

                section_names = list(self.query_dic.keys())

                for section_index, section_name in enumerate(section_names):
                    query = self.query_dic[section_name]

                    f.write(f'"{section_name}": [')

                    wrote_any = False
                    wrote_any_summary = False  # for hh_summary file

                    # Insert HH advisor for sisters
                    if section_name == self.party_section and \
                            party_contact_type == self.sister_party_contact_type and \
                            self.hh_advisor:
                        f.write(json.dumps(self.hh_advisor, default=str))
                        wrote_any = True

                    # ---- MAIN DB CURSOR ----
                    with self.conn.cursor(name=f"cursor_{party_id}_{section_name}") as cursor:
                        cursor.itersize = self.batch_size
                        cursor.execute(query, (party_id,))

                        for row in cursor:
                            data = row.get("data")
                            if not data:
                                continue

                            # Clean the JSON
                            data = self._clean_json(data)
                            if not data:
                                 continue  # skip if everything got removed

                            # Write to main output file
                            if wrote_any:
                                f.write(",")
                            f.write(json.dumps(data, default=str))
                            wrote_any = True

                            # Also write to hh_summary.json if conditions match
                            if (
                                    party_contact_type == self.master_party_contact_type
                                    and section_name == self.party_section
                                    and hh_summary_file
                            ):
                                if wrote_any_summary:
                                    hh_summary_file.write(",")
                                hh_summary_file.write(json.dumps(data, default=str))
                                wrote_any_summary = True

                    # ---- MERGE SUMMARY INTO BALANCE SECTION ----
                    if section_name == self.balance_section:
                        cursor_s = self.conn.cursor(name=f"cursor_{party_id}_summary")
                        try:
                            cursor_s.itersize = self.batch_size
                            cursor_s.execute(self.args["summary_query"], (party_id,))

                            for row in cursor_s:
                                data = row.get("data")
                                if not data:
                                    continue

                                # Clean the JSON
                                data = self._clean_json(data)
                                if not data:
                                    continue  # skip if everything got removed

                                if wrote_any:
                                    f.write(",")
                                f.write(json.dumps(data, default=str))
                                wrote_any = True

                        finally:
                            cursor_s.close()

                    f.write("]")

                    if section_index < len(section_names) - 1:
                        f.write(",")

                f.write("}}")  # end of main JSON

                '''
                Finish hh_summary.json if created. Ending with comma and then _create_hh_summary_json_in_disk
                will add the balance section later
                '''
                if hh_summary_file:
                    hh_summary_file.write("],")
                    hh_summary_file.close()

            print(f"JSON file written: {self.output_file}")
            if hh_summary_file:
                print(f"HH summary JSON file written: {self.hh_summary_output_file}")

        except Exception:
            print("[CRITICAL] Failed writing JSON")
            print(traceback.format_exc())
            return

        # ---- Upload main file to S3 ----
        s3_path = self._upload_to_s3(party_id, party_contact_type)

        # Append new entry to the kafka message
        if self.accountType != self.group_party_account_type:
            self.kafka_metadata.append({
                "accountNumber": party_id,
                "type": party_contact_type,
                "fileName": s3_path
            })
        else:
            self.kafka_metadata.append({
                "accountNumber": party_id,
                "fileName": s3_path
            })

        # Remove output_file from disk
        try:
            os.remove(self.output_file)
        except FileNotFoundError:
            pass

    def _create_hh_summary_json_in_disk(self, master_account):

        with open(self.hh_summary_output_file, "a") as f:

            f.write('"Balance":[')

            # Pull hh_summary from database
            wrote_any = False
            with self.conn.cursor(name=f"cursor_{master_account}_hh_summary") as cursor:
                cursor.itersize = self.batch_size
                cursor.execute(self.args['hh_summary_query'], (master_account,))
                for row in cursor:
                    data = row.get("data")
                    if not data:
                        continue

                    # Clean the JSON
                    data = self._clean_json(data)
                    if not data:
                        continue  # skip if everything got removed

                    if wrote_any:
                        f.write(",")

                    f.write(json.dumps(data, default=str))

                    wrote_any = True

            f.write("]}}")

        print(f"Created hh_summary.json at {self.hh_summary_output_file}")

        # Upload to hh_summary file to S3
        s3_path = self._upload_to_s3(master_account, self.summary_party_contact_type_kafka)

        #Remove hh_summary file
        try:
            os.remove(self.hh_summary_output_file)
        except FileNotFoundError:
            pass

        self.kafka_metadata.append({
            "accountNumber": master_account, "type": 'SUMMARY', "fileName": s3_path
        })


    # ------------------------------------------------------------
    def _upload_to_s3(self, party_id, party_contact_type):
        try:
            filename = self._generate_s3_filename(party_id, party_contact_type)
            key = f"{self.args['s3_wip']}/{filename}"

            if party_contact_type != self.summary_party_contact_type_kafka:
                self.s3.upload_file(self.output_file, self.bucket, key)
            else:
                self.s3.upload_file(self.hh_summary_output_file, self.bucket, key)

            path = f"{self.bucket}/{key}"
            self.logger.success("Uploaded file to S3", {"path": path})
            return key

        except Exception as e:
            self.logger.error("S3 upload failed", {"record": party_id}, e)
            raise

    # ------------------------------------------------------------

    def _generate_s3_filename(self, account, contact_type):
        try:
            proc_type = self._get_process_type("fileName")
            return f"{self.client_name}_{contact_type}_{account}_{proc_type}_{self.uuid_value}.json"
        except Exception as e:
            self.logger.error("Failed to generate S3 file name", {}, e)
            raise

    def _get_process_type(self, key):
        try:
            ranges = json.loads(self.args["periodDateRange"])
            current_day = self.execution_time.day
            for row in ranges:
                symbol, num = row["daterange"].split(" ")
                num = int(num)
                if ((symbol == ">" and current_day > num) or (symbol == "<" and current_day < num)) and key in row:
                    return row[key]
            raise ValueError("No valid process match found")
        except Exception as e:
            self.logger.error("Failed to resolve process type", {"key": key}, e)
            raise

    def _publish_kafka(self):
        try:
            msg = {
                "clientName": self.client_name,
                "accountType": self.accountType,
                "accountNumber": self.account_number,
                "uuid": self.uuid_value,
                "bucketName": self.bucket,
                "runType": self._get_process_type("topicName"),
                "createdTimestamp": self.timestamp,
                "env": self.args["runType"],
                "fileLocation": self.kafka_metadata,
            }
            self.kafka.send(self.input_file, msg)
            aqa_glue_job_json = json.loads(self.aqa_report_glue_job_name)

            if len(aqa_glue_job_json) == 1:
                self._invoke_aqa_report_generation(msg, aqa_glue_job_json)
            else:
                self.logger.info(
                    f"Skipping AQA report execution as the aqa glue job name received is:{self.aqa_report_glue_job_name}",
                    {"file": self.input_file})

        except Exception as e:
            self.logger.error("Kafka publishing failure", {"file": self.input_file}, e)
            # Continue raising so pipeline halts cleanly
            raise

    def _invoke_aqa_report_generation(self, msg, aqa_glue_job_json):
        """
        Adding for invocation of the aqa report glue_client
        #todo: Need to remove later as this is only for the QA team to test it.
                a good solution is required to generate the aqa report.
        """
        try:
            # adding the origin into the kafka message
            msg["origin"] = self.origin

            # create as a list to send to glue as a format.
            kafka_messages = [msg]

            # invocation of aqa report generator
            response = self.glue.start_job_run(
                JobName=list(aqa_glue_job_json.values())[0],
                # Optional: Pass arguments if your job requires them
                Arguments={
                    '--region': self.region,
                    '--dpm_to_wifcore_kafka_messages': json.dumps(kafka_messages)
                }
            )

            self.logger.info(f"AQA report execution Job Run ID: { response['JobRunId'] }", {"file": self.input_file})

        except Exception as e:
            self.logger.error("AQA report execution failure", {"file": self.input_file}, e)
            # Continue raising so pipeline halts cleanly
            raise


    def _clean_json(self, data):
        """Recursively remove empty, null, or meaningless JSON entries."""
        empty_values = (None, {}, [], [{}])

        if isinstance(data, dict):
            return {
                k: v_clean
                for k, v in data.items()
                if (v_clean := self._clean_json(v)) not in empty_values
            }

        elif isinstance(data, list):
            return [
                item_clean
                for item_clean in (self._clean_json(i) for i in data)
                if item_clean not in empty_values
            ]

        else:
            return data


# ============================================================
def main():
    log = LoggerManager()
    try:
        args = JobArguments()
        job = DataPostgres(args)
        job._create_db_json()
    except Exception as e:
        log.critical("Glue job aborted", {}, e)
        raise


if __name__ == "__main__":
    main()