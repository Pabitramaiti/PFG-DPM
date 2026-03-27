# Driver Creation - Split ZIPs per size & Manifest file (Optimized)
import json
import sys
import boto3
import os
from awsglue.utils import getResolvedOptions
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import splunk
from io import BytesIO
import zipfile
import traceback
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

s3 = boto3.client("s3")
ssm_client = boto3.client("secretsmanager")

# -----------------------------------------
# Helpers
# -----------------------------------------
def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:cdm_statement_driver_creation:" + account_id


def get_secret_values(secret_name):
    response = ssm_client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])


# -----------------------------------------
# Main class
# -----------------------------------------
class DataPostgres:
    def __init__(self):
        self.conn = None
        self.engine = None

        self.trigger_file_name = os.getenv("trigger_file_name")
        self.run_type = (
            self.trigger_file_name.split("_")[1]
            if len(self.trigger_file_name.split("_")) > 2
            else ""
        )

        self.sql_queries = json.loads(os.getenv("sql_queries"))

        # Direct values
        self.query_staging_tables = self.sql_queries["query_staging_tables"]
        self.column_names = self.sql_queries["column_names"]
        self.merge_and_drop_historical_tables = self.sql_queries[
            "merge_and_drop_historical_tables"
        ]
        self.drop_staging_tables = self.sql_queries["drop_staging_tables"]
        self.fetch_staging_tables = self.sql_queries["fetch_staging_tables"]
        self.combinations = self.sql_queries["combinations"]
        self.filetype = self.sql_queries["filetype"]
        self.batch_size = int(self.sql_queries["batch_size"])
        self.zip_file_size = int(self.sql_queries["zip_file_size"])

        # DB params
        self.tableinfo = json.loads(os.getenv("tableInfo"))
        self.region = self.tableinfo["region"]
        self.dbkey = self.tableinfo["dbkey"]
        self.dbname = self.tableinfo["dbname"]
        self.schema = self.tableinfo["schema"]

        self.s3_bucket = os.getenv("s3_bucket")
        self.s3_wip = os.getenv("s3_wip")

        self.message = ""

        self.create_connection()

    # -----------------------------------------
    # DB Connection
    # -----------------------------------------
    def create_connection(self):
        try:
            ssm = boto3.client("ssm", self.region)
            secret_name = ssm.get_parameter(
                Name=self.dbkey, WithDecryption=True
            )["Parameter"]["Value"]
            host = get_secret_values(secret_name)

            self.engine = create_engine(
                f"postgresql://{host['username']}:{host['password']}@{host['host']}:{host['port']}/{self.dbname}",
                future=True,
            )
            self.conn = self.engine.connect()

            splunk.log_message(
                {
                    "Status": "success",
                    "InputFileName": "test file",
                    "Message": "Connection with Aurora Postgres established",
                },
                get_run_id(),
            )

        except Exception as e:
            msg = f"Connection failed: {e}"
            splunk.log_message(
                {"Status": "failed", "InputFileName": "test file", "Message": msg},
                get_run_id(),
            )
            raise

    # -----------------------------------------
    # Fetch staging table records
    # -----------------------------------------
    def fetch_staging_records(self):
        try:
            uuid_part = self.s3_wip.split("/")[-1].replace("-", "_")
            table_pattern = f"staging_table_{uuid_part}_%"

            with self.conn.begin():
                self.conn.execute(text(f"SET search_path TO {self.schema}"))

                sql = self.fetch_staging_tables.format(
                    schema_name=self.schema, table_name_pattern=table_pattern
                )
                result = self.conn.execute(text(sql))
                tables = [row[0] for row in result.fetchall()]

                if not tables:
                    print("No staging tables found.")
                    return []

                all_records = []
                for tbl in tables:
                    try:
                        q = self.query_staging_tables.format(table_name=tbl)
                        result = self.conn.execute(text(q))
                        all_records.extend([dict(r) for r in result.fetchall()])
                    except Exception as e:
                        print(f"Error reading {tbl}: {e}")
                        continue

                return all_records

        except Exception as e:
            print(f"Error fetching staging records: {e}")
            raise

    # -----------------------------------------
    # Parallel S3 download
    # -----------------------------------------
    def download_file(self, file):
        """Download one file from S3 and return (filename, content)."""
        s3_key = file["s3path"]
        filename = os.path.basename(s3_key)

        start = time.time()
        obj = s3.get_object(Bucket=self.s3_bucket, Key=s3_key)
        content = obj["Body"].read()
        end = time.time()

        #print(f"Downloaded {filename} in {end - start:.3f}s")
        return filename, content

    # -----------------------------------------
    # Fast ZIP creation
    # -----------------------------------------
    def create_zip_and_manifest(self, base_filename, files):
        max_zip_size_bytes = self.zip_file_size * 1024 * 1024

        manifest_lines = []
        zip_index = 1
        current_zip_files = []
        current_zip_size = 0

        # Parallel S3 downloads
        print("Starting parallel S3 downloads...")
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {executor.submit(self.download_file, file): file for file in files}

            for future in as_completed(futures):
                filename, content = future.result()
                file_size = len(content)

                # Check ZIP size threshold
                if (
                    current_zip_size + file_size > max_zip_size_bytes
                    and current_zip_files
                ):
                    zip_name = f"{base_filename}_{zip_index}.zip"

                    start_zip = time.time()
                    self.upload_zip(zip_name, current_zip_files)
                    end_zip = time.time()

                    print(
                        f"Created+Uploaded ZIP {zip_name} in {end_zip - start_zip:.3f}s"
                    )

                    manifest_lines.append(f"{zip_name}|{len(current_zip_files)}")
                    zip_index += 1
                    current_zip_files = []
                    current_zip_size = 0

                current_zip_files.append((filename, content))
                current_zip_size += file_size

        # Remaining ZIP
        if current_zip_files:
            zip_name = f"{base_filename}_{zip_index}.zip"

            start = time.time()
            self.upload_zip(zip_name, current_zip_files)
            end = time.time()

            print(f"Created+Uploaded final ZIP {zip_name} in {end:.3f}s")
            manifest_lines.append(f"{zip_name}|{len(current_zip_files)}")

        # Manifest upload
        total_files = sum(int(line.split("|")[1]) for line in manifest_lines)
        manifest_lines.append(f"total_files:{total_files}")
        manifest = "\n".join(manifest_lines).encode("utf-8")

        manifest_key = f"{self.s3_wip}/{base_filename}_manifest.txt"
        s3.upload_fileobj(BytesIO(manifest), self.s3_bucket, manifest_key)
        print(f"Uploaded manifest file to s3://{self.s3_bucket}/{manifest_key}")

    # -----------------------------------------
    # ZIP upload helper
    # -----------------------------------------
    def upload_zip(self, zip_name, files):
        zip_buffer = BytesIO()
        with zipfile.ZipFile(
            zip_buffer, "w", compression=zipfile.ZIP_DEFLATED
        ) as zipf:
            for filename, content in files:
                zipf.writestr(filename, content)

        zip_buffer.seek(0)
        s3_key = f"{self.s3_wip}/{zip_name}"
        s3.upload_fileobj(zip_buffer, self.s3_bucket, s3_key)
        print(f"Uploaded ZIP: s3://{self.s3_bucket}/{s3_key}")

    # -----------------------------------------
    # Drop staging tables + merge historical tables
    # -----------------------------------------
    def drop_merge_tables(self):
        uuid_part = self.s3_wip.split("/")[-1].replace("-", "_")

        staging_pattern = f"staging_table_{uuid_part}_%"
        historical_pattern = f"historical_table_{uuid_part}_%"

        q1 = self.drop_staging_tables.format(
            table_name_pattern=staging_pattern, schema_name=self.schema
        )
        q2 = self.merge_and_drop_historical_tables.format(
            table_name_pattern=historical_pattern,
            schema_name=self.schema,
            run_type=self.run_type,
        )

        with self.conn.begin():
            self.conn.execute(text(f"SET search_path TO {self.schema}"))
            self.conn.execute(text(q1))
            self.conn.execute(text(q2))

    # -----------------------------------------
    # Process all combinations
    # -----------------------------------------
    def process_combinations(self):
        all_records = self.fetch_staging_records()
        if not all_records:
            print("No records in staging tables.")
            return

        for combo in self.combinations:
            category = combo["category"]
            template = combo["base_filename"]

            filtered = [r for r in all_records if r["category"] == category]
            if not filtered:
                print(f"No files found for category {category}")
                continue

            base_filename = template.format(
                mmddyyyy=datetime.now().strftime("%m%d%Y"),
                timestamp=datetime.now().strftime("%H%M%S"),
            )

            self.create_zip_and_manifest(base_filename, filtered)


# -----------------------------------------
# Glue Params Loader
# -----------------------------------------
def set_job_params_as_env_vars():
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith("--"):
            os.environ[sys.argv[i][2:]] = sys.argv[i + 1]


# -----------------------------------------
# Main
# -----------------------------------------
def main():
    try:
        set_job_params_as_env_vars()
    except Exception as e:
        msg = f"Failed to parse input parameters: {e}"
        splunk.log_message(
            {"Status": "failed", "InputFileName": "Test", "Message": msg},
            get_run_id(),
        )
        raise

    try:
        dp = DataPostgres()
        dp.process_combinations()
        dp.drop_merge_tables()
    except Exception as e:
        tb = traceback.format_exc()
        print(f"ERROR TRACE:\n{tb}")

        msg = f"Driver process failed: {e}"
        splunk.log_message(
            {"Status": "failed", "InputFileName": "Test", "Message": msg},
            get_run_id(),
        )
        raise


if __name__ == "__main__":
    main()