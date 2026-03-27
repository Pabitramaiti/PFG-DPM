"""LPL quarterly merge – Spark-friendly job.

This is a refactor of `pytest/lpl_quarterly_merge.py` into a parameterized
script suitable to run on Spark (AWS Glue / EMR / Databricks).

Important: the merge itself is a file-level operation (preserve line order),
so it runs on the Spark *driver* and does not distribute work across executors.
Spark is used as the runtime container.

Input contract:
- 3 input files (paths can be local FS or s3://... depending on runtime)
- one output file
- record types list (default matches original script)

If you run on AWS Glue and use S3 paths:
- prefer s3://bucket/key
- Glue provides libraries to access S3. For simplicity and testability, we
  use `boto3` for S3 and normal `open()` for local paths.

Supported path types:
- local paths (Windows/Linux)
- s3://bucket/key
"""

from __future__ import annotations

import argparse
import io
import logging
import os
from collections import OrderedDict
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, Iterator, List, Optional, Tuple
import splunk

# Glue-specific imports
from awsglue.context import GlueContext
from pyspark.context import SparkContext

try:
    import boto3  # type: ignore
except Exception:  # pragma: no cover
    boto3 = None

LOGGER = logging.getLogger("lpl_quarterly_merge")
# Constants
DEFAULT_RECORD_TYPES = ["001", "002", "050", "060", "065", "070", "200", "300", "500", "998", "999"]
HEADER_TYPES = {"001", "002"}
TRAILER_TYPES = {"998", "999"}


def get_record_type(line: str) -> str:
    return line[:3]

def get_run_id() -> str:
    if boto3 is None:
        return "arn:dpm:glue:glue_lpl_qtrly_merge:unknown"

    try:
        account_id = boto3.client("sts").get_caller_identity()["Account"]
        return f"arn:dpm:glue:glue_lpl_qtrly_merge:{account_id}"
    except Exception:
        LOGGER.exception("Unable to resolve AWS account id for run_id")
        return "arn:dpm:glue:glue_lpl_qtrly_merge:unknown"


def get_account_number(line: str) -> str:
    # bytes 13+ (0-based index 12)
    return line[12:21].strip()


def is_002_with_001(line: str) -> bool:
    return get_record_type(line) == "002" and line[9:12] == "001"


def _is_s3_path(path: str) -> bool:
    return path.lower().startswith("s3://")


def _parse_s3_uri(uri: str) -> Tuple[str, str]:
    # s3://bucket/key
    no_scheme = uri[5:]
    bucket, _, key = no_scheme.partition("/")
    if not bucket or not key:
        raise ValueError(f"Invalid s3 uri: {uri}")
    return bucket, key

def add_datetime_suffix(filepath: str) -> str:
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")

    # Split folder path and file name using the last "/"
    last_slash = filepath.rfind("/")

    if last_slash >= 0:
        path_part = filepath[:last_slash + 1]
        filename = filepath[last_slash + 1:]
    else:
        path_part = ""
        filename = filepath

    # Split filename by the last "."
    last_dot = filename.rfind(".")

    if last_dot <= 0:
        new_filename = f"{filename}_{timestamp}"
    else:
        base = filename[:last_dot]
        ext = filename[last_dot:]
        new_filename = f"{base}_{timestamp}{ext}"

    return path_part + new_filename

@contextmanager
def open_text(path: str, mode: str, *, s3_client=None, encoding: str = "utf-8") -> Iterator[io.TextIOBase]:
    """Open local or S3 path as a text file.

    For S3:
      - read: downloads object into memory (ok for these statement-like files)
      - write: buffers in memory then uploads on close
    """

    if "b" in mode:
        raise ValueError("Binary mode not supported for open_text")

    if not _is_s3_path(path):
        # Local FS
        f = open(path, mode, encoding=encoding, newline="")
        try:
            yield f
        finally:
            f.close()
        return

    if s3_client is None:
        if boto3 is None:
            raise ImportError("boto3 is required to read/write s3:// paths")
        s3_client = boto3.client("s3")

    bucket, key = _parse_s3_uri(path)

    if "r" in mode:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        body = obj["Body"]
        f = io.TextIOWrapper(body, encoding=encoding, newline="")
        try:
            yield f
        finally:
            f.close()
        return

    if "w" in mode:
        buf = io.StringIO()
        try:
            yield buf
            data = buf.getvalue().encode(encoding)
            s3_client.put_object(Bucket=bucket, Key=key, Body=data)
        finally:
            buf.close()
        return

    raise ValueError(f"Unsupported mode: {mode}")


@dataclass
class MergeInputs:
    file_1_path: str
    file_2_path: str
    file_3_path: str
    output_path: str
    record_types: List[str]


def validate_record_types(record_types: List[str]) -> List[str]:
    if not record_types:
        raise ValueError("record_types must not be None or empty")

    invalid = set(record_types) - set(DEFAULT_RECORD_TYPES)
    if invalid:
        raise ValueError(f"Invalid record_types: {sorted(invalid)}")

    return list(dict.fromkeys(record_types))

def read_trigger_file(trigger_file_path: str, *, s3_client=None) -> List[str]:
    with open_text(trigger_file_path, "r", s3_client=s3_client) as f:
        files = [line.strip() for line in f if line.strip()]

    if len(files) != 4:
        raise ValueError(
            f"Trigger file must contain exactly 4 non-empty lines, found {len(files)}"
        )

    return files


def validate_inputs(inputs: MergeInputs) -> None:
    for field_name, value in [
        ("file_1_path", inputs.file_1_path),
        ("file_2_path", inputs.file_2_path),
        ("file_3_path", inputs.file_3_path),
        ("output_path", inputs.output_path),
    ]:
        if value is None or not str(value).strip():
            raise ValueError(f"{field_name} must be a non-empty string")

    validate_record_types(inputs.record_types)

def merge_files(inputs: MergeInputs, *, s3_client=None) -> None:
    run_id = get_run_id()
    splunk.log_message({
        "Status": "merge files Started",
        "Message": "merge_files input validation successful",
        "file_1_path": inputs.file_1_path,
        "file_2_path": inputs.file_2_path,
        "file_3_path": inputs.file_3_path,
        "output_path": inputs.output_path,
        "record_types": inputs.record_types,
        "record_type_count": len(inputs.record_types)}, run_id)

    files = [inputs.file_1_path, inputs.file_2_path, inputs.file_3_path]

    # 1) Build account number set and order from file_3
    file_3_accounts: "OrderedDict[str, None]" = OrderedDict()
    with open_text(inputs.file_3_path, "r", s3_client=s3_client) as f3:
        for line in f3:
            rt = get_record_type(line)
            if rt not in HEADER_TYPES and rt not in TRAILER_TYPES:
                acct = get_account_number(line)
                if acct and acct not in file_3_accounts:
                    file_3_accounts[acct] = None

    # 2) Collect records needed by account + record type
    account_records: Dict[str, "OrderedDict[str, List[str]]"] = {acct: OrderedDict() for acct in file_3_accounts}
    account_065: Dict[str, List[str]] = {acct: [] for acct in file_3_accounts}

    # Pre-create all buckets so ordering is stable
    for acct in file_3_accounts:
        for rt in inputs.record_types:
            if rt not in HEADER_TYPES and rt not in TRAILER_TYPES and rt != "065":
                account_records[acct][rt] = []

    # 2a) merge and write Output
    with open_text(inputs.output_path, "w", s3_client=s3_client) as fout:
        # headers: write record types 001/002 from all files
        for rt in inputs.record_types:
            if rt in HEADER_TYPES:
                for fpath in files:
                    with open_text(fpath, "r", s3_client=s3_client) as fin:
                        for line in fin:
                            if get_record_type(line) == rt:
                                if rt == "002":
                                    if is_002_with_001(line):
                                        fout.write(line)
                                else:
                                    fout.write(line)

        # collect 065 from file_3 only
        if "065" in inputs.record_types:
            with open_text(inputs.file_3_path, "r", s3_client=s3_client) as f3:
                for line in f3:
                    if get_record_type(line) == "065":
                        acct = get_account_number(line)
                        if acct in account_065:
                            account_065[acct].append(line)

        # collect other types per account, from file_1, file_2, file_3 order
        needed_types = [
            rt for rt in inputs.record_types
            if rt not in HEADER_TYPES and rt not in TRAILER_TYPES and rt != "065"
        ]

        if needed_types:
            for fpath in files:
                with open_text(fpath, "r", s3_client=s3_client) as fin:
                    for line in fin:
                        rt = get_record_type(line)
                        if rt not in needed_types:
                            continue
                        acct = get_account_number(line)
                        if acct in account_records:
                            account_records[acct][rt].append(line)

        # 3) Write grouped records by account and requested record_types order
        for acct in file_3_accounts:
            LOGGER.info("Processing account number: %s", acct)
            for rt in inputs.record_types:
                if rt == "065":
                    for line in account_065[acct]:
                        fout.write(line)
                elif rt in account_records[acct]:
                    for line in account_records[acct][rt]:
                        fout.write(line)

        # 4) trailer records from file_3 only
        for rt in inputs.record_types:
            if rt in TRAILER_TYPES:
                with open_text(inputs.file_3_path, "r", s3_client=s3_client) as f3:
                    for line in f3:
                        if get_record_type(line) == rt:
                            fout.write(line)

    splunk.log_message(
        {
            "Status": "success",
            "Message": "merge_files completed successfully",
            "output_path": inputs.output_path,
        },run_id)

def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="LPL Quarterly Merge (Spark job)")

    parser.add_argument("--trigger-file", required=False, dest="trigger_file")
    parser.add_argument("--file-1", required=False, dest="file_1")
    parser.add_argument("--file-2", required=False, dest="file_2")
    parser.add_argument("--file-3", required=False, dest="file_3")
    parser.add_argument("--output", required=False, dest="output")
    parser.add_argument(
        "--record-types",
        default=",".join(DEFAULT_RECORD_TYPES),
        help="Comma-separated record types in the output order",
    )

    args, _ = parser.parse_known_args(list(argv) if argv is not None else None)

    if args.trigger_file:
        return args

    required = [args.file_1, args.file_2, args.file_3, args.output]
    if not all(required):
        parser.error(
            "Either --trigger-file OR all of --file-1 --file-2 --file-3 --output must be provided"
        )
    return args

def build_inputs_from_args(args: argparse.Namespace) -> MergeInputs:
    record_types = [x.strip() for x in (args.record_types or "").split(",") if x.strip()]
    if not record_types:
        record_types = DEFAULT_RECORD_TYPES
    else:
        record_types = validate_record_types(record_types)

    if args.trigger_file:
        qtrly_files = read_trigger_file(args.trigger_file)
        return MergeInputs(
            file_1_path=qtrly_files[0],
            file_2_path=qtrly_files[1],
            file_3_path=qtrly_files[2],
            output_path=add_datetime_suffix(qtrly_files[3]),
            record_types=record_types,
        )

    return MergeInputs(
        file_1_path=args.file_1,
        file_2_path=args.file_2,
        file_3_path=args.file_3,
        output_path=add_datetime_suffix(args.output),
        record_types=record_types,
    )

def main(argv: Optional[Iterable[str]] = None) -> int:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(), format="%(message)s")
    try:
        args = parse_args(argv)
        inputs = build_inputs_from_args(args)
        validate_inputs(inputs)

        merge_files(inputs)
        LOGGER.info("Merge completed successfully")
        return 0
    except Exception as exc:
        try:
            splunk.log_message({ "Status": "Merge filesfailure", "Message": str(exc),},get_run_id(),)
        except Exception:
            LOGGER.exception("Failed to write failure log to Splunk", Exception)
        return 1

if __name__ == "__main__":
    rc = main()
    if rc:
        raise SystemExit(rc)
