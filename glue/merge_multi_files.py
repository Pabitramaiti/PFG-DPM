import argparse
import io
import logging
import os
import json
from datetime import datetime
from collections import OrderedDict
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

try:
    import boto3  # type: ignore
except Exception:
    boto3 = None

import splunk

LOGGER = logging.getLogger("lpl_quarterly_merge")
DEFAULT_RECORD_TYPES = ["050", "060", "065", "070", "200", "300", "500"]
DEFAULT_HEADER_TYPES = ["001", "002"]
DEFAULT_TRAILER_TYPES = ["998", "999"]


def get_record_type(line: str) -> str:
    return line[:3]


def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return f"arn:dpm:glue:glue_lpl_qtrly_merge:" + account_id


def get_account_number(line: str) -> str:
    return line[12:21].strip()


def _is_s3_path(path: str) -> bool:
    return path.lower().startswith("s3://")


def _parse_s3_uri(uri: str):
    if not uri.lower().startswith("s3://"):
        raise ValueError(f"Invalid s3 uri: {uri}")
    no_scheme = uri[5:] if uri.startswith("s3://") else uri
    parts = no_scheme.split('/', 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError(f"Invalid s3 uri: {uri}")
    bucket, key = parts
    return bucket, key


@contextmanager
def open_text(path: str, mode: str, *, s3_client=None, encoding: str = "utf-8"):
    if "b" in mode:
        raise ValueError("Binary mode not supported for open_text")
    if not _is_s3_path(path):
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
    LOGGER.debug(f"open_text: path={path}, bucket={bucket}, key={key}, mode={mode}")
    if "r" in mode:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        body = obj["Body"]
        f = io.TextIOWrapper(body, encoding=encoding)
        try:
            yield f
        finally:
            f.detach()
        return
    if "w" in mode:
        import tempfile
        with tempfile.NamedTemporaryFile(mode="w+", encoding=encoding, delete=False) as tmpfile:
            yield tmpfile
            tmpfile.flush()
            tmpfile.seek(0)
            s3_client.put_object(Bucket=bucket, Key=key, Body=tmpfile.read().encode(encoding))
        return
    raise ValueError(f"Unsupported mode: {mode}")


@dataclass
class MergeInputs:
    file_1_path: str
    file_2_path: str
    file_3_path: str
    output_path: str
    record_types: List[str]
    header_types: List[str]
    trailer_types: List[str]
    sub_firm: str
    report_folder: str
    s3_bucket: str


def validate_record_types(record_types: list[str]) -> list[str]:
    if not record_types:
        raise ValueError("record_types must not be None or empty")
    invalid = set(record_types) - set(DEFAULT_RECORD_TYPES)
    if invalid:
        raise ValueError(f"Invalid record_types: {sorted(invalid)}")
    return [rt for rt in record_types if rt in DEFAULT_RECORD_TYPES]


def get_file_paths(input_file, s3_bucket, input_folder, output_folder):
    with open_text(input_file, "r") as f:
        lines = [line.strip() for line in f if line.strip()]
    if len(lines) != 4:
        raise ValueError("Input file must contain exactly 4 lines (3 input files, 1 output file)")

    def normalize_s3_path(line, folder):
        if line.lower().startswith("s3://"):
            return line
        if "/" in line and not line.lower().startswith("s3://"):
            return f"s3://{line}"
        return f"s3://{s3_bucket}/{folder}/{line}"

    file_1 = normalize_s3_path(lines[0], input_folder)
    file_2 = normalize_s3_path(lines[1], input_folder)
    file_3 = normalize_s3_path(lines[2], input_folder)
    output = normalize_s3_path(lines[3], output_folder)
    return file_1, file_2, file_3, output


def merge_files(inputs: MergeInputs, *, s3_client=None) -> None:
    files = [inputs.file_1_path, inputs.file_2_path, inputs.file_3_path]
    file_3_accounts = OrderedDict()

    # Count record_type "050" in file_3_path and product classes
    BETA_050_RECORDS = 0
    CCCA = 0
    PRODUCT_CLASS_BOI = 0
    PRODUCT_CLASS_QOI = 0
    with open_text(inputs.file_3_path, "r", s3_client=s3_client) as f3:
        for line in f3:
            if get_record_type(line) == "050":
                BETA_050_RECORDS += 1
                act_rep_no = line[480:484]
                if act_rep_no == "CCCA":
                    CCCA += 1
                product_class = line[440:443]
                if product_class == "BOI":
                    PRODUCT_CLASS_BOI += 1
                if product_class == "QOI":
                    PRODUCT_CLASS_QOI += 1

    print(f'Number of "050" records in file_3_path: {BETA_050_RECORDS}')
    print(f'Number of "050" records with ACT-REP-NO "CCCA": {CCCA}')
    print(f'Number of "050" records with PRODUCT-CLASS "BOI": {PRODUCT_CLASS_BOI}')
    print(f'Number of "050" records with PRODUCT-CLASS "QOI": {PRODUCT_CLASS_QOI}')

    with open_text(inputs.file_3_path, "r", s3_client=s3_client) as f3:
        for line in f3:
            rt = get_record_type(line)
            if rt not in inputs.header_types and rt not in inputs.trailer_types:
                acct = get_account_number(line)
                if acct and acct not in file_3_accounts:
                    file_3_accounts[acct] = None
    account_lines = {acct: {rt: [] for rt in inputs.record_types} for acct in file_3_accounts}
    with open_text(inputs.file_3_path, "r", s3_client=s3_client) as f3:
        for line in f3:
            rt = get_record_type(line)
            if rt == "065":
                acct = get_account_number(line)
                if acct in account_lines:
                    account_lines[acct]["065"].append(line)
    for rt in inputs.record_types:
        if rt in inputs.header_types or rt in inputs.trailer_types or rt == "065":
            continue
        for fpath in files:
            with open_text(fpath, "r", s3_client=s3_client) as fin:
                for line in fin:
                    if get_record_type(line) == rt:
                        acct = get_account_number(line)
                        if acct in account_lines:
                            account_lines[acct][rt].append(line)
    with open_text(inputs.output_path, "w", s3_client=s3_client) as fout:
        header_001 = []
        header_002 = []
        for fpath in files:
            with open_text(fpath, "r", s3_client=s3_client) as fin:
                for line in fin:
                    rt = get_record_type(line)
                    if rt == "001":
                        header_001.append(line)
                    elif rt == "002":
                        sub_firm_val = line[9:12]
                        if sub_firm_val == inputs.sub_firm:
                            header_002.append(line)
        for line in header_001:
            fout.write(line)
        for line in header_002:
            fout.write(line)
        for acct in file_3_accounts:
            for rt in inputs.record_types:
                for line in account_lines[acct][rt]:
                    fout.write(line)
        for rt in inputs.trailer_types:
            with open_text(inputs.file_3_path, "r", s3_client=s3_client) as f3:
                for line in f3:
                    if get_record_type(line) == rt:
                        fout.write(line)

    # --- Write JSON report at the end ---
    now = datetime.now().strftime("%Y-%m_%dT%H%M%S")
    report_filename = f"dpm-enhanced-report-{now}.json"
    # Always treat report_folder as S3 key prefix, never as a full S3 URI or local path
    key_prefix = inputs.report_folder.strip("/")
    report_path = f"s3://{inputs.s3_bucket}/{key_prefix}/{report_filename}"
    print(f"Final report_path: {report_path}")

    report_data = {
        "BETA_050_RECORDS": BETA_050_RECORDS,
        "TOTAL_050_RECORDS_BY_SUBFIRM": BETA_050_RECORDS,
        "BETA_050_RECORDS_BY_SUBFIRM": BETA_050_RECORDS,
        "CCCA": CCCA,
        "PRODUCT_CLASS_BOI": PRODUCT_CLASS_BOI,
        "PRODUCT_CLASS_QOI": PRODUCT_CLASS_QOI,
        "TOTAL_BOI_QOI_ACCOUNTS_PRODUCED": PRODUCT_CLASS_BOI + PRODUCT_CLASS_QOI
    }

    with open_text(report_path, "w", s3_client=s3_client, encoding="utf-8") as report_file:
        json.dump(report_data, report_file, indent=2)
    print(f"Enhanced report written to: {report_path}")


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="LPL Quarterly Merge (Spark job)")
    p.add_argument("--input-file", required=True, help="File containing input and output filenames")
    p.add_argument("--s3-bucket", required=True, help="S3 bucket name")
    p.add_argument("--input-folder", required=True, help="S3 input folder")
    p.add_argument("--output-folder", required=True, help="S3 output folder")
    p.add_argument(
        "--record-types",
        default=",".join(DEFAULT_RECORD_TYPES),
        help="Comma-separated record types in the output order",
    )
    p.add_argument(
        "--header-types",
        default=",".join(DEFAULT_HEADER_TYPES),
        help="Comma-separated header record types (default: 001,002)",
    )
    p.add_argument(
        "--trailer-types",
        default=",".join(DEFAULT_TRAILER_TYPES),
        help="Comma-separated trailer record types (default: 998,999)",
    )
    p.add_argument(
        "--sub-firm",
        default="001",
        help="Sub firm code to filter 002 header records (default: 001)",
    )
    p.add_argument(
        "--report-folder",
        required=True,
        help="S3 key prefix (not a full S3 URI) under the bucket to write the enhanced JSON report, e.g. lpl_priceje/report"
    )
    args, _ = p.parse_known_args(list(argv) if argv is not None else None)
    return args


def normalize_input_file_path(input_file, s3_bucket, input_folder):
    if input_file.lower().startswith("s3://"):
        return input_file
    if "/" in input_file and not input_file.lower().startswith("s3://"):
        return f"s3://{input_file}"
    return f"s3://{s3_bucket}/{input_folder}/{input_file}"


def main(argv: Optional[Iterable[str]] = None) -> int:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(), format="%(message)s")
    args = parse_args(argv)
    record_types = [x.strip() for x in (args.record_types or "").split(",") if x.strip()]
    header_types = [x.strip() for x in (args.header_types or "").split(",") if x.strip()]
    trailer_types = [x.strip() for x in (args.trailer_types or "").split(",") if x.strip()]
    if not record_types:
        record_types = list(DEFAULT_RECORD_TYPES)
    else:
        record_types = validate_record_types(record_types)
    input_file = normalize_input_file_path(args.input_file, args.s3_bucket, args.input_folder)
    file_1, file_2, file_3, output = get_file_paths(
        input_file, args.s3_bucket, args.input_folder, args.output_folder
    )
    LOGGER.info("file_1_path: %s", file_1)
    LOGGER.info("file_2_path: %s", file_2)
    LOGGER.info("file_3_path: %s", file_3)
    LOGGER.info("output_path: %s", output)
    print("file_1_path:", file_1)
    print("file_2_path:", file_2)
    print("file_3_path:", file_3)
    print("output_path:", output)
    inputs = MergeInputs(
        file_1_path=file_1,
        file_2_path=file_2,
        file_3_path=file_3,
        output_path=output,
        record_types=record_types,
        header_types=header_types,
        trailer_types=trailer_types,
        sub_firm=args.sub_firm,
        report_folder=args.report_folder,
        s3_bucket=args.s3_bucket,
    )
    if any(
            f is None or not str(f).strip()
            for f in [
                inputs.file_1_path,
                inputs.file_2_path,
                inputs.file_3_path,
                inputs.output_path,
            ]
    ):
        raise ValueError("All input paths must be non-empty strings")
    merge_files(inputs)
    return 0


if __name__ == "__main__":
    main()
