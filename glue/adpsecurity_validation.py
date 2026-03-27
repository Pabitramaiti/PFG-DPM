import sys
import boto3
import json
import os
from datetime import datetime
from awsglue.utils import getResolvedOptions
import splunk
import pytz
import io
import csv
from typing import Tuple, List, Set

# ----------------------------------------------------------------------------------------
def get_run_id(program_name: str) -> str:
    return f"arn:dpm:glue:{program_name}:{boto3.client('sts').get_caller_identity()['Account']}"

# ----------------------------------------------------------------------------------------
def get_configs():
    """Load Glue job parameters"""
    args_list = ['bucket_name', 'validate_folder', 'validate_save_folder', 'validate_failed_folder', 'input_folder','duplicated_folder', 'input_file']
    return getResolvedOptions(sys.argv, args_list)

# ----------------------------------------------------------------------------------------
def check_security_number(
    bucket_name: str,
    validate_folder: str,
    validate_save_folder: str,
    validate_failed_folder: str,
    input_folder: str,
    duplicated_folder: str,
    input_file: str
):
    """
    Validate CSVs in validate_folder against reference files and check the ADPSECURITYNUMBERs.
    Exclude .sha256 objects from validation processing but move them together with their data pairs.
    """
    s3 = boto3.client("s3")

    reference_prefixes = [
        "ETS50300_BIOS.C117.OUT.B228",
        "ETS50300_BIOS.C127.OUT.B228",
        "ETS50300_BIOS.C381.OUT.B228",
    ]
    validated_prefixes = ["STMTMMKT", "EURORATE"]

    valid_numbers: set = set()
    compared_numbers: set = set()

    # ------------------------------------------------------------------ #
    # STEP 1 –  discover objects
    # ------------------------------------------------------------------ #
    all_existing_files = []
    sha256_files = []
    paginator = s3.get_paginator("list_objects_v2")
    valid_prefixes = reference_prefixes + validated_prefixes
    for page in paginator.paginate(Bucket=bucket_name, Prefix=validate_folder):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            relname = key[len(validate_folder):]
            if not relname or "/" in relname:
                continue
            fname = relname.strip()

            # collect normal files (skip .sha256)
            if any(fname.startswith(pfx) for pfx in valid_prefixes):
                if fname.lower().endswith(".sha256"):
                    # separate list just for checksum files
                    sha256_files.append(fname)
                else:
                    all_existing_files.append(fname)

    print(f"Files (no .sha256) → {all_existing_files}")
    print(f"Checksum (.sha256) → {sha256_files}")

    if not all_existing_files:
        msg = f"No usable data/reference files under s3://{bucket_name}/{validate_folder}"
        splunk.log_message({"Status": "Info", "Message": msg}, get_run_id(program_name))
        print(msg)
        return True, [], []

    # ------------------------------------------------------------------ #
    # New logic block to handle input_file comparison against existing CSVs
    # ------------------------------------------------------------------ #

    # Ensure both STMTMMKT and EURORATE CSVs are already in all_existing_files
    required_csvs_exist = all(
        any(f.startswith(pfx) and f.lower().endswith(".csv") for f in all_existing_files)
        for pfx in validated_prefixes
    )

    if required_csvs_exist:
        print("Both STMTMMKT and EURORATE CSVs exist in all_existing_files — proceeding with new comparison logic.")

        input_prefix = None
        for pfx in validated_prefixes:
            if os.path.basename(input_file).startswith(pfx):
                input_prefix = pfx
                break

        if input_prefix:
            print(f"Checking input file with prefix {input_prefix}")
            input_filename = os.path.basename(input_file)
            input_base = os.path.splitext(input_filename)[0]

            # Gather all validate_folder files with the same prefix
            matching_existing_csv = [
                f for f in all_existing_files if f.startswith(input_prefix) and f.lower().endswith(".csv")
            ]
            matching_existing_sha = [
                f for f in sha256_files if f.startswith(input_prefix) and f.lower().endswith(".sha256")
            ]

            print(f"Existing CSVs for {input_prefix}: {matching_existing_csv}")
            print(f"Existing SHAs for {input_prefix}: {matching_existing_sha}")

            # If there are any existing CSVs of the same prefix
            if matching_existing_csv:
                for exist_csv in matching_existing_csv:
                    exist_base = os.path.splitext(exist_csv)[0]
                    if exist_base != input_base:
                        print(f"File name mismatch: input '{input_base}' vs existing '{exist_base}'")
                        # Move mismatched csv and corresponding sha256 from validate_folder to validate_failed_folder
                        to_move_files = [exist_csv]
                        for sha in matching_existing_sha:
                            if sha.startswith(exist_base):
                                to_move_files.append(sha)

                        for f in to_move_files:
                            src = f"{validate_folder}{f}"
                            dst = f"{validate_failed_folder}{f}"
                            try:
                                s3.copy_object(
                                    Bucket=bucket_name,
                                    CopySource={"Bucket": bucket_name, "Key": src},
                                    Key=dst,
                                )
                                s3.delete_object(Bucket=bucket_name, Key=src)
                                print(f"Moved mismatched {f} from validate_folder → validate_failed_folder")
                            except Exception as e:
                                print(f"Move failed for {f}: {str(e)}")

                # After moving, rebuild the file lists
                all_existing_files = []
                sha256_files = []
                paginator = s3.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=bucket_name, Prefix=validate_folder):
                    for obj in page.get("Contents", []):
                        key = obj["Key"]
                        relname = key[len(validate_folder):]
                        if not relname or "/" in relname:
                            continue
                        fname = relname.strip()
                        if any(fname.startswith(pfx) for pfx in valid_prefixes):
                            if fname.lower().endswith(".sha256"):
                                sha256_files.append(fname)
                            else:
                                all_existing_files.append(fname)

                print(f"Rebuilt files (no .sha256) → {all_existing_files}")
                print(f"Rebuilt checksum (.sha256) → {sha256_files}")
        else:
            print(f"Input file '{input_file}' does not start with validated prefixes {validated_prefixes}")
    else:
        print(
            "Skipping new comparison logic — both STMTMMKT and EURORATE CSVs do not yet exist in all_existing_files.")

    # ------------------------------------------------------------#
    # identify reference and csv data
    # ------------------------------------------------------------ #
    reference_flag = False
    csv_flag = False
    unique_reference_flag = False
    unique_csv_flag = False

    reference_files = [
        f for f in all_existing_files if any(f.startswith(p) for p in reference_prefixes)
    ]
    csv_files = [
        f for f in all_existing_files
        if any(f.startswith(p) for p in validated_prefixes) and f.lower().endswith(".csv")
    ]

    # ---------------- Reference files logic ---------------- #
    if len(reference_files) < len(reference_prefixes):
        # missing reference files, don't proceed with csv check
        reference_flag = False
        unique_reference_flag = True
    elif len(reference_files) > len(reference_prefixes):
        unique_reference_flag = False
        for f in reference_files:
            src, dst = f"{validate_folder}{f}", f"{duplicated_folder}{f}"
            try:
                s3.copy_object(Bucket=bucket_name, CopySource={"Bucket": bucket_name, "Key": src}, Key=dst)
                s3.delete_object(Bucket=bucket_name, Key=src)
            except Exception as e:
                print(f"Move failed for {f}: {str(e)}")
        msg = (f"Duplicated reference files detected. Expecting {len(reference_prefixes)} but found {len(reference_files)}.\n"
               f"Found duplicated files of {list(reference_files)}")
        splunk.log_message({"Status": "Error", "Message": msg}, get_run_id(program_name))
        print(msg)
        #return False, [], reference_files
        raise ValueError(msg)
    else:
        # equal length — check that all prefixes are present
        matched_refs = all(any(pfx in rf for rf in reference_files) for pfx in reference_prefixes)
        if matched_refs:
            reference_flag = True
            unique_reference_flag = True
        else:
            unique_reference_flag = False
            for f in reference_files:
                src, dst = f"{validate_folder}{f}", f"{duplicated_folder}{f}"
                try:
                    s3.copy_object(Bucket=bucket_name, CopySource={"Bucket": bucket_name, "Key": src}, Key=dst)
                    s3.delete_object(Bucket=bucket_name, Key=src)
                except Exception as e:
                    print(f"Move failed for {f}: {str(e)}")
            msg = (f"Reference file name mismatch or duplicates found: {reference_files}.\n"
                   f"Found mismatch or duplicates files of {list(reference_files)}")
            splunk.log_message({"Status": "Error", "Message": msg}, get_run_id(program_name))
            print(msg)
            #return False, [], reference_files
            raise ValueError(msg)
    # ---------------- CSV files logic ---------------- #
    if len(csv_files) < len(validated_prefixes):
        csv_flag = False
        unique_csv_flag = True
    elif len(csv_files) > len(validated_prefixes):
        unique_csv_flag = False
        duplicated_files = csv_files + sha256_files
        for f in duplicated_files:
            src, dst = f"{validate_folder}{f}", f"{duplicated_folder}{f}"
            try:
                s3.copy_object(Bucket=bucket_name, CopySource={"Bucket": bucket_name, "Key": src}, Key=dst)
                s3.delete_object(Bucket=bucket_name, Key=src)
            except Exception as e:
                print(f"Move failed for {f}: {str(e)}")
        msg = (f"Duplicated CSV files detected. Expecting {len(validated_prefixes)} but found {len(csv_files)}.\n"
               f"Found duplicated files of {list(csv_files)}")
        splunk.log_message({"Status": "Error", "Message": msg}, get_run_id(program_name))
        print(msg)
        #return False, [], csv_files
        raise ValueError(msg)
    else:
        # equal length — check that each validated prefix is represented in file names
        matched_csvs = all(any(pfx in cf for cf in csv_files) for pfx in validated_prefixes)
        if matched_csvs:
            csv_flag = True
            unique_csv_flag = True
        else:
            unique_csv_flag = False
            duplicated_files = csv_files + sha256_files
            for f in duplicated_files:
                src, dst = f"{validate_folder}{f}", f"{duplicated_folder}{f}"
                try:
                    s3.copy_object(Bucket=bucket_name, CopySource={"Bucket": bucket_name, "Key": src}, Key=dst)
                    s3.delete_object(Bucket=bucket_name, Key=src)
                except Exception as e:
                    print(f"Move failed for {f}: {str(e)}")
            msg = (f"CSV file name mismatch or duplicates found: {csv_files}.\n"
                   f"Found mismatch or duplicates files of {list(csv_files)}")
            splunk.log_message({"Status": "Error", "Message": msg}, get_run_id(program_name))
            print(msg)
            #return False, [], csv_files
            raise ValueError(msg)

    '''
    if not unique_reference_flag or not unique_csv_flag:
        msg = (f"Reference or CSV file name mismatch or duplicates found.\n"
                f"Found mismatch or duplicates files of {list(csv_files)} or {list(reference_files)}")
        splunk.log_message({"Status": "Error", "Message": msg}, get_run_id(program_name))
        print(msg)
        return False, [], csv_files + reference_files
    '''
    # ---------------- Final check for both ---------------- #
    if not reference_flag or not csv_flag:
        msg = "Missing input files; nothing to validate."
        splunk.log_message({"Status": "Info", "Message": msg}, get_run_id(program_name))
        print(msg)
        return True, [], csv_files + reference_files

    # ------------------------------------------------------------------ #
    # STEP 2 –  collect ADPSECURITYNUMBERs in references
    # ------------------------------------------------------------------ #
    for file_name in reference_files:
        key = f"{validate_folder}{file_name}"
        try:
            body = s3.get_object(Bucket=bucket_name, Key=key)["Body"].read().decode("utf-8")
        except Exception as e:
            print(f"Failed to read {key}: {e}")
            continue

        lines = [ln for ln in body.splitlines() if ln.strip()]
        if len(lines) < 3:
            continue

        header = lines[0].split("||")
        try:
            idx_sec1 = header.index("50300-POS-ADP-SEC-1")
            idx_sec2_7 = header.index("50300-POS-ADP-SEC-2-7")
        except ValueError:
            continue

        for row in lines[1:]:
            if row.startswith("TRAILER"):
                break
            cols = row.split("||")
            if len(cols) <= max(idx_sec1, idx_sec2_7):
                continue
            s1, s2 = cols[idx_sec1].strip(), cols[idx_sec2_7].strip()
            if s1 == "A" and s2.startswith("000"):
                valid_numbers.add(f"{s1}{s2}")

    print(f"Collected {len(valid_numbers)} reference numbers")

    # ------------------------------------------------------------------ #
    # STEP 3 –  read CSVs and collect numbers
    # ------------------------------------------------------------------ #
    for file_name in csv_files:
        key = f"{validate_folder}{file_name}"
        try:
            body = s3.get_object(Bucket=bucket_name, Key=key)["Body"].read().decode("utf-8")
        except Exception as e:
            msg = f"Failed to read CSV {key}: {e}"
            splunk.log_message({"Status": "Error", "Message": msg}, get_run_id(program_name))
            print(msg)
            return False, [], csv_files

        lines = body.strip().splitlines()
        if not lines:
            continue
        hdr, data = lines[0], [l for l in lines[1:] if not l.startswith("RECORDS,")]
        reader = csv.DictReader(io.StringIO("\n".join([hdr] + data)))
        nums = {r.get("ADPSECURITYNUMBER", "").strip() for r in reader if r.get("ADPSECURITYNUMBER")}
        compared_numbers.update(nums)
        print(f"{file_name}: found {len(nums)} ADPSECURITYNUMBERs")

    # ------------------------------------------------------------------ #
    # STEP 4 –  compare sets and move files
    # ------------------------------------------------------------------ #
    missing = sorted(valid_numbers - compared_numbers)
    if not missing:
        # success – move all to SAVE and emit trigger
        all_files_to_move = reference_files + csv_files + sha256_files
        for f in all_files_to_move:
            src, dst = f"{validate_folder}{f}", f"{validate_save_folder}{f}"
            try:
                s3.copy_object(Bucket=bucket_name, CopySource={"Bucket": bucket_name, "Key": src}, Key=dst)
                s3.delete_object(Bucket=bucket_name, Key=src)
            except Exception as e:
                print(f"Move failed for {f}: {e}")

        timestamp = datetime.now().strftime('%Y%m%dT%H%M%S')
        trig_key = f"{input_folder}VALIDATION_ADP_{timestamp}"
        s3.put_object(Bucket=bucket_name, Key=trig_key, Body=b"adpsecurity validated")

        msg = f"Validation successful for {csv_files}"
        splunk.log_message({"Status": "Success", "Message": msg}, get_run_id(program_name))
        print(msg)
        return True, list(compared_numbers), csv_files
    else:
        msg = (
            f"Missing {len(missing)} ADPSECURITYNUMBERs: {missing}. "
            )
        splunk.log_message({"Status": "Error", "Message": msg}, get_run_id(program_name))
        print(msg)
        return False, missing, csv_files

# ----------------------------------------------------------------------------------------
if __name__ == "__main__":
    program = os.path.basename(sys.argv[0])
    program_name = program.split('.')[0]
    args = get_configs()
    bucket_name = args['bucket_name']
    validate_folder = args['validate_folder']
    validate_save_folder = args['validate_save_folder']
    validate_failed_folder = args['validate_failed_folder']
    input_folder = args['input_folder']
    duplicated_folder = args['duplicated_folder']
    input_file = args['input_file']
    if not all([bucket_name, validate_folder, validate_save_folder, validate_failed_folder, input_folder, duplicated_folder, input_file]):
        message = f"{program_name} failed: missing required parameters!"
        splunk.log_message({"Status": "Failed", "Message": message}, get_run_id(program_name))
        raise ValueError(message)

    is_valid, missing_numbers, files = check_security_number(
        bucket_name, validate_folder, validate_save_folder, validate_failed_folder, input_folder, duplicated_folder, input_file
    )
    '''
    if not is_valid:
        message = (
            f"Valid ADPSECURITYNUMBER(s) not found in EURORATE and STMTMMKT files.\n"
            f"CSV files checked: {files}\n"
            f"Missing ADPSECURITYNUMBER(s): {missing_numbers}"
        )
        splunk.log_message({"Status": "Error", "Message": message}, get_run_id(program_name))
        print(message)
        raise ValueError(message)
    '''
    finish_msg = f"Finished {program_name} at {datetime.now()}"
    splunk.log_message({"Status": "Info", "Message": finish_msg}, get_run_id(program_name))
    print(finish_msg)