import sys
import boto3
import os
from datetime import datetime
from awsglue.utils import getResolvedOptions
import splunk

# ----------------------------------------------------------------------------------------
def get_run_id(program_name: str) -> str:
    return f"arn:dpm:glue:{program_name}:{boto3.client('sts').get_caller_identity()['Account']}"

# ----------------------------------------------------------------------------------------
def get_configs():
    args_list = [
        'bucket_name',
        'validate_folder',
        'validate_save_folder',
        'validate_failed_folder',
        'input_folder',
        'duplicated_folder'
    ]
    return getResolvedOptions(sys.argv, args_list)

# ----------------------------------------------------------------------------------------
def check_quarter_files(
    bucket_name: str,
    validate_folder: str,
    validate_save_folder: str,
    validate_failed_folder: str,
    input_folder: str,
    duplicated_folder: str,
    program_name: str
):

    s3 = boto3.client("s3")
    validated_prefixes = "BIOS.DPM"

    print("STEP 1 – Discovering files...")

    # --------------------------------------------------------------
    # STEP 1 – Discover objects in validate_folder
    # Only include files starting with validated_prefixes
    # --------------------------------------------------------------
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=validate_folder)

    valid_files = []

    if "Contents" in response:
        for obj in response["Contents"]:
            key = obj["Key"]
            file_name = os.path.basename(key)

            if file_name.startswith(validated_prefixes):
                valid_files.append(file_name)

    file_count = len(valid_files)

    print(f"Discovered {file_count} valid file(s): {valid_files}")

    # --------------------------------------------------------------
    # STEP 2 – Check file_count
    # --------------------------------------------------------------

    # ✅ CASE 1: Exactly 3 files → SUCCESS
    if file_count == 3:
        print("Exactly 3 files received. Moving to validate_save_folder...")

        for file_name in valid_files:
            src_key = f"{validate_folder}{file_name}"
            dest_key = f"{validate_save_folder}{file_name}"

            try:
                s3.copy_object(
                    Bucket=bucket_name,
                    CopySource={"Bucket": bucket_name, "Key": src_key},
                    Key=dest_key
                )
                s3.delete_object(Bucket=bucket_name, Key=src_key)
                print(f"Moved {file_name} to save folder")
            except Exception as e:
                print(f"Move failed for {file_name}: {e}")
                raise

        # Create trigger file
        timestamp = datetime.now().strftime('%Y%m%dT%H%M%S')
        trig_key = f"{input_folder}VALIDATION_QUARTER_{timestamp}"
        s3.put_object(Bucket=bucket_name, Key=trig_key, Body=b"validated")

        msg = "Validation successful: 3 monthly files received."
        splunk.log_message({"Status": "Success", "Message": msg}, get_run_id(program_name))
        print(msg)
        return True

    # ✅ CASE 2: Less than 3 files → WAIT (no action)
    elif file_count < 3:
        msg = f"Waiting for more files. Current count: {file_count}"
        print(msg)
        splunk.log_message({"Status": "Info", "Message": msg}, get_run_id(program_name))
        return True

    # ✅ CASE 3: More than 3 files → ERROR
    else:
        print("More than 3 files detected. Moving to duplicated_folder...")

        for file_name in valid_files:
            src_key = f"{validate_folder}{file_name}"
            dest_key = f"{duplicated_folder}{file_name}"

            try:
                s3.copy_object(
                    Bucket=bucket_name,
                    CopySource={"Bucket": bucket_name, "Key": src_key},
                    Key=dest_key
                )
                s3.delete_object(Bucket=bucket_name, Key=src_key)
                print(f"Moved {file_name} to duplicated folder")
            except Exception as e:
                print(f"Move failed for {file_name}: {e}")

        msg = f"Error: Too many files received ({file_count})."
        splunk.log_message({"Status": "Error", "Message": msg}, get_run_id(program_name))
        print(msg)
        ##raise ValueError(msg)

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

    if not all([
        bucket_name,
        validate_folder,
        validate_save_folder,
        validate_failed_folder,
        input_folder,
        duplicated_folder

    ]):
        message = f"{program_name} failed: missing required parameters!"
        splunk.log_message({"Status": "Failed", "Message": message}, get_run_id(program_name))
        raise ValueError(message)

    check_quarter_files(
        bucket_name,
        validate_folder,
        validate_save_folder,
        validate_failed_folder,
        input_folder,
        duplicated_folder,
        program_name
    )

    finish_msg = f"Finished {program_name} at {datetime.now()}"
    splunk.log_message({"Status": "Info", "Message": finish_msg}, get_run_id(program_name))
    print(finish_msg)