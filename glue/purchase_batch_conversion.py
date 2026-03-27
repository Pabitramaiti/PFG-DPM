import boto3
import os
import sys
import pandas as pd
from io import StringIO
from datetime import datetime
from awsglue.utils import getResolvedOptions
import splunk

#####################################################################################
# Glue run id
#####################################################################################
def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return f"arn:dpm:glue:PurchaseBatchConvert:{account_id}"

#####################################################################################
# Load Glue parameters
#####################################################################################
def get_configs():
    return getResolvedOptions(
        sys.argv,
        ['bucket_name', 'input_file', 'output_folder', 'output_file']
    )

#####################################################################################
# Purchase XLSX → CSV conversion
#####################################################################################
def purchase_convert(s3_client, bucket_name, output_folder, output_file, xls_bytes):
    try:
        # Read Excel without assuming header
        df_raw = pd.read_excel(xls_bytes, header=None)

        # Find header row
        header_row_idx = df_raw[
            df_raw.apply(lambda x: x.astype(str).str.contains('Input Trid').any(), axis=1)
        ].index[0]

        # Read again using detected header
        df = pd.read_excel(xls_bytes, skiprows=header_row_idx)

        final_rows = []

        for _, row in df.iterrows():
            # Normalize Input Trid
            trid_value = "" if pd.isna(row['Input Trid']) else str(row['Input Trid']).strip()
            typ = "" if pd.isna(row['Type']) else str(row['Type']).strip()
            buysell = "" if pd.isna(row['Buy/Sell']) else str(row['Buy/Sell']).strip()

            # Normalize Cmmsn/Cxl
            cmmsn = row['Cmmsn/Cxl']
            cmmsn_clean = (
                "" if pd.isna(cmmsn)
                or str(cmmsn).strip().lower() in ['nan', 'none', 'null', '']
                else str(cmmsn).strip()
            )

            description = row['Description']

            # --- Business logic ---
            if trid_value in ("G,W,Z", "G,W,Z, ,"):
                for c in ['G', 'W', 'Z', ' ']:
                    c = c.strip()
                    mkey = f"{c}{typ}{buysell}{cmmsn_clean}"
                    final_rows.append({
                        "MultiKey": mkey,
                        "Input Trid": c,
                        "Type": typ,
                        "Buy/Sell": buysell,
                        "Cmmsn/Cxl": cmmsn_clean,
                        "Description": description
                    })

            elif trid_value == "V":
                mkey = f"V{typ}{buysell}{cmmsn_clean}"
                final_rows.append({
                    "MultiKey": mkey,
                    "Input Trid": "V",
                    "Type": typ,
                    "Buy/Sell": buysell,
                    "Cmmsn/Cxl": cmmsn_clean,
                    "Description": description
                })

            elif trid_value == "":
                # ✅ Blank TRID logic
                mkey = f"{typ}{buysell}{cmmsn_clean}"
                final_rows.append({
                    "MultiKey": mkey,
                    "Input Trid": "",
                    "Type": typ,
                    "Buy/Sell": buysell,
                    "Cmmsn/Cxl": cmmsn_clean,
                    "Description": description
                })

            else:
                continue

        # Build final dataframe
        out_df = pd.DataFrame(final_rows)
        out_df.sort_values("MultiKey", inplace=True)
        out_df.reset_index(drop=True, inplace=True)

        # Write CSV to memory
        csv_buffer = StringIO()
        out_df[
            ["MultiKey", "Input Trid", "Type", "Buy/Sell", "Cmmsn/Cxl", "Description"]
        ].to_csv(csv_buffer, index=False)

        s3_client.put_object(
            Bucket=bucket_name,
            Key=f"{output_folder}{output_file}",
            Body=csv_buffer.getvalue()
        )

        message = f"{program_name} Convert purchase XLSX to CSV successfully"
        splunk.log_message({'Status': 'Success', 'Message': message}, get_run_id())

    except Exception as e:
        message = f"{program_name} Purchase convert failed: {str(e)}"
        splunk.log_message({'Status': 'Failed', 'Message': message}, get_run_id())
        raise

#####################################################################################
# Batch XLSX → CSV conversion
#####################################################################################
def batch_convert(s3_client, bucket_name, output_folder, output_file, xls_bytes):
    try:
        df_raw = pd.read_excel(xls_bytes, header=None)

        header_row_idx = df_raw[
            df_raw.apply(lambda x: x.astype(str).str.contains('BATCH_CD').any(), axis=1)
        ].index[0]

        df = pd.read_excel(xls_bytes, skiprows=header_row_idx)

        df['LOOKUP'] = df['BATCH_CD'].astype(str) + df['ENTRY_CD'].astype(str)

        if '# of Lines' in df.columns:
            df['# of Lines'] = df['# of Lines'].apply(
                lambda x: "" if pd.isna(x)
                else str(int(x)) if isinstance(x, (int, float)) and float(x).is_integer()
                else str(x)
            )

        if 'Display B228 Des. Lines' in df.columns:
            df['Display B228 Des. Lines'] = df['Display B228 Des. Lines'].fillna("")

        df_csv = df[
            [
                'BATCH_CD',
                'ENTRY_CD',
                'LOOKUP',
                'Description',
                'Section',
                'Override to Section',
                'Display B228 Des. Lines',
                '# of Lines'
            ]
        ]

        csv_buffer = StringIO()
        df_csv.to_csv(csv_buffer, index=False)

        s3_client.put_object(
            Bucket=bucket_name,
            Key=f"{output_folder}{output_file}",
            Body=csv_buffer.getvalue()
        )

        message = f"{program_name} Convert batch XLSX to CSV successfully"
        splunk.log_message({'Status': 'Success', 'Message': message}, get_run_id())

    except Exception as e:
        message = f"{program_name} Batch convert failed: {str(e)}"
        splunk.log_message({'Status': 'Failed', 'Message': message}, get_run_id())
        raise

#####################################################################################
# Main Glue job
#####################################################################################
def purchase_batch_convert():
    s3_client = boto3.client('s3')
    start_time = datetime.now()

    splunk.log_message(
        {'Status': 'Starting', 'Message': f"Process started at {start_time}"},
        get_run_id()
    )

    params = get_configs()

    try:
        file_key = f"{params['output_folder']}{params['input_file']}"
        obj = s3_client.get_object(
            Bucket=params['bucket_name'],
            Key=file_key
        )

        xls_bytes = obj['Body'].read()

        if "purchase" in params['input_file'].lower():
            purchase_convert(
                s3_client,
                params['bucket_name'],
                params['output_folder'],
                params['output_file'],
                xls_bytes
            )

        elif "batch" in params['input_file'].lower():
            batch_convert(
                s3_client,
                params['bucket_name'],
                params['output_folder'],
                params['output_file'],
                xls_bytes
            )

    except Exception as e:
        message = f"{program_name} Job failed: {str(e)}"
        splunk.log_message({'Status': 'Failed', 'Message': message}, get_run_id())
        raise

#####################################################################################
# Entry point
#####################################################################################
if __name__ == "__main__":
    program_name = os.path.basename(sys.argv[0])
    purchase_batch_convert()