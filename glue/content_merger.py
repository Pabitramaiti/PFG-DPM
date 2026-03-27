import boto3
import sys
import io
import zipfile
import tempfile
import json
import argparse
import dbhandler
s3_client = boto3.client("s3")

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--group_name", required=True, help="Run ID or group name")
    p.add_argument("--group_items", required=True, help="List of file items in JSON string")
    p.add_argument("--output_prefix", required=True, help="S3 prefix for merged output file")
    p.add_argument("--file_monitoring_table", required=True, help="DynamoDB table to update")
    p.add_argument("--grp_initial_status", required=True, help="Initail Status of Group")
    p.add_argument("--grp_completion_status", required=True, help="Completion Status of Group")
    args, _ = p.parse_known_args()

    try:
        args.group_items = json.loads(args.group_items)
    except Exception as e:
        print(f"[ERROR] Failed to parse group_items JSON: {e}", file=sys.stderr)
        raise
    return args
            
def merge_zip_files(group_name, group_items, output_prefix, table_name,grp_initial_status,grp_completion_status):
    try:
        merged_files = {}
        for group in group_items:
            content_merge_status = group['content_merge_status']
            output_prefix = group['output_prefix']
            bucket = group['bucket']
            object_key = group['object_key']
            file_name = group['file_name']
            key = object_key + file_name
            s3_obj = s3_client.get_object(Bucket=bucket, Key=key)
            zip_bytes = io.BytesIO(s3_obj['Body'].read())
        
            with zipfile.ZipFile(zip_bytes, 'r') as z:
                for filename in z.namelist():
                    with z.open(filename) as f:
                        data = f.read()
                        merged_files.setdefault(filename, []).append(data)
        output_bytes = io.BytesIO()
        with zipfile.ZipFile(output_bytes, 'w', zipfile.ZIP_DEFLATED) as merged_zip:
            for fname, contents_list in merged_files.items():
                merged_data = b''.join(contents_list)
                merged_zip.writestr(fname, merged_data)
    
        output_bytes.seek(0)
    
        out_key = output_prefix.rstrip("/") + "/" + f"{group_name}.zip"
        s3_client.upload_fileobj(output_bytes, bucket, out_key)
        objFM = dbhandler.DynamoDBHandler(table_name)
        print("updating the group with completed status")
        objFM.update_all_group_items("grp_run_id",group_name, "content_merge_status", grp_completion_status,"content_merge_status", grp_initial_status)
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        raise e

if __name__ == '__main__':
    args = parse_args()
    try:
        merge_zip_files(args.group_name,args.group_items,args.output_prefix,args.file_monitoring_table,args.grp_initial_status,args.grp_completion_status)
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        raise e