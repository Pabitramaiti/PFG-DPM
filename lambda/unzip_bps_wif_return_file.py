import json
import boto3
import zipfile
import io
import os

s3 = boto3.client('s3')


def unzip_file(bucket, key):
    print(f"Unzipping file: {key}")
    
    response = s3.get_object(Bucket=bucket, Key=key)
    zip_content = response['Body'].read()
    
    with zipfile.ZipFile(io.BytesIO(zip_content)) as z:
        for file_name in z.namelist():
            print(f"Extracting {file_name}")
            
            file_data = z.read(file_name)
            
            extract_key = os.path.join(os.path.dirname(key), file_name)
            
            s3.put_object(
                Bucket=bucket,
                Key=extract_key,
                Body=file_data
            )
    
    print(f"Finished extracting {key}")
    
    #  Delete zip after extraction
    s3.delete_object(Bucket=bucket, Key=key)
    print(f"Deleted zip file: {key}")


def unzip_pdf_folder(bucket, pdf_prefix):
    print(f"Checking PDF folder: {pdf_prefix}")
    
    response = s3.list_objects_v2(Bucket=bucket, Prefix=pdf_prefix)
    
    if 'Contents' not in response:
        print("No PDF zip files found.")
        return
    
    for obj in response['Contents']:
        pdf_key = obj['Key']
        
        if pdf_key.endswith('.zip'):
            unzip_file(bucket, pdf_key)


def lambda_handler(event, context):
    
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        manifest_key = record['s3']['object']['key']
        
        print(f"Triggered by file: {manifest_key}")
        
        if not manifest_key.endswith('.zip'):
            print("Not a zip file. Skipping.")
            continue
        
        # Trigger only when manifest zip uploaded
        if "/manifest/" in manifest_key:
            
            # Example:
            # to-wedbush/manifest/2026-03-08/manifest-file-confirms.zip
            
            parts = manifest_key.split("/")
            
            # parts = ['to-wedbush', 'manifest', '2026-03-08', 'manifest-file.zip']
            
            root_folder = parts[0]             
            date_folder = parts[2]
            file_name = os.path.basename(manifest_key)              
            
            pdf_prefix = f"{root_folder}/confirms/{date_folder}/"

            date_stmt = "-".join(date_folder.split("-")[:2])
            statement_prefix = f"{root_folder}/statements/monthly/{date_stmt}/"
            
            print(f"Checking PDF folder: {pdf_prefix}")
            
            # Step 1: Unzip only PDFs from same date folder
            if "confirms" in file_name.lower():
                print(f"Checking confirms folder: {pdf_prefix}")
                unzip_pdf_folder(bucket, pdf_prefix)

            elif "statements" in file_name.lower():
                print(f"Checking statements folder: {statement_prefix}")
                unzip_pdf_folder(bucket, statement_prefix)

            else:
                print("Manifest type not recognized. Skipping PDF extraction.")

            # Step 2: Unzip the triggered manifest zip
            unzip_file(bucket, manifest_key)

            
        
        else:
            print("File not in manifest folder. Skipping.")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Processing completed successfully')
    }