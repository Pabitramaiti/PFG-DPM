import sys
import json
import py7zr
import urllib.parse
import boto3
import zipfile
import datetime
import subprocess
import os
import splunk
from io import BytesIO
import subprocess
import gzip
import tarfile
from awsglue.utils import getResolvedOptions
#Function to get Glue job run id to pass to splunk
import jsonformatter

import traceback


s3 = boto3.client('s3')

count = 0
def get_run_id():
    # glue_client = boto3.client("glue")
    # job_name = sys.argv[0].split('/')[-1]
    # job_name = job_name[:-3]
    # print(job_name)
    # response = glue_client.get_job_runs(JobName = job_name)
    # job_run_id = response["JobRuns"][0]["Id"]
    # return job_run_id
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:uncompress_files:"+account_id

class Extract:
    def __init__(self, bucket, key):
        self.bucket = bucket
        self.key = key
        self.subpath=''
        if '/' in key:
            self.subpath=key.split('/')[0]+'/'
        else:
            pass
        self.buffer = None
        self.message = ''
        
    def get_s3_buffer(self):

        
        #Get the details of the file in bytes
        response = s3.get_object(Bucket=self.bucket, Key=self.key)["Body"].read()
        #response = s3.get_object(Bucket=self.bucket, Key="pp_input_files/Cetera.PVIRAFIL.Copybook.IRA.12122023.zip")
        #response=response["Body"].read()

        #Get the details of the file in bytes
        response = s3.get_object(Bucket=self.bucket, Key=self.key)["Body"].read()
            

        #create file like object .BytesIO classes to mimic a normal file with all the properties of the file
        self.buffer = BytesIO(response)
            
        if key.endswith(".tar") :
            #create a subfolder name variable
                self.extract_tar_file()
        elif key.endswith(".gz"):
                
            self.extract_gz_file()
                
        elif key.endswith(".zip"):
                
            self.extract_zip_file()
        elif key.endswith(".7z"):
             
            self.extract_7z_file()

    def extract_tar_file(self):
        try:
            global count
            uncompressed_key = ''

            with tarfile.open(fileobj=self.buffer) as tar:
                for tar_resource in tar:
                    if tar_resource.isfile():
                        with tar.extractfile(tar_resource) as inner_file:
                            while True:
                                chunk = inner_file.read(1024)  # Adjust the chunk size as needed
                                if not chunk:
                                    break

                                if self.subpath:
                                    s3.put_object(Body=chunk, Bucket=self.bucket,
                                              Key=self.subpath+uncompressed_key + self.key.split(".tar")[0])
                                    count += 1
                                else:
                                    s3.put_object(Body=chunk, Bucket=self.bucket,
                                              Key=uncompressed_key + self.key.split(".tar")[0])
                                    count += 1
            splunk.log_message({'Status': 'success', 'Message': f'{self.key} Tar file is uncompressed SUCCESSFULLY'},  get_run_id())
        except Exception as e:
            self.message = "Tar extraction failed due to error " + str(e)
            splunk.log_message({'Status': 'failed', 'InputFileName': self.key, 'Message': self.message},  get_run_id())
            raise e
 
    def extract_gz_file(self):
        try:
            global count
            #Read only binary mode
            uncompressed_key=''
            #open the gzip file in read binary mode and uncompress it
            fo=gzip.GzipFile(None, 'rb', fileobj=self.buffer)
            print(fo)
            if self.subpath:
                s3.upload_fileobj(Fileobj=fo, Bucket=self.bucket, Key=self.subpath+uncompressed_key+ self.key.split(".gz")[0])
            else:
                s3.upload_fileobj(Fileobj=fo, Bucket=self.bucket, Key=uncompressed_key+ self.key.split(".gz")[0])
            count=1
            splunk.log_message({'Status': 'success', 'Message': f'{self.key} GZIP file is uncompressed SUCCESSFULLY'},  get_run_id())
        except Exception as e:
            self.message = "Gz extraction failed due to error " + str(e)
            splunk.log_message({'Status': 'failed', 'InputFileName': self.key, 'Message': self.message},  get_run_id())
            raise e
    def extract_zip_file(self):
        try:
            global count
            uncompressed_key=''
            #open the file archive
            zipped = zipfile.ZipFile(self.buffer)
            #get the list of files in zipped archive
            for file in zipped.namelist():
                
                final_file_path = file 
                #upload_fileobj method accepts a readable file-like object. 
                #The file object must be opened in binary mode, not text mode
                if self.subpath:
                    s3.upload_fileobj(Fileobj=zipped.open(file), Bucket=self.bucket, Key=self.subpath+uncompressed_key+ file)
                else:
                    s3.upload_fileobj(Fileobj=zipped.open(file), Bucket=self.bucket, Key=uncompressed_key+ file)
                #count = count + 1
            splunk.log_message({'Status': 'success', 'Message': f'{self.key} ZIP file is uncompressed SUCCESSFULLY'},  get_run_id())    
        except Exception as e:
            self.message = "Zip extraction failed due to error " + str(e)
            splunk.log_message({'Status': 'failed', 'InputFileName': self.key, 'Message': self.message},  get_run_id())
            raise e
    def extract_7z_file(self):
        try:
            global count
            print('inside try statement')
            uncompressed_key=''
            zipped = py7zr.SevenZipFile(self.buffer, mode = 'r')
            print('zipped')
            zipped.extractall(path = r"\tmp")
            #print(os.listdir(r"\tmp"))
            for file in os.listdir(r"\tmp"):
                if self.subpath:
                    s3.upload_file(os.path.join(r"\tmp",file),Bucket=self.bucket, Key=self.subpath+uncompressed_key+ file )
                else:
                    s3.upload_file(os.path.join(r"\tmp",file),Bucket=self.bucket, Key=uncompressed_key+ file )
                print("s3 upload")
                count = count + 1
                print("count")
            result = splunk.log_message({'Status': 'success', 'Message': f'{self.key} 7Z file is uncompressed SUCCESSFULLY'},  get_run_id()) 
            print(result)
        except Exception as e:
            self.message = "7z extraction failed due to error " + str(e) 
            result = splunk.log_message({'Status': 'failed', 'InputFileName': self.key, 'Message': self.message},  get_run_id())
            print(result)
            raise e
   

now = datetime.datetime.now()

s3_resource = boto3.resource('s3')
error_message = ''
key = None
flag = "SUCCESSFUL"
try:
    
    iparm = getResolvedOptions(sys.argv, ['bucket', 'key','trackrecords'])
    
    bucket=iparm['bucket']
    key=iparm['key']
    trackrecsfn=iparm['trackrecords']
    result = splunk.log_message({'Status': 'success', 'Message': f'uncompression process for {key} started SUCCESSFULLY'}, get_run_id()) 
    extract_obj = Extract(bucket, key)
   
    extract_obj.get_s3_buffer()
except Exception as e:
     #import traceback
     #print(traceback.format_exc())
     #result = splunk.log_message({'message': traceback.format_exc()},  get_run_id())
     #print(result)
     error_message = 'Uncompression of file {} failed due to {}'.format(key , e)
     result = splunk.log_message({'InputFileName': key, 'Status': 'failed', 'Message': error_message}, get_run_id())
     flag = "UNSUCCESSFUL"
    #  lambda_client = boto3.client("lambda")
     #Pass paramteres to Lambda_Track_Records
    #  data = {
    #  "body": {
    #  "STEP" : "UNCOMPRESSION",
    #  "FILE_NAME" : key,
    #  "STATUS" : flag,
    #  "TOTAL_DOCUMENTS" : count,
    #  "DATE" : str(now),
     #"ERROR": error_message
    #      }
    #  }
    #  re = lambda_client.invoke(FunctionName = trackrecsfn, InvocationType = 'RequestResponse', Payload = json.dumps(data))
     raise e
     
     
# lambda_client = boto3.client("lambda")
#Pass paramteres to Lambda_Track_Records
# data = {
# "body": {
# "STEP" : "UNCOMPRESSION",
# "FILE_NAME" : key,
# "STATUS" : flag,
# "TOTAL_DOCUMENTS" : count,
# "DATE" : str(now),
# #"ERROR": error_message
#     }

# }
# re = lambda_client.invoke(FunctionName = trackrecsfn, InvocationType = 'RequestResponse', Payload = json.dumps(data))
