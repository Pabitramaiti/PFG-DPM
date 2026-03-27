import sys, os, boto3, io, json, re
import ebcdic
import codecs
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
import splunk
from pyspark import SparkConf

def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:ebcdic_to_ascii:" + account_id

def log_message(status, message):
    splunk.log_message({'FileName':inputFileName,'Status': status, 'Message': message}, get_run_id())

# Set environment variables from job arguments
def set_job_params_as_env_vars():
    i = 1
    while i < len(sys.argv):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]
            # Check if next argument exists and is not another key
            if i + 1 < len(sys.argv) and not sys.argv[i + 1].startswith('--'):
                value = sys.argv[i + 1]
                os.environ[key] = value
                i += 2
            else:
                # No value for this key, skip to next
                i += 1
        else:
            # Stray value, skip it
            i += 1

# S3 utility for downloading/uploading
class FileUtility:
    def __init__(self):
        self.s3_client = boto3.client('s3')

    def download_from_s3(self, bucketName, key):
        response = self.s3_client.get_object(Bucket=bucketName, Key=key)
        return response['Body'].read().decode('utf-8')

    def upload_to_s3(self, bucketName, key, content):
        self.s3_client.put_object(Bucket=bucketName, Key=key, Body=content)

def get_matching_s3_keys(bucket, prefix, regex_pattern):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    pattern = re.compile(regex_pattern)

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if pattern.search(os.path.basename(key)):
                yield key
                
def ebcdic_to_ascii_string(ebcdic_in):
    return codecs.decode(ebcdic_in, 'cp037').replace('\x00', ' ')

def convert_string(line):
    return ebcdic_to_ascii_string(line)

def convert_numeric1(line, signed, precision):
    tempValue = ebcdic_to_ascii_string(line)
    if len(tempValue.strip()) == 0:
        return tempValue
    lastZonedByte = "{:02X}".format(line[-1])
    highNibble = lastZonedByte[0]
    lowNibble = lastZonedByte[1]
    sign = ""
    if signed:
        if highNibble.lower() == "c": sign = "+"
        elif highNibble.lower() == "d": sign = "-"
    temp = tempValue[:-1] + lowNibble
    if int(precision) > 0:
        return f"{sign}{temp[:-precision]}.{temp[-precision:]}"
    return f"{sign}{temp}"

def convert_numeric2(line,signed,precision,picture):
    value,intValue,decValue="","",""
    spaceCount=0
    tempValue=""
    sign=""
    for i in range(len(line)):
        if line[i] == 0x40:
            spaceCount += 1
        
    if(len(line)==spaceCount):
        #value=ebcdic_to_ascii_string(line)
        num_digits=len(line)*2
        if(signed==True):
            value=" "*num_digits
        else:
            value=" "*(num_digits-1)
        
    else:
        for i in range(len(line)):
            tempValue+="{:02X}".format(line[i])
              
        lastChar=tempValue[len(tempValue)-1]
            
        if lastChar == 'F':
            sign = ""
        elif lastChar == 'C':
            sign = "+"
        elif lastChar == 'D':
            sign = "-"
        if(int(precision)>0):
                
            intValue=tempValue[0:len(tempValue)-int(precision)-1]
            decValue=tempValue[len(tempValue)-int(precision)-1:len(tempValue)-1]
            value=intValue+"."+decValue
        else:
            if(lastChar.isalpha()):
                value=tempValue[0:len(tempValue)-1]
            if(lastChar.isdigit()):
                value=tempValue[0:len(tempValue)]
            
        if(signed==True):
            value=sign+value
        
    return value

def convert_date(line, picture):
    cyy, mm, dd = line[:2], line[2:4], line[4:6] if len(line) > 4 else b''
    return convert_numeric2(cyy, False, 0, picture) + \
           convert_numeric1(mm, False, 0) + \
           (convert_numeric1(dd, False, 0) if dd else "")

def convert_date1(line):
    return ebcdic_to_ascii_string(line)

class Layout:
    RECORD_LEN = 0
    recordPosition = 0
    recDetails = {}
    record_input_defs = []
    record_out_defs = []

    def __init__(self, record_type, line):
        self.record_type = record_type
        self.data_record = line

    def byte_sub_str(self, byte_array_in, start_idx, end_idx):
        return byte_array_in[start_idx - 1:end_idx]

    def get_def_index(self, rec_type, fld_name):
        fld_name = fld_name.strip().upper()
        for index, record_def in enumerate(Layout.record_input_defs):
            if record_def["rec_type"].upper() == rec_type.upper() and record_def["field_name"].strip().upper() == fld_name:
                return index
        return -1

    def format_raw_value(self, rec_def_in):
        byte_array = self.byte_sub_str(self.data_record, int(rec_def_in['start']), int(rec_def_in['end']))
        dtype = rec_def_in["data_type"].lower()
        if dtype == "string":
            return convert_string(byte_array)
        elif dtype == "numeric1":
            return convert_numeric1(byte_array, rec_def_in["sign"], rec_def_in["precision"])
        elif dtype == "numeric2":
            return convert_numeric2(byte_array, rec_def_in["sign"], rec_def_in["precision"], rec_def_in['picture'])
        elif dtype == "date":
            return convert_date(byte_array, rec_def_in["picture"])
        elif dtype == "date1":
            return convert_date1(byte_array)
        else:
            return ""

    def get_data(self, rec_type, fld_name):
        idx = self.get_def_index(rec_type, fld_name)
        if idx < 0:
            raise ValueError(f"Field '{fld_name}' not defined in input config")
        return self.format_raw_value(Layout.record_input_defs[idx])

    def getOutputFieldsToLine(self, rec_type, delimeter_flag):
        try:
            list_size = len(Layout.record_out_defs)
            rec_info = ""
            for idx in range(list_size):
                rec_def = Layout.record_out_defs[idx]
                if rec_def["rec_type"].lower() == rec_type.lower():
                    fields = rec_def['fields']
                    for field in fields:  
                        # print("field",field)
                        if field in {'REC-CODE','REC-TYPE'}:
                           
                            if self.get_data(rec_type, field)==rec_type:
                                if delimeter_flag=="True":
                                    rec_info += self.get_data(rec_type, field)+"~"
                                else:
                                    rec_info += self.get_data(rec_type, field)    
                            else:
                                if delimeter_flag=="True":
                                    rec_info += rec_type +"~"
                                else:
                                    rec_info += rec_type                            
                        else:
                            
                            if delimeter_flag=="True":
                                rec_info += self.get_data(rec_type, field)+"~"
                            else:
                                rec_info += self.get_data(rec_type, field)
                    return rec_info
        except Exception as e:
            error_message = 'failed to get the output fields'
            log_message('failed',error_message)
            raise ValueError(error_message)

    def getOutputFieldsToLineWithoutAscii(layout, rec_type, delimeter_flag):
        try:
            rec_info = ""
            for i in Layout.record_input_defs:
                if i["rec_type"].lower() == rec_type.lower():
                    if i["field_name"] in {'REC-CODE', 'REC-TYPE'}:
                        if layout.format_raw_value(i, layout.data_record) == rec_type:
                            if delimeter_flag == "True":
                                rec_info += layout.format_raw_value(i, layout.data_record) + "~"
                            else:
                                rec_info += layout.format_raw_value(i, layout.data_record)
                        elif layout.format_raw_value(i, layout.data_record) == rec_type[0]:
                            if delimeter_flag == "True":
                                rec_info += rec_type + "~"
                            else:
                                rec_info += rec_type
                    else:
                        if i["rec_type"][0].lower() == rec_type[0].lower():
                            if delimeter_flag == "True":
                                rec_info += layout.format_raw_value(i, layout.data_record) + "~"
                            else:
                                rec_info += layout.format_raw_value(i, layout.data_record)
            return rec_info
        except Exception as e:
            error_message = 'failed to get the output fields'
            log_message('failed',error_message)
            return f"ERROR: Failed to get output fields without ascii copybook: {str(e)}"


    @staticmethod
    def getRecSection(e0304_record):
        recSection = ""
    
        # 1) Slice mode support
        for rec_key, detail in Layout.recDetails.items():
            if detail.get("mode") == "slice":
                start, end = detail.get("start"), detail.get("end")
                val = ebcdic_to_ascii_string(e0304_record[start:end]).strip()
                if val == rec_key:
                    return rec_key
    
        # 2) Byte mode (single position check)
        for rec_key, detail in Layout.recDetails.items():
            if detail.get("mode") == "byte":
                start = detail.get("start", 0)
                char_val = ebcdic_to_ascii_string(e0304_record[start:start+1]).strip()
                if char_val == rec_key:
                    return rec_key
    
        # 3) Constant mode
        for rec_key, detail in Layout.recDetails.items():
            if detail.get("mode") == "constant":
                return detail["value"]
    
        # 4) Expression mode
        for rec_key, detail in Layout.recDetails.items():
            if detail.get("mode") == "expression":
                pos = detail.get("start", 0)
                base_char = ebcdic_to_ascii_string(e0304_record[pos:pos+1]).strip()
                if base_char == rec_key:
                    try:
                        result = eval(detail["expr"], {
                            "e0304_record": e0304_record,
                            "convert_string": convert_string,
                            "convert_numeric1": convert_numeric1,
                            "convert_numeric2": convert_numeric2
                        })
                        log_message("info", f"Expression result for {rec_key}: {result}")
                        return str(result) if result not in [None, "", "None"] else rec_key
                    except Exception as e:
                        log_message("failed", f"Expression eval failed for {rec_key}: {e}")
                        return rec_key
    
        log_message('failed', f"***** Unable to identify record type *****")
        sys.exit(1)

    @staticmethod
    def readCommonConfig(file_type):
        found = False
        try:
            lines = Layout.common_cfg_content.split("\n")
            for i, line in enumerate(lines):
                rec = line.strip()
                if rec.startswith("#") or not rec:
                    continue
    
                if not found and rec.startswith("["):
                    section_name = rec[1:rec.index("]")].strip()
                    if section_name.lower() == file_type.lower():
                        found = True
                        for subline in lines[i+1:]:
                            rec_line = subline.strip()
                            if rec_line.startswith("[") or not rec_line:
                                break
    
                            records = rec_line.split("|")
                            if len(records) < 2:
                                continue
    
                            key = records[0].strip().lower()
    
                            if key == "record_length":
                                Layout.RECORD_LEN = int(records[1].strip())
                            elif key == "record_position":
                                Layout.recordPosition = int(records[1].strip())
                            elif key == "rec-type":
                                rec_key = records[1].strip()
    
                                # Slice mode: e.g. REC-TYPE|0001|0:4
                                if len(records) == 3 and ":" in records[2]:
                                    start, end = map(int, records[2].split(":"))
                                    Layout.recDetails[rec_key] = {
                                        "mode": "slice",
                                        "start": start,
                                        "end": end,
                                        "raw": rec_line
                                    }
    
                                # Byte mode: e.g. REC-TYPE|A|11
                                elif len(records) == 3 and records[2].isdigit():
                                    Layout.recDetails[rec_key] = {
                                        "mode": "byte",
                                        "start": int(records[2].strip()),
                                        "raw": rec_line
                                    }
    
                                # Constant mode: e.g. REC-TYPE|CONST|0|"HDR"
                                elif len(records) == 4 and rec_key.upper() == "CONSTANT":
                                    start_index = int(records[2].strip())
                                    const_value = records[3].replace('"', '').strip()
                                    Layout.recDetails[rec_key] = {
                                        "mode": "constant",
                                        "start": start_index,
                                        "value": const_value,
                                        "raw": rec_line
                                    }
    
                                # Expression mode: e.g. REC-TYPE|S|11|<python expr>
                                elif len(records) > 3 and rec_key.upper() != "CONSTANT":
                                    Layout.recDetails[rec_key] = {
                                        "mode": "expression",
                                        "start": int(records[2].strip()) if records[2].isdigit() else 0,
                                        "expr": records[3].strip(),
                                        "raw": rec_line
                                    }
    
            if not found:
                log_message('failed', f"Section [{file_type}] not found in config.")
                raise ValueError(f"Section [{file_type}] not found in config.")
            if Layout.RECORD_LEN == 0:
                log_message('failed', f" RECORD_LEN is 0 — check the 'record_length' line in config")
                raise ValueError("RECORD_LEN is 0 — check the 'record_length' line in config")
    
        except Exception as e:
            log_message('failed', f"Failed to read common config for file_type {file_type}: {e}")
            raise RuntimeError(f"Failed to read common config for file_type {file_type}: {e}")
    
    
    @staticmethod
    def load_common_config_from_s3(bucketName, file_util, COMMON_cfg_key):
        Layout.common_cfg_content = file_util.download_from_s3(bucketName, COMMON_cfg_key)

    @staticmethod
    def load_json_config_from_s3(bucketName, file_util, key):
        content = file_util.download_from_s3(bucketName, key)
        if "EBCDIC" in key:
            Layout.record_input_defs = json.loads(content)
        elif "ASCII" in key:
            Layout.record_out_defs = json.loads(content)
            
def process_record(record, delimeter_flag, input_ascii_flag, CheckOutputAsciiFlag):
    # Don't catch exceptions - let them propagate to fail the job
    rec_type = Layout.getRecSection(record)
    layout = Layout(rec_type, record)
    
    # Process the record
    if input_ascii_flag and input_ascii_flag.lower() == "true":
        result = layout.getOutputFieldsToLine(rec_type, delimeter_flag)
    else:
        result = layout.getOutputFieldsToLineWithoutAscii(layout, rec_type, delimeter_flag)
        
    # Always return the appropriate type based on CheckOutputAsciiFlag
    if CheckOutputAsciiFlag and CheckOutputAsciiFlag.lower() == "true":
        return (result, rec_type)  # Return tuple
    else:
        return result  # Return string

def main():
    # sc = SparkContext()
    sc = SparkContext(
        conf=SparkConf().set("spark.driver.maxResultSize","4g")
    )
    glueContext = GlueContext(sc)
    set_job_params_as_env_vars()
    print("sys.argv:", sys.argv)
    print("filetype from env:", os.getenv('filetype'))
    print("All env:", dict(os.environ))
    global inputFileName
    inputFileName = os.getenv('inputFileName')
    bucketName = os.getenv('bucketName')
    file_path = os.getenv('file_path')
    file_pattern = os.getenv('file_pattern')
    file_key = os.getenv('file_key')
    out_ASCII_path = os.getenv('out_ASCII_path')
    EBCDIC_Mapping_Key = os.getenv('EBCDIC_Mapping_Key')
    ASCII_Mapping_Key = os.getenv('ASCII_Mapping_Key')
    COMMON_cfg_key = os.getenv('COMMON_cfg_key')
    filetype = os.getenv('filetype')
    CheckOutputAsciiFlag = os.getenv('CheckOutputAsciiFlag')

    global delimeter_flag
    delimeter_flag = os.getenv('delimeter_flag')
    input_ascii_flag = os.getenv('input_ascii_flag')
    print("filetype",filetype)
    print("EBCDIC_Mapping_Key",EBCDIC_Mapping_Key)
    print("ASCII_Mapping_Key",ASCII_Mapping_Key)
    print("file_path",file_path)
    print("file_pattern",file_pattern)
    
    file_util = None
    matched_filename = None  # Track if we successfully matched a file
    
    try:
        is_int_file = bool(re.search('^JSC\.INT\.(DLY|MTY)\.MERGED\.\d{8}\.(zip|ZIP)$', inputFileName, re.IGNORECASE))
        files_to_process = []
        
        if not file_key.strip():
            log_message('info', "[DEBUG:isOptional] No file_key provided, checking S3 for matches") 
            matching_keys = list(get_matching_s3_keys(bucketName, file_path, file_pattern))

            if not matching_keys:
                is_optional = os.getenv('isOptional', 'false').lower() == 'true'
                log_message('failed', f"[DEBUG:isOptional] No matching files found. isOptional={is_optional}")    
                if is_optional:
                    log_message('info', f"[INFO:isOptional] Skipping optional file (no match found) for pattern: {file_pattern}")
                    return  
                else:
                    error_message = (
                    f"[ERROR:isOptional] Required file missing — no files found for pattern {file_pattern} in {file_path}"
                    )
                    log_message('failed', error_message)
                    raise FileNotFoundError(error_message)
            if is_int_file:
                files_to_process=matching_keys    
                log_message('success', f"[INT FILE] Processing ALL {len(files_to_process)} matching files: {files_to_process}")  
            else:  
                files_to_process  = [matching_keys[0]]
                log_message('success', f"[DEBUG:isOptional] Using matched file_key: {files_to_process[0]}")
            
            # Store the matched filename for error reporting
            matched_filename = os.path.basename(files_to_process[0])
        else:
            files_to_process = [file_key]
            log_message('success', f"[DEBUG:isOptional] Using specified file_key: {file_key}")
            # Store the matched filename for error reporting
            matched_filename = os.path.basename(file_key)
            
        file_util = FileUtility()
        
        print(f"Looking for filetype [{filetype}] in config sections...")
        Layout.load_common_config_from_s3(bucketName, file_util, COMMON_cfg_key)
        Layout.readCommonConfig(filetype)
        Layout.load_json_config_from_s3(bucketName, file_util, EBCDIC_Mapping_Key)
        if input_ascii_flag.lower() == "true":
            Layout.load_json_config_from_s3(bucketName, file_util, ASCII_Mapping_Key)
        print("out_ASCII_path:", out_ASCII_path)
        print("file_key:", file_key)
        s3 = boto3.client('s3')
        # ebcdic_bytes = s3.get_object(Bucket=bucketName, Key=file_key)['Body'].read()
        record_len = Layout.RECORD_LEN
        if record_len == 0:
            raise ValueError("RECORD_LEN is 0 — check the common config file for 'record_length'")
        
        for current_file_key in files_to_process:
            log_message('info', f"Processing file: {current_file_key}")
            
            # Extract the actual filename from the S3 key for error reporting
            actual_filename = os.path.basename(current_file_key)
            
            # records = [ebcdic_bytes[i:i+record_len] for i in range(0, len(ebcdic_bytes), record_len)]
            # Parallel processing using Spark RDD
            # rdd = sc.parallelize(records)
            rdd = sc.binaryRecords(f"s3://{bucketName}/{current_file_key}", record_len)
            # Broadcast delimeter_flag if needed
            # global delimeter_flag
            
            if CheckOutputAsciiFlag and CheckOutputAsciiFlag.lower() == "true":
                # Pass all parameters to process_record using lambda
                results = rdd.map(lambda record: process_record(record, delimeter_flag, input_ascii_flag, CheckOutputAsciiFlag)).collect()
                output_lines = [result[0] for result in results]
                rec_sections = [result[1] for result in results]
                
                # Create and upload recSectionsFile.cfg
                rec_sections_content = "\n".join(rec_sections)
                if is_int_file:
                    recsections_file_key = file_path + os.path.basename(current_file_key) + '.recSectionsFile.cfg'
                else:
                    recsections_file_key = file_path + 'recSectionsFile.cfg'
                log_message('success', f"Creating recSectionsFile.cfg at:{recsections_file_key}")
                print("Creating recSectionsFile.cfg at:", recsections_file_key)
                s3.put_object(Bucket=bucketName, Key=recsections_file_key, Body=rec_sections_content)
                log_message('success', f"recSectionsFile.cfg created successfully")
                print("recSectionsFile.cfg created successfully")
            else:
                # Pass all parameters to process_record using lambda
                output_lines = rdd.map(lambda record: process_record(record, delimeter_flag, input_ascii_flag, CheckOutputAsciiFlag)).collect()

                # output_lines = rdd.map(process_record).collect()
                # output_lines = []
                # for record in records:
                #     rec_type = Layout.getRecSection(record)
                #     layout = Layout(rec_type, record)
                #     line = layout.getOutputFieldsToLine(rec_type, delimeter_flag)
                #     output_lines.append(line)

            output_data = "\n".join(output_lines)
            print("output_data",output_data)
            output_key = os.path.basename(current_file_key) + ".asc"
            # out_ASCII_path= "citizen/wip/c03116cf-db25-4960-95e2-fdc613deecef/output/"
            print("output_key",output_key)
            sys.stdout.flush()
            print("out_ASCII_path",out_ASCII_path)
            sys.stdout.flush()

            file_util.upload_to_s3(bucketName, out_ASCII_path + output_key, output_data)
            log_message('success', f" Output file uploaded successfully")
            print("Output file uploaded successfully")
            sys.stdout.flush()
        
        # After successful processing, delete any existing failure JSON files
        try:
            failed_folder = out_ASCII_path.rstrip('/') + '/failed_conversion/'
            s3 = boto3.client('s3')
            
            # Check for JSON files with matched filename or file pattern
            json_files_to_delete = []
            
            if matched_filename:
                json_files_to_delete.append(failed_folder + f"{matched_filename}.json")
            
            # Also check for file_pattern based JSON
            if file_pattern:
                json_files_to_delete.append(failed_folder + f"{file_pattern}.json")
            
            # Try to delete each potential JSON file
            for json_key in json_files_to_delete:
                try:
                    s3.head_object(Bucket=bucketName, Key=json_key)
                    # File exists, delete it
                    s3.delete_object(Bucket=bucketName, Key=json_key)
                    log_message('info', f"Deleted existing failure JSON: {json_key}")
                    print(f"Deleted existing failure JSON: {json_key}")
                except s3.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == '404':
                        # File doesn't exist, skip
                        pass
                    else:
                        # Some other error, log but don't fail the job
                        print(f"Error checking/deleting {json_key}: {str(e)}")
        except Exception as cleanup_error:
            # Don't fail the job if cleanup fails
            log_message('info', f"Failed to cleanup old failure JSONs: {str(cleanup_error)}")
            print(f"Failed to cleanup old failure JSONs: {str(cleanup_error)}")
            sys.stdout.flush()
            
    except Exception as e:
        import traceback
        from datetime import datetime
        
        # Get just the exception message without traceback
        error_msg = f"Job failed: {str(e)}"
        log_message('failed', error_msg)
        print(error_msg)
        sys.stdout.flush()
        
        # Create single failure JSON - only once per execution
        failure_json_created = False
        try:
            if file_util is None:
                file_util = FileUtility()
            
            failed_folder = out_ASCII_path.rstrip('/') + '/failed_conversion/'
            
            # Determine filename and JSON filename based on whether we matched a file
            if matched_filename:
                # If we matched a file, use the actual filename
                filename_field = matched_filename
                json_filename = f"{matched_filename}.json"
            else:
                # If we failed before matching, use file_pattern for both filename field and JSON name
                filename_field = file_pattern
                json_filename = f"{file_pattern}.json"
            
            failure_data = {
                "filename": filename_field,
                "error": error_msg
            }
            json_content = json.dumps(failure_data, indent=4)
            json_key = failed_folder + json_filename
            
            file_util.upload_to_s3(bucketName, json_key, json_content)
            failure_json_created = True
            log_message('info', f"Failure JSON created at: {json_key}")
            print(f"Failure JSON created at: {json_key}")
            sys.stdout.flush()
        except Exception as json_error:
            log_message('failed', f"Failed to create failure JSON: {str(json_error)}")
            print(f"Failed to create failure JSON: {str(json_error)}")
            sys.stdout.flush()
        
        # Only re-raise if we successfully created the error report
        if failure_json_created:
            sys.exit(1)  # Exit with error code instead of raising to prevent retries

inputFileName=''
if __name__ == "__main__":
    main()
