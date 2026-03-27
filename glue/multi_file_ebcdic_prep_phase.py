import boto3
import re
from collections import defaultdict
import os
import sys
import codecs
import ebcdic
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
import splunk
import zipfile
import io
import json
import pandas as pd

s3_client = boto3.client('s3')


def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:ebcdic_validation:" + account_id


def log_message(status, message):
    splunk.log_message({'FileName': fileName, 'Status': status, 'Message': message}, get_run_id())


def parse_config_data(data):
    config = defaultdict(lambda: defaultdict(list))
    lines = data.decode('utf-8').splitlines()
    section = None
    for line in lines:
        line = line.strip()
        if line.startswith('[') and line.endswith(']'):
            section = line[1:-1]
        elif section and '|' in line:
            key, value = line.split('|', 1)
            config[section][key].append(value)
    log_message("logging", f"config data : {config}")
    return config


def read_s3_file(bucket, key):
    # s3 = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        data = response['Body'].read()
        if not data:  # Check if the file is empty
            if allow_empty_file == "True":
                log_message('logging', f"The file {key} in bucket {bucket} is empty, allowing the file")
                return bytes()
            else:
                log_message('failed', f"The file {key} in bucket {bucket} is empty.")
                raise ValueError(f"The file {key} in bucket {bucket} is empty.")
        return data
    except (NoCredentialsError, PartialCredentialsError):
        raise RuntimeError("Credentials for AWS S3 are not configured properly.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            log_message('failed', f" The file {key} does not exist in bucket {bucket}.")
            raise FileNotFoundError(f"The file {key} does not exist in bucket {bucket}.")
        log_message('failed', f" Failed to access file {key} in bucket {bucket}: {e}")
        raise RuntimeError(f"Failed to access file {key} in bucket {bucket}: {e}")


def insert_s3_file(bucket, key, data):
    # s3 = boto3.client('s3')
    try:
        import_data = s3_client.put_object(Bucket=bucket, Key=key, Body=data)
        if not import_data:  # Check if the file is empty
            log_message('failed', f"The file {key} in bucket {bucket} is empty.")
            raise ValueError(f"The file {key} in bucket {bucket} is empty.")
        log_message('success', f"The file {key} in bucket {bucket} imported successfully.")
        return True
    except (NoCredentialsError, PartialCredentialsError):
        raise RuntimeError("Credentials for AWS S3 are not configured properly.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            log_message('failed', f" The file {key} does not exist in bucket {bucket}.")
            raise FileNotFoundError(f"The file {key} does not exist in bucket {bucket}.")
        log_message('failed', f" Failed to access file {key} in bucket {bucket}: {e}")
        raise RuntimeError(f"Failed to access file {key} in bucket {bucket}: {e}")


def set_job_params_as_env_vars():
    # Loop through all command-line arguments starting from the second argument (skip the script name)
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]  # Remove the leading '--'
            value = sys.argv[i + 1]
            os.environ[key] = value
            # log_message('logging',f" Set environment variable {key} to {value}")


def getFileNames(bucket_name, path, pattern):
    log_message("logging", f"starting in getFileNames bucket_name: {bucket_name} | path: {path} |pattern: {pattern}")
    regex = re.compile(pattern)
    try:
        def filter_keys_by_pattern(objects, pattern):
            return [obj['Key'] for obj in objects.get('Contents', []) if pattern.search(obj['Key'].split('/')[-1])]

        objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=path)
        files = filter_keys_by_pattern(objects, regex)
        log_message('logging', f'files list : {files}')
        if not files:
            error_message = 'No schema files found matching the pattern'
            log_message('failed', error_message)
            raise ValueError(error_message)
        log_message("logging", f"files in getFileNames files: {files}")
        return files[0]
    except Exception as e:
        log_message('failed', f" failed to get fileNames:{str(e)}")
        raise e
        
def get_actual_env_details(json_data,s3_bucket,ebcdicPath,data_prep_file_name,env_key):
    #getting environment details from data preparation file
    try:
        log_message("logging",f"environment key: {env_key}")
        actual_env = None
        file_field_details = json_data.get(env_key,"")
        if file_field_details and len(file_field_details) == 1:
            file_details = file_field_details[0]
            actual_env = file_details.get("env_type", "")
            return actual_env
        else:
            error_message = f"Test_Prod File Mismatch Check {ebcdicPath + data_prep_file_name}"
            log_message('failed', error_message)
            raise ValueError(error_message)
    except Exception as e:
        error_message = f"{str(e)}"
        log_message('failed', error_message)
        raise ValueError(error_message)
        
def file_count_in_zip_validation(bucket, zip_file_path, filename, expected_zip_count, mandatory_file_patterns):
    """Count Files Inside Zip File for statements"""
    try:
        log_message("logging", f"count validation started | Expected Number of Files {expected_zip_count}")
        log_message("logging", f"bucket : '{bucket}' | zip_file_path : '{zip_file_path}' | filename : '{filename}'")
        # reading zip file and fetching file names
        file_key = zip_file_path+filename
        data = read_s3_file(bucket,file_key)
        zip_file = zipfile.ZipFile(io.BytesIO(data))
        files = zip_file.namelist()
        actual_number_of_files = len(files)
        log_message("logging", f"filenames : '{files}' | Actual Number of Files'{actual_number_of_files}'")
        for file_pattern in mandatory_file_patterns:
            file = [file for file in files if re.search(file_pattern,file)]
            if file:
                continue
            else:
                error_message = 'Mandatory files are missing'
                raise ValueError(error_message)
        else:
            log_message("success", "All Mandatory Files Available")
            
        if isinstance(expected_zip_count,int) and (expected_zip_count == actual_number_of_files):
            log_message("success", "Zip File Count Validation Successfull for extact count")
            return True
        elif isinstance(expected_zip_count,range) and (actual_number_of_files in expected_zip_count):
            log_message("success", "Zip File Count Validation Successfull in the given range")
            return True
        else:
            error_message = 'Expected Input File Missing'
            raise ValueError(error_message)
    except Exception as e:
        log_message("failed", f"Expected Input File Missing : {str(e)}")
        error_message = f"{str(e)}"
        raise ValueError(error_message)

def get_column_positions(header_line,report_headers):
    # Find start positions of each header word
    try:
        positions = {}
        for h in report_headers:
            idx = header_line.find(h)
            if idx != -1:
                positions[h] = idx
        return positions
    except Exception as e:
        error_message = f"getting error while getting  get_column_positions, error : {str(e)}"
        log_message('failed', error_message)
        raise ValueError(error_message)

def extract_janus_total_from_line(line, positions):
    """
    Extract JANUS total from a balance report line using column positions.
    """
    try:
        janus_start = positions["DST/JANUS TOTALS"]
        rrd_start = positions["RRD TOTALS"]

        # Slice only the JANUS column region
        janus_str = line[janus_start:rrd_start].strip().replace(",", "")
        try:
            return int(janus_str)
        except ValueError:
            return None
    except Exception as e:
        error_message = f"getting error while getting  extract_janus_total_from_line, error : {str(e)}"
        log_message('failed', error_message)
        raise ValueError(error_message)

def patch_line_fixed(line, janus_total, rrd_total, diff, result):
    """
    Rewrite a balance line keeping description aligned,
    adding JANUS totals, RRD totals, diff, and result.
    """
    return (
        f"{line[:40]}"  # Preserve description alignment
        f"{str(janus_total or 0).rjust(15)}"
        f"{str(rrd_total).rjust(20)}"
        f"{str(diff).rjust(20)}"
        f"{result.rjust(28)}\n"
    )

def get_total_tin_values(report_form_keys,json_data):
    try:
        report_rows_data = {}
        total_tin_values = []
        for prefix, key in report_form_keys.items():
            records = json_data.get(key, [])
            # total records
            report_rows_data[f"{prefix}_total_records"] = len(records)
            # unique by tin_value
            unique_tins = {rec["tin_value"] for rec in records}
            report_rows_data[f"{prefix}_unique_records"] = len(unique_tins)
            # collect for global unique
            total_tin_values.extend(unique_tins)
        report_rows_data["tin_forms"] = len(set(total_tin_values))
        return report_rows_data
    except Exception as e:
        error_message = f"getting error while getting get_total_tin_values, error : {str(e)}"
        log_message('failed', error_message)
        raise ValueError(error_message)

def get_output_lines(read_report_text,report_rows,report_rows_data,positions):
    try:
        output_lines = []
        for line in read_report_text.splitlines(keepends=True):
            patched = None
            for rule in report_rows:
                if rule["description"] in line:
                    janus_total = extract_janus_total_from_line(line, positions)
                    rrd_total = report_rows_data.get(rule["rdd_totals"])
                    diff = (janus_total or 0) - rrd_total
                    result = "IN BALANCE" if diff == 0 else "OUT OF BALANCE"
                    patched = patch_line_fixed(line, janus_total, rrd_total, diff, result)
                    # break  # stop once we found a matching validation rule
            output_lines.append(patched if patched else line)
        return output_lines
    except Exception as e:
        error_message = f"getting error while getting get_output_lines, error : {str(e)}"
        log_message('failed', error_message)
        raise ValueError(error_message)
# <---------------------------------- number of files validations logic starts here --------------------------------------->
from ebcdic_to_ascii import get_record_definition

def extract_records_data(bucket, ebcdic_path, config_path, imports_data, fetch_all_records=False):
    """ # Test/Prod Validation --> WRK4905C/MUSC9558
        # Reading ebcdic data from json data MUSC9558.TXT, key should be  jhi_stmt/config/WRK4905-EBCDIC.json
        # Reading json data WRK4905-EBCDIC.json
    """
    try:
        if bucket and ebcdic_path and config_path and imports_data:
            file_name = imports_data.get('file_pattern')
            section_name = imports_data.get('section_name')
            # read ebcdic json ex: WRK4905-EBCDIC.json
            section_key = getFileNames(bucket, config_path, section_name)
            read_json_data = read_s3_file(bucket, section_key)
            ebcdic_json_data = json.loads(read_json_data.decode('utf-8'))
            # log_message("logging", f"json configuration : {ebcdic_json_data}")
            # record length
            record_length = ebcdic_json_data[-1]['end']

            # read ebcdic data ex : MUSC9558.TXT
            ebcdic_key = getFileNames(bucket, ebcdic_path, file_name)
            ebcdic_data = read_s3_file(bucket, ebcdic_key)
            
            if not ebcdic_data and allow_empty_file == "True":
                return []
            buffer_data = io.BytesIO(ebcdic_data)
            buffer_len = len(buffer_data.getvalue())

            total_statements, start_position = 0, 0
            end_position = record_length

            field_names_mapping = imports_data.get("field_names", {})
            rec_type = imports_data.get("rec_type", "")
            all_records_fields_data = [] 
            if not rec_type:
                fail_message = f"rec_type is not provided in validation configuration, please check config...."
                log_message("failed", fail_message)
                raise ValueError(fail_message)
            while True:
                buffer_data.seek(start_position)
                data_to_reading = end_position - start_position
                raw_data_record = buffer_data.read(data_to_reading)
                records_fields_data = list(
                    filter(lambda field: field['field_name'] in list(field_names_mapping.values()) and (field['rec_type'] == str(rec_type)),
                           ebcdic_json_data))
                # log_message("logging", f"records_fields_data : {records_fields_data}")
                data = get_record_definition(raw_data_record, records_fields_data)
                # {'MIMS-RUN-TYPE-CDE': '1', 'INVESTOR-STATEMENT-RUN-CD': 'P'}
                # log_message("logging", f"data of the filetered records : {data}")
                
                if not raw_data_record:
                    break
                data_mapping = {key:data.get(value,"") for key,value in field_names_mapping.items()}
                if fetch_all_records and data_mapping:
                    all_records_fields_data.append(data_mapping)
                elif data_mapping and data_mapping not in all_records_fields_data:
                    all_records_fields_data.append(data_mapping)
                if not data:
                    pass
                total_statements += 1
                if end_position > buffer_len:
                    break
                start_position = end_position
                end_position += record_length
            # log_message("logging", f"all_records_fields_data : {all_records_fields_data}")
            return all_records_fields_data
        else:
            log_message("logging",
                        f"s3_bucket :{bucket}, ebcdic_path :{ebcdic_path}, config_path :{config_path}")
    except Exception as e:
        log_message("failed", f"extract records data failed : {str(e)}")
        error_message = 'extract records data failed'
        raise ValueError(error_message)
    # log_message("logging", f"get_config_json_data : {get_config_json_data}")

# <---------------------------------- number of files validations logic ends here   --------------------------------------->
"""  <-----------------------------------Duplicate Balance Report Data Prepration and Validation ----------------------------------->
 ebcdic_path , report file path, ebcdic config path, report row & types, report file types (1099DIV,5498...),data_validation for file type 

    Title : Duplicate Balance Report validation starts from here
    Author : S Hari Krishna
    Date : 
    Description : 
"""
#######  ................. Return error details in JSON format to consume in step functions or other functions ................########
def json_error_response(status,message,**kwargs):
    return {
        "Status":status,
        "Message":message,
        **kwargs
    }
#   .............target_report file, source brx_total , matched keys/ file types, mapping columns to read or update
def update_matched(target,source_list,match_key,source_key,column_map):
    try:
        log_message("logging",f" target {target} column_map {column_map}  ......\n source_list {source_list} ")
        match_val = target.get(match_key)
        match=None
        for row in source_list:
            if row.get(source_key) == match_val:
                match = row
                break
        if match:
            for tgt_key,src_key in column_map.items():
                if tgt_key in target:
                    target[tgt_key]=match.get(src_key,0)
            log_message("logging",f"matched record job done ..............")
        else:
            for tgt_key,src_key in column_map.items():
                if tgt_key in target:
                    target[tgt_key]=0
        return target
    except Exception as e:
        log_message("logging",
                    f"Error while matching the records target :{str(e)}")
        error_message = 'Error while matching the records target'
        raise ValueError(error_message)


###.............printing lines back with original format based on config file ..............########
def print_line(record,layout):
    log_message("logging",f"Printing each line according to layout file")
    try:
        layout = layout['COLUMNS']
        row =[' '] * max(field['end'] for field in layout)
        for field in layout:      
            val = str(record.get(field['field_name'],''))
            start,end = field['start'],field['end']
            length = end-start
            if 'text_align' in field:
                align = field['text_align']
                if align =='LEFT':
                    padded = val.ljust(length)[:length]
                if align =='RIGHT':
                    padded = val.rjust(length)[:length]
            else:
                padded = val.rjust(length)[:length]
            row[start-1:end] = padded
        return ''.join(row)
    except Exception as e:
        log_message("logging",f"Error while printing the line check the data :{ record } or layout config, Error is : {str(e)}")
        error_message = 'Error while printing the line check the data'
        raise ValueError(error_message)

### ................Parsing single line get key value pairs based on config ..............#######
def parse_line(line,config):
    try:
        row ={}
        for col in config['COLUMNS']:
            field = col['field_name']
            start = col['start']
            end = col['end']
            row[field]=line[start-1:end].strip()
        return row
    except Exception as e:
        log_message("logging",f"Error while parsing the line check the data, Error :{ str(e) } ")
        error_message = 'Error while parsing the line check the data '
        raise ValueError(error_message)
        
 ### Validate the summary file with data we captured ###       
def validate_records(file_types, records,report_data_validation_list):
    for file_type in file_types:
        filtered_file_types = [item for item in report_data_validation_list if item.get('file_type') == file_type]
        for ftype in filtered_file_types:
            compare_fields = ftype.get("compare_records",[])
            mapped_name = ftype.get("mapped_name","")
            #field that maps file type and file type field in Ebcdic data
            file_type_field = ftype.get('file_type_field','')
            if mapped_name:
                mapped_filetype = mapped_name
            else:
                mapped_filetype = ftype.get('file_type')
            
            for record in records:
                log_message("logging",f" Record details {record} {file_type_field} \n")
                if record.get(file_type_field) in mapped_filetype:
                    for left,right in compare_fields.items():
                        normalized_left = record.get(left)
                        if int(normalized_left.replace(",","")) != int(record.get(right)):
                            return False
        
                else:
                    continue
    return True

### split the lines to start and endlines to compare the values
def split_report_file(lines,skip_lines_code):
    try:
        start_lines,end_lines=[],[]
        for line in lines:
            if not line:
                continue
            first_char = line[0]
            if first_char.isdigit() and skip_lines_code.isdigit():
                if int(first_char) < int(skip_lines_code):
                    start_lines.append(line)
                else:
                    end_lines.append(line)
            else:
                log_message("logging",f"Wrong data is reading from the line please check the data ")
                error_message = 'Error while parsing the line check the data '
                raise ValueError(error_message)
        return start_lines,end_lines

    except Exception as e:
        log_message("logging",f"Error reading the lines fromt the report file, Error : {str(e)}")
        error_message = 'Error reading the lines fromt the report file'
        raise ValueError(error_message)

def dup_bal_report_data(bucket,config_path,ebcdic_path,file_pattern, ebcdic_config_file,report_file_types,report_data_validation_list,columns_from_data):
    """
        file pattern --> JSC.MUSD6377.TXT
        report path --> ^JSC\.(MUSC4340)\.\d{8}\.\d{6}\s\d\.(TXT|txt)$
        confign path --> WRK5233-EBCDIC.json
        report file types --> ['1099DIV', '5498', '1099R', '1099B']
        report data validation -> Name, fields, headers
        columns_from_data --> which columns to read
    """
    #print(f"reading balance report data from ebcidc {file_pattern} report file types {report_file_types}  columns_from_data {columns_from_data}")
    try:
        if bucket and ebcdic_path and file_pattern and ebcdic_config_file and report_file_types and report_data_validation_list and columns_from_data:
            
            # read ebcdic json ex: WRK5233-EBCDIC.json
            section_key = getFileNames(bucket, config_path, ebcdic_config_file)
            read_json_data = read_s3_file(bucket, section_key)
            ebcdic_json_data = json.loads(read_json_data.decode('utf-8'))
            #read record length from json 1800
            record_length = ebcdic_json_data[-1]['end']
            #read ebcdic data from file ex: JSC.MUSD6377.TXT
            ebcdic_key = getFileNames(bucket, ebcdic_path, file_pattern)
            ebcdic_data = read_s3_file(bucket, ebcdic_key)

            #read the file in bytes of the ebcdic data
            buffer_data = io.BytesIO(ebcdic_data)
            buffer_len = len(buffer_data.getvalue())
            
            combined_df = None
            brx_all_counts = []
            for file_type in report_file_types:
                file_types = [item for item in report_data_validation_list if item.get('file_type') == file_type]
                
                for ftype in file_types:
                    rec_type = ftype.get('rec_type')
                    fields_to_read = ftype.get('fields_to_read')
                    fields_to_count = ftype.get('fields_to_count')
                    mapped_name = ftype.get("mapped_name","")
                    file_code_field = ftype.get("file_code_field")
                    
                    total_records, start_position = 0,0
                    end_position = record_length
                    field_names_mapping = dict(zip(fields_to_read,fields_to_read))

                    #get individual file type records
                    all_records_data =[]
                    if not rec_type:
                        fail_message = f"rec_type is not provided in validation configuration, please check config...."
                        log_message("rec_type is not provided in validation configuration", fail_message)
                        raise ValueError(fail_message)
                    while True:
                        buffer_data.seek(start_position)
                        data_to_reading = end_position - start_position
                        raw_data_record = buffer_data.read(data_to_reading)
                    
                        record_fields_data = list(filter(lambda field:field['field_name'] in list(field_names_mapping.values()) and (field['rec_type'] == str(rec_type)),ebcdic_json_data))
                        data = get_record_definition(raw_data_record,record_fields_data)
                        if not raw_data_record:
                            break
                        data_mapping = {key:data.get(value,"") for key,value in field_names_mapping.items()}
                        data_mapping.update({'file_type':ftype.get('file_type'),'rec_type':rec_type})
                        if not data:
                            pass
                        all_records_data.append(data_mapping)
                        total_records+=1
                        if end_position > buffer_len:
                            break
                    
                        start_position = end_position
                        end_position += record_length

                    #individual data frames for each file type
                    df = pd.DataFrame(all_records_data)
                    df = df[df[file_code_field]==str(rec_type)]
                    #adding brx-count columns and file types 
                    #mapping the which fields to count and append to brx_all_counts
                    brx_counts = {
                        target_col:df[source_col].eq(val).sum() if source_col in df.columns else 0 for target_col, condition in fields_to_count.items() for source_col, val in condition.items()
                    }                    
                    #Map name for the 5498 and 5498ESA 
                    if mapped_name:
                        mapped_filetype = mapped_name
                    else:
                        mapped_filetype = ftype.get('file_type')
                        log_message("failed", f"Mapped column / alternate column name not vailable")
                        
                    brx_all_counts.append({
                        'file_type':mapped_filetype,**brx_counts
                    })
                    
                    combined_df = pd.concat([combined_df,df],ignore_index=True,sort=False)     
            log_message("logging",f"data read from ebcdic completed and added to DataFrame for each file type")
            print("brx records has beed completed and returned .............")
            return brx_all_counts,combined_df
        else:
            log_message("logging",
                        f"s3_bucket :{bucket}, ebcdic_path :{ebcdic_path}, mapping_config_path :{config_path},report file types : {report_file_types} ")
            error_message = f'Error while reading data from ebcdic file check for error logs'
            raise ValueError(error_message)
    except Exception as e:
        log_message("failed", f"extract records data failed : {str(e)}")
        error_message = f'duplicate balance report failed, check the file name and location {str(e)}'
        raise ValueError(error_message)

def main():
    try:
        set_job_params_as_env_vars()

        global fileName,allow_empty_file
        fileName = os.getenv('fileName')
        log_message('logging', f" EBCDIC Validation")
        s3_bucket = os.getenv('s3_bucket')
        ebcdicPath = os.getenv('ebcdicPath')
        destination_path = os.getenv('destination_path')
        config_file_key = os.getenv('config_file_key')  # config file path/key
        configuration_path = "/".join(config_file_key.split("/")[:-1])
        config_file_validation = os.getenv('validation_config')
        sdlc_env = os.getenv('sdlc_env')
        allow_empty_file = os.getenv('allow_empty_file')

        # Read the configuration file directly from S3 without saving locally
        try:
            config_data = read_s3_file(s3_bucket, config_file_key)
            config = parse_config_data(config_data)
        except FileNotFoundError as e:
            log_message('failed', f" Configuration file error: {e}")
            raise RuntimeError(f"Configuration file error: {e}")
        except Exception as e:
            log_message('failed', f" Configuration file error: {e}")
            raise e

        # read validations json file
        # ebcdic_data = read_s3_file(s3_bucket, ebcdicKey)
        if config_file_validation:
            #read validation file to import the data
            read_validation_config = read_s3_file(s3_bucket, config_file_validation).decode('utf-8')
            load_validation_json = json.loads(read_validation_config)
            log_message("logging", f"load_validation_json {load_validation_json}")
            data_preparation = load_validation_json.get("data_preparation", {})
            data_prep_file_name = data_preparation.get("output_file_name","")
            log_message("logging", f"data_preparation - {data_preparation}")
            data_validation = load_validation_json.get("data_validation",{})
            active_validations = data_validation.get("active_validations",{})
            active_tasks = data_preparation.get("active_tasks", {})
            log_message("logging", f"active_tasks - {active_tasks}")
            file_patterns = data_preparation.get("file_pattern",{})
            modified_active_tasks = {}
            for key, value in active_tasks.items():
                if isinstance(value,bool):
                    modified_active_tasks[key] = value
                else:
                    for file_type,pattern in file_patterns.items():
                        if key == file_type and re.search(pattern,fileName):
                            modified_active_tasks.update(value)
            log_message("logging", f"modified_active_tasks - {modified_active_tasks}")
            files_for_extraction = data_preparation.get("files_for_extraction", [])
            log_message("logging", f"files_for_extraction - {files_for_extraction}")
            modified_active_tasks = {k:v for k,v in modified_active_tasks.items() if v}
            json_data = {}
            for task, task_flag in modified_active_tasks.items():
                if task_flag and files_for_extraction:
                    try:
                        task_inputs = list(filter(lambda file_info : file_info['id'] == task, files_for_extraction))
                        if task_inputs:
                            task_inputs = task_inputs[0]
                            log_message("logging", f"task_inputs - {task_inputs}")
                            if "fetch_all_records" in task_inputs:
                                fetch_all_records = task_inputs.get("fetch_all_records")
                                fields_data = extract_records_data(s3_bucket, ebcdicPath, configuration_path, task_inputs,fetch_all_records)
                                fields_data = [{"tin_value": item["tin_field"] + item["office_code"] + item["sector_code"] + item["segment_code"],"form_type": item["form_type"]} for item in fields_data]
                            else:
                                fields_data = extract_records_data(s3_bucket, ebcdicPath, configuration_path, task_inputs)
                            json_data[task] = fields_data
                        else:
                            log_message('logging', f'task inputs not for provided for {task}')
                    except Exception as e:
                        error_message = f"getting error while importing data  | error : {str(e)}"
                        log_message("failed", error_message)
                        raise ValueError(error_message)
            # if json_data:
            try:
                output_file_name = data_preparation.get("output_file_name", "")
                if output_file_name:
                    key = ebcdicPath + output_file_name
                    insert_s3_file(s3_bucket, key, json.dumps(json_data, indent=4))
                    log_message("logging",
                                f"imported data in | bucket : {s3_bucket} | path : {key} successfull")
                else:
                    error_message = f"output_file_name is not available in configuration, please check config file..."
                    log_message("failed", error_message)
                    raise ValueError(error_message)
            except Exception as e:
                error_message = f"getting error while inserting data | error : {str(e)}"
                log_message("failed", error_message)
                raise ValueError(error_message)
            #one time data validation 
            for validation,flag in active_validations.items():
                if flag and validation == "file_count_in_zip":
                    #created to validate number of files in zip file are equal to expected count
                    try:
                        file_count_in_zip = data_validation.get(validation,[])
                        validation_data = [pattern for pattern in file_count_in_zip if re.search(pattern['file_pattern'],fileName)]

                        if validation_data:
                            validation_data = validation_data[0]
                            mandatory_file_patterns = validation_data.get("files_within_zip",{}).get("mandatory",[])
                            file_count_rule = validation_data.get("file_count_rule")
                            expected_zip_file_count = None
                            if "type" in file_count_rule and file_count_rule.get("type") == "exact":
                                expected_zip_file_count =  file_count_rule.get("expected_count")
                            elif "type" in file_count_rule and file_count_rule.get("type") == "range":
                                min_count = file_count_rule.get("min_count")
                                max_count = file_count_rule.get("max_count")
                                expected_zip_file_count = range(min_count,max_count+1)
                        else:
                            error_message = f"no pattern found to validate zip files count"
                            log_message("failed",error_message)
                            raise ValueError(error_message)    
                        file_count_in_zip_validation(s3_bucket, ebcdicPath, fileName, expected_zip_file_count, mandatory_file_patterns)
                        log_message("success",f"count validation successfull")
                    except Exception as e:
                        error_message = f"{str(e)}"
                        log_message("failed",error_message)
                        raise ValueError(error_message)
                elif flag and validation == "environment":
                    # getting actual env details
                    environment = data_validation.get(validation,{})
                    log_message("logging",f"environment validation config: {environment}")
                    env_data_list = environment.get("env_data_list",[])
                    env_config = environment.get("env_config",[])
                    for env_key in env_data_list:
                        if env_key in modified_active_tasks:
                            actual_environment = get_actual_env_details(json_data,s3_bucket,ebcdicPath,data_prep_file_name,env_key)
                            allowed_values = {}
                            for values in env_config:
                                if values.get("name", "") == sdlc_env:
                                    allowed_values = values.get("allowed_values")
                            log_message("logging", f"actual_environment: {actual_environment} | allowed_values: {allowed_values}")
                            if actual_environment is not None and allowed_values and actual_environment in allowed_values:
                                log_message("success","environment validation successfull")
                            else:
                                error_message = f"Test_Prod File Mismatch"
                                log_message("failed",error_message)
                                raise ValueError(error_message)
                elif flag and validation == "balance_report_validation":
                    try:
                        # read balance report data
                        report_validation_list = data_validation.get(validation,[])
                        for report_validation in report_validation_list:
                            log_message("logging", f"report_validation - {report_validation}")
                            file_pattern = report_validation.get("file_pattern","")
                            if re.search(file_pattern,fileName):
                                report_pattern = report_validation.get("report_file_pattern","")
                                report_headers = report_validation.get("report_headers",[])
                                report_rows = report_validation.get("report_rows",[])
                                report_form_keys = report_validation.get("form_keys",{})
                                report_output_path = report_validation.get("output_report_path","")
                                try:
                                    report_file_key = getFileNames(s3_bucket,ebcdicPath,report_pattern)
                                    read_report_text = read_s3_file(s3_bucket,report_file_key).decode('utf-8')
                                except Exception as e:
                                    check_balance_report = getFileNames(s3_bucket,ebcdicPath,report_output_path.split("/")[-1])
                                    if check_balance_report:
                                        continue
                                    log_message("logging",f"balance report not found {str(e)}")
                                    raise ValueError(f"balance report not found")
                                log_message("logging", f"report_pattern: {report_pattern} | report_headers: {report_headers} | report_rows: {report_rows} | report_form_keys: {report_form_keys} | report_output_path: {report_output_path}")
                                report_rows_data = get_total_tin_values(report_form_keys,json_data)
                                # global unique TINs across all form types
                                log_message("logging",f"report_rows_data: {report_rows_data}")
                                header_line = next((l for l in read_report_text.splitlines() if "DESCRIPTION" in l), None)
                                if not header_line:
                                    raise ValueError("Could not find header line in balance report")
                                positions = get_column_positions(header_line,report_headers)
                                log_message("logging",f"read_report_text: {read_report_text}")
                                output_lines = get_output_lines(read_report_text,report_rows,report_rows_data,positions)
                                
                                log_message("logging",f"output_lines: {output_lines}")
                                patched_report = "".join(output_lines)
                                destination_key = ebcdicPath + report_output_path
                                log_message("logging", f"destination_key: {destination_key}")
                                
                                # Upload patched report
                                insert_s3_file(s3_bucket, destination_key, patched_report.encode("utf-8"))
                                log_message("success", f"Patched balance report stored: {destination_key}")
                    except Exception as e:
                        error_message = f"getting error report validation | error : {str(e)}"
                        log_message("failed",error_message)
                        raise ValueError(error_message)
                #### Duplicate blance report validation starts from here #####
                elif flag and validation == "duplicate_balance_report":
                    try:
                        duplicate_balance_report = load_validation_json.get(validation,{})
                        report_validation_list = duplicate_balance_report.get("dup_bal_report_validation",[])
                        report_data_validation_list = duplicate_balance_report.get("dup_bal_data_validation",[])
                        
                        #Store result after comparing all file types
                        compare_results = []
                        for report_validation in report_validation_list:
                            file_pattern = report_validation.get("file_pattern","")
                            zip_pattern = report_validation.get("zip_pattern","")
                            if re.search(zip_pattern,fileName):
                                log_message('logging', f'searching for file patterns in the location , pattern {file_pattern}')
                                report_pattern = report_validation.get("report_pattern","")
                                report_output_path = report_validation.get("output_report_path","")
                                report_file_types = report_validation.get("file_types",[])
                                report_fields_config = report_validation.get("fields_config","")
                                report_config = report_validation.get("report_config","")
                                columns_from_data = report_validation.get("columns_from_data",[])
                                columns_from_report = report_validation.get("columns_from_report",[])
                                total_ssn_zip_combo = report_validation.get("total_ssn_zip_combo",[])
                                header_record = report_validation.get('header_record','')
                                total_record = report_validation.get('total_record','')
                                skip_lines_code = report_validation.get('skip_lines_code','')
                                file_type_field = report_validation.get('file_type_field','')
                                
                                brx,ebcdic_trans= dup_bal_report_data(s3_bucket,configuration_path,ebcdicPath,file_pattern,report_fields_config,report_file_types,report_data_validation_list,columns_from_data)
                                ### columns mapping from edcdic file records to report file records####
                                if len(columns_from_data) == len(columns_from_report):
                                    columns_map=dict(zip(columns_from_report,columns_from_data))
                                else:
                                    log_message("logging",f"Mapping columns from validation file is error")
                                    error_message = f"Columns not mapped correctly check columns_from_data in validation config file"
                                    raise ValueError(error_message)

                                """Calculating the summary like total records of Corrected, Duplicated, Subsequent """
                                brx_total ={col:0 for col in columns_from_data}
                                ###........counting the records for all the form types .......###
                                for col in columns_from_data:
                                    brx_total[col]=(lambda c:sum(b.get(c,0) for b in brx))(col)
                                
                                brx_total.update({'file_type':total_record})
                                """  Mapping columns for he header rows and adding header records"""
                                brx_header = dict(zip(columns_from_data,columns_from_report))
                                brx_header.update({'file_type':header_record})
                                #columns_map
                                """ append the records of header, Summary total records to the file """
                                brx.append(brx_total)
                                brx.append(brx_header)                    
                                
                                log_message('logging', f'"Brx headers added successfully......!')
                                #read balance report config and balance report
                                try:                                    
                                    # read ebcdic json ex: DUPBAL_REPORT.json
                                    report_config_key = getFileNames(s3_bucket, configuration_path, report_config)
                                    read_config_json = read_s3_file(s3_bucket, report_config_key)
                                    report_layout = json.loads(read_config_json.decode('utf-8'))

                                    # read report summary data ex : JSC.MUSC4407.20250820.091230 3.TXT
                                    report_file_key = getFileNames(s3_bucket, ebcdicPath, report_pattern)
                                    dup_bal_report = read_s3_file(s3_bucket, report_file_key).decode('utf-8')
                                except Exception as e:
                                    log_message("logging",f"getting exception while getting/ reading report file")
                                    error_message = f"balance report not found {str(e)}"
                                    raise ValueError(error_message)
                                ### Spliting the report data to lines
                                lines = dup_bal_report.splitlines()
                                start_lines, end_lines = split_report_file(lines,skip_lines_code)
                                #parse the start lines and divide as per the layout file 
                                parsed_rows = [parse_line(line,report_layout) for line in start_lines]
                                for row in parsed_rows:
                                    update_matched(row,brx,file_type_field,'file_type',columns_map)
                                log_message("logging",f"rows updated with matched values")
                                uniq_ssn = len(ebcdic_trans[total_ssn_zip_combo].drop_duplicates())
                                ##appending the unique count of SSN ZIP combo to the line where  SSN Combo
                                if end_lines:
                                    end_lines[0] = end_lines[0].rstrip("\n") + str(uniq_ssn).ljust(10) + "\n"
                                else:
                                    log_message("logging",f"End lines is missing or report file is empty ")
                                    error_message = f"End lines is missing or report file is empty"
                                    raise ValueError(error_message)
                                
                                log_message("logging",f"..............output report is ready ....................\n")
                                formatted_start_lines = [print_line(row,report_layout) for row in parsed_rows]
                                formatted_end_lines = [line.rstrip("\n") for line in end_lines]
                                full_report = formatted_start_lines + formatted_end_lines
                                
                                output_text = "\n".join(full_report)
                                destination_key = ebcdicPath + report_output_path                                    
                                log_message("logging", f"destination_key: {destination_key}")
                                try:    
                                    # Upload patched report
                                    report_ouput_status = insert_s3_file(s3_bucket, destination_key, output_text.encode("utf-8"))
                                    if report_ouput_status:
                                        log_message("success", f"Patched balance report stored: {destination_key}")
                                        
                                        #Compare the records in report file with data from ebcdic file
                                        compare_records_status = validate_records(report_file_types,parsed_rows,report_data_validation_list)
                                        compare_results.append({
                                            "file_name":file_pattern,
                                            "report_file":report_output_path.split("/")[-1],
                                            "status":compare_records_status
                                        }) 
                                    else:
                                        log_message("logging",f"File not created check the logs")
                                        error_message = f"File not created check the logs"
                                        raise ValueError(error_message)

                                except Exception as e:
                                    error_message = f"getting error while reading output lines, error :{str(e)} "
                                    log_message('failed', error_message)
                                    raise ValueError(error_message)

                        if compare_results:
                            failed_status = [res for res in compare_results if res['status']!=True ]
                            if failed_status:
                                error_message = json_error_response(status = "REPORT VALIDATION FAILED",message=f"Validation failed for the following report files, please check the attachments.",report_files = [res['report_file'] for res in failed_status],result = failed_status)
                                log_message('failed', error_message)
                                raise ValueError(json.dumps(error_message))
                            else:
                                log_message('logging',"Duplicate Balance Report Generated, Records matched with report fiels")
                                
                    except Exception as e:
                        log_message("failed", f"extract records data failed : {str(e)}")
                        error_message = f"extract validation data from json failed {str(e)}"
                        raise ValueError(error_message)
    except Exception as e:
        log_message("failed", f"Error occurred in main: {str(e)}")
        raise


fileName = ''
if __name__ == "__main__":
    main()