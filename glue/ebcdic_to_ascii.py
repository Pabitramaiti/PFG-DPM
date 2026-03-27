import boto3
import io
import codecs
import ebcdic
import os
import sys
import json
import re
import splunk


def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:ebcdic_to_ascii:" + account_id

def log_message(status, message):
    splunk.log_message({'FileName':inputFileName,'Status': status, 'Message': message}, get_run_id())

# Regarding output file I/O operations
class FileUtility:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.bw = None

    def openWriteFile(self, bucketName, file_path):
        try:
            self.bw = io.StringIO()  # Use StringIO for writing in-memory
            self.output_file = file_path
        except Exception as e:
            log_message('failed',f" failed to get Openfile:{str(e)}")
            raise e
            
    def close_write_file(self):
        if self.bw:
            try:
                self.upload_to_s3()
                self.bw.close()
            except IOError as e:
                log_message('failed',f" failed to Closefile:{str(e)}")
                raise IOError("failed to Closefile:{str(e)}")
            except Exception as e:
                log_message('failed',f" failed to Closefile:{str(e)}")
                raise e

    def write(self, content):
        if self.bw:
            self.bw.write(content+"\n")  # Debugging line
        
    def upload_to_s3(self):
        bucket, key = self.output_file.replace('s3://', '').split('/', 1)
        self.s3_client.put_object(Bucket=bucket, Key=key, Body=self.bw.getvalue())

    def download_from_s3(self, bucketName, key):
        response = self.s3_client.get_object(Bucket=bucketName, Key=key)
        return response['Body'].read().decode('utf-8')


# Regarding input file I/O operations
class ReadEBCDICRecord:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.fis = None

    def openEBCDICFileForRead(self, bucketName, file_key):
        try:
            response = self.s3_client.get_object(Bucket=bucketName, Key=file_key)
            self.fis = io.BytesIO(response['Body'].read())
        except IOError as e:
            log_message('failed',f" failed to Openfile:{str(e)}")
            raise IOError("failed to Openfile:{str(e)}")
            
        except Exception as e:
            log_message('failed',f" failed to get Openfile:{str(e)}")
            raise e

    def closeEBCDICFileForRead(self):
        if self.fis:
            try:
                self.fis.close()
            except IOError as e:
                log_message('failed',f" failed to get Closefile:{str(e)}")
                raise e
            except Exception as e:
                log_message('failed',f" failed to get Closefile:{str(e)}")
                raise e

    def getEBCDICRecord(self, record, rec_length):
        try:
            rc = self.fis.read(rec_length)
            if not rc:  # End of file
                return -1
            record[:len(rc)] = rc  # Debugging line
            return record
            log_message('success',f" EBCDIC records fetched successfully")
        except IOError as e:
            log_message('failed',f" Error obtaining BK EBCDIC record due to:{str(e)}")
            raise IOError(" Error obtaining BK EBCDIC record due to:{str(e)}")
        except Exception as e:
            log_message('failed',f" Error obtaining BK EBCDIC record due to:{str(e)}")
            raise e



# functions for conversions
def convert_string(line):
    ascii_string=""
    try:
        ascii_string = ebcdic_to_ascii_string(line) # Debugging line
    except Exception as e:
        log_message('failed',f" failed to convert string:{str(e)}")
        raise ValueError(" failed to convert string:{str(e)}")
    return ascii_string

def convert_numeric1(line,signed,precision):
    intValue,decValue="",""
    lowNibble,highNibble,sign="","",""
    tempValue=ebcdic_to_ascii_string(line)
    if(len(tempValue.strip())==0):
        value=tempValue
        
    else:
        lastZonedByte="{:02X}".format(line[len(line)-1])

        highNibble=lastZonedByte[0]
        lowNibble=lastZonedByte[1]
        if(signed==True):
            if(highNibble.lower()=="c"):
                sign="+"
            elif(highNibble.lower()=="d"):
                sign="-"
    
        temp=tempValue[0:len(tempValue)-1]+lowNibble
        if(int(precision)>0):
            intValue=temp[0:len(temp)-int(precision)]
            decValue=temp[len(temp)-int(precision):]
            value=sign+intValue+"."+decValue
        else:
            value=sign+temp
    return value

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
            value=tempValue[0:len(tempValue)-1]
            
        if(signed==True):
            value=sign+value
        
    return value

# def convert_numeric2(line, signed, precision,picture):
    
#     if len(picture) != 0:
#         if '(' in picture:
#             picture_length = int(picture[picture.index('(') + 1:picture.index(')')])
#         else:
#             picture_length = 1  # default to 1 digit if no parentheses
#         total_digits = picture_length + precision  # e.g., from picture clause S9(7)V99
        
#     else:
#         total_digits = (len(line) * 2 - 1) - precision
#     digit_str = ""
#     sign = ""
#     # Check for EBCDIC spaces (0x40)
#     if all(b == 0x40 for b in line):
#         return " " * (total_digits + (1 if precision > 0 else 0))

#     # Extract digits from COMP-3
#     for i in range(len(line)):
#         byte = line[i]
#         high = (byte >> (len(line)-1)) & 0x0F
#         low = byte & 0x0F
        
#         # Last byte's low nibble is sign
#         if i == len(line) - 1:
#             if low == 0xD:
#                 sign = "-"
#             elif low == 0xC or low == 0xF:
#                 sign = "+" if signed else ""
#             else:
#                 sign = ""
#             digit_str += str(high)
#         else:
#             digit_str += str(high) + str(low)

#     # Trim to correct number of digits
#     digit_str = digit_str.zfill(total_digits)[:total_digits]

#     # Split into int and decimal
#     if precision > 0:
#         int_part = digit_str[:-precision]
#         dec_part = digit_str[-precision:]
#         return f"{sign}{int_part}.{dec_part}"
#     else:
#         return f"{sign}{digit_str}"


def convert_date(line, picture):
    cyy=bytearray(2)
    mm=bytearray(2)
    dd=bytearray(2)
    cyy=line[0:2]
    mm=line[2:4]
    strDd=""
    if(len(line)>4):
        dd=line[4:6]
    strCyy = convert_numeric2(cyy, "false", 0, picture)
    strMm = convert_numeric1(mm, "false", 0)
    strDd = convert_numeric1(dd, "false", 0) if dd else ""
    return strCyy + strMm + strDd


def convert_date1(line):
    try:
        value=ebcdic_to_ascii_string(line)
        return value
    except Exception as e:
        log_message('failed',f" failed to get Convert date1:{str(e)}")
        raise ValueError(" failed to get Convert date1:{str(e)}")
    
    
def ebcdic_to_ascii_string(ebcdic_in):
    try:
        return codecs.decode(ebcdic_in, 'cp1047').replace('\x00', ' ')
        log_message('success',f"EBCDIC to ASCII decoded successfully")
    except UnicodeDecodeError as e:
        log_message('failed',f" Unable to decode EBCDIC to ASCII:{str(e)}")
        raise e
    except Exception as e:
        log_message('failed',f" Unable to decode EBCDIC to ASCII:{str(e)}")
        raise e




# Reagarding handling of functions which loads json objects , calls conversion functions.
class Layout:
    RECORD_LEN = 0
    content=""
    recordPosition=0
    recDetails=dict()
    record_input_defs={}
    record_out_defs={}
    
    def __init__(self, record_type, line):
        self.record_type = record_type
        self.data_record = line

    def byte_sub_str(self, byte_array_in, start_idx, end_idx):
        return byte_array_in[start_idx - 1:end_idx]
        
    def get_def_index(self,rec_type, fld_name,record_input_defs):
        fld_name = fld_name.strip().upper() 
        for index, record_def in enumerate(record_input_defs):
            if record_def["rec_type"].upper() == rec_type.upper() and record_def["field_name"].strip().upper() == fld_name:
                return index
        return -1
    
    def format_raw_value(self,rec_def_in, raw_data_record):
        byte_array = self.byte_sub_str(raw_data_record, int(rec_def_in['start']), int(rec_def_in['end']))
        value = ''
        if rec_def_in["data_type"].lower() == "string":
            value = convert_string(byte_array)
        elif rec_def_in["data_type"].lower() == "numeric1":
            value = convert_numeric1(byte_array, rec_def_in["sign"], rec_def_in["precision"])
        elif rec_def_in["data_type"].lower() == "numeric2":
            value = convert_numeric2(byte_array, rec_def_in["sign"], rec_def_in["precision"], rec_def_in['picture'])
        elif rec_def_in["data_type"].lower() == "date":
            value = convert_date(byte_array, rec_def_in["picture"])
        elif rec_def_in["data_type"].lower() == "date1":
            value = convert_date1(byte_array)
        else:
            log_message('failed',f" UNKNOWN DATA TYPE")
        return value
    
    
    def get_data(self,rec_type, fld_name):
        temp=""
        idx = self.get_def_index(rec_type, fld_name,Layout.record_input_defs)
        if idx < 0:
            raise ValueError(f"Raw data field '{fld_name}' not defined! Aborting process.")
        rec_def = Layout.record_input_defs[idx]
        temp=self.format_raw_value(rec_def,self.data_record)
        return temp
        
    # def getOutputFieldsToFileWithoutAscii(self,fOut,rec_type,delimeter_flag):
    #     try:
    #         rec_info=""
    #         for i in Layout.record_input_defs:
    #             if i["rec_type"].lower() == rec_type.lower():
    #                 if delimeter_flag=="True":
    #                     rec_info+=self.format_raw_value(i, self.data_record) + "~"
    #                 else:
    #                     rec_info+=self.format_raw_value(i, self.data_record)
    #         fOut.write(rec_info)
    #     except Exception as e:
    #         error_message = 'failed to get the output fields without ascii copybook'
    #         log_message('failed',error_message)
    #         raise ValueError(error_message)
        
    # def getOutputFieldsToFile(self,fOut,rec_type,delimeter_flag): 
    #     try:
    #         list_size = len(Layout.record_out_defs)
    #         rec_info = ""
    #         for idx in range(list_size):
    #             rec_def = Layout.record_out_defs[idx]
    #             if rec_def["rec_type"].lower() == rec_type.lower():
    #                 fields = rec_def['fields']
    #                 for field in fields:  
    #                     if delimeter_flag=="True":
    #                         rec_info += self.get_data(rec_type, field)+"~"
    #                     else:
    #                         rec_info += self.get_data(rec_type, field)
    #                 fOut.write(rec_info)
    #     except Exception as e:
    #         error_message = 'failed to get the output fields'
    #         log_message('failed',error_message)
    #         raise ValueError(error_message)
            
            
    def getOutputFieldsToFileWithoutAscii(self,fOut,rec_type,delimeter_flag):
        try:
            rec_info=""
            for i in Layout.record_input_defs:

                if(i["rec_type"].lower()==rec_type.lower()):
                    if(i["field_name"] in {'REC-CODE','REC-TYPE'}):
                        if(self.format_raw_value(i, self.data_record) == rec_type):
                            if delimeter_flag=="True":
                                rec_info+=self.format_raw_value(i, self.data_record) + "~"
                            else:
                                rec_info+=self.format_raw_value(i, self.data_record)
                        elif(self.format_raw_value(i, self.data_record) == rec_type[0]):
                            if delimeter_flag=="True":
                                rec_info+=rec_type + "~"
                            else:
                                rec_info+=rec_type
                    else:
                        if(i["rec_type"][0].lower() == rec_type[0].lower()):
                            if delimeter_flag=="True":
                                rec_info+=self.format_raw_value(i, self.data_record) + "~"
                            else:
                                rec_info+=self.format_raw_value(i, self.data_record)
            fOut.write(rec_info)
        except Exception as e:
            error_message = 'failed to get the output fields without ascii copybook'
            log_message('failed',error_message)
            raise ValueError(error_message)
        
    def getOutputFieldsToFile(self,fOut,rec_type,delimeter_flag): 
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
                    fOut.write(rec_info)
        except Exception as e:
            error_message = 'failed to get the output fields'
            log_message('failed',error_message)
            raise ValueError(error_message)
        
    def readCommonConfig(file_type):
        rec = None
        found = False
        rec_type = None
        try:
        
            for line in Layout.common_cfg_content.split("\n"):
                rec = line.strip()
                if rec.startswith("#") or len(line) == 0:
                    continue
                if found==False and rec.startswith("["):
                    rec_type = rec[1:rec.index(']')].strip()
                    if rec_type.lower() == file_type.lower():
                        found = True
                        idx=Layout.common_cfg_content.split("\n").index(line)
                        for rec in Layout.common_cfg_content.split("\n")[idx+1:]:
                            rec = rec.strip()
                            if rec.startswith("["):
                                break
                            else:
                                records = rec.split("|")
                                if len(records) > 0:
                                    if records[0].lower() == "record_length":
                                        Layout.RECORD_LEN = int(records[1].strip())
                                    elif records[0].lower() == "record_position":
                                        Layout.recordPosition = int(records[1].strip())
                                    elif records[0].lower() == "rec-type":
                                        Layout.recDetails[records[1].strip()] = rec
            if not found:
                log_message('failed',f" Not found configuration details for fileType-{file_type}")
                sys.exit(0)

        except IOError as e:
            log_message('failed',f" failed to get OpenCommonConfigfile:{str(e)}")
            raise IOError("failed to get OpenCommonConfigfile:{str(e)}")
            
        except Exception as e:
            log_message('failed',f" failed to get OpenCommonConfigfile:{str(e)}")
            raise e
        
    def getRecSection(e0304_record):
        recSection = ""
        byte_rec_type=bytearray(2)
        byte_rec_type = e0304_record[Layout.recordPosition:Layout.recordPosition+1]
        try:
            recSection = codecs.decode(byte_rec_type,'cp1047')
            recSection = recSection.replace('\x00', ' ')
            recSection=recSection.strip()
        except UnicodeDecodeError as e:
            log_message('failed',f" failed to get Decode Record Section:{str(e)}")
            raise UnicodeDecodeError("failed to get Decode Record Section:{str(e)}")
            
        except Exception as e:
            log_message('failed',f" failed to get Decode Record Section:{str(e)}")
            raise e
        if recSection in Layout.recDetails:
            recsecs = Layout.recDetails[recSection].split("|") 
            if len(recsecs) == 3:
                recSection = recsecs[1]  
            elif len(recsecs) > 3:
                recSection=eval(recsecs[3])
        else:
            log_message('failed',f" ***** Unable to identify record type *****")
            sys.exit(0) 
        return recSection
			
    def load_common_config_from_s3(bucketName,file_util, COMMON_cfg_key):
        try:
            Layout.common_cfg_content = file_util.download_from_s3(bucketName,COMMON_cfg_key)
            log_message('success',f" Common config JSON from s3 loaded successfully")
        except Exception as e:
            log_message('failed',f" Error loading JSON from S3: {str(e)}")
            raise e
            
    def load_json_config_from_s3(bucketName,file_util,key):
        try:
            content=file_util.download_from_s3(bucketName,key)
            if("EBCDIC"in key):
                Layout.record_input_defs=json.loads(content)
            elif("ASCII" in key):
                Layout.record_out_defs=json.loads(content)
        except Exception as e:
            log_message('failed',f" Error loading JSON from S3: {str(e)}")
            raise e
            
        
    
    @staticmethod
    def processEBCDICRecords(bucketName,file_key, out_ASCII, EBCDIC_Mapping_Key, ASCII_Mapping_Key,COMMON_cfg_key,file_type,delimeter_flag,input_ascii_flag,CheckOutputAsciiFlag,file_path):
        try:
            file_util = FileUtility()
            file_util.openWriteFile(bucketName, out_ASCII)
            read_ebcdic = ReadEBCDICRecord()
            read_ebcdic.openEBCDICFileForRead(bucketName, file_key)
            record = bytearray(Layout.RECORD_LEN)
            if input_ascii_flag=="True":
                Layout.load_json_config_from_s3(bucketName,file_util,ASCII_Mapping_Key)
                log_message('success',f" ASCII copybook JSON config from s3 loaded successfully")
            Layout.load_json_config_from_s3(bucketName,file_util,EBCDIC_Mapping_Key)
            log_message('success',f" EBCDIC copybook JSON config from s3 loaded successfully")
            Layout.load_common_config_from_s3(bucketName,file_util,COMMON_cfg_key)
            log_message('success',f" common config from s3 loaded successfully")
            Layout.readCommonConfig(file_type)
        
            rec_sections_buffer = io.StringIO()
            count=0
            while True:
                read_status = read_ebcdic.getEBCDICRecord(record, Layout.RECORD_LEN)
                if read_status == -1:
                    break
                rec_type = Layout.getRecSection(record)
                log_message('success',rec_type)
                if str(CheckOutputAsciiFlag).lower() == "true":
                    rec_sections_buffer.write(rec_type + '\n')
                layout = Layout(rec_type, record)
                if input_ascii_flag=="True":
                    layout.getOutputFieldsToFile(file_util,rec_type,delimeter_flag)
                else:
                    layout.getOutputFieldsToFileWithoutAscii(file_util,rec_type,delimeter_flag)
                count=count+1
                
                
            read_ebcdic.closeEBCDICFileForRead()
            file_util.close_write_file()
            if str(CheckOutputAsciiFlag).lower() == "true":
                rec_sections_content = rec_sections_buffer.getvalue()  # Get the content of the buffer
                recsections_file_key = file_path+'recSectionsFile.cfg'  # Construct the S3 key
                s3_client = boto3.client('s3')
                s3_client.put_object(Bucket=bucketName, Key=recsections_file_key, Body=rec_sections_content)
                log_message('success',f" successfully created and uploaded rec_sections_file to s3 bucket")
            # Cleanup
            rec_sections_buffer.close()
        except Exception as e:
            error_message = 'failed to process ebcdic records'
            log_message('failed',error_message)
            raise ValueError(error_message)
            
def get_matching_s3_keys(bucket, prefix='', pattern=''):

    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = os.path.basename(obj['Key'])
        
            if re.match(pattern, key):
                keys.append(obj['Key'])
    return keys[0]

def set_job_params_as_env_vars():
    # Loop through all command-line arguments starting from the second argument (skip the script name)
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]  # Remove the leading '--'
            value = sys.argv[i + 1]
            os.environ[key] = value

# Main Execution
def main():
    try:
        set_job_params_as_env_vars()

        global inputFileName
        inputFileName = os.getenv('inputFileName')
        log_message('logging',f" logging into conversion main function")
        bucketName = os.getenv('bucketName')
        file_path = os.getenv('file_path')
        file_pattern = os.getenv('file_pattern')
        file_key = os.getenv('file_key')
        out_ASCII_path = os.getenv('out_ASCII_path')
        EBCDIC_Mapping_Key = os.getenv('EBCDIC_Mapping_Key') 
        ASCII_Mapping_Key = os.getenv('ASCII_Mapping_Key')
        COMMON_cfg_key= os.getenv('COMMON_cfg_key')
        filetype = os.getenv('filetype')
        delimeter_flag = os.getenv('delimeter_flag') 
        input_ascii_flag= os.getenv('input_ascii_flag')
        CheckOutputAsciiFlag= os.getenv('CheckOutputAsciiFlag')
        
        file_key = file_key if file_key !=" " else  get_matching_s3_keys(bucketName, file_path, file_pattern)
        file_name = os.path.basename(file_key)
        output_key = file_name + '.asc'
        out_ASCII = f"{bucketName}/{out_ASCII_path}{output_key}"

        Layout.processEBCDICRecords(
            bucketName,
            file_key,
            out_ASCII,
            EBCDIC_Mapping_Key,
            ASCII_Mapping_Key,
            COMMON_cfg_key,
            filetype,
            delimeter_flag,
            input_ascii_flag,
            CheckOutputAsciiFlag,
            file_path
        )
        log_message('success',f" EBCDIC to ASCII conversion done successfully")
    except Exception as e:
        log_message('failed',f" EBCDIC to ASCII conversion failed")
        raise e
inputFileName=''
if __name__ == "__main__":
    main()