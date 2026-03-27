import ebcdic
import io 
import codecs

def log_message(status, message):
    splunk.log_message({'FileName':inputFileName,'Status': status, 'Message': message}, get_run_id())

def byte_sub_str(byte_array_in, start_idx, end_idx):
        return byte_array_in[start_idx - 1:end_idx]

def ebcdic_to_ascii_string(ebcdic_in):
    try:
        return codecs.decode(ebcdic_in, 'cp1047').replace('\x00', ' ')
        log_message('success',f"EBCDIC to ASCII decoded successfully")
    except UnicodeDecodeError as e:
        prilog_messagent('failed',f" Unable to decode EBCDIC to ASCII:{str(e)}")
        raise e
    except Exception as e:
        log_message('failed',f" Unable to decode EBCDIC to ASCII:{str(e)}")
        raise e

# functions for conversions
def convert_string(line):
    ascii_string=""
    try:
        ascii_string = ebcdic_to_ascii_string(line) # Debugging line
        # print(ascii_string)
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



def get_record_definition(raw_data_record,ebcdic_json_data):
    try:
        fields_data = {}
        for rec_def_in in ebcdic_json_data:
            # print(rec_def_in,"rec_def_inrec_def_in")
            byte_array = byte_sub_str(raw_data_record, int(rec_def_in['start']), int(rec_def_in['end']))
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
                print('failed',f" UNKNOWN DATA TYPE")
            fields_data[rec_def_in['field_name']] = value
        return fields_data
    except Exception as e:
        log_message('failed',f" failed to get record definition:{str(e)}")
        raise ValueError(f" failed to get record definition:{str(e)}")
    


