import json, sys,boto3,re, os, splunk
from awsglue.utils import getResolvedOptions
# FUNCTIONS TO HANDLE THE HIERARCHICAL STACK #
#to handle child and parent attribute

def get_run_id():
    # glue_client = boto3.client("glue")
    # job_name = sys.argv[0].split('/')[-1]
    # job_name = job_name[:-3]
    # print(job_name)
    # response = glue_client.get_job_runs(JobName = job_name)
    # job_run_id = response["JobRuns"][0]["Id"]
    # return job_run_id
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:parse_copybook_to_json:"+account_id


def fGetSetack():
    global stack, output
    tmp = output
    for k in stack:
        tmp = tmp[stack[k]]
    return tmp
#for only  child attribute
def fRemStack(iStack,iLevel):
    NewStack = {}
    for k in iStack:
        if k < iLevel: NewStack[k] = iStack[k]
    return NewStack

#PIC 999, 9(3), XXX, X(3)...
def getPicSize(arg):
    if arg.find("(") > 0: 
        return int(arg[arg.find("(")+1:arg.find(")")])
    else:
        return len(arg)

# TYPE AND LENGTH CALCULATION. Get the length of attribute #
def getLenType(atr, p):
    ret = {}
    #FirstCh = atr[3][:1].upper()
    #Picture = atr[3].upper()
    FirstCh = atr[p][:1].upper()
    Picture = atr[p].upper()
#Get the type of attribute
    #data type
    if   'COMP-3'in atr and FirstCh=='S': ret['type'] = "pd+"
    elif 'COMP-3'in atr:                  ret['type'] = "pd"
    elif 'COMP'  in atr and FirstCh=='S': ret['type'] = "bi+"
    elif 'COMP'  in atr:                  ret['type'] = "bi"
    elif FirstCh=='S':                    ret['type'] = "zd+"
    elif FirstCh=='9':                    ret['type'] = "zd"
    else:                                 ret['type'] = "ch"

    #Total data length for decimal type
    PicNum = Picture.replace("V"," ").replace("S","").replace("-","").split()
    
    Lgt = getPicSize(PicNum[0])

    if len(PicNum) == 1 and FirstCh !='V':
        ret['dplaces'] = 0
    elif FirstCh !='V':
        ret['dplaces'] = getPicSize(PicNum[1])
        Lgt += ret['dplaces']
    else:
        ret['dplaces'] = getPicSize(PicNum[0])

    ret['length'] = Lgt

    #Data size in bytes
    if   ret['type'][:2] == "pd": ret['bytes'] = int(Lgt/2)+1
    elif ret['type'][:2] == "bi": 
        if   Lgt <  5:             ret['bytes'] = 2
        elif Lgt < 10:             ret['bytes'] = 4
        else         :             ret['bytes'] = 8
    else:                          
        if FirstCh=='-': Lgt += 1
        ret['bytes'] = Lgt
        
    return ret

############# DICTIONARY AND HIERARCHICAL LOGIC ###########################
#For each attribute dictionary is created to store in JSON output
def add2dict(lvl, grp, itm, stt, id):

    global cur, output, last, stack, FillerCount

    if itm.upper() == "FILLER":
        FillerCount += 1
        itm = itm + "_" + str(FillerCount)

    if lvl <= cur: stack = fRemStack(stack, lvl)

    stk = fGetSetack()
    stk[itm]= {}
    stk[itm]['id'] = id
    stk[itm]['level'] = lvl
    stk[itm]['group'] = grp
    
    if 'OCCURS'   in stt: 
        if 'TIMES' in stt:
            stk[itm]['occurs'] = int(stt[stt.index('TIMES')-1])
        else:
            raise Exception('OCCURS WITHOU TIMES?' + ' '.join(stt))

    if 'REDEFINES'in stt: stk[itm]['redefines'] = stt[stt.index('REDEFINES')+1]

    if grp == True:
        stack[lvl] = itm
        cur = lvl
    else:
        tplen = {}
        pic = stt.index('PIC')+1
        tplen = getLenType(stt, pic)
        #stk[itm]['pict'] = stt[3]
        stk[itm]['pict'] = stt[pic]
        stk[itm]['type'] = tplen['type']
        stk[itm]['length'] = tplen['length']
        stk[itm]['bytes'] = tplen['bytes']
        stk[itm]['dplaces'] = tplen['dplaces']

############################### MAIN ###################################
# READS, CLEANS AND JOINS LINES #

FillerCount=0
cur=0
output={}
stack = {}
# we skip if line starts with * in copybook(checking 7th column)
def toDict(lines):

    id = 0
    stt = ""
    for line in lines: 
        if len(line[6:72].strip()) > 1:
            if line[6] in [' ' , '-']: 
                if not line[6:72].split()[0] in ['SKIP1','SKIP2','SKIP3']:
                    stt += line[6:72].replace('\t', ' ')
            elif line[6] != '*':
                print('Unnexpected character in column 7:', line) 
                quit()

    # READS FIELD BY FIELD / SPLITS ATTRIBUTES #
    for variable in stt.split("."):
        
        attribute=variable.split()
#if PIC is present it is considered as child and value(true) is passed to addtodict()
        if len(attribute) > 0:
            if attribute[0] != '88': 
                id += 1
                add2dict(int(attribute[0]), False if 'PIC'in attribute else True, attribute[1], attribute, id)

    return output


def DisParam(arg):
    desc = {
        '-copy_book_bucket'    : 'REQUIRED: Copybook file name',
        'copy-book-key': 'copybook location in s3 ',
        'json-bucket':'Location of bucket for supplemental file',
        }


###### Create the extraction parameter file
def CreateExtraction(obj, altstack=[], partklen=0, sortklen=0):
    global lrecl
    global altpos
    for k in obj:
        if type(obj[k]) is dict:
            t = 1 if 'occurs' not in obj[k] else obj[k]['occurs']

            iTimes = 0
            while iTimes < t:
                iTimes +=1
#Recursive function. if redifine is not there it keeps adding the attribute to trans otherwise to trans+type
                if 'redefines' not in obj[k]:
                    if obj[k]['group'] == True:
                        altstack.append(k)
                        CreateExtraction(obj[k], altstack, partklen, sortklen)
                        altstack.remove(k)
                    else:
                        item = {}
                        item['type'] = obj[k]['type']
                        item['bytes']  = obj[k]['bytes']
                        item['offset']  = lrecl
                        item['dplaces']  = obj[k]['dplaces']
                        item['name'] = k
                        item['part-key'] = True if (lrecl + obj[k]['bytes']) <= partklen else False
                        item['sort-key'] = True if (lrecl + obj[k]['bytes']) <= (sortklen + partklen) and (lrecl + obj[k]['bytes']) > partklen else False
                        transf.append(item)

                        lrecl = lrecl + obj[k]['bytes']
                else:
                    add2alt = True
                    for x in altlay:
                        if x[list(x)[0]]['newname'] == k:
                            add2alt = False
                    if add2alt:
                        red = {}
                        red[obj[k]['redefines']] = obj[k].copy()
                        red[obj[k]['redefines']]['newname'] = k
                        red[obj[k]['redefines']]['stack'] = altstack.copy()
                        altpos+=1
                        altlay.insert(altpos,red)
                
############################### MAIN ###################################
print("--------------------------------------------------------------------------------------")
try:
    s3 = boto3.client('s3')
    
    iparm = getResolvedOptions(sys.argv, ['copybook_bucket', 'copybook_key', 'json_bucket','trackrecords'])
    print(iparm)
    DisParam(iparm)
    #obj reads copybook 
    obj = s3.get_object(Bucket=iparm['copybook_bucket'], Key=iparm['copybook_key'])
    #Passing copybook data to todict()
    output = toDict(obj['Body'].read().decode('utf-8').splitlines())
    
    altlay = []
    transf = []
    lrecl = 0
    altpos = 0
    partklen = 0
    sortklen = 0
    CreateExtraction(output, [], partklen, sortklen)
    
    param = {}
    param['lrecl'] = lrecl
    param['transf'] = transf
    param['json_bucket'] = iparm['json_bucket']  if 'json_bucket' in iparm else iparm['copybook_bucket']
    param['copybook_key'] = iparm['copybook_key']  if 'copybook_key' in iparm else ''
    param['copybook_bucket'] = iparm['copybook_bucket']  if 'copybook_bucket' in iparm else ''
    
    
    ialt = 0
    for r in altlay:
        transf = []
        lrecl = 0
        redfkey = list(r.keys())[0]
    
        #POSITIONS ON REDEFINES
       
        newout = output
        for s in r[redfkey]['stack']:
            newout = newout[s]
    
        newout[redfkey] = r[redfkey].copy()
        temp_red=newout[redfkey].pop('redefines')
        
        
        altpos = ialt
        # use regression to extract number after TYPE
        re_match_obj = re.match(".*TYPE(\d+)",r[redfkey]['newname'])
        #assigns the matched string
        if re_match_obj:
            trans_index=re_match_obj.groups()[0]
        else:
            trans_index = ialt
        CreateExtraction(output, [], partklen, sortklen)
        ialt += 1
        #param['transf' + str(ialt)] = transf
        param['transf' + str(trans_index)] = transf
    col_check = None
    
    s3_client = boto3.client("s3")
    key=os.path.basename(param['copybook_key']).split(".txt")[0]+".json"
    s3_client.put_object(Bucket=param['json_bucket'], Key=key, Body=json.dumps(param,indent=4))
    #with open(iparm['output'],"w") as fout: fout.write(json.dumps(param,indent=4))
    splunk.log_message({'InputFileName': param['copybook_key'], 'Status': 'success', 'Message': f"{param['copybook_key']} is successfully converted to json"},  get_run_id())
except Exception as e:
    splunk.log_message({'InputFileName': param['copybook_key'], 'Status': 'failed', 'Message': f"failed due to {param['copybook_key']} is not converted to Json, error: {e}"},  get_run_id())
    raise e

print("--------------------------------------------------------------------------------------")


