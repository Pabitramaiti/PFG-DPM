import sys, os, boto3, io, json, re
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
import splunk
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel
from awsglue.job import Job
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, expr, count, col, explode, sum as _sum,first
import pandas as pd
import numpy as np
from pyspark.sql import functions as F
import operator
from functools import reduce
import csv
from datetime import datetime

s3_client = boto3.client('s3')

def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:ebcdic_validation:" + account_id

def log_message(status, message):
    try:
        splunk.log_message({'FileName': fileName, 'Status': status, 'Message': message}, get_run_id())
    except Exception as e:
        print(f"Logging failed: {e} | Status: {status} | Message: {message}")

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

def read_s3_file(bucket, key):
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        data = response['Body'].read()
        if not data:  # Check if the file is empty
            log_message('failed', f"The file {key} in bucket {bucket} is empty.")
            raise ValueError(f"The file {key} in bucket {bucket} is empty.")
        return data
    except Exception as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            log_message('failed', f" The file {key} does not exist in bucket {bucket}.")
            raise FileNotFoundError(f"The file {key} does not exist in bucket {bucket}.")
        log_message('failed', f" Failed to access file {key} in bucket {bucket}: {e}")
        raise RuntimeError(f"Failed to access file {key} in bucket {bucket}: {e}")
def insert_s3_file(bucket, key, data):
    try:
        import_data = s3_client.put_object(Bucket=bucket, Key=key, Body=data)
        if not import_data:  # Check if the file is empty
            log_message('failed', f"The file {key} in bucket {bucket} is empty.")
            raise ValueError(f"The file {key} in bucket {bucket} is empty.")
        log_message('success', f"The file {key} in bucket {bucket} imported successfully.")
        return True
    except Exception as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            log_message('failed', f" The file {key} does not exist in bucket {bucket}.")
            raise FileNotFoundError(f"The file {key} does not exist in bucket {bucket}.")
        log_message('failed', f" Failed to access file {key} in bucket {bucket}: {e}")
        raise RuntimeError(f"Failed to access file {key} in bucket {bucket}: {e}")
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
# load the config json
def load_config(bucket, key):
    print(f"bucket {bucket}  file {key}")
    try:
        s3 = boto3.client("s3")
        s3_key = key
        obj = s3.get_object(Bucket=bucket, Key=s3_key)
        data = obj["Body"].read().decode("utf-8")
        return json.loads(data)
    except Exception as e:
        print(f"Glue job failed: {str(e)}")
        raise ValueError(f"Failed at config load stage: {str(e)}")

#load xml file to SparkContext
def load_xml(spark,input_path,row_tag):
    print("input path ",input_path,"\n spark ",spark)
    try:
        return (spark.read.format("com.databricks.spark.xml")
          .option("rowTag",row_tag)
          .option("excludeAttribute",False)
          .option("valueTag","_text")
          .load(input_path))
    except Exception as e:
        print(f"Glue job failed: {str(e)}")
        raise ValueError(f"Failed at XML load stage: {str(e)}")
def select_columns_safe(df,cols,fill_missing=True,blank=False):
    for c in cols:
        if c not in df.columns:
            df[c]=np.nan
    df= df[cols]
    if blank:
        df = df.fillna("")
    return df

def export_to_csv(full_df,df,config,bucket,report_creation_path,report_name,zip_file_name,output_path):
    try:
        delimiter = config["report_template"]["delimiter"]
        quotes=config["report_template"]["quotes"]
        product_from_filename = config["products"]["product_from_filename"]
        report_columns = config["report_template"]["columns"]
        email_delimiter = config["email_template"]["delimiter"]
        email_columns = config["email_template"]["columns"]
        
        base_file_pattern = config["products"]["base_file_pattern"]
        pattern = re.compile(base_file_pattern)
        #read rndate, corp and cyle from fileName
        match = pattern.search(fileName)
        if match:
            product_name = match.group(config["products"]["product_loc"])
            corp_value = match.group(config["products"]["corp_loc"])
            run_date = match.group(config["products"]["date_loc"])
            cycle_value = match.group(config["products"]["cycle_loc"])
            print(f" product : {product_name}   date :{run_date} corp : {corp_value}  cycle : {cycle_value}")

        if not product_from_filename:
            base_name_field = config["products"]["base_name_field"]
            no_of_characters = config["products"]["no_of_characters"]
            product_name = full_df.filter(col(base_name_field).isNotNull()).select(base_name_field).first()[0][:no_of_characters]

        file_name = report_name.format(product=product_name,MMDDYYYY=run_date,corp = corp_value , cycle = cycle_value)
        
        dfpdf = df.toPandas()
        float_cols = dfpdf.select_dtypes(include=["float"]).columns
        dfpdf[float_cols]= dfpdf[float_cols].astype('Int64')
        re_order_df = select_columns_safe(dfpdf,report_columns)
        email_report = f"s3://{bucket}/{report_creation_path.rstrip('/')}/email_report.csv"
        report_location = f"s3://{bucket}/{report_creation_path.rstrip('/')}/{file_name}"
        report_to_custlocation = f"s3://{bucket}/{output_path.rstrip('/')}/{file_name}"
        #Adding additional column with corp details for email csv file
        corp_record = {"Description":product_name,"Packages":run_date, "Sheets":corp_value,"Images":cycle_value}
        email_df = pd.concat([pd.DataFrame([corp_record]), dfpdf], ignore_index=True)
        email_df_selected = select_columns_safe(email_df,email_columns)
        email_df_selected.to_csv(email_report, sep=email_delimiter,index=False)
                          
        if quotes=="true":
            re_order_df.to_csv(report_location,sep=delimiter,quotechar='"',quoting=csv.QUOTE_ALL,index=False)
            re_order_df.to_csv(report_to_custlocation,sep=delimiter,quotechar='"',quoting=csv.QUOTE_ALL,index=False)
        else:
            re_order_df.to_csv(report_location,sep=delimiter,index=False)
            re_order_df.to_csv(report_to_custlocation,sep=delimiter,index=False)
        return report_location
    except Exception as e:
        print(f"Glue job failed: {str(e)}")
        raise ValueError(f"Failed at report creation: {str(e)}")

#Build conditions based on the operations in config file
def build_condition(cond):
    col_name = cond.get("tle_name")
    op = cond.get("op")
    value = cond.get("value")
    # Validate column name
    if col_name is None:
        raise ValueError(f"ERROR: Missing 'col' key in condition: {cond}")

    c = F.col(col_name)

    if op == "==":
        return c == value
    elif op == "!=":
        return c != value
    elif op == ">":
        return c > value
    elif op == "<":
        return c < value
    elif op == ">=":
        return c >= value
    elif op == "<=":
        return c <= value
    elif op == "contains":
        return c.contains(value)
    elif op == "isin":
        if not isinstance(value, list):
            raise ValueError(f"isin requires list, got {value}")
        return c.isin(value)
    elif op == "between":
        if not isinstance(value, (list, tuple)) or len(value) != 2:
            raise ValueError(f"between requires [min,max], got {value}")
        return c.between(value[0], value[1])
    elif op == "rlike":
        return c.rlike(value)
    else:
        raise ValueError(f"Unsupported operator '{op}' in condition: {cond}")

#Filters based on rules in config
def build_filters(df, metric_cfg):
    
    and_conds = metric_cfg.get("and", [])
    or_conds = metric_cfg.get("or", [])
    change_op = metric_cfg.get("change_op",False)

    and_expr = None
    or_expr = None
    # Build AND block: cond1 & cond2 & cond3 ...
    if and_conds:
        and_expr = reduce(
            operator.and_,
            [build_condition(c) for c in and_conds]
        )

    # Build OR block: cond1 | cond2 | cond3 ...
    if or_conds:
        or_expr = reduce(
            operator.or_,
            [build_condition(c) for c in or_conds]
        )

    # both exist → AND block AND OR block
    if and_expr is not None and or_expr is not None:
        if bool(change_op):
            return and_expr | or_expr
        else:
            return and_expr & or_expr

    if and_expr is not None:
        return and_expr

    if or_expr is not None:
        return or_expr

    return F.lit(True)

def compute_metric(df, metric_cfg):
    metric_type = metric_cfg.get("type")
    target_col = metric_cfg.get("source_field") or metric_cfg.get("source_field")

    if target_col is None:
        raise ValueError(f"Missing target_column in metric: {metric_cfg}")

    filt = build_filters(df, metric_cfg)
    filtered = df.filter(filt)
    
    if metric_type == "count":
        return filtered.agg(F.countDistinct(F.col(target_col)).alias(target_col)).first()[target_col]

    elif metric_type == "sum":
        sum_count = filtered.agg(F.sum(F.col(target_col).cast("int")).alias(target_col)).first()[target_col]
        return sum_count or 0
        
    else:
        raise ValueError(f"Unsupported metric type: {metric_type}")
 
def process_config(spark,df, config):
    rows = []
    for description, rule_cfg in config.items():

        long_desc = rule_cfg.get("Description", description)
        row = {
            "Description": long_desc
        }
        headings = list(config[description].keys())
        for header in headings[1:]:
            if header in rule_cfg:
                row[header] = compute_metric(df, rule_cfg[header])

        rows.append(row)

    return spark.createDataFrame(rows)
def repartition_size(df,row_count):
    if row_count <=10000:
        parts = 10
    elif row_count <= 50000:
        parts = 20 
    elif row_count <= 100000:
        parts = 40
    elif row_count <=300000:
        parts = 80
    elif row_count <=500000:
        parts = 100
    elif row_count <=1000000:
        parts = 150
    elif row_count <= 5000000:
        parts = 200
    else:
        parts = 300
    return df.repartition(parts)
    
def main():
    try:
        set_job_params_as_env_vars()
        global fileName
        fileName = os.getenv("fileName","") #JANUS.TAXES_YETAX.F4631.11182025.R.tst.rpt.zip
        object_key = os.getenv("objectKey") # zip file key JANUS.TAXES_YETAX.F4631.11182025.R.tst.rpt.zip
        bucket_name = os.getenv("bucket_name") #br-icsdev-dpmpusalas-dataingress-us-east-1-s3

        file_key = os.getenv("file_key") # jhi_taxes  JANUS.TAXES_YETAX.11052025.100705.dat.afp 1.xml
        file_pattern = os.getenv("file_pattern") #((JANUS)\\.(TAXES_(YETAX|5498ESA|5498))\\.([A-Za-z0-9]+)\\.(\\d{8})\\.([A-Za-z0-9]+)\\.(afpds)\\.(xml))$
        file_path = os.getenv("file_path") #jhi/taxes/wip/47252106-8c0c-4f7d-b884-4b8f4e47e816/unzip/
        
        config_path = os.getenv("config_path") #jhi/taxes/config/stmts_reports_config.json
        report_file_name = os.getenv("report_file_name") #"JHI.tax.yend.summary.report.txt
        report_creation_path = os.getenv("report_creation_path") #jhi/taxes/wip/47252106-8c0c-4f7d-b884-4b8f4e47e816/reports/
        output_path = os.getenv("output_path") #jhi/taxes/output/to_cust/

        # load the config json
        config_full_path=f"s3://{bucket_name}/{config_path}"
        xml_path = getFileNames(bucket_name,file_path,file_pattern)

        conf = (SparkConf()
        .set("spark.sql.shuffle.partitions","200")
        .set("spark.default.parallelism","200")
        .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
        .set("spark.sql.files.maxPartitionBytes","134217728")
        .set("spark.sql.adaptive.shuffle.targetPostShuffleInputSize","134217728")
        .set("spark.sql.adaptive.enabled", "true").set("spark.sql.adaptive.coalescePartitions.enabled", "true").set("spark.sql.adaptive.skewJoin.enabled", "true"))
        sc = SparkContext(conf=conf)
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
        ### load config and xml with spark
        config = load_config(bucket_name,config_path)
        full_xml_path = f"s3://{bucket_name}/{xml_path}"
        df = load_xml(spark,full_xml_path,"Statement")
        df = df.withColumn(
                "filteredProps",
                F.filter("Properties.Property",
                    lambda p: (p["_Type"] == "TLE") & p["_Name"].isNotNull() & (F.length(F.trim(p["_Name"])) > 0)
                )
            )
        df_with_map = df.withColumn("propMap",
                F.map_from_arrays(F.transform("filteredProps", lambda x: x["_Name"]),F.transform("filteredProps", lambda x: x["_Value"])
                )
            )
        # key = output column name, value = _Name to extract
        tle_properties = config["tle_properties"]
        cols = [F.col("propMap")[v].alias(k) for k, v in tle_properties.items()]
        
        #  Select these columns with root attributes
        df_result = df_with_map.select(
            *cols,
            F.col("_Images").alias("_Images"),
            F.col("_Seq").alias("_Seq")
        )
        
        #re partition based on the count size of data DataFrame
        df_count = df_result.count()
        df_result = repartition_size(df_result,df_count)
        if df_count <= 150000:
            df_result = df_result.persist(StorageLevel.MEMORY_ONLY)
        else:
            df_result = df_result.persist(StorageLevel.MEMORY_AND_DISK)
        
        final_df = process_config(spark,df_result, config["aggregation_columns"])

        export_to_csv(df_result,final_df,config,bucket_name,report_creation_path,report_file_name,fileName,output_path)
        
        return {
            'statusCode': 200,
            'body': {
                'message': 'XML processing completed successfully',
                'output_base_path': report_creation_path,
                'report_creation_path': report_file_name,
            }
        }
        
    except Exception as e:
        log_message("failed","Glue Job Failed")
        print(f"Glue job failed: {str(e)}")
        raise ValueError(f"Glue job failed: {str(e)}")


if __name__ == "__main__":
    log_message('success', "Statements Reports Started.")
    main()