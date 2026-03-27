import sys
import json
import boto3
import os
import splunk
import xml.etree.ElementTree as ET
import re
from collections import defaultdict
from awsglue.utils import getResolvedOptions
import csv
import io
from datetime import datetime

def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:ebcdic_validation:" + account_id
    
def log_message(status, message):
    splunk.log_message({'FileName':fileName,'Status': status, 'Message': message}, get_run_id())
    
class XMLProcessor:
    def __init__(self, config, bucket_name=None):
        self.config = config
        self.s3_client = boto3.client('s3')
        if not bucket_name:
            raise ValueError("bucket_name is required - no default bucket allowed")
        self.bucket_name = bucket_name
        
    def parse_s3_path(self, s3_path):
        """Parse S3 path into bucket and key"""
        if s3_path.startswith('s3://'):
            path_parts = s3_path[5:].split('/', 1)
            bucket = path_parts[0]
            key = path_parts[1] if len(path_parts) > 1 else ''
            return bucket, key
        else:
            # If no s3:// prefix, assume it's a relative path and use default bucket
            return self.bucket_name, s3_path
    
    def read_s3_file(self, s3_path):
        """Read file content from S3"""
        try:
            bucket, key = self.parse_s3_path(s3_path)
            print(f"Reading from S3: bucket={bucket}, key={key}")
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            content = response['Body'].read().decode('utf-8')
            return content
        except Exception as e:
            print(f"Error reading S3 file {s3_path}: {str(e)}")
            log_message("failed", f"Error reading S3 file {s3_path}: {str(e)}")
            raise ValueError(f"Error reading S3 file {s3_path}: {str(e)}")
            
    def getFileNames(self,bucket_name, path, pattern):
        log_message("logging", f"starting in getFileNames bucket_name: {bucket_name} | path: {path} |pattern: {pattern}")
        regex = re.compile(pattern)
        try:
            def filter_keys_by_pattern(objects, pattern):
                return [obj['Key'] for obj in objects.get('Contents', []) if pattern.search(obj['Key'].split('/')[-1])]
    
            objects = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=path)
            files = filter_keys_by_pattern(objects, regex)
            log_message('logging', f'files list : {files}')
            if not files:
                error_message = f'No schema files found matching the pattern: {pattern}'
                log_message('failed', error_message)
                raise ValueError(error_message)
            log_message("logging", f"files in getFileNames files: {files}")
            return files[0]
        except Exception as e:
            log_message('failed', f" failed to get fileNames:{str(e)}")
            raise e
    
    def read_xml_file(self, object_key):
        try:
            """Read XML content from S3 based on configuration with regex pattern support"""
            if object_key:
                file_path = f"s3://{self.bucket_name}/{object_key}"
            else:
                raise ValueError(f"{object_key} not found")
            
            # Read from S3
            xml_content = self.read_s3_file(file_path)
            if xml_content:
                return xml_content
            else:
                raise ValueError("Failed To Read XML")
        except Exception as e:
            log_message("failed", f"failed in read_xml_file")
            raise ValueError(str(e))
            
    def read_csv_file(self, object_key):
        try:
            """Read XML content from S3 based on configuration with regex pattern support"""
            if object_key:
                file_path = f"s3://{self.bucket_name}/{object_key}"
            else:
                raise ValueError(f"{object_key} not found")
            
            # Read from S3
            csv_content = self.read_s3_file(file_path)
            if csv_content:
                return csv_content
            else:
                raise ValueError("Failed To Read XML")
        except Exception as e:
            log_message("failed", f"failed in read_xml_file")
            raise ValueError(str(e))
            
    def get_file_fileds(self,filename,field_defs):
        # getting file fields
        pattern = r"\.".join([rf"(?P<{k}>{v})" for k, v in field_defs.items()])
        match = re.search(pattern, filename)
        if match:
            return {key: datetime.strptime(value, "%m%d%Y").strftime("%Y%m%d") if key.strip() == "UC_RUNDATE" else value for key, value in match.groupdict().items()}
        else:
            return {}
    def prep_prod_report_data(self,template,statement_data,file_name,field_defs,email = False):
        try:
            delimiter = template.get("delimiter","")
            columns = template.get("sections","")[0].get("columns")
            get_file_defs = self.get_file_fileds(file_name,field_defs)
            headers_col = [f'''{col["columnName"]:<{len(col["columnName"])+4 if len(col["columnName"]) > col["columnWidth"] else col["columnWidth"]}}''' for col in columns]
            headers = delimiter.join(headers_col)
            email_row_data = []
            rows_data = []
            head_diff = delimiter + "".join([('-'*(len(hcol))) + "+" for hcol in headers_col])
            first_row = '+'+"".join([('-'*(len(hcol))) + "+" for hcol in headers_col])
            rows_data.append(first_row)
            rows_data.append(delimiter+headers+delimiter)
            rows_data.append(head_diff)
            for data in statement_data:
                row_values = []
                for col in columns:
                    column_data = col.get("columnData", "").strip()
                    column_field = col.get("columnField","")
                    column_width = col.get("columnWidth", "")
                    column_name = col.get("columnName", "")
                    column_count_field = col.get("columnCountField", "")
                    column_expected_values = col.get("columnExpectedValues", "")
                    column_if_put_value = col.get("columnIfPutValue", "")
                    column_else_put_value = col.get("columnElsePutValue", "")
                    
                    if len(column_name) > column_width:
                        column_width = len(column_name) + 4
                        
                    if column_data in ["UC_RUNDATE","UC_CORP","UC_CYCLE"]:
                        value = get_file_defs.get(column_data)
                    elif column_data in ["UC_DOCUMENT_DELIVERY_PREFERENCE","UC_DOCUMENT_CLASSIFICATION_01"]:
                        value = data.get(column_data)[0]
                        if value in column_expected_values:
                            value = column_if_put_value
                        else:
                            value = column_else_put_value
                    elif column_data == "DOC_SHEET_COUNT":
                        field_data = data.get(column_data)
                        value = sum([int(i) for i in field_data])
                    elif column_data == "PAPER1_COUNT" or column_data == "PAPER2_COUNT":
                        doc_sheet_count = data.get(column_count_field)
                        sub_types = data.get(column_field)
                        zip_data = zip(sub_types,doc_sheet_count)
                        value = 0
                        for zdata in zip_data:
                            if zdata and zdata[0] in column_expected_values:
                                value += int(zdata[1])
                    elif column_data == "TOTAL_FORMS":
                        value = len(data.get(column_field))
                    elif column_data in ["COUNT_1099DIV","COUNT_1099B","COUNT_1099R","COUNT_5498","COUNT_5498ESA"] :
                        sub_type = data.get(column_field)
                        value = sub_type.count(column_expected_values)
                    else:
                        field_data = data.get(column_data)
                        if field_data:
                            value = field_data[0]
                        else:
                            value = ""
                    row_values.append(f'''{str(value):<{column_width}}''')
                if "EMAIL" in data.get("UC_DOCUMENT_DELIVERY_PREFERENCE") and email:
                    email_row_data.append(delimiter + delimiter.join(row_values) + delimiter)
                else:
                    rows_data.append(delimiter + delimiter.join(row_values) + delimiter)
            if email:
                return email_row_data
            rows_data.append(first_row)
            return "\n".join(rows_data)
        except Exception as e:
            log_message("failed",f"getting error in prep_prod_report_data : {e}")
            raise ValueError(str(e))
    
    def csv_prep_prod_report_data(self,template,csv_transformed_data,file_name,field_defs,xml_template,stmt_tle_data,calculations):
        try:
            delimiter = template.get("delimiter","")
            columns = template.get("sections","")[0].get("columns")
            get_file_defs = self.get_file_fileds(file_name,field_defs)
            headers_col = [f'''{col["columnName"]:<{len(col["columnName"])+4 if len(col["columnName"]) > col["columnWidth"] else col["columnWidth"]}}''' for col in columns]
            headers = delimiter.join(headers_col)
            # print(headers)
            rows_data = []
            head_diff = delimiter + "".join([('-'*(len(hcol))) + "+" for hcol in headers_col])
            first_row = '+'+"".join([('-'*(len(hcol))) + "+" for hcol in headers_col])
            rows_data.append(first_row)
            rows_data.append(delimiter+headers+delimiter)
            rows_data.append(head_diff)
            for data in csv_transformed_data.values():
                row_values = []
                for col in columns:
                    column_data = col.get("columnData", "").strip()
                    column_field = col.get("columnField","")
                    column_width = col.get("columnWidth", "")
                    column_name = col.get("columnName", "")
                    column_count_field = col.get("columnCountField", "")
                    column_expected_values = col.get("columnExpectedValues", "")
                    column_if_put_value = col.get("columnIfPutValue", "")
                    column_else_put_value = col.get("columnElsePutValue", "")
                    column_count_index = col.get("columnCountIndex", 0)
                    column_target_index = col.get("columnTargetIndex", 0)
                    
                    if len(column_name) > column_width:
                        column_width = len(column_name) + 4
                        
                    if column_data in ["UC_RUNDATE","UC_CORP","UC_CYCLE"]:
                        value = get_file_defs.get(column_data)
                    elif column_data in ["UC_DOCUMENT_DELIVERY_PREFERENCE","UC_DOCUMENT_CLASSIFICATION_01"]:
                        value = data[0][column_count_index]
                        if value in column_expected_values:
                            value = column_if_put_value
                        else:
                            value = column_else_put_value
                    elif column_data == "DOC_SHEET_COUNT":
                        value = sum([int(i[column_count_index]) for i in data])
                    elif column_data == "PAPER1_COUNT" or column_data == "PAPER2_COUNT":
                        value = 0
                        for hdata in data:
                            doc_sheet_count = hdata[column_count_index]
                            sub_types = hdata[column_target_index]
                            if sub_types and sub_types in column_expected_values:
                                value += int(doc_sheet_count)
                    elif column_data == "TOTAL_FORMS":
                        value = len(data)
                    elif column_data in ["COUNT_1099DIV","COUNT_1099B","COUNT_1099R","COUNT_5498","COUNT_5498ESA"] :
                        sub_type = [i[column_count_index] for i in data]
                        value = sub_type.count(column_expected_values)
                    else:
                        field_data = data[0][column_count_index]
                        value = field_data if field_data else ""
                    row_values.append(f'''{str(value):<{column_width}}''')
                rows_data.append(delimiter + delimiter.join(row_values) + delimiter)
            xml_row_data = self.prep_prod_report_data(xml_template,stmt_tle_data,file_name,field_defs,True)
            rows_data = rows_data + xml_row_data
            rows_data.append(first_row)
            return "\n".join(rows_data)
        except Exception as e:
            log_message("failed",f"getting error in csv_prep_prod_report_data : {e}")
            raise ValueError(str(e))
    def extract_tle_properties_from_xml(self, xml_content):
        """Extract TLE properties from XML content with multiple statements"""
        try:
            root = ET.fromstring(xml_content)
            
            # Aggregate TLE data from all statements
            
            statement_tle_data = []
            statement_count = 0
            total_images = 0
            total_pages = 0
            total_records = 0
            total_documents = 0
            
            # Process all statements
            for statement in root.findall('.//Statement'):
                # print(statement,"statementstatement")
                aggregated_tle_data = defaultdict(list)
                statement_count += 1
                
                # Get statement-level attributes
                images = int(statement.get('Images', '0'))
                records = int(statement.get('Records', '0'))
                total_images += images
                # total_records += records
                
                # Count pages in this statement
                pages = len(statement.findall('.//Page'))
                total_pages += pages
                statement_properties = statement.findall('.//Properties')
                statement_tle_property = [properties for properties in statement_properties if properties.findall('.//Property[@Type="TLE"]')]
                # total_records += len(statement_tle_property) 
                # if len(statement_tle_property) > 1:
                #     total_documents += 1
                prop_count = 0
                for Property in statement_properties:
                    for prop in Property.findall('.//Property'):
                        name = prop.get('Name')
                        if name == "DOC_ACCOUNT_NUMBER":
                            total_records += 1
                            prop_count += 1
                        value = prop.get('Value')
                        if "UC_CUSTOMER_EMAIL_ADDRESS" in value:
                            aggregated_tle_data["UC_CUSTOMER_EMAIL_ADDRESS"] = [value.rsplit("UC_CUSTOMER_EMAIL_ADDRESS")[-1].strip()]
                        # NOP type 
                        if name and value:
                            aggregated_tle_data[name].append(value)
                if prop_count > 1:
                    total_documents += 1
                statement_tle_data.append(aggregated_tle_data)
            production_report_template = self.config.get("production_report_template")
            calculated_totals = {
                'Total_Records': total_records,
                'Total_Accounts' : statement_count,
                'Total_Documents': total_documents
            }
            
            print(f"Extracted data: {statement_count} statements, {total_images} images, {total_pages} pages")
            
            # Return empty dicts for unused data structures
            return statement_tle_data,calculated_totals
            
        except ET.ParseError as e:
            log_message("failed",f"Error parsing XML: {e}")
            print(f"Error parsing XML: {e}")
            return {}, {}, {}
    
    def extract_statement_level_counts(self,mapping, xml_content):
        try:
            root = ET.fromstring(xml_content)
            calc_dict = {}
            for statement in root.findall('.//Statement'):
                statement_properties = statement.findall('.//Properties')
                tle_property = mapping.get("tle_property",{})
                if tle_property:
                    first_tle_property = [properties for properties in statement_properties if properties.findall('.//Property[@Type="TLE"]')][0]
                    for prop in first_tle_property:
                        for count, source_data in tle_property.items():
                            if prop.get('Name') == source_data.get("source_field"):
                                if prop.get('Value') == source_data.get("expected_source_filed_value"):
                                    if count in calc_dict:
                                        calc_dict[count] += 1
                                    else:
                                        calc_dict[count] = 1
            return {
                "Total_Foreign" : calc_dict.get("total_foreign_count",0),
                "Total_Print" : calc_dict.get("total_print_count",0),
                "Total_edelivery" : calc_dict.get("total_email_count",0),
                "Total_StopMail" : calc_dict.get("total_stop_mail_count",0),
                "Total_Mail_Packages" : calc_dict.get("total_print_count",0)
            }
        except Exception as e:
            log_message("failed",f"getting error in extract_statement_level_counts: {str(e)}")
            raise str(e)
    
    def extract_property_level_counts(self,xml_content):
        try:
            root = ET.fromstring(xml_content)
            # Aggregate TLE data from all statements
            # Process all statements
            total_ome1 = 0
            total_ome2 = 0
            total_ome3 = 0
            total_white = 0
            total_perf = 0
            total_pages = 0
            total_images = 0
            for statement in root.findall('.//Statement'):
                statement_properties = statement.findall('.//Properties')
                tle_property = [properties for properties in statement_properties if properties.findall('.//Property[@Type="TLE"]')]
                doc_sheet_count = 0
                for properties in tle_property:
                    prop_dict = {i.get('Name'):i.get('Value') for i in properties}
                    for key, value in prop_dict.items():
                        if key == 'DOC_SHEET_COUNT':
                            doc_sheet_count += int(value)
                            total_pages += int(value)
                        if key == 'DOC_IMAGE_COUNT':
                            total_images += int(value)
                        if key == 'UC_DOCUMENT_SUB_TYPE':
                            if value in ["1099-Q","1099-B","1099-DIV"]:
                                total_white += int(prop_dict.get("DOC_SHEET_COUNT"))
                            elif value == "1099-R":
                                total_perf += int(prop_dict.get("DOC_SHEET_COUNT"))
                if 1 <= doc_sheet_count <= 7:
                    total_ome1 += 1
                if 8 <= doc_sheet_count <= 13:
                    total_ome2 += 1
                if 14 <= doc_sheet_count <= 60:
                    total_ome3 += 1
                        
            return {
                "Total_OME1" : total_ome1,
                "Total_OME2" : total_ome2,
                "Total_OME3" : total_ome3,
                "Total_White" : total_white,
                "Total_Perf" : total_perf,
                "Total_Pages" : total_pages
            }
        except Exception as e:
            log_message("failed",f"getting error in extract_property_level_counts: {str(e)}")
            raise str(e)
    def calculate_delivery_metrics_from_xml(self, xml_content, calculated_totals):
        """Calculate delivery metrics by analyzing individual statements from XML"""
        try:
            root = ET.fromstring(xml_content)
            
            # Initialize counters
            total_unique_accounts = []
            doc_sheet_count = 0
            # Get classification values from consolidated configuration
            classifications = self.config.get('document_classification', {}).get('classifications', {})
            
            # Process each statement individually
            
            for statement in root.findall('.//Statement'):
                # Get delivery preference and classification for this specific statement
                delivery_pref = None
                classification_code = None
                doc_sheets = 0
                doc_images = int(statement.get('Images', '0'))
                for properties in statement.findall('.//Properties'):
                    for prop in properties.findall('.//Property[@Type="TLE"]'):
                        prop_name = prop.get('Name')
                        if prop_name == 'DOC_ACCOUNT_NUMBER':
                            doc_acc_number = prop.get('Value')
                            if doc_acc_number not in total_unique_accounts:
                                total_unique_accounts.append(doc_acc_number)
                # break
                
                # Count pages from XML if not in TLE properties
                if doc_sheets == 0:
                    doc_sheets = len(statement.findall('.//Page'))
            
            # Update calculations dictionary with aggregated values
            calculations = {
                # "Total_Records":100,
                # 'Total_Records':doc_sheets,
                'Total_Accounts': len(total_unique_accounts),
                'Total_Pages': doc_sheet_count,
                # 'Total_White':total_white,
                # 'Total_Perf': total_perf,
            }
            calculations.update(calculated_totals)
            
            return calculations
            
        except Exception as e:
            print(f"Error calculating delivery metrics from XML: {e}")
            return {}
    
    def process_xml_file(self, object_key):
        """Main processing method - orchestrates the entire XML processing workflow"""
        try:
            print(f"Starting XML processing for: {object_key} ")
            log_message("logging", f"Starting XML processing for: {object_key}")
            # Step 1: Read XML file from S3
            xml_content = self.read_xml_file(object_key)
            
            # Step 2: Extract TLE properties and calculated totals from XML
            statement_tle_data, calculated_totals = self.extract_tle_properties_from_xml(xml_content)
            
            statement_level_mapping = self.config.get("statement_level_mapping")
            statement_level_count = self.extract_statement_level_counts(statement_level_mapping,xml_content)
            print(statement_level_count,"statement_level_countstatement_level_count")
            
            extract_ome_counts = self.extract_property_level_counts(xml_content)
            print(extract_ome_counts,"extract_ome_countsextract_ome_countsextract_ome_counts")
            
            # Step 4: Calculate delivery metrics from individual XML statements
            calculations = self.calculate_delivery_metrics_from_xml(xml_content, calculated_totals)
            
            calculations.update(statement_level_count)
            calculations.update(extract_ome_counts)
            # Step 5: Set suppression count from txt file
            
            
            # Count No Mail and Stop Mail documents from XML statements (not from suppression)
            no_mail_count = 0
            stop_mail_count = 0
            no_1099R = 0
            yes_1099R = 0
            total_1099DIV = 0
            total_1099B = 0
            total_1099Q = 0
            total_1099R = 0
            total_5498 = 0
            total_5498esa = 0  
            
            # Parse XML to count special classifications
            root = ET.fromstring(xml_content)
            classifications = self.config.get('document_classification', {}).get('classifications', {})
            no_mail_values = classifications.get('no_mail', [])
            stop_mail_values = classifications.get('stop_mail', [])
            no_1099R_values = classifications.get('not_1099R', [])
            yes_1099R_values = classifications.get('1099R', [])
            values_1099R = classifications.get('1099R', [])
            values_1099DIV = classifications.get('1099DIV', [])
            values_1099Q = classifications.get('1099Q', [])
            values_1099B = classifications.get('1099B', [])
            values_5498 = classifications.get('5498', [])
            values_5498esa = classifications.get('5498esa', [])
            
            for statement in root.findall('.//Statement'):
                for properties in statement.findall('.//Properties'):
                    classification_code = None
                    for prop in properties.findall('.//Property[@Type="TLE"]'):
                        if prop.get('Name') == 'UC_DOCUMENT_SUB_TYPE':
                            classification_code = prop.get('Value')
                            break
                    if classification_code:
                        if str(classification_code).upper().strip() in [v.upper().strip() for v in values_1099R]:
                            total_1099R += 1
                        elif str(classification_code).upper().strip() in [v.upper().strip() for v in values_1099DIV]:
                            total_1099DIV += 1
                        elif str(classification_code).upper().strip() in [v.upper().strip() for v in values_1099Q]:
                            total_1099Q += 1
                        elif str(classification_code).upper().strip() in [v.upper().strip() for v in values_1099B]:
                            total_1099B += 1
                        elif str(classification_code).upper().strip() in [v.upper().strip() for v in values_5498]:
                            total_5498 += 1
                        elif str(classification_code).upper().strip() in [v.upper().strip() for v in values_5498esa]:
                            total_5498esa += 1

            calculations.update({
                'Total_1099R' : total_1099R,
                'Total_1099DIV' : total_1099DIV,
                'Total_1099Q' : total_1099Q,
                'Total_1099B' : total_1099B,
                'Total_5498' : total_5498,
                'Total_5498esa' : total_5498esa
            })
        except (ValueError, TypeError, ZeroDivisionError) as e:
            # print(f"Error in calculations: {e}") 
            log_message("failed",f"Error in calculations: {e}")
            raise ValueError(f"Error in calculations: {e}")
          
        return statement_tle_data, calculations
        
    def is_int(self,value):
        try:
            int(value)
            return True
        except (ValueError, TypeError):
            return False
            
    def process_csv_file(self,object_key):
        try:
            csv_data = defaultdict(list)
            print(f"Starting XML processing for: {object_key} ")
            log_message("logging", f"Starting XML processing for: {object_key}")
            # Step 1: Read XML file from S3
            csv_content = self.read_csv_file(object_key)
            content_as_file = io.StringIO(csv_content)
            rows = csv.reader(content_as_file)
            prev = ""
            for row in rows:
                if row and row[0].strip() == "Trailer":
                    continue
                if len(row) >=1 and self.is_int(row[1]):
                    csv_data.update({row[2]:[row]})
                    prev = row[2]
                elif len(row) >=1 and row[1].strip() == "":
                    csv_data[prev].append(row)
            return csv_data
        except (ValueError, TypeError, ZeroDivisionError) as e:
            # print(f"Error in calculations: {e}") 
            log_message("failed",f"Error in processing CSV File: {e}")
            raise ValueError(f"Error in processing CSV File: {e}")
            
    def summary_report_calculations_csv(self, csv_transformed_data):
        try:
            final_counts = {
                    "Total_Records": 0,
                    "Total_Accounts": 0,
                    "Total_Documents": 0,
                    "Total_Pages" : 0,
                    "Total_White" : 0,
                    "Total_Perf" : 0,
                    "Total_OME1" : 0,
                    "Total_OME2" : 0,
                    "Total_OME3" : 0,
                }
            csv_fields_index_details = self.config.get("csv_fields_index_details")
            for account , details in csv_transformed_data.items():
                doc_sheet_count = 0
                for data in details:
                    final_counts["Total_Records"] += 1
                    for key,value in csv_fields_index_details.items():
                        if key in final_counts:
                            if "check_index" in value and "allowed_values" in value:
                                if data[value.get("check_index")] in value["allowed_values"]:
                                    final_counts[key] += int(data[value.get("count_index")])
                            elif "equal_value" in value:
                                if data[value.get("count_index")] == value["equal_value"]:
                                    final_counts[key] += 1
                            elif "count_index" in value:
                                final_counts[key] += int(data[value.get("count_index")])
                                doc_sheet_count += int(data[value.get("count_index")])
                        else:
                            if "check_index" in value and "allowed_values" in value:
                                if data[value.get("check_index")] in value["allowed_values"]:
                                    get_value = int(data[value.get("count_index")])
                                    if get_value:
                                        final_counts[key] = int(data[value.get("count_index")])
                                    else:
                                        final_counts[key] = 0
                                else:
                                    final_counts[key] = 0
                            elif "equal_value" in value:
                                if data[value.get("count_index")] == value["equal_value"]:
                                    final_counts[key] = 1
                                else:
                                    final_counts[key] = 0
                            elif "count_index" in value:
                                get_value = int(data[value.get("count_index")])
                                if get_value:
                                    final_counts[key] = get_value
                                    doc_sheet_count = get_value
                                else:
                                    final_counts[key] = 0
                            else:
                                final_counts[key] = 0
                if 1 <=  doc_sheet_count <= 7:
                    if "Total_OME1" in final_counts:
                        final_counts["Total_OME1"] += 1
                if 8 <= doc_sheet_count <= 13:
                    if "Total_OME2" in final_counts:
                        final_counts["Total_OME2"] += 1
                if 14 <= doc_sheet_count <= 60:
                    if "Total_OME3" in final_counts:
                        final_counts["Total_OME3"] += 1
                if len(details) > 1:
                    final_counts["Total_Documents"] += 1
                final_counts["Total_Accounts"] += 1
            return final_counts
        except Exception as e:
            log_message("failed", f"Error in CSV summary calculations : {str(e)}")
            raise ValueError(f"Error in CSV summary calculations : {str(e)}")
        # return statement_tle_data, calculations
    def xml_email_summary_report_calculatons(self,xml_rows_data):
        try:
            final_counts = {
                    "Total_Records": 0,"Total_Accounts": 0,"Total_Documents": 0,"Total_Pages":0,"Total_White":0,
                    "Total_Perf":0,"Total_OME1" : 0,"Total_OME2" : 0,"Total_OME3" : 0,"Total_Mail_Packages" : 0,
                    'Total_1099R' : 0,'Total_1099DIV' : 0,'Total_1099Q' : 0,'Total_1099B' : 0,'Total_5498' : 0,
                    'Total_5498esa' : 0,"Total_Foreign" : 0,"Total_Print" : 0,"Total_edelivery" : 0,"Total_StopMail" : 0
                }
            for row in xml_rows_data:
                final_counts["Total_Records"] += 1
                final_counts["Total_Accounts"] += 1
                doc_sheet_count = row.get("DOC_SHEET_COUNT")[0]
                doc_sub_type = row.get("UC_DOCUMENT_SUB_TYPE")[0] if row.get("UC_DOCUMENT_SUB_TYPE") else ""
                del_pref = row.get("UC_DOCUMENT_DELIVERY_PREFERENCE")[0] if row.get("UC_DOCUMENT_DELIVERY_PREFERENCE") else ""
                email_type = row.get("UC_MAIL_TYPE")[0] if row.get("UC_MAIL_TYPE") else ""
                stop_mail_flag = row.get("CUSTOM_DOC_STOPMAIL_FLAG")[0] if row.get("CUSTOM_DOC_STOPMAIL_FLAG") else ""
                if doc_sheet_count:
                    final_counts["Total_Pages"] += int(doc_sheet_count)
                    if doc_sub_type in ["1099-Q","1099-B","1099-DIV"]:
                        final_counts["Total_White"] += 1
                    elif doc_sub_type == "1099-R":
                        final_counts["Total_Perf"] += 1
                if 1 <=  int(doc_sheet_count) <= 7:
                    if "Total_OME1" in final_counts:
                        final_counts["Total_OME1"] += 1
                if 8 <= int(doc_sheet_count) <= 13:
                    if "Total_OME2" in final_counts:
                        final_counts["Total_OME2"] += 1
                if 14 <= int(doc_sheet_count) <= 60:
                    if "Total_OME3" in final_counts:
                        final_counts["Total_OME3"] += 1
                if "PRINT" in del_pref:
                    final_counts["Total_Mail_Packages"] += 1
                    final_counts["Total_Print"] += 1
                elif "EMAIL" in del_pref:
                    final_counts["Total_edelivery"] += 1
                if doc_sub_type == "1099-R":
                    final_counts["Total_1099R"] += 1
                elif doc_sub_type == "1099-DIV":
                    final_counts["Total_1099DIV"] += 1
                elif doc_sub_type == "1099-Q":
                    final_counts["Total_1099Q"] += 1
                elif doc_sub_type == "1099-B":
                    final_counts["Total_1099B"] += 1
                elif doc_sub_type == "5498":
                    final_counts["Total_5498"] += 1
                elif doc_sub_type == "5498ESA":
                    final_counts["Total_5498esa"] += 1
                if email_type == "F":
                    final_counts["Total_Foreign"] += 1
                if stop_mail_flag == "Y":
                    final_counts["Total_StopMail"] += 1
            return final_counts
        except Exception as e:
            log_message("failed", f"Error in XML Email summary calculations : {str(e)}")
            raise ValueError(f"Error in XML Email calculations : {str(e)}")
        
    def generate_formatted_report_from_template(self,template, calculations,file_name="",field_defs={}):
        """Generate formatted report using the report template configuration"""
        try:
            # template = self.config.get('summary_report_template', {})
            delimiter = template.get("delimiter","|")
            sections = template.get('sections', [])
            if "1042" in file_name or "5498" in file_name:
                calculations.update({"Total_Documents":calculations.get("Total_Records")})
            report_lines = []
            if file_name and field_defs:
                get_file_defs = self.get_file_fileds(file_name,field_defs)
                report_lines.extend(
                        [
                            f"Corp: {get_file_defs.get('UC_CORP')}",
                            f"Cycle: {get_file_defs.get('UC_CYCLE')}",
                            f"Date: {get_file_defs.get('UC_RUNDATE')}\n",
                        ]
                    )
            # Process each section
            for section in sections:
                section_rows = section.get('rows', [])
                for row in section_rows:
                    description = row.get('description', '')
                    packages_key = row.get('packages', '')
                    
                    # Get values from calculations
                    packages_val = calculations.get(packages_key, '') if packages_key else ''
                    print(f"{description}{delimiter}{packages_val}")
                    row_line = f"{description:<30}{delimiter:^3}{packages_val:>5}{delimiter:^3}"
                    report_lines.append(row_line)
            
            return '\n'.join(report_lines)
            
        except Exception as e:
            print(f"Error generating formatted report from template: {e}")
            return ""

    def save_report_to_s3(self, report_content, bucket, path, file_name):
        """Save report content to S3"""
        try:
            if not path.endswith("/"):
                path += "/"
            key = path + file_name
            # Upload the report content to S3
            self.s3_client.put_object(Bucket=bucket, Key=key, Body=report_content)
            print(f"Report saved successfully to s3://{bucket}/{key}")
        except Exception as e:
            print(f"Error saving report to S3: {e}")
    
def set_job_params_as_env_vars():
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]
            value = sys.argv[i + 1]
            os.environ[key] = value

def main():
    """Main Glue job function"""
    try:
        set_job_params_as_env_vars()
        global fileName
        fileName = os.getenv("fileName")
        object_key = os.getenv("objectKey")
        bucket_name = os.getenv("bucket_name")
        # xml_file_key  = os.getenv("xml_file_key")
        config_path = os.getenv("config_path")
        output_path = os.getenv("output_path")
        summary_report_file_name = os.getenv("summary_report_file_name","")
        production_report_file_name = os.getenv("production_report_file_name","")
        recon_report_file_name = os.getenv("recon_report_file_name","")
        report_creation_path = os.getenv("report_creation_path","")
        file_fields_order = json.loads(os.getenv("file_fields_order",{}))
        file_path = os.getenv("file_path")
        file_pattern = os.getenv("file_pattern")
        csv_file_pattern = os.getenv("csv_file_pattern")
        file_type = os.getenv("file_type")
        
        # Initialize processor with bucket name
        processor = XMLProcessor({}, bucket_name)
        file_path = file_path if file_path.endswith("/") else file_path + "/"
        print(f"bucket name : {bucket_name} | file_path : {file_path} | file_pattern : {file_pattern}")
        object_key = processor.getFileNames(bucket_name,file_path,file_pattern)
        file_name = object_key.split("/")[-1]

        print(f"""{output_path} | {report_creation_path} | {summary_report_file_name} | {production_report_file_name} | {production_report_file_name}""")
        # Read configuration from S3 using processor's method
        print(f"Reading config from S3: {config_path}")
        config_content = processor.read_s3_file(config_path)
        config = json.loads(config_content)
        
        # Update processor with loaded config
        processor.config = config
        
        summary_report_template = config.get("summary_report_template")
        production_report_template = config.get("production_report_template")
        csv_production_report_template = config.get("csv_production_report_template")
        
        if file_type != "False" and file_type in ["YETAX","DUPCTAX"]:
            if csv_file_pattern and csv_file_pattern != "False":
                csv_object_key = processor.getFileNames(bucket_name,file_path,csv_file_pattern)
                csv_file_name = object_key.split("/")[-1]
                
                statement_tle_data,calculations = processor.process_xml_file(object_key)
                # filter email rows
                xml_row_data = [row for row in statement_tle_data if "UC_DOCUMENT_DELIVERY_PREFERENCE" in row and "EMAIL" in row.get("UC_DOCUMENT_DELIVERY_PREFERENCE")]
                # calculations from xml email records
                xml_calcuations = processor.xml_email_summary_report_calculatons(xml_row_data)
                csv_transformed_data = processor.process_csv_file(csv_object_key)
                csv_summary_report_calculations = processor.summary_report_calculations_csv(csv_transformed_data)
                # final calculations from xml and csv file for summary reports
                final_calculations = {k: xml_calcuations[k] + csv_summary_report_calculations[k] for k in xml_calcuations.keys()}
                csv_summary_report_content = processor.generate_formatted_report_from_template(summary_report_template,final_calculations,csv_file_name)
                
                if summary_report_file_name and summary_report_file_name != "False":
                    if output_path:
                        processor.save_report_to_s3(csv_summary_report_content,bucket_name,output_path, summary_report_file_name)
                    if report_creation_path:
                        processor.save_report_to_s3(csv_summary_report_content, bucket_name,report_creation_path,summary_report_file_name)
                
                
                # generate production report with template 
                # email_prod_report_content = processor.prep_prod_report_data(production_report_template,statement_tle_data,file_name,file_fields_order,True)
                
                csv_production_report_content = processor.csv_prep_prod_report_data(
                                csv_production_report_template,
                                csv_transformed_data,
                                csv_file_name,
                                file_fields_order,
                                production_report_template,
                                statement_tle_data,
                                calculations
                            )
                
                if production_report_file_name and production_report_file_name != "False":
                    if output_path:
                        processor.save_report_to_s3(csv_production_report_content,bucket_name,output_path, production_report_file_name)
                    if report_creation_path:
                        processor.save_report_to_s3(csv_production_report_content, bucket_name,report_creation_path,production_report_file_name)
            else:
                raise ValueError("CSV Pattern Not Found")
        elif file_type != "False":
            # statement tle data and calculations
            statement_tle_data,calculations = processor.process_xml_file(object_key)
            # generate production report with template 
            production_report_content = processor.prep_prod_report_data(production_report_template,statement_tle_data,file_name,file_fields_order)
            
            # generate summary report with template
            summary_report_content = processor.generate_formatted_report_from_template(summary_report_template,calculations,file_name)
            
            # generate recon report with template
            recon_report_template = config.get("recon_report_template")
            recon_report_content = processor.generate_formatted_report_from_template(recon_report_template,calculations,file_name, file_fields_order)
            
            if summary_report_file_name and summary_report_file_name != "False":
                if output_path:
                    processor.save_report_to_s3(summary_report_content,bucket_name,output_path, summary_report_file_name)
                if report_creation_path:
                    processor.save_report_to_s3(summary_report_content, bucket_name,report_creation_path,summary_report_file_name)
            if production_report_file_name and production_report_file_name != "False":
                if output_path:
                    processor.save_report_to_s3(production_report_content,bucket_name,output_path, production_report_file_name)
                if report_creation_path:
                    processor.save_report_to_s3(production_report_content, bucket_name,report_creation_path,production_report_file_name)
            if recon_report_file_name and recon_report_file_name != "False":
                if output_path:
                    processor.save_report_to_s3(recon_report_content,bucket_name,output_path, recon_report_file_name)
                if report_creation_path:
                    processor.save_report_to_s3(recon_report_content, bucket_name,report_creation_path,recon_report_file_name)
            #read csv bundle report 
        
        # Return summary for Glue job output
        return {    
            'statusCode': 200,
            'body': {
                'message': 'XML processing completed successfully',
                'output_base_path': output_path,
                'report_creation_path': report_creation_path,
            }
        }
            
    except Exception as e:
        log_message("failed","Glue Job Failed")
        print(f"Glue job failed: {str(e)}")
        raise ValueError(f"Glue job failed: {str(e)}")

if __name__ == "__main__":
    main()