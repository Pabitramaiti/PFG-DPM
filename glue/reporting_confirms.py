import sys
import json
import boto3
import os
import splunk
import xml.etree.ElementTree as ET
import re
from collections import defaultdict
from awsglue.utils import getResolvedOptions

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
            raise
    
    def find_matching_file(self, s3_base_path, file_pattern):
        """Find file matching regex pattern in S3 directory"""
        try:
            bucket = self.bucket_name
            prefix = s3_base_path.rstrip('/')
            
            print(f"Searching for files matching pattern '{file_pattern}' in s3://{bucket}/{prefix}/")
            
            # List objects in the S3 directory
            response = self.s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix + '/',
                Delimiter='/'
            )
            
            if 'Contents' not in response:
                print(f"No files found in s3://{bucket}/{prefix}/")
                return None
            
            # Compile regex pattern
            pattern = re.compile(file_pattern)
            matching_files = []
            
            # Check each file against the pattern
            for obj in response['Contents']:
                file_key = obj['Key']
                file_name = file_key.split('/')[-1]  # Get just the filename
                
                if pattern.match(file_name):
                    matching_files.append({
                        'key': file_key,
                        'name': file_name,
                        'last_modified': obj['LastModified'],
                        'size': obj['Size']
                    })
                    print(f"Found matching file: {file_name}")
            
            if not matching_files:
                print(f"No files matching pattern '{file_pattern}' found")
                return None
            
            # Sort by last modified date (most recent first)
            matching_files.sort(key=lambda x: x['last_modified'], reverse=True)
            
            selected_file = matching_files[0]
            print(f"Selected most recent file: {selected_file['name']} (modified: {selected_file['last_modified']})")
            
            return selected_file['key']
            
        except Exception as e:
            print(f"Error searching for files: {str(e)}")
            return None
    
    def read_xml_file(self, xml_file_key):
        """Read XML content from S3 based on configuration with regex pattern support"""
        products = self.config.get('products', {})
        
        if xml_file_key not in products:
            available_keys = list(products.keys())
            raise ValueError(f"Product key '{xml_file_key}' not found. Available: {available_keys}")
        
        product_info = products[xml_file_key]
        s3_base_path = product_info.get('s3_base_path')
        
        # Check if using regex pattern or direct filename
        file_name_pattern = product_info.get('file_name_pattern')
        direct_file_name = product_info.get('file_name')
        
        if not s3_base_path:
            raise ValueError(f"Missing s3_base_path for product '{xml_file_key}'")
        
        if file_name_pattern:
            # Use regex pattern to find matching file
            print(f"Using regex pattern: {file_name_pattern}")
            file_key = self.find_matching_file(s3_base_path, file_name_pattern)
            
            if not file_key:
                raise ValueError(f"No files found matching pattern '{file_name_pattern}' in {s3_base_path}")
            
            # Extract actual filename from the key
            actual_file_name = file_key.split('/')[-1]
            file_path = f"s3://{self.bucket_name}/{file_key}"
            
        elif direct_file_name:
            # Use direct filename (backward compatibility)
            print(f"Using direct filename: {direct_file_name}")
            file_path = f"s3://{self.bucket_name}/{s3_base_path.rstrip('/')}/{direct_file_name}"
            actual_file_name = direct_file_name
            
        else:
            raise ValueError(f"Missing both file_name_pattern and file_name for product '{xml_file_key}'")
        
        # Add file_path and actual filename to product_info for backward compatibility
        product_info['file_path'] = file_path
        product_info['actual_file_name'] = actual_file_name
        
        print(f"Processing XML file: {xml_file_key}")
        print(f"Product type: {product_info.get('product_type')}")
        print(f"File path: {file_path}")
        print(f"Document type: {product_info.get('document_type', 'N/A')}")
        
        # Read from S3
        xml_content = self.read_s3_file(file_path)
        return xml_content, product_info
    
    def extract_tle_properties_from_xml(self, xml_content):
        """Extract TLE properties from XML content with multiple statements"""
        try:
            root = ET.fromstring(xml_content)
            
            # Aggregate TLE data from all statements
            aggregated_tle_data = defaultdict(list)
            statement_count = 0
            total_images = 0
            total_pages = 0
            total_records = 0
            
            # Process all statements
            for statement in root.findall('.//Statement'):
                statement_count += 1
                
                # Get statement-level attributes
                images = int(statement.get('Images', '0'))
                records = int(statement.get('Records', '0'))
                total_images += images
                total_records += records
                
                # Count pages in this statement
                pages = len(statement.findall('.//Page'))
                total_pages += pages
                
                # Extract TLE properties from this statement
                for prop in statement.findall('.//Property[@Type="TLE"]'):
                    name = prop.get('Name')
                    value = prop.get('Value')
                    if name and value:
                        aggregated_tle_data[name].append(value)
            
            # Create final TLE data with aggregated/consolidated values
            final_tle_data = {}
            for name, values in aggregated_tle_data.items():
                # For most fields, take the first non-empty value or consolidate
                if len(set(values)) == 1:
                    # All values are the same
                    final_tle_data[name] = values[0]
                else:
                    # Multiple different values - take first non-empty value
                    # This handles cases where different statements have different values
                    final_tle_data[name] = values[0] if values[0] else (values[1] if len(values) > 1 else '')
            
            # Add calculated totals
            calculated_totals = {
                'Total_Statements': statement_count,
                'Total_Images_Calculated': total_images,
                'Total_Pages_Calculated': total_pages,
                'Total_Records': total_records
            }
            
            print(f"Extracted data: {statement_count} statements, {total_images} images, {total_pages} pages")
            
            # Return empty dicts for unused data structures
            return final_tle_data, {}, calculated_totals
            
        except ET.ParseError as e:
            print(f"Error parsing XML: {e}")
            return {}, {}, {}
    
    def detect_document_classification(self, tle_data, calculated_totals):
        """Detect document classification based on configured source field"""
        # Get the classification code
        classification_field = self.config.get('document_classification', {}).get('source_field')
        if not classification_field:
            raise ValueError("document_classification.source_field must be configured")
        classification_code = None
        
        # Try to get from TLE data first
        if classification_field in tle_data:
            classification_code = tle_data[classification_field]
        
        # Get delivery preference (may be needed for some logic)
        delivery_preference = self.get_mapped_field_value('delivery_preference', tle_data, calculated_totals)
        
        # Get classification values from configuration
        classifications = self.config.get('document_classification', {}).get('classifications', {})
        
        # Initialize result with default values
        result = {
            'is_no_mail': False,
            'is_stop_mail': False,
            'is_dual_delivery': False,
            'is_mail2': False,
            'is_add_mail': False,
            # New flags
            'is_mail1': False,
            'is_email': False,
            'classification_code': classification_code,
            'delivery_preference': delivery_preference
        }
        
        if classification_code:
            # Check each classification type
            for class_type, values in classifications.items():
                is_match = str(classification_code).upper() in [v.upper() for v in values]
                if class_type == 'no_mail':
                    result['is_no_mail'] = is_match
                elif class_type == 'stop_mail':
                    result['is_stop_mail'] = is_match
                elif class_type == 'dual_delivery':
                    result['is_dual_delivery'] = is_match
                elif class_type == 'mail2':
                    result['is_mail2'] = is_match
                elif class_type == 'add_mail':
                    result['is_add_mail'] = is_match
                # New mappings
                elif class_type == 'mail1':
                    result['is_mail1'] = is_match
                elif class_type == 'email':
                    result['is_email'] = is_match
        
        return result

    def get_classification_value(self, classification_type, tle_data, calculated_totals):
        """Get classification value for a specific type using consolidated configuration"""
        classification = self.detect_document_classification(tle_data, calculated_totals)
        
        if classification_type == 'no_mail':
            return classification['is_no_mail']
        elif classification_type == 'stop_mail':
            return classification['is_stop_mail']
        elif classification_type == 'dual_delivery':
            return classification['is_dual_delivery']
        elif classification_type == 'mail2':
            return classification['is_mail2']
        elif classification_type == 'add_mail':
            return classification['is_add_mail']
        # New: support Mail1 and Email indicators from config
        elif classification_type == 'mail1':
            return classification.get('is_mail1', False)
        elif classification_type == 'email':
            return classification.get('is_email', False)
        else:
            return False

    def get_suppression_counts_from_txt_file(self, tle_data, calculated_totals):
        """Get suppression counts from txt file by matching UC_FILE_NAME - config driven detection"""
        try:
            # Get UC_FILE_NAME from TLE data
            uc_file_name = tle_data.get('UC_FILE_NAME', '')
            if not uc_file_name:
                print("No UC_FILE_NAME found in TLE data")
                return {'documents_suppressed': 0, 'suppression_details': []}
            
            print(f"Looking for suppressions matching UC_FILE_NAME: {uc_file_name}")
            
            # Get suppression detection configuration
            suppression_config = self.config.get('suppression_detection', {})
            detection_methods = suppression_config.get('methods', [])
            
            # Default methods if no config provided (backward compatibility)
            if not detection_methods:
                detection_methods = [
                    {
                        "type": "regex",
                        "pattern": ".*[Ss]uppressed.*",
                        "description": "Regex pattern for suppressed variations"
                    }
                ]
            
            # Get report base path from current product configuration
            report_base_path = self.current_report_base_path
            
            # Find txt files in the report directory
            try:
                bucket = self.bucket_name
                prefix = report_base_path.rstrip('/')
                
                response = self.s3_client.list_objects_v2(
                    Bucket=bucket,
                    Prefix=prefix + '/',
                )
                
                if 'Contents' not in response:
                    print(f"No files found in s3://{bucket}/{prefix}/")
                    return {'documents_suppressed': 0, 'suppression_details': []}
                
                # Find rpt files that match UC_FILE_NAME
                txt_files = []
                for obj in response['Contents']:
                    file_key = obj['Key']
                    if file_key.endswith('.rpt') and uc_file_name in file_key:
                        txt_files.append(file_key)
                        print(f"Found matching rpt file: {file_key}")
                
                if not txt_files:
                    print(f"No txt files found matching UC_FILE_NAME: {uc_file_name}")
                    return {'documents_suppressed': 0, 'suppression_details': []}
                
                # Parse all matching txt files and aggregate suppressed counts
                total_suppressed = 0
                details = []
                
                for txt_file_key in txt_files:
                    try:
                        txt_content = self.read_s3_file(f"s3://{bucket}/{txt_file_key}")
                        
                        # Count suppressed entries using configured methods
                        suppressed_count = 0
                        lines = txt_content.strip().split('\n')
                        
                        for line in lines:
                            line = line.strip()
                            if not line:
                                continue
                            
                            # Try each detection method in order until one matches
                            for method in detection_methods:
                                method_type = method.get('type')
                                found_suppression = False
                                
                                if method_type == 'regex':
                                    try:
                                        pattern = method.get('pattern', r'.*[Ss]uppressed.*')
                                        if re.search(pattern, line):
                                            suppressed_count += 1
                                            print(f"Found suppression via regex '{pattern}': {line[:50]}...")
                                            found_suppression = True
                                    except re.error as regex_err:
                                        print(f"Invalid regex pattern '{pattern}': {regex_err}")
                                
                                elif method_type == 'json_field':
                                    try:
                                        # Try to parse as JSON
                                        if line.startswith('{') and line.endswith('}'):
                                            doc_data = json.loads(line)
                                            field_path = method.get('field_path', 'isSuppressed')
                                            expected_value = method.get('expected_value', True)
                                            
                                            # Navigate nested field path (e.g., "document.isSuppressed")
                                            field_value = doc_data
                                            for field in field_path.split('.'):
                                                if isinstance(field_value, dict) and field in field_value:
                                                    field_value = field_value[field]
                                                else:
                                                    field_value = None
                                                    break
                                            
                                            if field_value == expected_value:
                                                suppressed_count += 1
                                                account_key = doc_data.get('document', {}).get('accountKey', 'unknown')
                                                print(f"Found suppression via JSON field '{field_path}': account {account_key}")
                                                found_suppression = True
                                    except (json.JSONDecodeError, ValueError, KeyError):
                                        # Not valid JSON or missing fields, continue with other methods
                                        pass
                                
                                # Break after first successful match to avoid double counting
                                if found_suppression:
                                    break
                        
                        print(f"File {txt_file_key} suppressed entries: {suppressed_count}")
                        
                        total_suppressed += suppressed_count
                        details.append({'source_file': txt_file_key, 'count': suppressed_count})
                        
                    except Exception as e:
                        print(f"Error reading/parsing txt file {txt_file_key}: {e}")
                        # continue with other files
                
                print(f"Total suppressed documents found across all files: {total_suppressed}")
                
                return {
                    'documents_suppressed': total_suppressed,
                    'suppression_details': details
                }
                
            except Exception as e:
                print(f"Error reading txt files: {e}")
                return {'documents_suppressed': 0, 'suppression_details': []}
                
        except Exception as e:
            print(f"Error getting suppression counts from txt file: {e}")
            return {'documents_suppressed': 0, 'suppression_details': []}

    def categorize_envelope_size_from_xml(self, xml_content):
        """Categorize envelope size based on XML content (6x9, 9x12, Oversize)

        Meaning of: "Envelope sizing counts all physical deliverables except No/Stop Mail (including Add Mail, Mail1, Dual print side, Mail2 duplicates)":
        - We only size statements that produce a physical mail piece (an envelope).
        - Excluded: classifications in no_mail or stop_mail lists (archive-only or halted).
        - Included:
          * Mail1: normal print documents.
          * Dual (Email/Mail1): only the print copy (email has no envelope).
          * Add Mail: extra physical set; counted once like a normal document.
          * Mail2: double print. Current logic counts it once per statement; if you need each duplicate envelope counted twice, you would multiply counts here.
        - Sizing uses sheet/image thresholds to assign each physical piece to 6x9, 9x12, or Oversize.
        NOTE: This function does not currently double Mail2 envelope counts; duplication is reflected in sheet/image totals elsewhere.
        """
        try:
            root = ET.fromstring(xml_content)
            envelope_counts = {
                '6x9_Envelope_Documents': 0,
                '6x9_Envelope_Sheets': 0,
                '6x9_Envelope_Images': 0,
                '9x12_Envelope_Documents': 0,
                '9x12_Envelope_Sheets': 0,
                '9x12_Envelope_Images': 0,
                'Oversize_Envelope_Documents': 0,
                'Oversize_Envelope_Sheets': 0,
                'Oversize_Envelope_Images': 0
            }
            
            # Get classification values from consolidated configuration
            classifications = self.config.get('document_classification', {}).get('classifications', {})
            no_mail_values = classifications.get('no_mail', [])
            stop_mail_values = classifications.get('stop_mail', [])
            
            # Process each statement in the XML
            for statement in root.findall('.//Statement'):
                # Skip statements that are no mail or stop mail - they don't get physical envelopes
                classification_code = None
                classification_field = self.config.get('document_classification', {}).get('source_field')
                if not classification_field:
                    raise ValueError("document_classification.source_field must be configured")
                for prop in statement.findall('.//Property[@Type="TLE"]'):
                    if prop.get('Name') == classification_field:
                        classification_code = prop.get('Value')
                        break
                
                # Skip if this is a no mail or stop mail document
                if classification_code:
                    is_no_mail = str(classification_code).upper() in [v.upper() for v in no_mail_values]
                    is_stop_mail = str(classification_code).upper() in [v.upper() for v in stop_mail_values]
                    if is_no_mail or is_stop_mail:
                        continue
                
                # Get document metrics from statement
                doc_sheets = 0
                doc_images = int(statement.get('Images', '0'))
                
                # Get sheet/image counts from TLE properties or count pages
                for prop in statement.findall('.//Property[@Type="TLE"]'):
                    if prop.get('Name') == 'DOC_SHEET_COUNT':
                        doc_sheets = int(prop.get('Value', '0'))
                    elif prop.get('Name') == 'DOC_IMAGE_COUNT':
                        doc_images = int(prop.get('Value', '0'))
                
                if doc_sheets == 0:
                    doc_sheets = len(statement.findall('.//Page'))
                
                # Categorize based on sheet count and image count per document
                # 6x9 Envelope: 1-13 pages OR 2-26 images
                if (1 <= doc_sheets <= 13) or (2 <= doc_images <= 26):
                    envelope_counts['6x9_Envelope_Documents'] += 1
                    envelope_counts['6x9_Envelope_Sheets'] += doc_sheets
                    envelope_counts['6x9_Envelope_Images'] += doc_images
                
                # 9x12 Envelope: 14-60 pages OR 28-120 images
                elif (14 <= doc_sheets <= 60) or (28 <= doc_images <= 120):
                    envelope_counts['9x12_Envelope_Documents'] += 1
                    envelope_counts['9x12_Envelope_Sheets'] += doc_sheets
                    envelope_counts['9x12_Envelope_Images'] += doc_images
                
                # Oversize Envelope: 61+ pages OR 122+ images
                else:
                    envelope_counts['Oversize_Envelope_Documents'] += 1
                    envelope_counts['Oversize_Envelope_Sheets'] += doc_sheets
                    envelope_counts['Oversize_Envelope_Images'] += doc_images
            print(f"Categorized envelope sizes: {envelope_counts}")
            return envelope_counts
        except ET.ParseError as e:
            print(f"Error parsing XML for envelope categorization: {e}")
            return {
                '6x9_Envelope_Documents': 0,
                '6x9_Envelope_Sheets': 0,
                '6x9_Envelope_Images': 0,
                '9x12_Envelope_Documents': 0,
                '9x12_Envelope_Sheets': 0,
                '9x12_Envelope_Images': 0,
                'Oversize_Envelope_Documents': 0,
                'Oversize_Envelope_Sheets': 0,
                'Oversize_Envelope_Images': 0
            }
    
    def find_report_files(self, base_path):
        """Find report files in the given S3 base path"""
        try:
            bucket = self.bucket_name
            prefix = base_path.rstrip('/')
            
            print(f"Searching for report files in s3://{bucket}/{prefix}/")
            
            # List objects in the S3 directory
            response = self.s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix + '/',
                Delimiter='/'
            )
            
            report_files = []
            
            if 'Contents' in response:
                # Collect all report file keys
                for obj in response['Contents']:
                    file_key = obj['Key']
                    report_files.append(file_key)
                    print(f"Found report file: {file_key}")
            
            if not report_files:
                print(f"No report files found in s3://{bucket}/{prefix}/")
            
            return report_files
        except Exception as e:
            print(f"Error finding report files: {e}")
            return []
    
    def parse_report_file(self, file_key):
        """Parse a single report file to extract suppression counts"""
        try:
            print(f"Parsing report file: {file_key}")
            
            # Read the report file content from S3
            report_content = self.read_s3_file(f"s3://{self.bucket_name}/{file_key}")
            
            # Split into lines and process each line
            lines = report_content.strip().split(chr(10))
            suppression_counts = {}
            
            for line in lines:
                # Skip empty lines
                if not line.strip():
                    continue
                
                # Split by tab or pipe, depending on the report format
                if '\t' in line:
                    fields = line.split('\t')
                else:
                    fields = line.split('|')
                
                # Expecting two fields: Name and Count
                if len(fields) >= 2:
                    name = fields[0].strip()
                    count = int(fields[1].strip()) if fields[1].strip().isdigit() else 0
                    suppression_counts[name] = count
                    print(f"Found suppression count - {name}: {count}")
            
            return suppression_counts
        except Exception as e:
            print(f"Error parsing report file {file_key}: {e}")
            return {}
    
    def get_suppression_counts_from_reports(self, tle_data, calculated_totals):
        """Get suppression counts from report files based on TLE data and calculated totals"""
        try:
            report_base_path = self.current_report_base_path
            print(f"Getting suppression counts from reports in: {report_base_path}")
            
            # Find all relevant report files
            report_files = self.find_report_files(report_base_path)
            
            total_suppressed = 0
            suppression_details = []
            
            # Process each report file
            for file_key in report_files:
                # Parse the report file to get suppression counts
                suppression_counts = self.parse_report_file(file_key)
                
                # Aggregate suppression counts
                for name, count in suppression_counts.items():
                    if name == 'Total Suppressed':
                        total_suppressed += count
                    else:
                        suppression_details.append({
                            'name': name,
                            'count': count
                        })
            
            # Prepare final suppression data
            suppression_data = {
                'documents_suppressed': total_suppressed,
                'suppression_details': suppression_details
            }
            
            print(f"Total documents suppressed: {total_suppressed}")
            return suppression_data
        except Exception as e:
            print(f"Error getting suppression counts from reports: {e}")
            return {
                'documents_suppressed': 0,
                'suppression_details': []
            }

    def get_mapped_field_value(self, field_name, tle_data, calculated_totals):
        """Get field value based on mapping configuration"""
        field_mappings = self.config.get('field_mappings', {})
        
        if field_name not in field_mappings:
            print(f"No mapping found for field '{field_name}'")
            return None
        
        mapping = field_mappings[field_name]
        source_type = mapping.get('source_type')
        source_field = mapping.get('source_field')
        default_value = mapping.get('default_value')
        
        try:
            if source_type == 'tle_property':
                return tle_data.get(source_field, default_value)
            elif source_type == 'calculated':
                return calculated_totals.get(source_field, default_value)
            elif source_type == 'classification':
                # Use consolidated classification detection
                classification_type = mapping.get('classification_type')
                return self.get_classification_value(classification_type, tle_data, calculated_totals)
            else:
                print(f"Unknown source_type '{source_type}' for field '{field_name}'")
                return default_value
        except Exception as e:
            print(f"Error getting value for field '{field_name}': {e}")
            return default_value
    
    def detect_special_delivery_flags(self, tle_data, calculated_totals):
        """Detect No Mail and Stop Mail documents using UC_DOCUMENT_CLASSIFICATION_01"""
        # Use the consolidated detection method
        classification = self.detect_document_classification(tle_data, calculated_totals)
        
        # Return just the flags needed for this method
        return {
            'is_no_mail': classification['is_no_mail'],
            'is_stop_mail': classification['is_stop_mail'],
            'classification_code': classification['classification_code'],
            'delivery_preference': classification['delivery_preference']
        }
    
    def detect_dual_delivery_and_mail2(self, tle_data, calculated_totals):
        """Detect Email/Mail 1 and Mail 2 documents using UC_DOCUMENT_CLASSIFICATION_01"""
        # Use the consolidated detection method
        classification = self.detect_document_classification(tle_data, calculated_totals)
        
        # Return just the flags needed for this method
        return {
            'is_dual_delivery': classification['is_dual_delivery'],
            'is_mail2': classification['is_mail2'],
            'classification_code': classification['classification_code']
        }

    def calculate_delivery_metrics_from_xml(self, xml_content, calculated_totals):
        """Calculate delivery metrics by analyzing individual statements from XML

        Classification-driven counting only:

        Differences:
        - Mail2: counted and sheets/images doubled (physical duplication).
        - Dual (Email/Mail1): counted once but contributes to both email and print tallies.
        - Add Mail: counted normally (single physical set) – included in Delivery Count 1 like standard documents.
        - No Mail / Stop Mail: excluded entirely from delivery aggregation; later reported separately (archive-only style).
        - Mail1 / Email: counted only if classification indicates them; no fallback to delivery preference.
        """
        try:
            root = ET.fromstring(xml_content)
            
            # Initialize counters
            email_count = print_count = dual_delivery_count = mail2_count = add_mail_count = 0
            email_sheets = email_images = 0
            print_sheets = print_images = 0
            dual_delivery_sheets = dual_delivery_images = 0
            mail2_sheets = mail2_images = 0
            add_mail_sheets = add_mail_images = 0
            pref_print_docs = pref_print_sheets = pref_print_images = 0
            pref_email_docs = pref_email_sheets = pref_email_images = 0
            no_mail_count = no_mail_sheets = no_mail_images = 0
            stop_mail_count = stop_mail_sheets = stop_mail_images = 0
            total_doc_sheets_all = 0
            total_doc_images_all = 0
            
            # Get classification values from consolidated configuration
            classifications = self.config.get('document_classification', {}).get('classifications', {})
            dual_delivery_values = classifications.get('dual_delivery', [])
            mail2_values = classifications.get('mail2', [])
            no_mail_values = classifications.get('no_mail', [])
            stop_mail_values = classifications.get('stop_mail', [])
            add_mail_values = classifications.get('add_mail', [])
            mail1_values = classifications.get('mail1', [])
            email_values = classifications.get('email', [])
            
            # Get classification field from config
            classification_field = self.config.get('document_classification', {}).get('source_field')
            if not classification_field:
                raise ValueError("document_classification.source_field must be configured")
            
            # Process each statement individually
            for statement in root.findall('.//Statement'):
                # Get delivery preference and classification for this specific statement
                delivery_pref = None
                classification_code = None
                doc_sheets = 0
                doc_images = int(statement.get('Images', '0'))
                
                for prop in statement.findall('.//Property[@Type="TLE"]'):
                    prop_name = prop.get('Name')
                    if prop_name == 'UC_DOCUMENT_DELIVERY_PREFERENCE':
                        delivery_pref = prop.get('Value')  # retained for logging if needed, not used for counting
                    elif prop_name == classification_field:
                        classification_code = prop.get('Value')
                    elif prop_name == 'DOC_SHEET_COUNT':
                        doc_sheets = int(prop.get('Value', '0'))
                    elif prop_name == 'DOC_IMAGE_COUNT':
                        doc_images = int(prop.get('Value', '0'))
                
                # Count pages from XML if not in TLE properties
                if doc_sheets == 0:
                    doc_sheets = len(statement.findall('.//Page'))
                total_doc_sheets_all += doc_sheets
                total_doc_images_all += doc_images

                is_dual_delivery = classification_code and str(classification_code).upper() in [v.upper() for v in dual_delivery_values]
                is_mail2 = classification_code and str(classification_code).upper() in [v.upper() for v in mail2_values]
                is_no_mail = classification_code and str(classification_code).upper() in [v.upper() for v in no_mail_values]
                is_stop_mail = classification_code and str(classification_code).upper() in [v.upper() for v in stop_mail_values]
                is_add_mail = classification_code and str(classification_code).upper() in [v.upper() for v in add_mail_values]
                is_mail1 = classification_code and str(classification_code).upper() in [v.upper() for v in mail1_values]
                is_email_only = classification_code and str(classification_code).upper() in [v.upper() for v in email_values]
                delivery_pref_value = (delivery_pref or '').upper()
                pref_tokens = {token for token in re.split(r'[^A-Z]+', delivery_pref_value) if token}
                pref_has_print = 'PRINT' in pref_tokens
                pref_has_email = 'EMAIL' in pref_tokens
                if pref_has_print:
                    pref_print_docs += 1
                    pref_print_sheets += doc_sheets
                    pref_print_images += doc_images
                if pref_has_email:
                    pref_email_docs += 1
                    pref_email_sheets += doc_sheets
                    pref_email_images += doc_images

                if is_no_mail:
                    no_mail_count += 1
                    no_mail_sheets += doc_sheets
                    no_mail_images += doc_images
                    continue
                if is_stop_mail:
                    stop_mail_count += 1
                    stop_mail_sheets += doc_sheets
                    stop_mail_images += doc_images
                    continue
                if is_mail2:
                    # Mail 2 - Double Print (generates 2 print copies)
                    mail2_count += 1
                    mail2_sheets += doc_sheets  
                    mail2_images += doc_images  
                elif is_dual_delivery:
                    # Email/Mail 1 - Document sent both via email AND print
                    dual_delivery_count += 1
                    dual_delivery_sheets += doc_sheets
                    dual_delivery_images += doc_images
                elif is_add_mail:
                    add_mail_count += 1
                    add_mail_sheets += doc_sheets
                    add_mail_images += doc_images
                elif is_no_mail or is_stop_mail:
                    continue
                elif is_mail1:
                    print_count += 1
                    print_sheets += doc_sheets
                    print_images += doc_images
                elif is_email_only:
                    email_count += 1
                    email_sheets += doc_sheets
                    email_images += doc_images
            
            # Update calculations dictionary with aggregated values
            calculations = {
                'Documents_Created': email_count + print_count + dual_delivery_count + mail2_count + add_mail_count,
                'Email_Only_Documents': email_count,
                'Print_Only_Documents': print_count,
                'Email_Mail1_Documents': dual_delivery_count,
                'Mail2_Documents': mail2_count,
                'Add_Mail_Documents': add_mail_count,
                'Total_Output_Documents': email_count + print_count + dual_delivery_count + mail2_count + add_mail_count,
                'Total_Output_Sheets': email_sheets + print_sheets + dual_delivery_sheets + mail2_sheets + add_mail_sheets,
                'Total_Output_Images': email_images + print_images + dual_delivery_images + mail2_images + add_mail_images,
                'Email_Only_Sheets': email_sheets,
                'Email_Only_Images': email_images,
                'Print_Only_Sheets': print_sheets,
                'Print_Only_Images': print_images,
                'Email_Mail1_Sheets': dual_delivery_sheets,
                'Email_Mail1_Images': dual_delivery_images,
                'Mail2_Sheets': mail2_sheets,
                'Mail2_Images': mail2_images,
                'Add_Mail_Sheets': add_mail_sheets,
                'Add_Mail_Images': add_mail_images,
                'Delivery_Count_1_Documents': print_count + email_count + add_mail_count,
                'Delivery_Count_1_Sheets': print_sheets + email_sheets + add_mail_sheets,
                'Delivery_Count_1_Images': print_images + email_images + add_mail_images,
                'No_Mail_Documents': no_mail_count,
                'No_Mail_Sheets': no_mail_sheets,
                'No_Mail_Images': no_mail_images,
                'Stop_Mail_Documents': stop_mail_count,
                'Stop_Mail_Sheets': stop_mail_sheets,
                'Stop_Mail_Images': stop_mail_images,
                'Preference_Print_Documents': pref_print_docs,
                'Preference_Print_Sheets': pref_print_sheets,
                'Preference_Print_Images': pref_print_images,
                'Preference_Email_Documents': pref_email_docs,
                'Preference_Email_Sheets': pref_email_sheets,
                'Preference_Email_Images': pref_email_images,
                'All_Documents_Sheets': total_doc_sheets_all,
                'All_Documents_Images': total_doc_images_all
            }
            
            print(f"From XML analysis - Email: {email_count} docs, Print: {print_count} docs, Email/Mail1: {dual_delivery_count} docs, Mail2: {mail2_count} docs, Add Mail: {add_mail_count} docs")
            
            return calculations
            
        except Exception as e:
            print(f"Error calculating delivery metrics from XML: {e}")
            return {}
    
    def calculate_message_page_metrics(self, tle_data, calculated_totals):
        """Calculate Message Page metrics from TLE data"""
        try:
            # Get Message Page 1 metrics
            mp1_indicator = self.get_mapped_field_value('message_page1_indicator', tle_data, calculated_totals)
            mp1_sheet_count = self.get_mapped_field_value('message_page1_sheet_count', tle_data, calculated_totals)
            mp1_image_count = self.get_mapped_field_value('message_page1_image_count', tle_data, calculated_totals)
            
            # Get Message Page 2 metrics
            mp2_indicator = self.get_mapped_field_value('message_page2_indicator', tle_data, calculated_totals)
            mp2_sheet_count = self.get_mapped_field_value('message_page2_sheet_count', tle_data, calculated_totals)
            mp2_image_count = self.get_mapped_field_value('message_page2_image_count', tle_data, calculated_totals)
            
            # Count documents with message pages
            mp1_doc_count = 0
            mp2_doc_count = 0
            
            # Process aggregated TLE data to count documents
            if mp1_indicator:
                # If indicator is aggregated (comma-separated), count occurrences of '1'
                if ',' in str(mp1_indicator):
                    mp1_values = str(mp1_indicator).split(', ')
                    mp1_doc_count = mp1_values.count('1')
                elif str(mp1_indicator) == '1':
                    mp1_doc_count = calculated_totals.get('Total_Statements', 1)
            
            if mp2_indicator:
                # If indicator is aggregated (comma-separated), count occurrences of '1'
                if ',' in str(mp2_indicator):
                    mp2_values = str(mp2_indicator).split(', ')
                    mp2_doc_count = mp2_values.count('1')
                elif str(mp2_indicator) == '1':
                    mp2_doc_count = calculated_totals.get('Total_Statements', 1)
            
            # Convert counts to integers
            mp1_sheets = int(mp1_sheet_count or 0)
            mp1_images = int(mp1_image_count or 0)
            mp2_sheets = int(mp2_sheet_count or 0)
            mp2_images = int(mp2_image_count or 0)
            
            message_page_metrics = {
                'Message_Page1_Documents': mp1_doc_count,
                'Message_Page1_Sheets': mp1_sheets,
                'Message_Page1_Images': mp1_images,
                'Message_Page2_Documents': mp2_doc_count,
                'Message_Page2_Sheets': mp2_sheets,
                'Message_Page2_Images': mp2_images,
                'Total_Message_Page_Documents': mp1_doc_count + mp2_doc_count,
                'Total_Message_Page_Sheets': mp1_sheets + mp2_sheets,
                'Total_Message_Page_Images': mp1_images + mp2_images
            }
            
            print(f"Message Page metrics: MP1={mp1_doc_count} docs, MP2={mp2_doc_count} docs")
            return message_page_metrics
            
        except Exception as e:
            print(f"Error calculating message page metrics: {e}")
            return {
                'Message_Page1_Documents': 0,
                'Message_Page1_Sheets': 0,
                'Message_Page1_Images': 0,
                'Message_Page2_Documents': 0,
                'Message_Page2_Sheets': 0,
                'Message_Page2_Images': 0
            }

    def calculate_message_page_metrics_from_xml(self, xml_content):
        """Calculate Message Page metrics from individual XML statements"""
        try:
            root = ET.fromstring(xml_content)
            
            mp1_doc_count = 0
            mp1_sheet_count = 0
            mp1_image_count = 0
            mp2_doc_count = 0
            mp2_sheet_count = 0
            mp2_image_count = 0
            
            # Process each statement individually
            for statement in root.findall('.//Statement'):
                # Get message page indicators for this statement
                mp1_indicator = 0
                mp1_sheets = 0
                mp1_images = 0
                mp2_indicator = 0
                mp2_sheets = 0
                mp2_images = 0
                
                for prop in statement.findall('.//Property[@Type="TLE"]'):
                    prop_name = prop.get('Name')
                    prop_value = prop.get('Value', '0')
                    
                    if prop_name == 'CUSTOM_MESSAGE_PAGE1':
                        mp1_indicator = int(prop_value)
                    elif prop_name == 'CUSTOM_MP1_SHEET_COUNT':
                        mp1_sheets = int(prop_value)
                    elif prop_name == 'CUSTOM_MP1_IMAGE_COUNT':
                        mp1_images = int(prop_value)
                    elif prop_name == 'CUSTOM_MESSAGE_PAGE2':
                        mp2_indicator = int(prop_value)
                    elif prop_name == 'CUSTOM_MP2_SHEET_COUNT':
                        mp2_sheets = int(prop_value)
                    elif prop_name == 'CUSTOM_MP2_IMAGE_COUNT':
                        mp2_images = int(prop_value)
                
                # Count documents with message pages
                if mp1_indicator == 1:
                    mp1_doc_count += 1
                    mp1_sheet_count += mp1_sheets
                    mp1_image_count += mp1_images
                
                if mp2_indicator == 1:
                    mp2_doc_count += 1
                    mp2_sheet_count += mp2_sheets
                    mp2_image_count += mp2_images
            
            message_page_metrics = {
                'Message_Page1_Documents': mp1_doc_count,
                'Message_Page1_Sheets': mp1_sheet_count,
                'Message_Page1_Images': mp1_image_count,
                'Message_Page2_Documents': mp2_doc_count,
                'Message_Page2_Sheets': mp2_sheet_count,
                'Message_Page2_Images': mp2_image_count,
                'Total_Message_Page_Documents': mp1_doc_count + mp2_doc_count,
                'Total_Message_Page_Sheets': mp1_sheet_count + mp2_sheet_count,
                'Total_Message_Page_Images': mp1_image_count + mp2_image_count
            }
            
            print(f"Message Page metrics from XML: MP1={mp1_doc_count} docs, MP2={mp2_doc_count} docs")
            return message_page_metrics
            
        except Exception as e:
            print(f"Error calculating message page metrics from XML: {e}")
            return {
                'Message_Page1_Documents': 0,
                'Message_Page1_Sheets': 0,
                'Message_Page1_Images': 0,
                'Message_Page2_Documents': 0,
                'Message_Page2_Sheets': 0,
                'Message_Page2_Images': 0
            }

    def process_xml_file(self, xml_file_key):
        """Main processing method - orchestrates the entire XML processing workflow"""
        try:
            print(f"Starting XML processing for: {xml_file_key}")
            
            # Step 1: Read XML file from S3
            xml_content, file_info = self.read_xml_file(xml_file_key)
            
            # Set the current product's report base path for use in suppression counting
            self.current_report_base_path = file_info.get('report_base_path')
            
            # Step 2: Extract TLE properties and calculated totals from XML
            tle_data, trailer_data, calculated_totals = self.extract_tle_properties_from_xml(xml_content)
            total_xml_statements = calculated_totals.get('Total_Statements', 0)
            
            # Store UC_FILE_NAME for later use in filename generation
            self.uc_file_name = tle_data.get('UC_FILE_NAME', 'unknown')
            
            # Step 3: Get suppression counts from txt file (CORRECTED METHOD)
            suppression_data = self.get_suppression_counts_from_txt_file(tle_data, calculated_totals)
            
            # Step 4: Calculate delivery metrics from individual XML statements
            calculations = self.calculate_delivery_metrics_from_xml(xml_content, calculated_totals)
            
            calculations['Documents_Suppressed'] = suppression_data['documents_suppressed']
            suppressed_docs = calculations['Documents_Suppressed']
            
            # Step 6: Get envelope sizing from individual statements
            envelope_counts = self.categorize_envelope_size_from_xml(xml_content)
            
            # Step 7: Add envelope counts to calculations
            calculations.update(envelope_counts)
            
            # Step 7.5: Calculate Message Page metrics from XML (CORRECTED METHOD)
            message_page_metrics = self.calculate_message_page_metrics_from_xml(xml_content)
            calculations.update(message_page_metrics)
            
            # Step 8: Reuse No Mail and Stop Mail metrics already computed in calculate_delivery_metrics_from_xml
            no_mail_count = calculations.get('No_Mail_Documents', 0)
            stop_mail_count = calculations.get('Stop_Mail_Documents', 0)
            no_mail_sheets = calculations.get('No_Mail_Sheets', 0)
            stop_mail_sheets = calculations.get('Stop_Mail_Sheets', 0)
            no_mail_images = calculations.get('No_Mail_Images', 0)
            stop_mail_images = calculations.get('Stop_Mail_Images', 0)
            total_sheets_all = calculations.get('All_Documents_Sheets', 0)
            total_images_all = calculations.get('All_Documents_Images', 0)

            documents_created = max(total_xml_statements + suppressed_docs - stop_mail_count, 0)
            total_output_documents = max(documents_created - suppressed_docs, 0)
            calculations['Documents_Created'] = documents_created
            calculations['Total_Output_Documents'] = total_output_documents
            calculations['Total_Output_Sheets'] = max(total_sheets_all - stop_mail_sheets, 0)
            calculations['Total_Output_Images'] = max(total_images_all - stop_mail_images, 0)

            calculations.update({
                'Delivery_Count_2_Documents': (calculations['Delivery_Count_1_Documents']
                                            - no_mail_count
                                            - stop_mail_count),
                'Delivery_Count_2_Sheets': (calculations['Delivery_Count_1_Sheets']
                                        - no_mail_sheets
                                        - stop_mail_sheets),
                'Delivery_Count_2_Images': (calculations['Delivery_Count_1_Images']
                                        - no_mail_images
                                        - stop_mail_images)
            })
            
            # Email/Mail 1 and Mail 2 are already calculated in calculate_delivery_metrics_from_xml
            # DO NOT OVERRIDE THEM - they contain proper per-statement calculations
            
            # Final delivery totals (Email/Mail 1 generates both email AND print)
            email_mail1_docs = calculations.get('Email_Mail1_Documents', 0)
            pref_print_docs = calculations.get('Preference_Print_Documents', 0)
            pref_print_sheets = calculations.get('Preference_Print_Sheets', 0)
            pref_print_images = calculations.get('Preference_Print_Images', 0)
            pref_email_docs = calculations.get('Preference_Email_Documents', 0)
            pref_email_sheets = calculations.get('Preference_Email_Sheets', 0)
            pref_email_images = calculations.get('Preference_Email_Images', 0)
            
            calculations.update({
                'Print_Delivery_Documents': pref_print_docs,
                'Print_Delivery_Sheets': pref_print_sheets,
                'Print_Delivery_Images': pref_print_images,
                'Email_Delivery_Documents': pref_email_docs,
                'Email_Delivery_Sheets': pref_email_sheets,
                'Email_Delivery_Images': pref_email_images
            })
            total_delivery_documents = (
                calculations.get('Delivery_Count_2_Documents', 0) +
                calculations.get('Email_Mail1_Documents', 0) +
                calculations.get('Mail2_Documents', 0)
            )
            total_delivery_sheets = (
                calculations.get('Delivery_Count_2_Sheets', 0) +
                calculations.get('Email_Mail1_Sheets', 0) +
                calculations.get('Mail2_Sheets', 0)
            )
            total_delivery_images = (
                calculations.get('Delivery_Count_2_Images', 0) +
                calculations.get('Email_Mail1_Images', 0) +
                calculations.get('Mail2_Images', 0)
            )
            calculations.update({
                'Total_Delivery_Documents': total_delivery_documents,
                'Total_Delivery_Sheets': total_delivery_sheets,
                'Total_Delivery_Images': total_delivery_images
            })
            final_delivery_documents = calculations['Print_Delivery_Documents'] + calculations['Email_Delivery_Documents']
            final_delivery_sheets = calculations['Print_Delivery_Sheets'] + calculations['Email_Delivery_Sheets']
            final_delivery_images = calculations['Print_Delivery_Images'] + calculations['Email_Delivery_Images']
            calculations.update({
                'Final_Delivery_Total_Documents': final_delivery_documents,
                'Final_Delivery_Total_Sheets': final_delivery_sheets,
                'Final_Delivery_Total_Images': final_delivery_images
            })
            if email_mail1_docs > 0:
                print(f"Detected Email/Mail 1: {email_mail1_docs} documents")
        except (ValueError, TypeError, ZeroDivisionError) as e:
            print(f"Error in calculations: {e}") 
            return {}
          
        return calculations

    def generate_formatted_report_from_template(self, calculations):
        """Generate formatted report using the report template configuration"""
        try:
            template = self.config.get('report_template', {})
            sections = template.get('sections', [])
            
            # Table formatting setup
            col_widths = [61, 10, 8, 8, 8]  # Column widths
            total_width = sum(col_widths) + len(col_widths) + 1
            
            report_lines = []
            
            # Header
            header_line = "+" + "+".join(["-" * width for width in col_widths]) + "+"
            report_lines.append(header_line)
            
            # Column headers
            headers = ["Description", "Packages", "Sheets", "Images", "Archive"]
            header_row = "|"
            for i, header in enumerate(headers):
                if i == 0:  # Description column - left aligned
                    header_row += f" {header:<{col_widths[i]-1}} |"
                else:  # Numeric columns - right aligned
                    header_row += f" {header:>{col_widths[i]-1}} |"
            report_lines.append(header_row)
            report_lines.append(header_line)
            
            # Process each section
            for section in sections:
                section_rows = section.get('rows', [])
                
                for row in section_rows:
                    description = row.get('description', '')
                    packages_key = row.get('packages', '')
                    sheets_key = row.get('sheets', '')
                    images_key = row.get('images', '')
                    archive_key = row.get('archive', '')
                    
                    # Get values from calculations
                    packages_val = calculations.get(packages_key, '') if packages_key else ''
                    sheets_val = calculations.get(sheets_key, '') if sheets_key else ''
                    images_val = calculations.get(images_key, '') if images_key else ''
                    archive_val = calculations.get(archive_key, '') if archive_key else ''
                    
                    # Handle special archive calculations
                    if archive_key.startswith('+'):
                        archive_field = archive_key[1:]  # Remove the '+' prefix
                        archive_val = f"+{calculations.get(archive_field, 0)}"
                    elif archive_key and archive_key in calculations:
                        archive_val = calculations[archive_key]
                    elif archive_key == "0":
                        archive_val = "0"
                    
                    # Format the row
                    row_line = "|"
                    row_line += f" {description:<{col_widths[0]-1}} |"
                    row_line += f" {str(packages_val):>{col_widths[1]-1}} |" if packages_val != '' else f" {'':{col_widths[1]-1}} |"
                    row_line += f" {str(sheets_val):>{col_widths[2]-1}} |" if sheets_val != '' else f" {'':{col_widths[2]-1}} |"
                    row_line += f" {str(images_val):>{col_widths[3]-1}} |" if images_val != '' else f" {'':{col_widths[3]-1}} |"
                    row_line += f" {str(archive_val):>{col_widths[4]-1}} |" if archive_val != '' else f" {'':{col_widths[4]-1}} |"
                    
                    report_lines.append(row_line)
                
                # Add section separator
                report_lines.append(header_line)
            
            # Final bottom border
            report_lines.append(header_line)
            
            return '\n'.join(report_lines)
            
        except Exception as e:
            print(f"Error generating formatted report from template: {e}")
            return ""

    def save_report_to_s3(self, report_content, s3_path):
        """Save report content to S3"""
        try:
            bucket, key = self.parse_s3_path(s3_path)
            print(f"Saving report to S3: bucket={bucket}, key={key}")
            
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
        inputFileName = os.getenv("inputFileName")
        bucket_name = os.getenv("bucket_name")
        transactionId = os.getenv("transactionId")
        xml_file_key  = os.getenv("xml_file_key")
        config_s3_path = os.getenv("config_s3_path")
        output_s3_path = os.getenv("output_s3_path")
        
        # Initialize processor with bucket name
        processor = XMLProcessor({}, bucket_name)
        # Read content from S3 using processor's method
        print(f"Reading config from S3: {config_s3_path}")
        config_content = processor.read_s3_file(config_s3_path)
        config = json.loads(config_content)
        
        # Update processor with loaded config
        processor.config = config
        
        # Process XML file and get calculations (UC_FILE_NAME is extracted here)
        calculations = processor.process_xml_file(xml_file_key)
        
        # Get UC_FILE_NAME from TLE data that was already extracted during processing
        uc_file_name = getattr(processor, 'uc_file_name', 'unknown')
        
        # Generate dynamic output filename based on UC_FILE_NAME
        if uc_file_name and uc_file_name != 'unknown':
            dynamic_filename = f"{uc_file_name}_report.txt"
            full_output_path = f"{output_s3_path}{dynamic_filename}"  # No need for extra slash
            print(f"Generated dynamic output filename: {dynamic_filename}")
        else:
            # Fallback to default filename if UC_FILE_NAME not found
            full_output_path = f"{output_s3_path}janus-confirms-report.txt"  # No need for extra slash
            print("Using default output filename (UC_FILE_NAME not found)")
        
        wip_base = "/".join(config_s3_path.split("/")[:2])
        wip_path = f"{wip_base}/wip/{transactionId}/{dynamic_filename}"
        
        # Generate formatted table report using template
        report_content = processor.generate_formatted_report_from_template(calculations)
        
        # Save report to S3 with dynamic filename
        if full_output_path:
            processor.save_report_to_s3(report_content, full_output_path)
            processor.save_report_to_s3(report_content, wip_path)
        
        print("Job completed successfully")
        print(f"Report saved as: {full_output_path}")
        
        return { 'statusCode': 200 }
    except Exception as e:
        print(f"Glue job failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()