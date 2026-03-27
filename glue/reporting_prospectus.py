"""
Janus Henderson Prospectus Report Generator
"""
import json, csv, os, sys, xml.etree.ElementTree as ET
from datetime import datetime
from typing import Dict, List, Any, Optional
import tempfile
import re
try:
    import boto3
except ImportError:
    boto3 = None
try:
    import pandas as pd
except ImportError:
    pd = None

class JanusReportGenerator:
    def _resolve_s3_uri(self, uri: str) -> str:
        """Download s3:// URI to a temp file and return local path."""
        if not uri or not uri.startswith("s3://"):
            return uri
        if not boto3:
            print(f"WARNING: boto3 not available; cannot fetch {uri}")
            return uri
        try:
            bucket_key = uri[5:]
            bucket, key = bucket_key.split("/", 1)
            tmp_fd, tmp_path = tempfile.mkstemp(prefix="janus_", suffix=os.path.basename(key))
            os.close(tmp_fd)
            boto3.client("s3").download_file(bucket, key, tmp_path)
            return tmp_path
        except Exception as e:
            print(f"WARNING: Failed to download {uri}: {e}")
            return uri

    def __init__(self, config_path: str = None, s3_bucket: str = None):
        """Initialize strictly from provided configuration file (no fallback)."""
        if not config_path:
            raise ValueError("Configuration path is required (no fallback).")
        local_config = self._resolve_s3_uri(config_path)
        if not (local_config and os.path.exists(local_config)):
            raise FileNotFoundError(f"Configuration file not accessible: {config_path}")

        with open(local_config, 'r') as f:
            self.config = json.load(f)

        self.report_settings = self.config.get('report_settings', {})
        self.field_mappings = self.config.get('field_mappings', {})
        self.json_paths = self.config.get('json_field_paths', {})
        self.cusip_csv_config = self.config.get('cusip-stock_code_mapping_csv', {})

        # Initialize XML metadata values
        self.xml_corp = None
        self.xml_cycle = None
        self.xml_run_date = None  # Add run date storage
        
        # Initialize CUSIP mapping
        self.cusip_mapping = {}
        # Store S3 bucket if provided
        self.s3_bucket = s3_bucket  # ensure bucket stored

        self.load_cusip_mapping()

    def normalize_cusip(self, cusip_value: str) -> str:
        """Normalize CUSIP value by converting scientific notation to standard format"""
        if not cusip_value:
            return ""
        
        cusip_str = str(cusip_value).strip()
        
        # Handle scientific notation (e.g., 4.7103E+209 -> 47103E205)
        if re.match(r'^[0-9.]+e[+-][0-9]+$', cusip_str.lower()):
            try:
                # Parse the scientific notation manually for better control
                lower_str = cusip_str.lower()
                if 'e+' in lower_str:
                    base_str, exp_str = lower_str.split('e+')
                    exponent = int(exp_str)
                    
                    # Special handling for CUSIP-like patterns
                    base_no_decimal = base_str.replace('.', '')
                    
                    # Try multiple strategies based on the exponent value
                    if exponent >= 200:
                        # For large exponents, likely represents letter codes
                        if len(base_no_decimal) >= 5:
                            base_part = base_no_decimal[:5]  # First 5 digits
                            
                            # Map exponent to letter + number
                            if exponent == 209:
                                return f"{base_part}E205"
                            elif exponent == 205:
                                return f"{base_part}E205"
                            elif 200 <= exponent <= 299:
                                # E series (200-299 range)
                                letter = 'E'
                                suffix_variants = [
                                    str(exponent)[-2:],
                                    str(exponent)[-3:],
                                    f"{exponent-200:02d}",
                                    f"{exponent-205:02d}" if exponent >= 205 else f"0{exponent-200}"
                                ]
                                for suffix in suffix_variants:
                                    if suffix.isdigit() and len(suffix) <= 3:
                                        result = f"{base_part}{letter}{suffix.zfill(2) if len(suffix) <= 2 else suffix}"
                                        return result
                    
                    # Fallback: try standard mathematical conversion
                    num_value = float(cusip_str)
                    if num_value < 1e10:
                        int_result = int(num_value)
                        if len(str(int_result)) <= 9:
                            return str(int_result).zfill(9)
                
                return cusip_str.upper()
                
            except (ValueError, OverflowError):
                return cusip_str.upper()
        
        # Handle regular CUSIP format
        if cusip_str.isdigit() and len(cusip_str) <= 9:
            return cusip_str.zfill(9)
        
        # Return as-is for alphanumeric CUSIPs
        return cusip_str.upper()

    def load_cusip_mapping(self) -> None:
        """Load CUSIP to Stock Code mapping from configured file (no fallback)."""
        csv_folder = self.cusip_csv_config.get('prospectus_csv_input_path', 'jhi/confirms/reports')
        csv_filename = self.cusip_csv_config.get('cusip_mapping')
        if not csv_filename:
            print("WARNING: cusip_mapping filename missing in config.")
            return
        s3_bucket = self.s3_bucket or 'br-icsdev-dpmdi-dataingress-us-east-1-s3'
        s3_uri = f"s3://{s3_bucket}/{csv_folder}/{csv_filename}"
        resolved = self._resolve_s3_uri(s3_uri)

        if not os.path.exists(resolved):
            print(f"WARNING: CUSIP mapping file not found: {s3_uri}")
            return

        try:
            if csv_filename.lower().endswith('.xlsx'):
                if not pd:
                    print("WARNING: pandas not available to read XLSX")
                    return
                df = pd.read_excel(resolved, dtype=str)
                for idx, row in df.iterrows():
                    cusip_raw = (row.get('CUSIP') or row.get('cusip') or '').strip()
                    stock_code = (row.get('Stock Code') or row.get('stock_code') or row.get('STOCK_CODE') or '').strip()
                    if not cusip_raw or not stock_code:
                        continue
                    
                    cusip_normalized = self.normalize_cusip(cusip_raw)
                    if cusip_normalized:
                        self.cusip_mapping[cusip_normalized] = stock_code
            else:
                with open(resolved, 'r', encoding='utf-8') as f:
                    sample = f.read(1024); f.seek(0)
                    delimiter = '\t' if '\t' in sample else ','
                    reader = csv.DictReader(f, delimiter=delimiter)
                    for row in reader:
                        cusip_raw = (row.get('CUSIP') or row.get('cusip') or row.get('Cusip') or '').strip()
                        stock_code = (row.get('Stock Code') or row.get('stock_code') or
                                      row.get('Stock_Code') or row.get('STOCK_CODE') or '').strip()
                        if not cusip_raw or not stock_code:
                            continue
                        
                        cusip_normalized = self.normalize_cusip(cusip_raw)
                        if cusip_normalized:
                            self.cusip_mapping[cusip_normalized] = stock_code
            
            print(f"Loaded {len(self.cusip_mapping)} CUSIP mappings.")
                
        except Exception as e:
            print(f"WARNING: Failed to parse CUSIP mapping file: {e}")

    def get_stock_code_from_cusip(self, customer_data: Dict, account_data: Dict, fund_data: Dict) -> str:
        """Get stock code by mapping CUSIP from fund data"""
        cusip_raw = fund_data.get('CUSIP', '').strip()
        
        if not cusip_raw or cusip_raw == 'NO_CUSIP':
            return self.report_settings.get('stock_code', 'JHI')
        
        cusip_normalized = self.normalize_cusip(cusip_raw)
        stock_code = self.cusip_mapping.get(cusip_normalized)
        
        if stock_code:
            return stock_code
        else:
            return self.report_settings.get('stock_code', 'JHI')

    def _sanitize_xml(self, path: str) -> Optional[str]:
        """Create a sanitized temp XML file starting at first '<' if file has leading junk."""
        try:
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                data = f.read()
            first_tag = data.find('<')
            if first_tag > 0:
                cleaned = data[first_tag:]
                tmp_fd, tmp_path = tempfile.mkstemp(prefix="janus_xml_", suffix=".xml")
                os.close(tmp_fd)
                with open(tmp_path, 'w', encoding='utf-8') as out:
                    out.write(cleaned)
                return tmp_path
        except Exception as e:
            print(f"XML sanitize failed: {e}")
        return None

    def load_xml_metadata(self, xml_path: str) -> Dict:
        """Load and parse XML metadata file to extract CORP and CYCLE from filename and UC_FILE_NAME from properties."""
        if not xml_path:
            print("WARNING: No XML metadata path provided")
            return {}
        
        local_xml = self._resolve_s3_uri(xml_path)
        if not (local_xml and os.path.exists(local_xml)):
            print(f"XML metadata file not found: {xml_path}")
            return {}

        metadata = {}
        
        # Extract CORP, CYCLE, and run date from XML filename
        xml_filename = os.path.basename(xml_path)
        
        # Pattern: 2024Q4_20251105_A_JANUS.CONFIRMS.10072025.080658.tst.afp.xml
        # filename_pattern = r'^([^_]+)_(\d{8})_([A-Z])_.*\.xml$'
        filename_pattern = self.config.get('xml_metadata', {}).get('filename_pattern')
        print("filename_pattern",filename_pattern)
        match = re.match(filename_pattern, xml_filename)
        
        if match:
            corp = match.group(1)
            run_date = match.group(2)
            cycle = match.group(3)
            
            self.xml_corp = corp
            self.xml_cycle = cycle
            self.xml_run_date = run_date
            metadata['CORP'] = corp
            metadata['CYCLE'] = cycle
            metadata['RUN_DATE'] = run_date
            
            print(f"Extracted from XML filename - CORP: {corp}, CYCLE: {cycle}, RUN_DATE: {run_date}")
        else:
            print(f"WARNING: XML filename '{xml_filename}' doesn't match expected pattern")
            return {}

        # Parse XML to get UC_FILE_NAME from TLE properties
        def _parse(path: str):
            return ET.parse(path).getroot()

        try:
            root = _parse(local_xml)
        except Exception as e:
            sanitized = self._sanitize_xml(local_xml)
            if not sanitized:
                print("XML parsing aborted")
                return metadata
            try:
                root = _parse(sanitized)
            except Exception as e2:
                print(f"Sanitized XML parse failed: {e2}")
                return metadata
        
        # Look for UC_FILE_NAME in TLE properties
        uc_file_name = None
        
        for statement in root.findall('.//Statement'):
            props = statement.find('Properties')
            if props is not None:
                for prop in props.findall('Property'):
                    name = prop.get('Name')
                    val = prop.get('Value')
                    
                    if name == 'UC_FILE_NAME':
                        uc_file_name = val
                        metadata['UC_FILE_NAME'] = val
                        print(f"Found UC_FILE_NAME: {val}")
                        break
                        
            if uc_file_name:
                break
        
        if not uc_file_name:
            print("WARNING: UC_FILE_NAME not found in XML")
        
        # Construct JSON filename from UC_FILE_NAME
        if uc_file_name:
            json_filename = f"{uc_file_name}.json"
            metadata['JSON_FILENAME'] = json_filename

        # Store XML run date in report settings
        if self.xml_run_date:
            self.report_settings['xml_run_date'] = self.xml_run_date
        
        # Update input JSON key based on UC_FILE_NAME
        if uc_file_name:
            input_json_path = self.report_settings.get('input_json_path', 'jhi/confirms/reports/')
            json_key = f"{input_json_path}{json_filename}"
            self.report_settings['input_json_key'] = json_key
            
        return metadata


    def load_json_data(self, json_path: str) -> Dict:
        """Load the JSON data file (supports s3://)"""
        print(f"Loading JSON data from: {json_path}")
        local_json = self._resolve_s3_uri(json_path)
        if not (local_json and os.path.exists(local_json)):
            raise FileNotFoundError(f"Resolved JSON not accessible: {json_path}")
        with open(local_json, 'r') as f:
            return json.load(f)
    
    def format_date(self, date_str: str) -> str:
        """Convert date from M/D/YY to YYYYMMDD format"""
        if not date_str or date_str.strip() == "":
            return ""
        
        try:
            # Handle formats like "8/20/25"
            parts = date_str.split('/')
            if len(parts) == 3:
                month, day, year = parts
                # Convert 2-digit year to 4-digit
                year_int = int(year)
                if year_int > 50:  # Century cutoff
                    year = f"19{year}"
                else:
                    year = f"20{year}"
                
                return f"{year}{month.zfill(2)}{day.zfill(2)}"
        except Exception as e:
            print(f"Date format error: {e}")
        
        return ""
    
    def pad_account_number(self, account_num: str) -> str:
        """Pad account number to 11 digits"""
        if not account_num:
            return ""
        # Remove 'x' characters and pad with zeros
        clean_num = account_num.replace('x', '').strip()
        return clean_num.zfill(11)
    
    def get_investor_id(self, customer_data: Dict) -> str:
        """Extract investor ID based on business rules"""
        header_info = customer_data.get('headerInfo', {})
        investor_id = header_info.get('investorID', '')
        print(f"Found investor ID: {investor_id}")
        return investor_id
    
    def get_email_template(self, customer_data: Dict) -> str:
        """Determine email template based on delivery method"""
        if not self.is_email_delivery(customer_data):
            return ''
        
        doc_nop = customer_data.get('DOC-NOP', {})
        doc_sub_type = doc_nop.get('UC_DOCUMENT_SUB_TYPE', '')
        
        if doc_sub_type == 'NEWACCOUNT':
            return 'Existing New Account Setup'
        elif doc_sub_type == 'ADDMAIL':
            return 'Standard Email Template'
        else:
            return 'Standard Email Template'
    
    def is_email_delivery(self, customer_data: Dict) -> bool:
        """Determine if delivery method is email"""
        header_info = customer_data.get('headerInfo', {})
        doc_nop = customer_data.get('DOC-NOP', {})
        
        # Check for email ID
        if header_info.get('emailID'):
            return True
        
        # Check delivery preferences
        if doc_nop.get('UC_DOCUMENT_DELIVERY_PREFERENCE') == 'EMAIL':
            return True
            
        if doc_nop.get('CUSTOM_EMAIL_PRNT_IND') == 'EMAIL':
            return True
            
        # Check for E-Delivery flag
        if 'E-Delivery' in header_info:
            return True
            
        return False
    
    def determine_doc_type(self, customer_data: Dict) -> str:
        """Determine document type (Print or Email)"""
        return 'Email' if self.is_email_delivery(customer_data) else 'Print'

    def get_registration_lines(self, customer_data: Dict, account_data: Dict) -> List[str]:
        """Get registration lines from various sources"""
        lines = []
        for i in customer_data['DOC-NOP']['PH_SUBDOCUMENT_COMPOSITE_INDEX']:
            if i[:15].strip() == account_data['unmaskedAccountNumber'].strip():
                # First try account registration names
                reg_names = [i[j:j+50] for j in range(51, len(i), 50)]
                for name in reg_names:
                    if name and name.strip():
                        lines.append(name.strip())
                break
        # Pad to exactly 5 lines
        while len(lines) < 7:
            lines.append('')
        
        return lines[:7]

    def get_comp_index_date(self, customer_data: Dict, account_data: Dict, fund_data: Dict, date_type:str) -> str:
        """Get registration lines from various sources"""
        lines = []
        for i in customer_data['DOC-NOP']['PH_SUBDOCUMENT_COMPOSITE_INDEX']:
            if i[:15].strip() == account_data['unmaskedAccountNumber'].strip() and i[15:21].strip() == fund_data['fundId'].strip():
                # Check date type
                if date_type == 'establish_date':
                    return i[31:41].strip()
                elif date_type == 'confirmed_date':
                    return i[41:51].strip()
        return ""


    def extract_records(self, data: Dict) -> List[Dict]:
        """Extract all records from JSON data"""
        records = []
        
        customer_root = data.get('customerroot', [])
        
        for idx, customer_entry in enumerate(customer_root):
            customer_data = customer_entry.get('customers', {})
            accounts = customer_data.get('Accounts', {}).get('accounts', [])
            
            for account_idx, account in enumerate(accounts):
                funds = account.get('funds', [])
                
                # Create one record per fund (CUSIP)
                for fund_idx, fund in enumerate(funds):
                    record = self.create_record(customer_data, account, fund)
                    records.append(record)
        
        return records

    def get_field_value(self, field_name: str, customer_data: Dict, account_data: Dict, fund_data: Dict) -> str:
        """Get field value based on config mapping"""
        if field_name not in self.field_mappings:
            return ""
        
        mapping = self.field_mappings[field_name]
        mapping_type = mapping.get('type', 'static')
        
        if mapping_type == 'static':
            # Static value from report settings
            setting_key = mapping.get('value', '')
            
            if setting_key == 'fulfilled_date' and self.report_settings.get(setting_key) == 'auto':
                return datetime.now().strftime('%Y%m%d')
            elif setting_key == 'corp_cycle':
                # Always use XML-derived corp_cycle if XML was loaded
                if self.xml_corp and self.xml_cycle:
                    return f"{self.xml_corp}{self.xml_cycle}"
                else:
                    # Use config default only if no XML was provided
                    return self.report_settings.get(setting_key, mapping.get('default', ''))
            elif setting_key == 'stock_code':
                # Use CUSIP mapping for stock code
                return self.get_stock_code_from_cusip(customer_data, account_data, fund_data)
            else:
                return self.report_settings.get(setting_key, mapping.get('default', ''))
        
        elif mapping_type == 'json_path':
            # Extract from JSON using path
            path = mapping.get('path', '')
            default = mapping.get('default', '')
            transform = mapping.get('transform', '')
            
            value = self.extract_json_value(path, customer_data, account_data, fund_data)
            
            # Apply transformation
            if transform == 'strip':
                value = value.strip() if value else ''
            elif transform == 'pad_account_number':
                value = self.pad_account_number(value)
            elif transform == 'format_date':
                value = self.format_date(value)
            
            return value if value else default
        
        elif mapping_type == 'function':
            # Call specific function
            function_name = mapping.get('function', '')
            params = mapping.get('params', {})
            
            if function_name == 'get_email_template':
                return self.get_email_template(customer_data)
            elif function_name == 'determine_doc_type':
                return self.determine_doc_type(customer_data)
            elif function_name == 'get_registration_line':
                line_num = params.get('line_number', 0)
                reg_lines = self.get_registration_lines(customer_data, account_data)
                return reg_lines[line_num] if line_num < len(reg_lines) else ''
            elif function_name == 'get_stock_code_from_cusip':
                return self.get_stock_code_from_cusip(customer_data, account_data, fund_data)
            elif function_name == 'get_fund_name':
                return self.get_fund_name(fund_data)
            elif function_name == 'get_comp_index_date':
                return self.get_comp_index_date(customer_data, account_data, fund_data, date_type=mapping.get('date_type', ''))
            elif function_name == 'get_comp_index_date':
                return self.get_comp_index_date(customer_data, account_data, fund_data, date_type=mapping.get('date_type', ''))
                    
        # Default fallback for unmapped types
        return ""

    def extract_json_value(self, path: str, customer_data: Dict, account_data: Dict, fund_data: Dict) -> str:
        """Extract value from JSON using dot notation path"""
        try:
            # Map path context to actual data
            if path.startswith('headerInfo.'):
                data = customer_data.get('headerInfo', {})
                remaining_path = path[11:]  # Remove 'headerInfo.'
            elif path.startswith('account.'):
                data = account_data
                remaining_path = path[8:]  # Remove 'account.'
            elif path.startswith('fund.'):
                data = fund_data
                remaining_path = path[5:]  # Remove 'fund.'
            elif path.startswith('DOC-NOP.'):
                data = customer_data.get('DOC-NOP', {})
                remaining_path = path[8:]  # Remove 'DOC-NOP.'
            else:
                data = customer_data
                remaining_path = path
            
            # Navigate through nested structure
            for key in remaining_path.split('.'):
                if isinstance(data, dict) and key in data:
                    data = data[key]
                else:
                    return ""
            
            return str(data) if data is not None else ""
            
        except Exception as e:
            print(f"Error extracting path '{path}': {e}")
            return ""

    def create_record(self, customer_data: Dict, account_data: Dict, fund_data: Dict) -> Dict:
        """Create a single record for the report using config-driven field mapping"""
        record = {}
        
        # Get all field names from config
        for field_name in self.field_mappings.keys():
            record[field_name] = self.get_field_value(field_name, customer_data, account_data, fund_data)
        
        return record

    def get_fund_name(self, fund_data: Dict) -> str:
        """Extract fund name from fund data"""
        for k in ("fundName", "name", "description", "longName", "title"):
            v = fund_data.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        return ""

    def generate_report(self, json_path: str, output_path: str = None, xml_metadata_path: str = None) -> str:
        """Generate report. Output path ALWAYS comes from config report_output_key pattern."""
        print("JANUS HENDERSON PROSPECTUS REPORT GENERATOR")
        
        # Load XML metadata first to get CORP and CYCLE
        xml_metadata = {}
        if xml_metadata_path:
            xml_metadata = self.load_xml_metadata(xml_metadata_path)

        # Load data
        data = self.load_json_data(json_path)
        
        # Extract records
        records = self.extract_records(data)
        
        if not records:
            print("WARNING: No records found to process!")
            return ""
            
        # Always derive path from config
        key_pattern = self.report_settings.get('report_output_key')
        if not key_pattern:
            raise ValueError("report_output_key missing in config")
        
        # Use XML run date if available, otherwise use current date
        if self.xml_run_date:
            date_for_filename = self.xml_run_date
        else:
            date_for_filename = datetime.now().strftime('%Y%m%d')
        
        # Use the XML components directly - no joining needed
        cycle_base = self.xml_corp if self.xml_corp else ""      # e.g., "2024Q4"
        cycle_suffix = self.xml_cycle if self.xml_cycle else ""  # e.g., "A"
        
        # Format the pattern with the XML-extracted components
        resolved_key = key_pattern.format(
            cycle_base=cycle_base,
            cycle_suffix=cycle_suffix,
            date=date_for_filename
        )
        
        self.report_settings['resolved_report_output_key'] = resolved_key
        output_path = resolved_key
        out_dir = os.path.dirname(output_path)
        if out_dir and not os.path.exists(out_dir):
            os.makedirs(out_dir, exist_ok=True)
        ordered_fields = self.config.get('output_field_order') or list(self.field_mappings.keys())
        
        # Enhanced output configuration
        output_format = self.config.get('output_format', {})
        include_header = output_format.get('include_header', True)
        quote_all = output_format.get('quote_all_fields', True)
        encoding = output_format.get('encoding', 'utf-8')
        add_header_separator = output_format.get('add_header_separator', True)
        
        print(f"Writing report to: {output_path}")
        quoting = csv.QUOTE_ALL if quote_all else csv.QUOTE_MINIMAL
        with open(output_path, 'w', newline='', encoding=encoding) as f:
            writer = csv.writer(f, quoting=quoting)
            
            # Write header if enabled
            if include_header:
                writer.writerow(ordered_fields)
                
                # Add separator line between header and data
                if add_header_separator:
                    separator_row = ['-' * 20 for _ in ordered_fields]
                    writer.writerow(separator_row)
                    
            # Write data rows
            for rec in records:
                writer.writerow([rec.get(fn, '') for fn in ordered_fields])
        
        s3_key = self.report_settings.get('resolved_report_output_key')
        if self.s3_bucket and s3_key and boto3:
            try:
                print(f"Uploading to S3: s3://{self.s3_bucket}/{s3_key}")
                boto3.client('s3').upload_file(output_path, self.s3_bucket, s3_key)
                print("S3 upload: SUCCESS")
            except Exception as e:
                print(f"S3 upload: FAILED ({e})")
        elif self.s3_bucket and s3_key and not boto3:
            print("S3 upload skipped (boto3 not available)")
            
        print(f"Report completed - {len(records)} records")
        return output_path

def set_job_params_as_env_vars():
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]
            value = sys.argv[i + 1]
            os.environ[key] = value

def main():
    """Main execution without CLI parsing (config-driven only)"""
    set_job_params_as_env_vars()
    inputFileName = os.getenv('inputFileName')
    s3_bucket = os.getenv('s3_bucket')
    xml_file_key = os.getenv('xml_file_key')
    config_key = os.getenv('config_key')
    
    config_file = f's3://{s3_bucket}/{config_key}'
    xml_file = f"s3://{s3_bucket}/{xml_file_key}"
    output_file = None
    try:
        generator = JanusReportGenerator(config_file, s3_bucket=s3_bucket)
        
        # Load XML metadata to construct input_json_key dynamically
        if xml_file:
            xml_metadata = generator.load_xml_metadata(xml_file)
        
        # The input_json_key will be constructed dynamically from XML metadata
        input_json_key = generator.report_settings.get('input_json_key')
        if not input_json_key:
            print("Error: 'input_json_key' not constructed from XML metadata.")
            return 1
        input_file = f"s3://{s3_bucket}/{input_json_key}"

        print(f"Processing: {input_file}")

        output_path = generator.generate_report(input_file, output_file, xml_file)
        if output_path:
            print("Report successfully generated!")
            return 0
        else:
            print("Report generation failed!")
            return 1
    except FileNotFoundError as e:
        print(f"Config/File not found: {e}")
        return 1
    except json.JSONDecodeError as e:
        print(f"JSON parsing error: {e}")
        return 1
    except Exception as e:
        print(f"Error generating report: {e}")
        import traceback
        traceback.print_exc()
        return 1

inputFileName=''
if __name__ == "__main__":
    exit_code = main(); sys.exit(exit_code)
