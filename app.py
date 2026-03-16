from flask import Flask, render_template, render_template_string, request, redirect, flash, send_file, jsonify
import pandas as pd
import io
import math
import os
import json
from pathlib import Path
import tempfile
from werkzeug.utils import secure_filename
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime, timedelta
import re
import csv
from urllib.parse import urlparse

load_dotenv()

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "your-secret-key-change-this")

# Configuration
PER_PAGE = 100
BASE_DIR = Path(__file__).parent
CONFIG_PATH = BASE_DIR / "sheet_mapping_config.json"

# Excel folder path - CLOUD SAFE
EXCEL_FOLDER_PATH = BASE_DIR / "excel_data"

# Create excel folder if it doesn't exist
EXCEL_FOLDER_PATH.mkdir(exist_ok=True)

# Excel file paths - derived from EXCEL_FOLDER_PATH
MASTER_URL_DATA_PATH = EXCEL_FOLDER_PATH / "Website_mapping.xlsx"
BANK_NAME_MAPPING_PATH = EXCEL_FOLDER_PATH / "bank_name.xlsx"
IFSC_MAPPING_PATH = EXCEL_FOLDER_PATH / "ifsc_mapping.xlsx"

# Supabase Clients
supabase: Client = create_client(
    os.environ.get("SUPABASE_URL"),
    os.environ.get("SUPABASE_KEY")
)

# Social Media Accounts Supabase Client
SOCIAL_SUPABASE_URL = os.environ.get("SOCIAL_SUPABASE_URL")
SOCIAL_SUPABASE_KEY = os.environ.get("SOCIAL_SUPABASE_KEY")

social_supabase: Client = create_client(
    SOCIAL_SUPABASE_URL,
    SOCIAL_SUPABASE_KEY
)

PLATFORM_OPTIONS = [
    "Telegram", "WhatsApp", "Facebook", "Instagram", 
    "Threads", "YouTube", "X"
]

SCAM_TYPE_OPTIONS = [
    "Investment Scam", "Carding Scam", "Shopping Scam", 
    "Job Scam", "Subscription Scam", "Loan Scam", 
    "Currency Exchange Scam", "Fake Account Selling Scam"
]

# Social Media Platform Options - FIXED: all platforms included
SOCIAL_PLATFORM_OPTIONS = [
    "Facebook", "Amazon", "Instagram", "Telegram", "WhatsApp"
]

# Platform-specific account status options
PLATFORM_ACCOUNT_STATUS = {
    "Facebook": ["Active", "Block", "Restricted", "Permanent Block"],
    "Instagram": ["Active", "Block", "Permanent Block"],
    "Telegram": ["Active", "Frozen", "Permanent Block"],
    "WhatsApp": ["Active", "Block", "Permanent Block", "Restricted"],
    "Amazon": ["Active", "Block", "Permanent Block"]
}

# Define the 36 required columns
REQUIRED_COLUMNS = [
    'customer',
    'package_name',
    'channel_name',
    'bank_account_number',
    'bank_name',
    'upi_vpa',
    'ac_holder_name',
    'screenshot',
    'platform',
    'search_for',
    'status',
    'upi_bank_account_wallet',
    'priority',
    'flag',
    'cessation',
    'reviewed_status',
    'handle',
    'origin',
    'payment_gateway_name',
    'category_of_website',
    'screenshot_case_report_link',
    'payment_gateway_intermediate_url',
    'neft_imps',
    'transaction_method',
    'scam_type',
    'ifsc_code',
    'bank_branch_details',
    'payment_gateway_url',
    'upi_url',
    'website_url',
    'inserted_date',
    'reported_earlier',
    'approvd_status',
    'feature_type',
    'case_generated_time',
    'web_contact_no'
]

# Sheet type display names
SHEET_TYPES = {
    'upi': 'UPI_AML',
    'investment': 'Investment_Scam',
    'messaging': 'Messaging_Channel'
}

# Global variables for loaded data
MASTER_URL_DATA = {}
BANK_NAME_MAPPING = {}
IFSC_MAPPING = {}

def load_excel_data():
    """Load data from Excel files with cloud-safe paths"""
    global MASTER_URL_DATA, BANK_NAME_MAPPING, IFSC_MAPPING
    
    try:
        print(f"Loading Excel files from: {EXCEL_FOLDER_PATH}")
        print(f"Files in folder: {list(EXCEL_FOLDER_PATH.glob('*.xlsx'))}")
        
        # Load Master URL Data
        if MASTER_URL_DATA_PATH.exists():
            print(f"Loading master URL data from: {MASTER_URL_DATA_PATH}")
            df_master = pd.read_excel(MASTER_URL_DATA_PATH)
            MASTER_URL_DATA = {}
            for _, row in df_master.iterrows():
                url = str(row.get('website_url', '')).strip()
                if url and url.lower() not in ['na', 'nan', '']:
                    url_clean = url.lower().strip()
                    MASTER_URL_DATA[url_clean] = {
                        "origin": str(row.get('origin', 'NA')).strip(),
                        "category_of_website": str(row.get('category_of_website', 'NA')).strip()
                    }
            print(f"Loaded {len(MASTER_URL_DATA)} URLs from master data")
        else:
            print(f"Master URL data file not found: {MASTER_URL_DATA_PATH}")
            MASTER_URL_DATA = {}
        
        # Load Bank Name Mapping
        if BANK_NAME_MAPPING_PATH.exists():
            print(f"Loading bank mapping from: {BANK_NAME_MAPPING_PATH}")
            try:
                df_bank = pd.read_excel(BANK_NAME_MAPPING_PATH)
                BANK_NAME_MAPPING = {}
                
                print(f"Bank mapping columns: {list(df_bank.columns)}")
                
                key_col = None
                bank_col = None
                
                for col in df_bank.columns:
                    col_lower = str(col).lower()
                    if 'key' in col_lower or 'handle' in col_lower or 'upi' in col_lower:
                        key_col = col
                    elif 'bank' in col_lower and 'name' in col_lower:
                        bank_col = col
                
                if key_col is None and len(df_bank.columns) > 0:
                    key_col = df_bank.columns[0]
                if bank_col is None and len(df_bank.columns) > 1:
                    bank_col = df_bank.columns[1]
                
                if key_col and bank_col:
                    print(f"Using columns - Key: '{key_col}', Bank: '{bank_col}'")
                    
                    for _, row in df_bank.iterrows():
                        key_val = str(row.get(key_col, '')).strip().lower()
                        bank_val = str(row.get(bank_col, 'NA')).strip()
                        
                        if key_val and key_val.lower() not in ['na', 'nan', '']:
                            BANK_NAME_MAPPING[key_val] = bank_val
                    
                    print(f"Loaded {len(BANK_NAME_MAPPING)} bank mappings")
                    
                    if len(BANK_NAME_MAPPING) > 0:
                        sample_items = list(BANK_NAME_MAPPING.items())[:3]
                        print(f"Sample bank mappings: {sample_items}")
                    else:
                        print("Warning: No bank mappings loaded!")
                        
                else:
                    print("Error: Could not identify bank mapping columns")
                    BANK_NAME_MAPPING = {}
                    
            except Exception as e:
                print(f"Error reading bank mapping file: {e}")
                BANK_NAME_MAPPING = {}
        else:
            print(f"Bank name mapping file not found: {BANK_NAME_MAPPING_PATH}")
            BANK_NAME_MAPPING = {}
        
        # Load IFSC Mapping
        if IFSC_MAPPING_PATH.exists():
            print(f"Loading IFSC mapping from: {IFSC_MAPPING_PATH}")
            try:
                df_ifsc = pd.read_excel(IFSC_MAPPING_PATH)
                IFSC_MAPPING = {}
                
                print(f"IFSC mapping columns: {list(df_ifsc.columns)}")
                
                prefix_col = None
                bank_col = None
                
                for col in df_ifsc.columns:
                    col_lower = str(col).lower()
                    if 'ifsc' in col_lower or 'prefix' in col_lower or 'code' in col_lower:
                        prefix_col = col
                    elif 'bank' in col_lower and 'name' in col_lower:
                        bank_col = col
                
                if prefix_col is None and len(df_ifsc.columns) > 0:
                    prefix_col = df_ifsc.columns[0]
                if bank_col is None and len(df_ifsc.columns) > 1:
                    bank_col = df_ifsc.columns[1]
                
                if prefix_col and bank_col:
                    print(f"Using IFSC columns - Prefix: '{prefix_col}', Bank: '{bank_col}'")
                    
                    for _, row in df_ifsc.iterrows():
                        prefix_val = str(row.get(prefix_col, '')).strip().upper()
                        bank_val = str(row.get(bank_col, 'NA')).strip()
                        
                        if prefix_val and prefix_val.lower() not in ['na', 'nan', '']:
                            IFSC_MAPPING[prefix_val] = bank_val
                    
                    print(f"Loaded {len(IFSC_MAPPING)} IFSC mappings")
                    
                    if len(IFSC_MAPPING) > 0:
                        sample_items = list(IFSC_MAPPING.items())[:3]
                        print(f"Sample IFSC mappings: {sample_items}")
                    else:
                        print("Warning: No IFSC mappings loaded!")
                else:
                    print("Error: Could not identify IFSC mapping columns")
                    IFSC_MAPPING = {}
                    
            except Exception as e:
                print(f"Error reading IFSC mapping file: {e}")
                IFSC_MAPPING = {}
        else:
            print(f"IFSC mapping file not found: {IFSC_MAPPING_PATH}")
            IFSC_MAPPING = {}
            
    except Exception as e:
        print(f"Error loading Excel data: {e}")
        import traceback
        traceback.print_exc()
        MASTER_URL_DATA = {}
        BANK_NAME_MAPPING = {}
        IFSC_MAPPING = {}

# Load data on startup
load_excel_data()

# Load configuration
def load_config():
    """Load JSON configuration for sheet mappings"""
    try:
        if CONFIG_PATH.exists():
            with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                return json.load(f)
        else:
            return create_default_config()
    except Exception as e:
        print(f"Error loading config: {e}")
        return create_default_config()

def create_default_config():
    """Create default configuration"""
    default_config = {
        "sheet_mappings": {
            "upi": {
                "name": "UPI (AML)",
                "required_headers": ["UPI", "Screenshot", "Website URL", "Payment Gateway URL", "Transaction Method"],
                "column_mapping": {
                    "UPI": ["upi_vpa", "upi"],
                    "Screenshot": ["screenshot", "image", "proof"],
                    "Website URL": ["website_url", "url", "website"],
                    "Payment Gateway URL": ["payment_gateway_url", "payment_url", "gateway"],
                    "Transaction Method": ["transaction_method", "payment_method", "method"]
                }
            },
            "investment": {
                "name": "Investment Scam",
                "required_headers": ["UPI", "Account Holder Name", "Bank Account Number", "IFSC Code", 
                                   "Website URL", "Payment Gateway URL", "Transaction Method", 
                                   "Screenshot", "Contact Number", "Scam Type"],
                "column_mapping": {
                    "UPI": ["upi_vpa", "upi"],
                    "Account Holder Name": ["ac_holder_name", "account_holder", "holder_name", "customer"],
                    "Bank Account Number": ["bank_account_number", "account_number", "acc_no"],
                    "IFSC Code": ["ifsc_code", "ifsc", "bank_code"],
                    "Website URL": ["website_url", "url", "website"],
                    "Payment Gateway URL": ["payment_gateway_url", "payment_url", "gateway"],
                    "Transaction Method": ["transaction_method", "payment_method", "method"],
                    "Screenshot": ["screenshot", "image", "proof"],
                    "Contact Number": ["web_contact_no", "contact_number", "phone", "mobile"],
                    "Scam Type": ["scam_type", "type", "category"]
                }
            },
            "messaging": {
                "name": "Messaging Channel",
                "required_headers": ["UPI", "Account Holder Name", "Bank Account Number", "IFSC Code",
                                   "Website URL", "Screenshot", "Transaction Method", "Category"],
                "column_mapping": {
                    "UPI": ["upi_vpa", "upi"],
                    "Account Holder Name": ["ac_holder_name", "account_holder", "holder_name"],
                    "Bank Account Number": ["bank_account_number", "account_number", "acc_no"],
                    "IFSC Code": ["ifsc_code", "ifsc", "bank_code"],
                    "Website URL": ["website_url", "url", "website"],
                    "Screenshot": ["screenshot", "image", "proof"],
                    "Transaction Method": ["transaction_method", "payment_method", "method"],
                    "Category": ["category_of_website", "category", "type"]
                }
            }
        },
        "global_settings": {
            "date_format": "%Y-%m-%d",
            "na_values": ["NA", "N/A", "", "null", "NULL", "None", "nan", "NaN", "undefined"],
            "allowed_extensions": [".csv", ".xlsx", ".xls", ".xlsm", ".xlsb", ".ods"],
            "max_file_size_mb": 50
        }
    }
    
    with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
        json.dump(default_config, f, indent=2)
    
    return default_config

def get_allowed_extensions():
    """Get allowed file extensions from config"""
    config = load_config()
    if config and 'global_settings' in config:
        return config['global_settings'].get('allowed_extensions', ['.csv', '.xlsx', '.xls'])
    return ['.csv', '.xlsx', '.xls']

def is_allowed_file(filename):
    """Check if file extension is allowed"""
    if not filename:
        return False
    
    allowed_extensions = get_allowed_extensions()
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in [ext.lstrip('.') for ext in allowed_extensions]

def read_data_file(file_path, file_ext):
    """Read data from various file formats"""
    try:
        file_ext = file_ext.lower()
        
        if file_ext == 'csv':
            encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
            for encoding in encodings:
                try:
                    df = pd.read_csv(file_path, encoding=encoding)
                    return df
                except UnicodeDecodeError:
                    continue
            
            df = pd.read_csv(file_path, encoding=None, engine='python')
            return df
            
        elif file_ext in ['xlsx', 'xls', 'xlsm', 'xlsb']:
            df = pd.read_excel(file_path)
            return df
            
        elif file_ext == 'ods':
            df = pd.read_excel(file_path, engine='odf')
            return df
            
        else:
            raise ValueError(f"Unsupported file format: {file_ext}")
            
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        raise

def get_sheet_headers(sheet_type):
    """Get required headers for a sheet type"""
    config = load_config()
    if not config:
        return []
    
    sheet_config = config['sheet_mappings'].get(sheet_type)
    if not sheet_config:
        return []
    
    return sheet_config.get('required_headers', [])

def standardize_headers(headers, sheet_type):
    """Standardize headers according to sheet type mapping"""
    config = load_config()
    if not config:
        return headers
    
    sheet_config = config['sheet_mappings'].get(sheet_type)
    if not sheet_config:
        return headers
    
    standardized = []
    column_mapping = sheet_config.get('column_mapping', {})
    
    for header in headers:
        header_lower = str(header).lower().strip()
        mapped = False
        
        for target_col, source_cols in column_mapping.items():
            for source_col in source_cols:
                if header_lower == source_col.lower():
                    standardized.append(target_col)
                    mapped = True
                    break
            if mapped:
                break
        
        if not mapped:
            for target_col, source_cols in column_mapping.items():
                for source_col in source_cols:
                    if source_col.lower() in header_lower or header_lower in source_col.lower():
                        standardized.append(target_col)
                        mapped = True
                        break
                if mapped:
                    break
        
        if not mapped:
            standardized.append(header)
    
    return standardized

def clean_value(value):
    """Clean value by removing emojis and standardizing NA values"""
    if pd.isna(value) or value in ["NA", "", None, "null", "NULL", "None", "nan", "NaN", "undefined"]:
        return "NA"
    
    value_str = str(value).strip()
    
    value_str = ''.join(char for char in value_str if ord(char) < 0x10000)
    
    return value_str

def extract_handle(upi_vpa):
    """Extract handle from UPI VPA"""
    upi_vpa = clean_value(upi_vpa)
    
    if upi_vpa == "NA":
        return "NA"
    
    if '@' in upi_vpa:
        handle_part = upi_vpa.split('@')[1]
        if '.' in handle_part:
            handle_part = handle_part.split('.')[0]
        return handle_part.lower()
    return "NA"

def get_bank_name_from_handle(handle, ifsc_code=None):
    """Get bank name from handle using the mapping, with IFSC fallback"""
    if handle != "NA" and handle:
        handle_lower = handle.lower().strip()
        
        if handle_lower in BANK_NAME_MAPPING:
            return BANK_NAME_MAPPING[handle_lower]
        
        for key, value in BANK_NAME_MAPPING.items():
            if key in handle_lower or handle_lower in key:
                return value
        
        common_mappings = {
            'okaxis': 'Axis Bank',
            'okicici': 'ICICI Bank',
            'okhdfc': 'HDFC Bank',
            'axisbank': 'Axis Bank',
            'icici': 'ICICI Bank',
            'hdfc': 'HDFC Bank',
            'sbi': 'State Bank of India',
            'ybl': 'Yes Bank',
            'paytm': 'Paytm Payments Bank',
            'phonepe': 'Yes Bank (PhonePe)'
        }
        
        for pattern, bank_name in common_mappings.items():
            if pattern in handle_lower:
                return bank_name
    
    if ifsc_code and ifsc_code != "NA":
        try:
            ifsc_prefix = ifsc_code[:4].upper()
            
            if ifsc_prefix in IFSC_MAPPING:
                return IFSC_MAPPING[ifsc_prefix]
            
            common_ifsc_patterns = {
                'SBIN': 'State Bank of India',
                'ICIC': 'ICICI Bank',
                'HDFC': 'HDFC Bank',
                'UTIB': 'Axis Bank',
                'CNRB': 'Canara Bank',
                'BARB': 'Bank of Baroda',
                'BKID': 'Bank of India',
                'PUNB': 'Punjab National Bank',
                'UBIN': 'Union Bank of India',
                'INDB': 'IndusInd Bank',
                'YESB': 'Yes Bank',
                'KARB': 'Karnataka Bank',
                'FDRL': 'Federal Bank',
                'IDFB': 'IDFC First Bank',
                'RATN': 'RBL Bank'
            }
            
            if ifsc_prefix in common_ifsc_patterns:
                return common_ifsc_patterns[ifsc_prefix]
                
        except Exception as e:
            print(f"Error in IFSC lookup for {ifsc_code}: {e}")
    
    return "NA"

def extract_search_for_from_url(url):
    """Extract search_for from URL based on the specified platform mapping with proper domain validation"""
    url_value = clean_value(url)
    
    if url_value == "NA":
        return "Platform"
    
    url_lower = url_value.lower()
    
    try:
        parsed_url = urlparse(url_lower)
        domain = parsed_url.netloc
        
        if not domain:
            return "Platform"
        
        if domain.startswith('www.'):
            domain_without_www = domain[4:]
        else:
            domain_without_www = domain
        
        platform_domains = {
            't.me': 'Telegram',
            'wa.me': 'WhatsApp',
            'chat.whatsapp.com': 'WhatsApp',
            'facebook.com': 'Facebook',
            'instagram.com': 'Instagram',
            'telegram.org': 'Telegram',
            'threads.com': 'Thread',
            'youtube.com': 'YouTube',
            'x.com': 'X'
        }
        
        if domain in platform_domains:
            return platform_domains[domain]
        
        if domain_without_www in platform_domains:
            return platform_domains[domain_without_www]
        
        for platform_domain, platform_name in platform_domains.items():
            if domain.endswith(f'.{platform_domain}'):
                return platform_name
        
        if 'x.com' in url_lower:
            if domain == 'x.com' or domain.endswith('.x.com'):
                return 'X'
        
        if url_lower.startswith('https://') or url_lower.startswith('http://'):
            return "Web"
        
        return "Platform"
        
    except Exception as e:
        print(f"Error parsing URL {url_value}: {e}")
        return "Platform"

def lookup_origin_and_category_from_master(url):
    """Look up origin and category from master URL data"""
    url_value = clean_value(url)
    
    if url_value == "NA":
        return "NA", "NA"
    
    url_clean = url_value.lower().strip()
    
    if url_clean in MASTER_URL_DATA:
        return MASTER_URL_DATA[url_clean]["origin"], MASTER_URL_DATA[url_clean]["category_of_website"]
    
    if url_clean.startswith("http://"):
        https_url = "https://" + url_clean[7:]
        if https_url in MASTER_URL_DATA:
            return MASTER_URL_DATA[https_url]["origin"], MASTER_URL_DATA[https_url]["category_of_website"]
    
    if url_clean.startswith("https://"):
        http_url = "http://" + url_clean[8:]
        if http_url in MASTER_URL_DATA:
            return MASTER_URL_DATA[http_url]["origin"], MASTER_URL_DATA[http_url]["category_of_website"]
    
    try:
        parsed_url = urlparse(url_clean)
        domain = parsed_url.netloc
        
        for master_url, data in MASTER_URL_DATA.items():
            try:
                master_domain = urlparse(master_url).netloc
                if domain == master_domain:
                    return data["origin"], data["category_of_website"]
            except:
                continue
    except:
        pass
    
    return "NA", "NA"

def extract_category_from_input(row_data, sheet_type):
    """Extract category from input based on sheet type"""
    if sheet_type == 'messaging':
        if 'Category' in row_data and row_data['Category'] != "NA":
            return row_data['Category']
    
    if 'Website URL' in row_data:
        _, category = lookup_origin_and_category_from_master(row_data['Website URL'])
        return category
    
    return "NA"

def extract_case_time_and_date_from_npci_url(url):
    """Extract case_generated_time and inserted_date from NPCI PDF URL"""
    if not url or url == "NA":
        return "NA", "NA"
    
    match = re.search(r'npci-(\d{10})_', url)
    if not match:
        return "NA", "NA"
    
    try:
        ts = int(match.group(1))
        
        utc_dt = datetime.utcfromtimestamp(ts)
        ist_dt = utc_dt + timedelta(hours=5, minutes=30)
        
        case_generated_time = ist_dt.strftime("%Y-%m-%d %H:%M:%S")
        inserted_date = ist_dt.strftime("%Y-%m-%d")
        
        return case_generated_time, inserted_date
        
    except Exception:
        return "NA", "NA"

def generate_screenshot_urls(screenshot_url):
    """Generate three screenshot URLs from a single input URL by replacing the prefix"""
    screenshot_value = clean_value(screenshot_url)
    
    if screenshot_value == "NA":
        return "NA"
    
    try:
        parsed_url = urlparse(screenshot_value)
        path = parsed_url.path
        
        if not path:
            return "NA"
        
        filename = path.split('/')[-1]
        
        if not filename:
            return "NA"
        
        if '-' in filename:
            parts = filename.split('-', 1)
            if len(parts) == 2:
                prefix, rest_of_filename = parts
                
                urls = []
                new_prefixes = ['mfilterit', 'npci', 'without_header']
                
                for new_prefix in new_prefixes:
                    new_filename = f"{new_prefix}-{rest_of_filename}"
                    new_path = '/'.join(path.split('/')[:-1] + [new_filename])
                    new_url = f"{parsed_url.scheme}://{parsed_url.netloc}{new_path}"
                    urls.append(new_url)
                
                return ','.join(urls)
        
        return screenshot_value
        
    except Exception as e:
        print(f"Error generating screenshot URLs: {e}")
        return screenshot_value

def extract_payment_gateway_name(upi_url, website_url):
    """
    Extract domain name from upi_url and compare with website_url domain.
    """
    upi_url_value = clean_value(upi_url)
    website_url_value = clean_value(website_url)
    
    if upi_url_value == "NA":
        return "NA"
    
    try:
        parsed_upi = urlparse(upi_url_value)
        upi_domain = parsed_upi.netloc
        
        if not upi_domain:
            if parsed_upi.path:
                path = parsed_upi.path.lstrip('/')
                domain_part = path.split('/')[0]
                if '.' in domain_part:
                    upi_domain = domain_part
                else:
                    return "NA"
            else:
                return "NA"
        
        if upi_domain.startswith('www.'):
            upi_domain_clean = upi_domain[4:]
        else:
            upi_domain_clean = upi_domain
        
        if website_url_value == "NA":
            return upi_domain
        
        parsed_website = urlparse(website_url_value)
        website_domain = parsed_website.netloc
        
        if not website_domain:
            if parsed_website.path:
                path = parsed_website.path.lstrip('/')
                domain_part = path.split('/')[0]
                if '.' in domain_part:
                    website_domain = domain_part
                else:
                    return upi_domain
            else:
                return upi_domain
        
        if website_domain.startswith('www.'):
            website_domain_clean = website_domain[4:]
        else:
            website_domain_clean = website_domain
        
        if upi_domain_clean == website_domain_clean:
            return "NA"
        else:
            return upi_domain
        
    except Exception as e:
        print(f"Error extracting payment gateway name: {e}")
        return "NA"

def process_sheet_data(df, sheet_type):
    """Process data according to sheet type logic with updated requirements"""
    result_df = pd.DataFrame(columns=REQUIRED_COLUMNS)
    
    if df.empty:
        return result_df, {'total_values': 0, 'unique_upi_ids': 0, 'unique_bank_accounts': 0}
    
    input_headers = list(df.columns)
    standardized_headers = standardize_headers(input_headers, sheet_type)
    df.columns = standardized_headers
    
    unique_upi_ids = set()
    unique_bank_accounts = set()
    
    for idx in range(len(df)):
        row_data = {col: "NA" for col in REQUIRED_COLUMNS}
        
        row_data['case_generated_time'] = "NA"
        row_data['inserted_date'] = "NA"
        
        for std_header in standardized_headers:
            value = df.iloc[idx][std_header]
            cleaned_value = clean_value(value)
            
            if std_header == "UPI":
                row_data['upi_vpa'] = cleaned_value
                if cleaned_value != "NA":
                    unique_upi_ids.add(cleaned_value)
            elif std_header == "Account Holder Name":
                row_data['ac_holder_name'] = cleaned_value
            elif std_header == "Bank Account Number":
                row_data['bank_account_number'] = cleaned_value
                if cleaned_value != "NA":
                    unique_bank_accounts.add(cleaned_value)
            elif std_header == "IFSC Code":
                row_data['ifsc_code'] = cleaned_value
            elif std_header == "Website URL":
                row_data['website_url'] = cleaned_value
            elif std_header == "Payment Gateway URL":
                row_data['payment_gateway_url'] = cleaned_value
            elif std_header == "Transaction Method":
                row_data['transaction_method'] = cleaned_value
            elif std_header == "Screenshot":
                row_data['_original_screenshot'] = cleaned_value
                
                case_time, inserted_date = extract_case_time_and_date_from_npci_url(cleaned_value)
                row_data['case_generated_time'] = case_time
                row_data['inserted_date'] = inserted_date
                
                row_data['screenshot'] = generate_screenshot_urls(cleaned_value)
            elif std_header == "Contact Number":
                row_data['web_contact_no'] = cleaned_value
            elif std_header == "Scam Type":
                row_data['scam_type'] = cleaned_value
            elif std_header == "Category":
                row_data['category_of_website'] = cleaned_value
        
        if sheet_type == 'upi':
            row_data['customer'] = "Mystery Shopping"
            row_data['package_name'] = "com.mysteryshopping"
            row_data['channel_name'] = "Organic Search"
            row_data['status'] = "Active"
            row_data['priority'] = "High"
            row_data['flag'] = "1"
            row_data['cessation'] = "Open"
            row_data['reviewed_status'] = "1"
            row_data['reported_earlier'] = "No"
            row_data['approvd_status'] = "1"
            row_data['feature_type'] = "BS Money Laundering"
            row_data['platform'] = "NA"
            row_data['neft_imps'] = "NA"
            row_data['bank_branch_details'] = "NA"
            
            if row_data['upi_vpa'] != "NA" and row_data['upi_vpa'] != "":
                row_data['upi_bank_account_wallet'] = "UPI"
            else:
                row_data['upi_bank_account_wallet'] = "Bank Account"
            
            row_data['scam_type'] = "NA"
            
            if row_data['website_url'] != "NA":
                origin, category = lookup_origin_and_category_from_master(row_data['website_url'])
                row_data['origin'] = origin
                row_data['category_of_website'] = category
            else:
                row_data['origin'] = "NA"
                row_data['category_of_website'] = "NA"
            
        elif sheet_type == 'investment':
            row_data['customer'] = "Mystery Shopping"
            row_data['package_name'] = "com.mysteryshopping"
            row_data['channel_name'] = "Organic Search"
            row_data['status'] = "Active"
            row_data['priority'] = "High"
            row_data['flag'] = "1"
            row_data['cessation'] = "Open"
            row_data['reviewed_status'] = "1"
            row_data['reported_earlier'] = "No"
            row_data['approvd_status'] = "1"
            row_data['feature_type'] = "BS Investment Scam"
            row_data['platform'] = "NA"
            row_data['neft_imps'] = "NA"
            row_data['bank_branch_details'] = "NA"
            
            if row_data['upi_vpa'] != "NA" and row_data['upi_vpa'] != "":
                row_data['upi_bank_account_wallet'] = "UPI"
            else:
                row_data['upi_bank_account_wallet'] = "Bank Account"
            
            if row_data['scam_type'] != "NA" and row_data['category_of_website'] == "NA":
                row_data['category_of_website'] = row_data['scam_type']
            
            if row_data['website_url'] != "NA":
                origin, _ = lookup_origin_and_category_from_master(row_data['website_url'])
                row_data['origin'] = origin
            else:
                row_data['origin'] = "NA"
            
        elif sheet_type == 'messaging':
            row_data['customer'] = "Mystery Shopping"
            row_data['package_name'] = "com.mysteryshopping"
            row_data['channel_name'] = "Messaging Channel Platforms"
            row_data['status'] = "Active"
            row_data['priority'] = "High"
            row_data['flag'] = "1"
            row_data['cessation'] = "Open"
            row_data['reviewed_status'] = "1"
            row_data['reported_earlier'] = "No"
            row_data['approvd_status'] = "1"
            row_data['feature_type'] = "BS Money Laundering"
            row_data['platform'] = "NA"
            row_data['neft_imps'] = "NA"
            row_data['bank_branch_details'] = "NA"
            
            if row_data['upi_vpa'] != "NA" and row_data['upi_vpa'] != "":
                row_data['upi_bank_account_wallet'] = "UPI"
            else:
                row_data['upi_bank_account_wallet'] = "Bank Account"
            
            row_data['scam_type'] = "NA"
            
            row_data['origin'] = "India"
            
            if row_data['category_of_website'] == "NA":
                if 'Category' in row_data and row_data['Category'] != "NA":
                    row_data['category_of_website'] = row_data['Category']
                else:
                    row_data['category_of_website'] = "NA"
        
        handle = extract_handle(row_data['upi_vpa'])
        row_data['handle'] = handle
        
        row_data['bank_name'] = get_bank_name_from_handle(handle, row_data['ifsc_code'])
        
        row_data['search_for'] = extract_search_for_from_url(row_data['website_url'])
        
        if sheet_type != 'messaging' and row_data['category_of_website'] == "NA":
            row_data['category_of_website'] = extract_category_from_input(row_data, sheet_type)
        
        if 'screenshot' in row_data:
            row_data['screenshot_case_report_link'] = row_data['screenshot']
        else:
            row_data['screenshot_case_report_link'] = "NA"
        
        if '_original_screenshot' in row_data:
            del row_data['_original_screenshot']
        
        payment_gateway_url = row_data.get('payment_gateway_url', "NA")
        if payment_gateway_url != "NA":
            row_data['payment_gateway_intermediate_url'] = payment_gateway_url
            row_data['payment_gateway_url'] = payment_gateway_url
            row_data['upi_url'] = payment_gateway_url
            
            row_data['payment_gateway_name'] = extract_payment_gateway_name(
                row_data['upi_url'], 
                row_data['website_url']
            )
        else:
            row_data['payment_gateway_intermediate_url'] = "NA"
            row_data['payment_gateway_url'] = "NA"
            row_data['upi_url'] = "NA"
            row_data['payment_gateway_name'] = "NA"
        
        if row_data['inserted_date'] == "NA":
            row_data['inserted_date'] = "NA"
        
        if row_data['case_generated_time'] == "NA":
            row_data['case_generated_time'] = "NA"
        
        result_df.loc[idx] = [row_data.get(col, "NA") for col in REQUIRED_COLUMNS]
    
    preview_metrics = {
        'total_values': len(result_df),
        'unique_upi_ids': len(unique_upi_ids),
        'unique_bank_accounts': len(unique_bank_accounts)
    }
    
    return result_df, preview_metrics

# Routes
@app.route("/", methods=["GET"])
def index():
    page_type = request.args.get("page", "scraping")
    search_query = request.args.get("search", "").strip()
    scam_filter = request.args.get("scam_type", "").strip()
    platform_filter = request.args.get("platform", "").strip()
    page = int(request.args.get("page_num", 1))
    
    social_search = request.args.get("social_search", "").strip()
    social_platform = request.args.get("social_platform", "").strip()
    social_update_data = request.args.get("update_data", "").strip()
    social_activity_log = request.args.get("activity_log", "").strip()
    social_permanent_block = request.args.get("permanent_block", "").strip()

    items = []
    total_rows = 0
    total_pages = 1
    
    if page_type == "scraping":
        query = supabase.table("scrapping_data").select("*", count='exact')

        if search_query:
            like_term = f"%{search_query}%"
            query = query.or_(f"name.ilike.{like_term},platform.ilike.{like_term},post_url.ilike.{like_term},chat_number.ilike.{like_term},group_name.ilike.{like_term},chat_link.ilike.{like_term},scam_type.ilike.{like_term}")

        if scam_filter:
            query = query.eq("scam_type", scam_filter)

        if platform_filter:
            query = query.eq("platform", platform_filter)

        offset = (page - 1) * PER_PAGE
        query = query.range(offset, offset + PER_PAGE - 1)

        response = query.execute()
        items = response.data
        total_rows = response.count

        total_pages = max(1, math.ceil(total_rows / PER_PAGE)) if total_rows else 1
        
    elif page_type == "social":
        query = social_supabase.table("social_media_accounts").select("*", count='exact')
        
        if social_search:
            like_term = f"%{social_search}%"
            query = query.or_(
                f"login_user.ilike.{like_term}," +
                f"number.ilike.{like_term}," +
                f"full_name.ilike.{like_term}," +
                f"page_name.ilike.{like_term}," +
                f"platform.ilike.{like_term}," +
                f"account_status.ilike.{like_term}," +
                f"review_status.ilike.{like_term}," +
                f"login_device.ilike.{like_term}"
            )
        
        if social_platform and social_platform != "":
            query = query.eq("platform", social_platform)
        
        if social_permanent_block == "true":
            query = query.eq("account_status", "Permanent Block")
        else:
            # Permanent Block accounts are hidden from main Social Media view
            query = query.neq("account_status", "Permanent Block")
        
        if social_update_data == "true":
            query = query.select("id,login_user,number,login_device,account_status,review_status,blocked_date,unblock_date,platform")
        
        # CHANGE: Sort by ID ascending so records appear in data ID sequence order
        query = query.order("id", desc=False)
        
        offset = (page - 1) * PER_PAGE
        query = query.range(offset, offset + PER_PAGE - 1)
        
        try:
            response = query.execute()
            items = response.data
            total_rows = response.count
            
            total_pages = max(1, math.ceil(total_rows / PER_PAGE)) if total_rows else 1
            
            print(f"Social page query - search: '{social_search}', platform: '{social_platform}', results: {total_rows}")
            
        except Exception as e:
            print(f"Error fetching social media data: {e}")
            flash(f"Error fetching social media data: {str(e)}", "error")

    return render_template(
        "index.html",
        page_type=page_type,
        items=items,
        search_query=search_query,
        scam_filter=scam_filter,
        platform_filter=platform_filter,
        social_search=social_search,
        social_platform=social_platform,
        social_update_data=social_update_data,
        social_activity_log=social_activity_log,
        social_permanent_block=social_permanent_block,
        page_num=page,
        total_pages=total_pages,
        total_rows=total_rows,
        platform_options=PLATFORM_OPTIONS,
        scam_type_options=SCAM_TYPE_OPTIONS,
        social_platform_options=SOCIAL_PLATFORM_OPTIONS
    )

# ============================================================
# FIXED: Tracker Stats Endpoint - fetches real platform/status counts
# ============================================================
@app.route("/tracker-stats", methods=["GET"])
def tracker_stats():
    """Get tracker statistics for social media accounts - platform and status counts"""
    try:
        platforms = ["Facebook", "Amazon", "Instagram", "Telegram", "WhatsApp"]
        platform_counts = {}
        platform_status_counts = {}
        
        for platform in platforms:
            try:
                # Total count per platform
                response = social_supabase.table("social_media_accounts") \
                    .select("account_status", count='exact') \
                    .eq("platform", platform) \
                    .execute()
                
                platform_counts[platform] = response.count or 0
                
                # Status breakdown per platform
                status_map = {}
                if hasattr(response, 'data') and response.data:
                    for item in response.data:
                        status = item.get('account_status', 'Active') or 'Active'
                        status_map[status] = status_map.get(status, 0) + 1
                
                platform_status_counts[platform] = status_map
                
            except Exception as e:
                print(f"Error counting {platform}: {e}")
                platform_counts[platform] = 0
                platform_status_counts[platform] = {}
        
        # Total across all platforms
        try:
            total_response = social_supabase.table("social_media_accounts") \
                .select("*", count='exact') \
                .execute()
            total_accounts = total_response.count or 0
        except Exception as e:
            print(f"Error getting total count: {e}")
            total_accounts = sum(platform_counts.values())
        
        return jsonify({
            "success": True,
            "stats": {
                "platform_counts": platform_counts,
                "platform_status_counts": platform_status_counts,
                "total_accounts": total_accounts
            }
        })
    except Exception as e:
        print(f"Error getting tracker stats: {e}")
        return jsonify({"success": False, "error": str(e)})


@app.route("/get-platform-counts", methods=["GET"])
def get_platform_counts():
    """Get count of accounts per platform for the tracker"""
    try:
        platforms = ["Facebook", "Amazon", "Instagram", "Telegram", "WhatsApp"]
        platform_counts = {}
        status_counts = {}
        
        for platform in platforms:
            try:
                response = social_supabase.table("social_media_accounts").select("*", count='exact').eq("platform", platform).execute()
                platform_counts[platform] = response.count or 0
                
                status_response = social_supabase.table("social_media_accounts").select("account_status", count='exact').eq("platform", platform).execute()
                
                status_counts[platform] = {
                    "Active": 0,
                    "Block": 0,
                    "Restricted": 0,
                    "Frozen": 0,
                    "Permanent Block": 0
                }
                
                if hasattr(status_response, 'data'):
                    for item in status_response.data:
                        status = item.get('account_status', '')
                        if status:
                            status_lower = status.lower()
                            if 'active' in status_lower:
                                status_counts[platform]["Active"] = status_counts[platform].get("Active", 0) + 1
                            elif 'block' in status_lower and 'permanent' not in status_lower:
                                status_counts[platform]["Block"] = status_counts[platform].get("Block", 0) + 1
                            elif 'restricted' in status_lower:
                                status_counts[platform]["Restricted"] = status_counts[platform].get("Restricted", 0) + 1
                            elif 'frozen' in status_lower:
                                status_counts[platform]["Frozen"] = status_counts[platform].get("Frozen", 0) + 1
                            elif 'permanent' in status_lower:
                                status_counts[platform]["Permanent Block"] = status_counts[platform].get("Permanent Block", 0) + 1
                        
            except Exception as e:
                print(f"Error counting {platform}: {e}")
                platform_counts[platform] = 0
                status_counts[platform] = {
                    "Active": 0,
                    "Block": 0,
                    "Restricted": 0,
                    "Frozen": 0,
                    "Permanent Block": 0
                }
        
        total_response = social_supabase.table("social_media_accounts").select("*", count='exact').execute()
        platform_counts["Total"] = total_response.count or 0
        
        return jsonify({
            "success": True,
            "platform_counts": platform_counts,
            "status_counts": status_counts
        })
    except Exception as e:
        print(f"Error getting platform counts: {e}")
        return jsonify({"success": False, "error": str(e)})

@app.route("/update-social-data", methods=["POST"])
def update_social_data():
    """Update social media account data"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"success": False, "error": "No data provided"})
        
        account_id = data.get('id')
        field = data.get('field')
        value = data.get('value')
        
        if not account_id or not field:
            return jsonify({"success": False, "error": "Missing required fields: id and field"})
        
        valid_fields = [
            'login_user', 'number', 'login_device',
            'account_status', 'review_status',
            'blocked_date',
            'unblock_date',
            'recharge_date'
        ]
        if field not in valid_fields:
            return jsonify({"success": False, "error": f"Invalid field: {field}"})
        
        update_data = {field: value}
        
        response = social_supabase.table("social_media_accounts").update(update_data).eq("id", account_id).execute()
        
        if hasattr(response, 'data') and response.data:
            return jsonify({"success": True, "message": "Data updated successfully"})
        else:
            return jsonify({"success": False, "error": "No data was updated"})
            
    except Exception as e:
        print(f"Error updating social data: {e}")
        return jsonify({"success": False, "error": str(e)})

@app.route("/social-import", methods=["POST"])
def social_import():
    """Import social media accounts data"""
    try:
        file = request.files.get("file")
        
        if not file or file.filename == '':
            flash("No file selected", "error")
            return redirect("/?page=social")
        
        if not is_allowed_file(file.filename):
            allowed_extensions = get_allowed_extensions()
            flash(f"Only {', '.join(allowed_extensions)} files are allowed.", "error")
            return redirect("/?page=social")

        filename = secure_filename(file.filename)
        temp_path = os.path.join(tempfile.gettempdir(), filename)
        file.save(temp_path)
        
        file_ext = filename.rsplit('.', 1)[1].lower()
        df = read_data_file(temp_path, file_ext)
        
        df.columns = df.columns.astype(str).str.strip()
        df = df.fillna('')
        
        ALL_SOCIAL_COLUMNS = [
            'owned_by',
            'login_user',
            'number',
            'login_device',
            'sim_inserted_device',
            'account_status',
            'review_status',
            'number_type',
            'blocked_date',
            'unblock_date',
            'account_create_date',
            'sim_operator',
            'full_name',
            'recharge_date',
            'sim_buy_date',
            'account_type',
            'mail_id',
            'account_id',
            'password',
            'page_name',
            'platform',
        ]

        file_columns = list(df.columns)
        matched_columns = [col for col in file_columns if col in ALL_SOCIAL_COLUMNS and col != 'id']

        if not matched_columns:
            flash("Import Error: No matching column names found in file. Please make sure column names match the Supabase table.", "error")
            os.remove(temp_path)
            return redirect("/?page=social")

        try:
            max_id_response = social_supabase.table("social_media_accounts") \
                .select("id") \
                .order("id", desc=True) \
                .limit(1) \
                .execute()
            if max_id_response.data and len(max_id_response.data) > 0:
                next_id = int(max_id_response.data[0]['id']) + 1
            else:
                next_id = 1
        except Exception:
            next_id = None

        records = []
        for i, (_, row) in enumerate(df.iterrows()):
            record = {}
            if next_id is not None:
                record['id'] = next_id + i
            for col in matched_columns:
                value = str(row[col]).strip()
                record[col] = value if value else "NA"
            records.append(record)
        
        response = social_supabase.table("social_media_accounts").insert(records).execute()

        flash(f"File Imported Successfully! {len(records)} records added. ({len(matched_columns)} columns matched: {', '.join(matched_columns)})", "success")
        
        os.remove(temp_path)

    except Exception as e:
        flash(f"Import Error: {str(e)}", "error")

    return redirect("/?page=social")

# ============================================================
# FIXED: Social Export - exports from social_media_accounts table
# ============================================================
@app.route("/social-export", methods=["GET"])
def social_export():
    """Export social media accounts data"""
    try:
        social_search = request.args.get("social_search", "").strip()
        social_platform = request.args.get("social_platform", "").strip()
        social_permanent_block = request.args.get("permanent_block", "").strip()

        query = social_supabase.table("social_media_accounts").select("*")
        
        if social_search:
            like_term = f"%{social_search}%"
            query = query.or_(f"login_user.ilike.{like_term},number.ilike.{like_term},full_name.ilike.{like_term},page_name.ilike.{like_term},platform.ilike.{like_term}")
        
        if social_platform and social_platform not in ["", "All Platforms"]:
            query = query.eq("platform", social_platform)
        
        if social_permanent_block == "true":
            query = query.eq("account_status", "Permanent Block")
        
        # Sort by ID ascending
        query = query.order("id", desc=False)
        
        response = query.execute()
        df = pd.DataFrame(response.data)

        output = io.StringIO()
        df.to_csv(output, index=False, encoding='utf-8-sig')
        output.seek(0)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"social_media_accounts_{timestamp}.csv"

        return send_file(
            io.BytesIO(output.getvalue().encode('utf-8-sig')),
            download_name=filename,
            as_attachment=True,
            mimetype="text/csv"
        )
    except Exception as e:
        flash(f"Export Error: {str(e)}", "error")
        return redirect("/?page=social")

@app.route("/get-sheet-headers/<sheet_type>", methods=["GET"])
def get_sheet_headers_route(sheet_type):
    """Get required headers for a sheet type"""
    try:
        headers = get_sheet_headers(sheet_type)
        config = load_config()
        sheet_name = config['sheet_mappings'][sheet_type]['name'] if config and 'sheet_mappings' in config else sheet_type
        
        return jsonify({
            "success": True,
            "sheet_name": sheet_name,
            "headers": headers,
            "headers_count": len(headers)
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route("/download-template/<sheet_type>", methods=["GET"])
def download_template(sheet_type):
    """Download template with only input headers"""
    try:
        headers = get_sheet_headers(sheet_type)
        
        if not headers:
            flash("No headers found for this sheet type", "error")
            return redirect("/?page=sheet")
        
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(headers)
        
        output.seek(0)
        
        config = load_config()
        sheet_name = config['sheet_mappings'][sheet_type]['name'].replace(' ', '_') if config else sheet_type
        filename = f"{sheet_name}_Input_Template.csv"
        
        return send_file(
            io.BytesIO(output.getvalue().encode('utf-8-sig')),
            download_name=filename,
            as_attachment=True,
            mimetype="text/csv"
        )
    except Exception as e:
        flash(f"Error generating template: {str(e)}", "error")
        return redirect("/?page=sheet")

@app.route("/preview-sheet", methods=["POST"])
def preview_sheet():
    """Preview the full 36-column output"""
    try:
        sheet_type = request.form.get("sheet_type")
        file = request.files.get("file")
        
        if not sheet_type:
            return jsonify({"success": False, "error": "Please select a sheet type"})
            
        if not file or file.filename == '':
            return jsonify({"success": False, "error": "Please select a file"})
        
        if not is_allowed_file(file.filename):
            allowed_extensions = get_allowed_extensions()
            return jsonify({"success": False, "error": f"Only {', '.join(allowed_extensions)} files are allowed."})
        
        filename = secure_filename(file.filename)
        temp_path = os.path.join(tempfile.gettempdir(), filename)
        file.save(temp_path)
        
        try:
            file_ext = filename.rsplit('.', 1)[1].lower()
            df = read_data_file(temp_path, file_ext)
            
            if df.empty:
                return jsonify({"success": False, "error": "The uploaded file is empty"})
            
            config = load_config()
            sheet_config = config['sheet_mappings'][sheet_type]
            
            result_df, preview_metrics = process_sheet_data(df, sheet_type)
            
            os.remove(temp_path)
            
            preview_rows = result_df.fillna('').head(50).to_dict(orient='records')
            
            preview_data = {
                "success": True,
                "sheet_name": sheet_config['name'],
                "total_values": preview_metrics['total_values'],
                "unique_upi_ids": preview_metrics['unique_upi_ids'],
                "unique_bank_accounts": preview_metrics['unique_bank_accounts'],
                "total_columns": len(result_df.columns),
                "columns": list(result_df.columns),
                "preview_rows": preview_rows,
                "input_headers": list(df.columns),
                "output_headers": list(result_df.columns)
            }
            
            return jsonify(preview_data)
            
        except Exception as e:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            return jsonify({"success": False, "error": f"Error processing file: {str(e)}"})
            
    except Exception as e:
        return jsonify({"success": False, "error": f"Error previewing sheet: {str(e)}"})

@app.route("/generate-sheet", methods=["POST"])
def generate_sheet():
    """Generate and download the final CSV with 36 columns"""
    try:
        sheet_type = request.form.get("sheet_type")
        file = request.files.get("file")
        
        if not sheet_type:
            flash("Please select a sheet type", "error")
            return redirect("/?page=sheet")
            
        if not file or file.filename == '':
            flash("Please select a file", "error")
            return redirect("/?page=sheet")
        
        if not is_allowed_file(file.filename):
            allowed_extensions = get_allowed_extensions()
            flash(f"Only {', '.join(allowed_extensions)} files are allowed.", "error")
            return redirect("/?page=sheet")
        
        filename = secure_filename(file.filename)
        temp_path = os.path.join(tempfile.gettempdir(), filename)
        file.save(temp_path)
        
        try:
            file_ext = filename.rsplit('.', 1)[1].lower()
            df = read_data_file(temp_path, file_ext)
            
            if df.empty:
                flash("The uploaded file is empty", "error")
                return redirect("/?page=sheet")
            
            config = load_config()
            sheet_config = config['sheet_mappings'][sheet_type]
            
            result_df, _ = process_sheet_data(df, sheet_type)
            
            output = io.StringIO()
            result_df.to_csv(output, index=False, encoding='utf-8-sig')
            
            today_date = datetime.now().strftime("%Y-%m-%d")
            sheet_name_clean = SHEET_TYPES.get(sheet_type, sheet_type)
            filename = f"{sheet_name_clean}_{today_date}.csv"
            
            os.remove(temp_path)
            
            return send_file(
                io.BytesIO(output.getvalue().encode('utf-8-sig')),
                download_name=filename,
                as_attachment=True,
                mimetype="text/csv"
            )
            
        except Exception as e:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            flash(f"Error processing file: {str(e)}", "error")
            return redirect("/?page=sheet")
            
    except Exception as e:
        flash(f"Error generating sheet: {str(e)}", "error")
        return redirect("/?page=sheet")

@app.route("/get-excel-headers", methods=["GET"])
def get_excel_headers():
    """Get headers from Excel files"""
    try:
        master_headers = []
        bank_headers = []
        
        if MASTER_URL_DATA_PATH.exists():
            df_master = pd.read_excel(MASTER_URL_DATA_PATH)
            master_headers = list(df_master.columns)
        
        if BANK_NAME_MAPPING_PATH.exists():
            df_bank = pd.read_excel(BANK_NAME_MAPPING_PATH)
            bank_headers = list(df_bank.columns)
        
        return jsonify({
            "success": True,
            "master_url_data_headers": master_headers,
            "bank_name_mapping_headers": bank_headers,
            "master_url_data_count": len(MASTER_URL_DATA),
            "bank_name_mapping_count": len(BANK_NAME_MAPPING)
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route("/get-ifsc-headers", methods=["GET"])
def get_ifsc_headers():
    """Get headers from IFSC mapping file"""
    try:
        ifsc_headers = []
        
        if IFSC_MAPPING_PATH.exists():
            df_ifsc = pd.read_excel(IFSC_MAPPING_PATH)
            ifsc_headers = list(df_ifsc.columns)
        
        return jsonify({
            "success": True,
            "ifsc_mapping_headers": ifsc_headers,
            "ifsc_mapping_count": len(IFSC_MAPPING)
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route("/reload-data", methods=["POST"])
def reload_data():
    """Reload data from Excel files"""
    try:
        load_excel_data()
        return jsonify({
            "success": True,
            "message": f"Data reloaded successfully! URLs: {len(MASTER_URL_DATA)}, Bank mappings: {len(BANK_NAME_MAPPING)}, IFSC mappings: {len(IFSC_MAPPING)}"
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route("/upload", methods=["POST"])
def upload():
    if "file" not in request.files:
        flash("No file uploaded", "error")
        return redirect("/?page=scraping")

    file = request.files["file"]

    if not file or file.filename == '':
        flash("No file selected", "error")
        return redirect("/?page=scraping")
    
    if not is_allowed_file(file.filename):
        allowed_extensions = get_allowed_extensions()
        flash(f"Only {', '.join(allowed_extensions)} files are allowed.", "error")
        return redirect("/?page=scraping")

    try:
        filename = secure_filename(file.filename)
        temp_path = os.path.join(tempfile.gettempdir(), filename)
        file.save(temp_path)
        
        file_ext = filename.rsplit('.', 1)[1].lower()
        df = read_data_file(temp_path, file_ext)
        
        df.columns = df.columns.astype(str).str.strip()
        df = df.fillna('')
        
        required_cols = [
            "name", "platform", "post_url", "chat_number", "group_name",
            "chat_link", "inserted_date", "chat_status", "assigned_to",
            "assigned_at_datetime", "inserted_datetime", "priority",
            "extra_field_1", "extra_field_2", "extra_field_3",
            "extra_field_4", "extra_field_5", "screenshot", "scam_type"
        ]

        for col in required_cols:
            if col not in df.columns:
                df[col] = "NA"

        if 'inserted_date' not in df.columns or df['inserted_date'].isna().all():
            df['inserted_date'] = datetime.now().strftime("%Y-%m-%d")
        
        if 'inserted_datetime' not in df.columns or df['inserted_datetime'].isna().all():
            df['inserted_datetime'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        records = df[required_cols].to_dict(orient='records')

        response = supabase.table("scrapping_data").insert(records).execute()

        flash(f"File Imported Successfully! {len(records)} records added.", "success")
        
        os.remove(temp_path)

    except Exception as e:
        flash(f"Import Error: {str(e)}", "error")

    return redirect("/?page=scraping")

# ============================================================
# FIXED: /export route - only exports scraping data (filtered)
# ============================================================
@app.route("/export")
def export():
    """Export scraping data only - respects current filters"""
    try:
        search_query = request.args.get("search", "").strip()
        scam_filter = request.args.get("scam_type", "").strip()
        platform_filter = request.args.get("platform", "").strip()

        query = supabase.table("scrapping_data").select("*")
        
        if search_query:
            like_term = f"%{search_query}%"
            query = query.or_(f"name.ilike.{like_term},platform.ilike.{like_term},post_url.ilike.{like_term},chat_number.ilike.{like_term},group_name.ilike.{like_term},chat_link.ilike.{like_term},scam_type.ilike.{like_term}")
        
        if scam_filter:
            query = query.eq("scam_type", scam_filter)
        
        if platform_filter:
            query = query.eq("platform", platform_filter)

        response = query.execute()
        df = pd.DataFrame(response.data)

        output = io.StringIO()
        df.to_csv(output, index=False, encoding='utf-8-sig')
        output.seek(0)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"scam_reports_{timestamp}.csv"

        return send_file(
            io.BytesIO(output.getvalue().encode('utf-8-sig')),
            download_name=filename,
            as_attachment=True,
            mimetype="text/csv"
        )
    except Exception as e:
        flash(f"Export Error: {str(e)}", "error")
        return redirect("/?page=scraping")

@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint for Render and monitoring"""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "excel_data_loaded": {
            "master_url_data": len(MASTER_URL_DATA),
            "bank_name_mapping": len(BANK_NAME_MAPPING),
            "ifsc_mapping": len(IFSC_MAPPING)
        }
    })


# ============================================================
# UPDATE DATA PAGE - shows limited columns for editing
# CHANGES: Removed Enter-to-save; added "Update Data" button
# ============================================================
@app.route("/update-social-accounts", methods=["GET"])
def update_social_accounts():
    """Dedicated Update Data page with only editable columns"""
    search = request.args.get("search", "").strip()
    platform = request.args.get("platform", "").strip()
    page = int(request.args.get("page_num", 1))

    query = social_supabase.table("social_media_accounts").select(
        "id,login_user,number,login_device,account_status,review_status,blocked_date,unblock_date,recharge_date,platform,account_create_date,full_name",
        count='exact'
    )
    # Exclude Permanent Block accounts from Update Data view
    query = query.neq("account_status", "Permanent Block")

    if search:
        like_term = f"%{search}%"
        query = query.or_(
            f"login_user.ilike.{like_term},"
            f"number.ilike.{like_term},"
            f"platform.ilike.{like_term},"
            f"account_status.ilike.{like_term}"
        )

    if platform:
        query = query.eq("platform", platform)

    # Sort by ID ascending
    query = query.order("id", desc=False)

    offset = (page - 1) * PER_PAGE
    query = query.range(offset, offset + PER_PAGE - 1)

    try:
        response = query.execute()
        items = response.data
        total_rows = response.count
        total_pages = max(1, math.ceil(total_rows / PER_PAGE)) if total_rows else 1
    except Exception as e:
        print(f"Error fetching update data: {e}")
        items = []
        total_rows = 0
        total_pages = 1

    UPDATE_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Update Social Media Accounts</title>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
        body { background: #f5f7fa; color: #333; min-height: 100vh; padding-bottom: 60px; }
        .fixed-header { position: fixed; top: 0; left: 0; right: 0; z-index: 1000; background: #f5f7fa; padding: 12px 15px; box-shadow: 0 2px 10px rgba(0,0,0,0.08); border-bottom: 1px solid #e1e5eb; }
        .header { background: white; border-radius: 8px; padding: 12px 15px; box-shadow: 0 1px 8px rgba(0,0,0,0.08); display: flex; justify-content: space-between; align-items: center; }
        .logo h1 { font-size: 17px; color: #2c3e50; }
        .logo p { color: #7f8c8d; font-size: 12px; margin-top: 2px; }
        .header-actions { display: flex; gap: 8px; align-items: center; }
        .btn { padding: 6px 14px; border: none; border-radius: 5px; font-weight: 600; cursor: pointer; display: inline-flex; align-items: center; gap: 6px; font-size: 12px; transition: all 0.2s; height: 34px; text-decoration: none; }
        .btn-back { background: #6c757d; color: white; }
        .btn-back:hover { background: #5a6268; }
        .btn-edit { background: #f39c12; color: white; }
        .btn-edit:hover { background: #e67e22; }
        .btn-edit.active { background: #e67e22; box-shadow: 0 0 0 3px rgba(243,156,18,0.3); }
        .btn-update { background: #2ecc71; color: white; }
        .btn-update:hover { background: #27ae60; }
        .btn-update:disabled { background: #95a5a6; cursor: not-allowed; opacity: 0.7; }
        .container { padding: 15px; padding-top: 90px; }
        .filters { background: white; border-radius: 8px; padding: 12px 15px; margin-bottom: 12px; box-shadow: 0 1px 8px rgba(0,0,0,0.08); }
        .filters-top { display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px; }
        .filters-top h2 { font-size: 14px; color: #2c3e50; display: flex; align-items: center; gap: 6px; }
        .edit-mode-info { font-size: 12px; color: #e67e22; font-weight: 600; display: none; align-items: center; gap: 6px; }
        .edit-mode-info.show { display: flex; }
        .filters-row { display: flex; gap: 12px; align-items: flex-end; flex-wrap: wrap; }
        .filter-group { display: flex; flex-direction: column; gap: 4px; flex: 1; min-width: 160px; }
        .filter-group label { font-size: 12px; font-weight: 600; color: #2c3e50; }
        .filter-group input, .filter-group select { padding: 7px 10px; border: 1px solid #ddd; border-radius: 5px; font-size: 12px; background: white; height: 34px; }
        .filter-group input:focus, .filter-group select:focus { outline: none; border-color: #3498db; box-shadow: 0 0 0 2px rgba(52,152,219,0.2); }
        .table-container { background: white; border-radius: 8px; box-shadow: 0 1px 8px rgba(0,0,0,0.08); overflow-x: auto; overflow-y: auto; max-height: calc(100vh - 240px); }
        table { width: 100%; border-collapse: collapse; min-width: 900px; }
        th { background: #f8f9fa; padding: 9px 10px; text-align: left; font-weight: 600; color: #2c3e50; font-size: 12px; border-bottom: 2px solid #dee2e6; position: sticky; top: 0; z-index: 10; white-space: nowrap; }
        th.checkbox-col { width: 40px; text-align: center; }
        td { padding: 8px 10px; border-bottom: 1px solid #f0f0f0; font-size: 12px; vertical-align: middle; }
        tr:hover td { background: #fafbfc; }
        tr.row-active td { background-color: #f0fdf4; }
        tr.row-active:hover td { background-color: #dcfce7; }
        tr.row-block td, tr.row-blocked td { background-color: #fff5f5; }
        tr.row-block:hover td, tr.row-blocked:hover td { background-color: #fee2e2; }
        tr.row-permanent-block td { background-color: #fecaca; }
        tr.row-permanent-block:hover td { background-color: #fca5a5; }
        tr.row-restricted td { background-color: #fffbeb; }
        tr.row-frozen td { background-color: #eff6ff; }
        td.checkbox-col { text-align: center; }
        .row-checkbox { width: 15px; height: 15px; cursor: pointer; accent-color: #3498db; }
        .status-badge { padding: 2px 8px; border-radius: 12px; font-size: 11px; font-weight: 600; white-space: nowrap; display: inline-block; }
        .status-active { background: #d1fae5; color: #065f46; }
        .status-block, .status-blocked { background: #fee2e2; color: #991b1b; }
        .status-permanent-block { background: #fca5a5; color: #7f1d1d; }
        .status-restricted { background: #fef3c7; color: #92400e; }
        .status-frozen { background: #dbeafe; color: #1e40af; }
        td.editable { position: relative; }
        td.editable .cell-display { display: block; cursor: default; padding: 2px 4px; border-radius: 3px; min-height: 24px; line-height: 1.4; }
        td.editable.edit-active .cell-display { display: none; }
        td.editable .cell-input { display: none; width: 100%; padding: 4px 6px; border: 2px solid #3498db; border-radius: 4px; font-size: 12px; background: #fff; outline: none; }
        td.editable .cell-select { display: none; width: 100%; padding: 4px 6px; border: 2px solid #3498db; border-radius: 4px; font-size: 12px; background: #fff; outline: none; }
        td.editable.edit-active .cell-input, td.editable.edit-active .cell-select { display: block; }
        td.editable.saving { opacity: 0.6; }
        td.editable.saved .cell-display { background: #d1fae5; transition: background 1s; }
        td.editable.error-state .cell-display { background: #fee2e2; }
        /* Dirty indicator: highlight unsaved edited cells */
        td.editable.dirty .cell-input, td.editable.dirty .cell-select { border-color: #f39c12; background: #fffbf0; }
        tr.row-selected td { box-shadow: inset 0 0 0 2px #3498db; }
        .pagination { display: flex; justify-content: center; align-items: center; gap: 8px; margin-top: 12px; padding: 10px; background: white; border-radius: 8px; box-shadow: 0 1px 8px rgba(0,0,0,0.08); }
        .pagination-btn { padding: 5px 12px; background: #3498db; color: white; border: none; border-radius: 5px; cursor: pointer; font-size: 12px; display: inline-flex; align-items: center; gap: 4px; height: 30px; }
        .pagination-btn:hover:not(:disabled) { background: #2980b9; }
        .pagination-btn:disabled { background: #bdc3c7; cursor: not-allowed; }
        .pagination-info { color: #7f8c8d; font-size: 12px; }
        .toast { position: fixed; bottom: 70px; right: 15px; padding: 8px 16px; border-radius: 6px; font-size: 12px; font-weight: 600; color: white; z-index: 3000; animation: fadeUp 0.3s ease; box-shadow: 0 4px 12px rgba(0,0,0,0.15); }
        .toast.success { background: #2ecc71; }
        .toast.error { background: #e74c3c; }
        .toast.info { background: #3498db; }
        @keyframes fadeUp { from { opacity: 0; transform: translateY(15px); } to { opacity: 1; transform: translateY(0); } }
        .loading-overlay { display: none; position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(255,255,255,0.8); z-index: 2000; justify-content: center; align-items: center; }
        .spinner { border: 3px solid #f3f3f3; border-top: 3px solid #3498db; border-radius: 50%; width: 35px; height: 35px; animation: spin 1s linear infinite; }
        @keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }
        .modal-overlay { display: none; position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.6); z-index: 2000; justify-content: center; align-items: center; }
        .modal-overlay.show { display: flex; }
        .modal-box { background: white; border-radius: 12px; padding: 30px; max-width: 400px; width: 90%; text-align: center; box-shadow: 0 10px 40px rgba(0,0,0,0.2); }
        .modal-icon { font-size: 48px; color: #e74c3c; margin-bottom: 15px; }
        .modal-box h3 { font-size: 18px; color: #2c3e50; margin-bottom: 10px; }
        .modal-box p { font-size: 14px; color: #7f8c8d; margin-bottom: 5px; line-height: 1.5; }
        .modal-box .warn { font-size: 12px; color: #e74c3c; margin-top: 8px; }
        .modal-buttons { display: flex; justify-content: center; gap: 12px; margin-top: 24px; }
        .modal-btn { padding: 9px 22px; border: none; border-radius: 6px; font-size: 13px; font-weight: 600; cursor: pointer; display: inline-flex; align-items: center; gap: 6px; }
        .modal-btn.confirm { background: #e74c3c; color: white; }
        .modal-btn.confirm:hover { background: #c0392b; }
        .modal-btn.cancel { background: #f1f3f4; color: #2c3e50; border: 1px solid #dee2e6; }
        .modal-btn.cancel:hover { background: #e2e6ea; }
        .footer { position: fixed; bottom: 0; left: 0; right: 0; background: #2c3e50; color: white; padding: 8px 15px; text-align: right; font-size: 12px; font-weight: bold; border-top: 2px solid #3498db; }
        .footer span { color: #3498db; }
        .empty-state { text-align: center; padding: 40px; color: #7f8c8d; }
        .empty-state i { font-size: 32px; color: #ddd; display: block; margin-bottom: 10px; }
        .pending-count { background: #f39c12; color: white; border-radius: 50%; width: 18px; height: 18px; font-size: 10px; display: inline-flex; align-items: center; justify-content: center; font-weight: bold; margin-left: 2px; }
    </style>
</head>
<body>
<div class="loading-overlay" id="loadingOverlay"><div class="spinner"></div></div>

<div class="modal-overlay" id="permBlockModal">
    <div class="modal-box">
        <div class="modal-icon"><i class="fas fa-ban"></i></div>
        <h3>Permanent Block?</h3>
        <p>Are you sure you want to permanently block this account?</p>
        <p class="warn"><i class="fas fa-exclamation-triangle"></i> This will move the account to Permanent Block Accounts.</p>
        <div class="modal-buttons">
            <button class="modal-btn cancel" onclick="cancelPermBlock()"><i class="fas fa-times"></i> Cancel</button>
            <button class="modal-btn confirm" onclick="confirmPermBlock()"><i class="fas fa-check"></i> Yes, Block</button>
        </div>
    </div>
</div>

<div class="fixed-header">
    <div class="header">
        <div class="logo">
            <h1><i class="fas fa-edit" style="color:#3498db;"></i> Update Social Media Accounts</h1>
            <p>Check a row to enable editing &bull; Click <strong>Update Data</strong> to save changes &bull; Total: <strong>{{ total_rows }}</strong> records</p>
        </div>
        <div class="header-actions">
            <div class="edit-mode-info" id="editModeInfo">
                <i class="fas fa-pencil-alt"></i> Edit Mode Active — Edit cells, then click Update Data
            </div>
            <button class="btn btn-edit" id="editDataBtn" onclick="toggleEditMode()">
                <i class="fas fa-edit"></i> Edit Data
            </button>
            <button class="btn btn-update" id="updateDataBtn" onclick="saveAllPendingChanges()" disabled title="Save all pending changes">
                <i class="fas fa-save"></i> Update Data
            </button>
            <a href="/?page=social" class="btn btn-back">
                <i class="fas fa-arrow-left"></i> Back
            </a>
        </div>
    </div>
</div>

<div class="container">
    <div class="filters">
        <div class="filters-top">
            <h2><i class="fas fa-filter"></i> Filter Records</h2>
            <span id="selectedInfo" style="font-size:12px;color:#3498db;font-weight:600;display:none;">
                <i class="fas fa-check-square"></i> <span id="selectedCount">0</span> row(s) selected
            </span>
        </div>
        <form method="GET" action="/update-social-accounts" class="filters-row" id="filterForm">
            <div class="filter-group">
                <label><i class="fas fa-search"></i> Search</label>
                <input type="text" name="search" value="{{ search }}" placeholder="Search... (Enter)"
                       onkeydown="if(event.keyCode===13){event.preventDefault();this.form.submit();}">
            </div>
            <div class="filter-group">
                <label><i class="fas fa-mobile-alt"></i> Platform</label>
                <select name="platform" onchange="this.form.submit()">
                    <option value="">All Platforms</option>
                    {% for p in social_platform_options %}
                    <option value="{{ p }}" {% if platform == p %}selected{% endif %}>{{ p }}</option>
                    {% endfor %}
                </select>
            </div>
        </form>
    </div>

    <div class="table-container">
        <table id="updateTable">
            <thead>
                <tr>
                    <th class="checkbox-col"><input type="checkbox" id="selectAll" onchange="toggleSelectAll(this)" title="Select All"></th>
                    <th>ID</th>
                    <th>Login User</th>
                    <th>Number</th>
                    <th>Login Device</th>
                    <th>Account Status</th>
                    <th>Review Status</th>
                    <th>Blocked Date</th>
                    <th>Unblocked Date</th>
                    <th>Recharge Date</th>
                </tr>
            </thead>
            <tbody>
                {% if items %}
                {% for item in items %}
                {% set s = (item.account_status or 'active')|lower|replace(' ', '-') %}
                <tr id="row-{{ item.id }}" class="row-{{ s }}" data-id="{{ item.id }}" data-platform="{{ item.platform or '' }}">
                    <td class="checkbox-col">
                        <input type="checkbox" class="row-checkbox" data-id="{{ item.id }}" onchange="onRowCheckboxChange(this)">
                    </td>
                    <td style="color:#7f8c8d;font-size:11px;">#{{ item.id }}</td>

                    <td class="editable" data-field="login_user" data-id="{{ item.id }}" data-original="{{ item.login_user or '' }}">
                        <span class="cell-display">{{ item.login_user or 'N/A' }}</span>
                        <input class="cell-input" type="text" value="{{ item.login_user or '' }}" onchange="markDirty(this.closest('td'))">
                    </td>

                    <td class="editable" data-field="number" data-id="{{ item.id }}" data-original="{{ item.number or '' }}">
                        <span class="cell-display">{{ item.number or 'N/A' }}</span>
                        <input class="cell-input" type="text" value="{{ item.number or '' }}" onchange="markDirty(this.closest('td'))">
                    </td>

                    <td class="editable" data-field="login_device" data-id="{{ item.id }}" data-original="{{ item.login_device or '' }}">
                        <span class="cell-display">{{ item.login_device or 'N/A' }}</span>
                        <input class="cell-input" type="text" value="{{ item.login_device or '' }}" onchange="markDirty(this.closest('td'))">
                    </td>

                    <td class="editable" data-field="account_status" data-id="{{ item.id }}" data-original="{{ item.account_status or 'Active' }}">
                        {% set s_val = item.account_status or 'Active' %}
                        {% set sc = s_val|lower|replace(' ', '-') %}
                        <span class="cell-display"><span class="status-badge status-{{ sc }}">{{ s_val }}</span></span>
                        <select class="cell-select" onchange="handleStatusChange(event, this)">
                            {% set plat = item.platform or '' %}
                            {% if plat in platform_account_status %}
                                {% for opt in platform_account_status[plat] %}
                                <option value="{{ opt }}" {% if opt == s_val %}selected{% endif %}>{{ opt }}</option>
                                {% endfor %}
                            {% else %}
                                {% for opt in ['Active', 'Block', 'Restricted', 'Frozen', 'Permanent Block'] %}
                                <option value="{{ opt }}" {% if opt == s_val %}selected{% endif %}>{{ opt }}</option>
                                {% endfor %}
                            {% endif %}
                        </select>
                    </td>

                    <td class="editable" data-field="review_status" data-id="{{ item.id }}" data-original="{{ item.review_status or 'NA' }}">
                        <span class="cell-display">{{ item.review_status or 'NA' }}</span>
                        <select class="cell-select" onchange="markDirty(this.closest('td'))">
                            {% for opt in ['NA', 'Send', 'Appeal Submit', 'Video Verification Done'] %}
                            <option value="{{ opt }}" {% if opt == (item.review_status or 'NA') %}selected{% endif %}>{{ opt }}</option>
                            {% endfor %}
                        </select>
                    </td>

                    <td class="editable" data-field="blocked_date" data-id="{{ item.id }}" data-original="{{ item.blocked_date or '' }}">
                        <span class="cell-display">{{ item.blocked_date or 'N/A' }}</span>
                        <input class="cell-input" type="date" value="{{ item.blocked_date or '' }}" onchange="markDirty(this.closest('td'))">
                    </td>

                    <td class="editable" data-field="unblock_date" data-id="{{ item.id }}" data-original="{{ item.unblock_date or '' }}">
                        <span class="cell-display">{{ item.unblock_date or 'N/A' }}</span>
                        <input class="cell-input" type="date" value="{{ item.unblock_date or '' }}" onchange="markDirty(this.closest('td'))">
                    </td>

                    <td class="editable" data-field="recharge_date" data-id="{{ item.id }}" data-original="{{ item.recharge_date or '' }}">
                        <span class="cell-display">{{ item.recharge_date or 'N/A' }}</span>
                        <input class="cell-input" type="date" value="{{ item.recharge_date or '' }}" onchange="markDirty(this.closest('td'))">
                    </td>
                </tr>
                {% endfor %}
                {% else %}
                <tr><td colspan="10"><div class="empty-state"><i class="fas fa-users"></i>No records found. Try adjusting filters.</div></td></tr>
                {% endif %}
            </tbody>
        </table>
    </div>

    {% if total_pages > 1 %}
    <div class="pagination">
        <button class="pagination-btn" onclick="goToPage({{ page_num - 1 }})" {% if page_num <= 1 %}disabled{% endif %}><i class="fas fa-chevron-left"></i> Prev</button>
        <span class="pagination-info">Page {{ page_num }} of {{ total_pages }} &bull; {{ total_rows }} total</span>
        <button class="pagination-btn" onclick="goToPage({{ page_num + 1 }})" {% if page_num >= total_pages %}disabled{% endif %}>Next <i class="fas fa-chevron-right"></i></button>
    </div>
    {% endif %}
</div>

<div class="footer">Devloped by <span>Shubhankar Shukla</span></div>

<script>
    let editModeActive = false;
    let selectedRows = new Set();
    // pendingChanges: Map of "rowId|field" -> {id, field, value, td}
    let pendingChanges = new Map();
    let pendingPermBlock = { td: null, selectEl: null, oldValue: null };

    // ─── Edit Mode Toggle ───────────────────────────────────────
    function toggleEditMode() {
        editModeActive = !editModeActive;
        const btn = document.getElementById('editDataBtn');
        const info = document.getElementById('editModeInfo');
        if (editModeActive) {
            btn.classList.add('active');
            btn.innerHTML = '<i class="fas fa-times"></i> Exit Edit';
            info.classList.add('show');
        } else {
            btn.classList.remove('active');
            btn.innerHTML = '<i class="fas fa-edit"></i> Edit Data';
            info.classList.remove('show');
            document.querySelectorAll('td.editable.edit-active').forEach(td => deactivateCell(td));
            // Clear dirty state
            pendingChanges.clear();
            updateUpdateBtn();
        }
        selectedRows.forEach(id => {
            const row = document.getElementById('row-' + id);
            if (row) setRowEditState(row, editModeActive);
        });
    }

    function toggleSelectAll(cb) {
        document.querySelectorAll('.row-checkbox').forEach(c => {
            c.checked = cb.checked;
            const id = parseInt(c.dataset.id);
            if (cb.checked) selectedRows.add(id); else selectedRows.delete(id);
            const row = document.getElementById('row-' + id);
            if (row) { row.classList.toggle('row-selected', cb.checked); if (editModeActive) setRowEditState(row, cb.checked); }
        });
        updateSelectedInfo();
    }

    function onRowCheckboxChange(cb) {
        const id = parseInt(cb.dataset.id);
        const row = document.getElementById('row-' + id);
        if (cb.checked) {
            selectedRows.add(id); row.classList.add('row-selected');
            if (editModeActive) setRowEditState(row, true);
        } else {
            selectedRows.delete(id); row.classList.remove('row-selected');
            setRowEditState(row, false);
        }
        updateSelectedInfo();
    }

    function updateSelectedInfo() {
        const info = document.getElementById('selectedInfo');
        document.getElementById('selectedCount').textContent = selectedRows.size;
        info.style.display = selectedRows.size > 0 ? 'inline-flex' : 'none';
    }

    function setRowEditState(row, active) {
        row.querySelectorAll('td.editable').forEach(td => { if (active) activateCell(td); else deactivateCell(td); });
    }

    function activateCell(td) {
        td.classList.add('edit-active');
        const input = td.querySelector('.cell-input');
        const select = td.querySelector('.cell-select');
        if (input) input.removeAttribute('disabled');
        if (select) select.removeAttribute('disabled');
    }

    function deactivateCell(td) {
        td.classList.remove('edit-active', 'saving', 'saved', 'error-state', 'dirty');
        const input = td.querySelector('.cell-input');
        const select = td.querySelector('.cell-select');
        if (input) { input.setAttribute('disabled', ''); input.value = td.dataset.original || ''; }
        if (select) { select.setAttribute('disabled', ''); select.value = td.dataset.original || select.options[0]?.value || ''; }
    }

    // Mark a cell as having unsaved changes
    function markDirty(td) {
        const field = td.dataset.field;
        const rowId = td.dataset.id;
        const input = td.querySelector('.cell-input');
        const select = td.querySelector('.cell-select');
        const newValue = (input ? input.value : (select ? select.value : ''));
        const key = rowId + '|' + field;

        td.classList.add('dirty');
        pendingChanges.set(key, { id: rowId, field: field, value: newValue, td: td });
        updateUpdateBtn();
    }

    // Handle status change — intercept Permanent Block
    function handleStatusChange(e, selectEl) {
        const newValue = selectEl.value;
        const td = selectEl.closest('td.editable');
        if (newValue === 'Permanent Block') {
            pendingPermBlock = {
                td: td,
                selectEl: selectEl,
                oldValue: td.dataset.original || 'Active'
            };
            document.getElementById('permBlockModal').classList.add('show');
        } else {
            markDirty(td);
        }
    }

    function cancelPermBlock() {
        document.getElementById('permBlockModal').classList.remove('show');
        if (pendingPermBlock.selectEl) pendingPermBlock.selectEl.value = pendingPermBlock.oldValue || 'Active';
        pendingPermBlock = { td: null, selectEl: null, oldValue: null };
    }

    function confirmPermBlock() {
        document.getElementById('permBlockModal').classList.remove('show');
        if (pendingPermBlock.td) markDirty(pendingPermBlock.td);
        pendingPermBlock = { td: null, selectEl: null, oldValue: null };
    }

    // Update the "Update Data" button state
    function updateUpdateBtn() {
        const btn = document.getElementById('updateDataBtn');
        if (pendingChanges.size > 0) {
            btn.disabled = false;
            btn.innerHTML = '<i class="fas fa-save"></i> Update Data <span class="pending-count">' + pendingChanges.size + '</span>';
        } else {
            btn.disabled = true;
            btn.innerHTML = '<i class="fas fa-save"></i> Update Data';
        }
    }

    // Save ALL pending changes when "Update Data" is clicked
    async function saveAllPendingChanges() {
        if (pendingChanges.size === 0) {
            showToast('No changes to save', 'info');
            return;
        }

        const btn = document.getElementById('updateDataBtn');
        btn.disabled = true;
        btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Saving...';

        const changes = Array.from(pendingChanges.values());
        let successCount = 0;
        let errorCount = 0;

        for (const change of changes) {
            try {
                const response = await fetch('/save-social-field', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ id: change.id, field: change.field, value: change.value })
                });
                const data = await response.json();

                if (data.success) {
                    successCount++;
                    const td = change.td;
                    const displayEl = td.querySelector('.cell-display');
                    if (change.field === 'account_status') {
                        const sc = change.value.toLowerCase().replace(/ /g, '-');
                        displayEl.innerHTML = '<span class="status-badge status-' + sc + '">' + change.value + '</span>';
                        const row = td.closest('tr');
                        row.className = row.className.replace(/row-\S+/g, '').trim();
                        row.classList.add('row-' + sc);
                        // If permanent block, animate row out
                        if (change.value === 'Permanent Block') {
                            setTimeout(() => {
                                row.style.transition = 'all 0.5s ease';
                                row.style.opacity = '0.3';
                                setTimeout(() => row.remove(), 600);
                            }, 500);
                        }
                    } else {
                        displayEl.textContent = change.value || 'N/A';
                    }
                    td.dataset.original = change.value;
                    td.classList.remove('dirty', 'error-state');
                    td.classList.add('saved');
                    setTimeout(() => td.classList.remove('saved'), 2000);
                } else {
                    errorCount++;
                    change.td.classList.add('error-state');
                }
            } catch (err) {
                errorCount++;
                change.td.classList.add('error-state');
            }
        }

        // Remove successfully saved changes
        for (const [key, change] of pendingChanges.entries()) {
            if (!change.td.classList.contains('error-state')) {
                pendingChanges.delete(key);
            }
        }

        updateUpdateBtn();

        if (errorCount === 0) {
            showToast('All ' + successCount + ' change(s) saved successfully!', 'success');
        } else if (successCount > 0) {
            showToast(successCount + ' saved, ' + errorCount + ' failed. Check red cells.', 'error');
        } else {
            showToast('Failed to save changes. Please try again.', 'error');
        }
    }

    function goToPage(page) {
        const url = new URL(window.location.href);
        url.searchParams.set('page_num', page);
        window.location.href = url.toString();
    }

    function showToast(message, type) {
        const existing = document.querySelector('.toast');
        if (existing) existing.remove();
        const t = document.createElement('div');
        t.className = 'toast ' + (type || 'info');
        t.innerHTML = '<i class="fas fa-' + (type === 'error' ? 'times' : type === 'success' ? 'check' : 'info-circle') + '"></i> ' + message;
        document.body.appendChild(t);
        setTimeout(() => { if (t.parentNode) t.remove(); }, 3500);
    }

    document.addEventListener('DOMContentLoaded', () => {
        document.querySelectorAll('td.editable .cell-input, td.editable .cell-select').forEach(el => el.setAttribute('disabled', ''));
    });
</script>
</body>
</html>"""

    return render_template_string(UPDATE_TEMPLATE,
        items=items,
        search=search,
        platform=platform,
        page_num=page,
        total_pages=total_pages,
        total_rows=total_rows,
        social_platform_options=SOCIAL_PLATFORM_OPTIONS,
        platform_account_status=PLATFORM_ACCOUNT_STATUS
    )


# ============================================================
# SAVE SINGLE FIELD - called by Update Data batch save
# ============================================================
@app.route("/save-social-field", methods=["POST"])
def save_social_field():
    """Save a single field edit for a social media account"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"success": False, "error": "No data provided"})

        account_id = data.get('id')
        field = data.get('field')
        value = data.get('value', '').strip()

        if not account_id or not field:
            return jsonify({"success": False, "error": "Missing id or field"})

        EDITABLE_FIELDS = [
            'login_user', 'number', 'login_device',
            'account_status', 'review_status',
            'blocked_date',
            'unblock_date',
            'recharge_date'
        ]

        if field not in EDITABLE_FIELDS:
            return jsonify({"success": False, "error": f"Field '{field}' is not editable"})

        update_payload = {field: value if value else "NA"}

        # If setting Permanent Block, also set blocked_date
        if field == 'account_status' and value == 'Permanent Block':
            today = datetime.now().strftime("%Y-%m-%d")
            update_payload['blocked_date'] = today

        response = social_supabase.table("social_media_accounts") \
            .update(update_payload) \
            .eq("id", account_id) \
            .execute()

        if hasattr(response, 'data'):
            if response.data:
                return jsonify({
                    "success": True,
                    "message": "Saved successfully",
                    "updated_row": response.data[0]
                })
            else:
                try:
                    verify = social_supabase.table("social_media_accounts") \
                        .select("id") \
                        .eq("id", account_id) \
                        .execute()
                    if verify.data:
                        return jsonify({"success": False, "error": "Update failed - check Supabase API key permissions (need service_role key for writes)"})
                    else:
                        return jsonify({"success": False, "error": f"Row with id {account_id} not found"})
                except Exception as ve:
                    return jsonify({"success": False, "error": f"Update failed: {str(ve)}"})
        else:
            return jsonify({"success": False, "error": "No response from Supabase"})

    except Exception as e:
        print(f"Error saving social field: {e}")
        return jsonify({"success": False, "error": str(e)})


# ============================================================
# PERMANENT BLOCK ACCOUNTS - fetch for modal
# ============================================================
@app.route("/get-permanent-block-accounts", methods=["GET"])
def get_permanent_block_accounts():
    """Fetch permanently blocked accounts with active duration, platform, search"""
    try:
        search = request.args.get("search", "").strip()

        query = social_supabase.table("social_media_accounts") \
            .select("id,owned_by,number,login_device,blocked_date,account_create_date,platform") \
            .eq("account_status", "Permanent Block")

        if search:
            like_term = f"%{search}%"
            query = query.or_(
                f"owned_by.ilike.{like_term},"
                f"number.ilike.{like_term},"
                f"login_device.ilike.{like_term},"
                f"platform.ilike.{like_term}"
            )

        # Sort by ID ascending
        query = query.order("id", desc=False)

        response = query.execute()

        accounts = []
        for item in (response.data or []):
            b_date_str = item.get('blocked_date') or ''
            create_date_str = item.get('account_create_date') or ''

            active_duration = 'N/A'
            if b_date_str and create_date_str:
                try:
                    for fmt in ('%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y', '%Y/%m/%d'):
                        try:
                            b_date = datetime.strptime(b_date_str[:10], fmt)
                            c_date = datetime.strptime(create_date_str[:10], fmt)
                            days = (b_date - c_date).days
                            active_duration = f"{days} days" if days >= 0 else 'N/A'
                            break
                        except ValueError:
                            continue
                except Exception:
                    active_duration = 'N/A'

            accounts.append({
                'id': item.get('id'),
                'owned_by': item.get('owned_by') or 'N/A',
                'number': item.get('number') or 'N/A',
                'login_device': item.get('login_device') or 'N/A',
                'platform': item.get('platform') or 'N/A',
                'blocked_date': b_date_str or 'N/A',
                'active_duration': active_duration
            })

        return jsonify({
            "success": True,
            "accounts": accounts,
            "count": len(accounts)
        })

    except Exception as e:
        print(f"Error fetching permanent block accounts: {e}")
        return jsonify({"success": False, "error": str(e)})


# ============================================================
# ACTIVITY LOG - placeholder
# ============================================================
@app.route("/get-activity-log", methods=["GET"])
def get_activity_log():
    """Activity log placeholder"""
    return jsonify({
        "success": True,
        "placeholder": True,
        "message": "Implement Soon Have Some Patience"
    })

if __name__ == "__main__":
    EXCEL_FOLDER_PATH.mkdir(exist_ok=True)
    
    config = load_config()
    
    print("=" * 60)
    print(" SCAM INTELLIGENCE DASHBOARD")
    print("=" * 60)
    print(f" Base Directory: {BASE_DIR}")
    print(f" Excel Folder: {EXCEL_FOLDER_PATH}")
    print("=" * 60)
    print(" Page Options:")
    print(" 1. Scraping Data")
    print(" 2. Sheet Maker")
    print(" 3. Social Media Accounts (NEW)")
    print("=" * 60)
    print(f" Output format: CSV with 36 columns")
    print(f" File naming: <sheet_type>_<today_date>.csv")
    print("=" * 60)
    print(" DATA SOURCES:")
    
    load_excel_data()
    
    print(f" Master URL Data: {len(MASTER_URL_DATA)} records")
    print(f" Bank Name Mapping: {len(BANK_NAME_MAPPING)} records")
    print(f" IFSC Mapping: {len(IFSC_MAPPING)} records")
    print("=" * 60)
    print(" Social Media Accounts Database:")
    print(f" URL: {SOCIAL_SUPABASE_URL}")
    print("=" * 60)
    
    port = int(os.environ.get("PORT", 5000))
    debug_mode = os.environ.get("FLASK_DEBUG", "False").lower() == "true"
    
    print(f" Server starting on: http://0.0.0.0:{port}")
    print(f" Debug mode: {debug_mode}")
    print(f" Health check: http://0.0.0.0:{port}/health")
    print("=" * 60)
    
    app.run(debug=debug_mode, host='0.0.0.0', port=port)