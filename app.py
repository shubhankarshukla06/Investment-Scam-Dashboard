from flask import Flask, render_template, render_template_string, request, redirect, flash, send_file, jsonify, session
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
from functools import wraps

load_dotenv()

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "your-secret-key-change-this")

# ============================================================
# AUTH HELPERS — users stored in Supabase `dashboard_users` table
# ============================================================

def get_auth_supabase():
    url = os.environ.get("DASHBOARD_SUPABASE_URL") or os.environ.get("SOCIAL_SUPABASE_URL")
    key = os.environ.get("DASHBOARD_SUPABASE_KEY") or os.environ.get("SOCIAL_SUPABASE_KEY")
    return create_client(url, key)


DEMO_ADMIN = {
    "id": 0,
    "email": "test123@gmail.com",
    "password": "test123",
    "display_name": "Shubhankar Shukla (Test User)",
    "allowed_pages": ["scraping", "sheet", "social"],
    "is_admin": True,
    "is_active": True,
    "created_at": "2025-01-01"
}


def fetch_user_by_email(email: str):
    if email.lower().strip() == DEMO_ADMIN["email"]:
        return DEMO_ADMIN
    try:
        client = get_auth_supabase()
        res = client.table("dashboard_users") \
            .select("*") \
            .eq("email", email.lower().strip()) \
            .eq("is_active", True) \
            .limit(1) \
            .execute()
        if res.data:
            return res.data[0]
        return None
    except Exception as e:
        print(f"[AUTH] fetch_user_by_email error: {e}")
        return None


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if "user_id" not in session:
            flash("Please log in to access the dashboard.", "error")
            return redirect("/login")
        return f(*args, **kwargs)
    return decorated


def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if "user_id" not in session:
            return redirect("/login")
        if not session.get("is_admin"):
            flash("You don't have permission to access that page.", "error")
            return redirect("/")
        return f(*args, **kwargs)
    return decorated


def get_current_user():
    if "user_id" not in session:
        return None
    return {
        "id": session.get("user_id"),
        "email": session.get("email"),
        "display_name": session.get("display_name"),
        "allowed_pages": session.get("allowed_pages", []),
        "is_admin": session.get("is_admin", False),
    }


# ============================================================
# Configuration
# ============================================================
PER_PAGE = 100
BASE_DIR = Path(__file__).parent
CONFIG_PATH = BASE_DIR / "sheet_mapping_config.json"

EXCEL_FOLDER_PATH = BASE_DIR / "excel_data"
EXCEL_FOLDER_PATH.mkdir(exist_ok=True)

MASTER_URL_DATA_PATH = EXCEL_FOLDER_PATH / "Website_mapping.xlsx"
BANK_NAME_MAPPING_PATH = EXCEL_FOLDER_PATH / "bank_name.xlsx"
IFSC_MAPPING_PATH = EXCEL_FOLDER_PATH / "ifsc_mapping.xlsx"

supabase: Client = create_client(
    os.environ.get("SUPABASE_URL"),
    os.environ.get("SUPABASE_KEY")
)

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

SOCIAL_PLATFORM_OPTIONS = [
    "Facebook", "Amazon", "Instagram", "Telegram", "WhatsApp"
]

PLATFORM_ACCOUNT_STATUS = {
    "Facebook": ["Active", "Block", "Restricted", "Permanent Block"],
    "Instagram": ["Active", "Block", "Permanent Block"],
    "Telegram": ["Active", "Frozen", "Permanent Block"],
    "WhatsApp": ["Active", "Block", "Permanent Block", "Restricted"],
    "Amazon": ["Active", "Block", "Permanent Block"]
}

REQUIRED_COLUMNS = [
    'customer', 'package_name', 'channel_name', 'bank_account_number',
    'bank_name', 'upi_vpa', 'ac_holder_name', 'screenshot', 'platform',
    'search_for', 'status', 'upi_bank_account_wallet', 'priority', 'flag',
    'cessation', 'reviewed_status', 'handle', 'origin', 'payment_gateway_name',
    'category_of_website', 'screenshot_case_report_link',
    'payment_gateway_intermediate_url', 'neft_imps', 'transaction_method',
    'scam_type', 'ifsc_code', 'bank_branch_details', 'payment_gateway_url',
    'upi_url', 'website_url', 'inserted_date', 'reported_earlier',
    'approvd_status', 'feature_type', 'case_generated_time', 'web_contact_no'
]

SHEET_TYPES = {
    'upi': 'UPI_AML',
    'investment': 'Investment_Scam',
    'messaging': 'Messaging_Channel'
}

MASTER_URL_DATA = {}
BANK_NAME_MAPPING = {}
IFSC_MAPPING = {}


def load_excel_data():
    global MASTER_URL_DATA, BANK_NAME_MAPPING, IFSC_MAPPING
    try:
        if MASTER_URL_DATA_PATH.exists():
            df_master = pd.read_excel(MASTER_URL_DATA_PATH)
            MASTER_URL_DATA = {}
            for _, row in df_master.iterrows():
                url = str(row.get('website_url', '')).strip()
                if url and url.lower() not in ['na', 'nan', '']:
                    MASTER_URL_DATA[url.lower().strip()] = {
                        "origin": str(row.get('origin', 'NA')).strip(),
                        "category_of_website": str(row.get('category_of_website', 'NA')).strip()
                    }
        if BANK_NAME_MAPPING_PATH.exists():
            df_bank = pd.read_excel(BANK_NAME_MAPPING_PATH)
            BANK_NAME_MAPPING = {}
            key_col = next((c for c in df_bank.columns if any(k in str(c).lower() for k in ['key','handle','upi'])), df_bank.columns[0] if len(df_bank.columns) > 0 else None)
            bank_col = next((c for c in df_bank.columns if 'bank' in str(c).lower() and 'name' in str(c).lower()), df_bank.columns[1] if len(df_bank.columns) > 1 else None)
            if key_col and bank_col:
                for _, row in df_bank.iterrows():
                    k = str(row.get(key_col, '')).strip().lower()
                    v = str(row.get(bank_col, 'NA')).strip()
                    if k and k not in ['na', 'nan', '']:
                        BANK_NAME_MAPPING[k] = v
        if IFSC_MAPPING_PATH.exists():
            df_ifsc = pd.read_excel(IFSC_MAPPING_PATH)
            IFSC_MAPPING = {}
            prefix_col = next((c for c in df_ifsc.columns if any(k in str(c).lower() for k in ['ifsc','prefix','code'])), df_ifsc.columns[0] if len(df_ifsc.columns) > 0 else None)
            bank_col2 = next((c for c in df_ifsc.columns if 'bank' in str(c).lower() and 'name' in str(c).lower()), df_ifsc.columns[1] if len(df_ifsc.columns) > 1 else None)
            if prefix_col and bank_col2:
                for _, row in df_ifsc.iterrows():
                    k = str(row.get(prefix_col, '')).strip().upper()
                    v = str(row.get(bank_col2, 'NA')).strip()
                    if k and k.lower() not in ['na', 'nan', '']:
                        IFSC_MAPPING[k] = v
    except Exception as e:
        print(f"Error loading Excel data: {e}")
        MASTER_URL_DATA = {}; BANK_NAME_MAPPING = {}; IFSC_MAPPING = {}


load_excel_data()


def load_config():
    try:
        if CONFIG_PATH.exists():
            with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                return json.load(f)
        return create_default_config()
    except Exception as e:
        print(f"Error loading config: {e}")
        return create_default_config()


def create_default_config():
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
    config = load_config()
    if config and 'global_settings' in config:
        return config['global_settings'].get('allowed_extensions', ['.csv', '.xlsx', '.xls'])
    return ['.csv', '.xlsx', '.xls']


def is_allowed_file(filename):
    if not filename:
        return False
    allowed_extensions = get_allowed_extensions()
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in [ext.lstrip('.') for ext in allowed_extensions]


def read_data_file(file_path, file_ext):
    try:
        file_ext = file_ext.lower()
        if file_ext == 'csv':
            for encoding in ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']:
                try:
                    return pd.read_csv(file_path, encoding=encoding)
                except UnicodeDecodeError:
                    continue
            return pd.read_csv(file_path, encoding=None, engine='python')
        elif file_ext in ['xlsx', 'xls', 'xlsm', 'xlsb']:
            return pd.read_excel(file_path)
        elif file_ext == 'ods':
            return pd.read_excel(file_path, engine='odf')
        else:
            raise ValueError(f"Unsupported file format: {file_ext}")
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        raise


def get_sheet_headers(sheet_type):
    config = load_config()
    if not config:
        return []
    sheet_config = config['sheet_mappings'].get(sheet_type)
    if not sheet_config:
        return []
    return sheet_config.get('required_headers', [])


def standardize_headers(headers, sheet_type):
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
    if pd.isna(value) or value in ["NA", "", None, "null", "NULL", "None", "nan", "NaN", "undefined"]:
        return "NA"
    value_str = str(value).strip()
    value_str = ''.join(char for char in value_str if ord(char) < 0x10000)
    return value_str


def extract_handle(upi_vpa):
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
    if handle != "NA" and handle:
        handle_lower = handle.lower().strip()
        if handle_lower in BANK_NAME_MAPPING:
            return BANK_NAME_MAPPING[handle_lower]
        for key, value in BANK_NAME_MAPPING.items():
            if key in handle_lower or handle_lower in key:
                return value
        common_mappings = {
            'okaxis': 'Axis Bank', 'okicici': 'ICICI Bank', 'okhdfc': 'HDFC Bank',
            'axisbank': 'Axis Bank', 'icici': 'ICICI Bank', 'hdfc': 'HDFC Bank',
            'sbi': 'State Bank of India', 'ybl': 'Yes Bank',
            'paytm': 'Paytm Payments Bank', 'phonepe': 'Yes Bank (PhonePe)'
        }
        for pattern, bank_name in common_mappings.items():
            if pattern in handle_lower:
                return bank_name
    if ifsc_code and ifsc_code != "NA":
        try:
            ifsc_prefix = ifsc_code[:4].upper()
            if ifsc_prefix in IFSC_MAPPING:
                return IFSC_MAPPING[ifsc_prefix]
            common_ifsc = {
                'SBIN': 'State Bank of India', 'ICIC': 'ICICI Bank',
                'HDFC': 'HDFC Bank', 'UTIB': 'Axis Bank', 'CNRB': 'Canara Bank',
                'BARB': 'Bank of Baroda', 'BKID': 'Bank of India',
                'PUNB': 'Punjab National Bank', 'UBIN': 'Union Bank of India',
                'INDB': 'IndusInd Bank', 'YESB': 'Yes Bank',
                'KARB': 'Karnataka Bank', 'FDRL': 'Federal Bank',
                'IDFB': 'IDFC First Bank', 'RATN': 'RBL Bank'
            }
            if ifsc_prefix in common_ifsc:
                return common_ifsc[ifsc_prefix]
        except Exception as e:
            print(f"Error in IFSC lookup for {ifsc_code}: {e}")
    return "NA"


def extract_search_for_from_url(url):
    url_value = clean_value(url)
    if url_value == "NA":
        return "Platform"
    try:
        parsed_url = urlparse(url_value.lower())
        domain = parsed_url.netloc
        if not domain:
            return "Platform"
        domain_without_www = domain[4:] if domain.startswith('www.') else domain
        platform_domains = {
            't.me': 'Telegram', 'wa.me': 'WhatsApp',
            'chat.whatsapp.com': 'WhatsApp', 'facebook.com': 'Facebook',
            'instagram.com': 'Instagram', 'telegram.org': 'Telegram',
            'threads.com': 'Thread', 'youtube.com': 'YouTube', 'x.com': 'X'
        }
        if domain in platform_domains:
            return platform_domains[domain]
        if domain_without_www in platform_domains:
            return platform_domains[domain_without_www]
        if url_value.lower().startswith(('https://', 'http://')):
            return "Web"
        return "Platform"
    except Exception:
        return "Platform"


def lookup_origin_and_category_from_master(url):
    url_value = clean_value(url)
    if url_value == "NA":
        return "NA", "NA"
    url_clean = url_value.lower().strip()
    if url_clean in MASTER_URL_DATA:
        return MASTER_URL_DATA[url_clean]["origin"], MASTER_URL_DATA[url_clean]["category_of_website"]
    for alt in [("http://", "https://"), ("https://", "http://")]:
        if url_clean.startswith(alt[0]):
            alt_url = alt[1] + url_clean[len(alt[0]):]
            if alt_url in MASTER_URL_DATA:
                return MASTER_URL_DATA[alt_url]["origin"], MASTER_URL_DATA[alt_url]["category_of_website"]
    try:
        domain = urlparse(url_clean).netloc
        for master_url, data in MASTER_URL_DATA.items():
            try:
                if urlparse(master_url).netloc == domain:
                    return data["origin"], data["category_of_website"]
            except:
                continue
    except:
        pass
    return "NA", "NA"


def extract_case_time_and_date_from_npci_url(url):
    if not url or url == "NA":
        return "NA", "NA"
    match = re.search(r'npci-(\d{10})_', url)
    if not match:
        return "NA", "NA"
    try:
        ts = int(match.group(1))
        utc_dt = datetime.utcfromtimestamp(ts)
        ist_dt = utc_dt + timedelta(hours=5, minutes=30)
        return ist_dt.strftime("%Y-%m-%d %H:%M:%S"), ist_dt.strftime("%Y-%m-%d")
    except Exception:
        return "NA", "NA"


def generate_screenshot_urls(screenshot_url):
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
                _, rest_of_filename = parts
                urls = []
                for new_prefix in ['mfilterit', 'npci', 'without_header']:
                    new_filename = f"{new_prefix}-{rest_of_filename}"
                    new_path = '/'.join(path.split('/')[:-1] + [new_filename])
                    new_url = f"{parsed_url.scheme}://{parsed_url.netloc}{new_path}"
                    urls.append(new_url)
                return ','.join(urls)
        return screenshot_value
    except Exception as e:
        return screenshot_value


def extract_payment_gateway_name(upi_url, website_url):
    upi_url_value = clean_value(upi_url)
    website_url_value = clean_value(website_url)
    if upi_url_value == "NA":
        return "NA"
    try:
        parsed_upi = urlparse(upi_url_value)
        upi_domain = parsed_upi.netloc
        if not upi_domain:
            path = parsed_upi.path.lstrip('/')
            domain_part = path.split('/')[0]
            upi_domain = domain_part if '.' in domain_part else None
            if not upi_domain:
                return "NA"
        upi_domain_clean = upi_domain[4:] if upi_domain.startswith('www.') else upi_domain
        if website_url_value == "NA":
            return upi_domain
        parsed_website = urlparse(website_url_value)
        website_domain = parsed_website.netloc
        if not website_domain:
            path = parsed_website.path.lstrip('/')
            domain_part = path.split('/')[0]
            website_domain = domain_part if '.' in domain_part else None
            if not website_domain:
                return upi_domain
        website_domain_clean = website_domain[4:] if website_domain.startswith('www.') else website_domain
        return "NA" if upi_domain_clean == website_domain_clean else upi_domain
    except Exception as e:
        return "NA"


def process_sheet_data(df, sheet_type):
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
            row_data.update({
                'customer': "Mystery Shopping", 'package_name': "com.mysteryshopping",
                'channel_name': "Organic Search", 'status': "Active", 'priority': "High",
                'flag': "1", 'cessation': "Open", 'reviewed_status': "1",
                'reported_earlier': "No", 'approvd_status': "1",
                'feature_type': "BS Money Laundering", 'platform': "NA",
                'neft_imps': "NA", 'bank_branch_details': "NA", 'scam_type': "NA"
            })
            row_data['upi_bank_account_wallet'] = "UPI" if row_data['upi_vpa'] != "NA" else "Bank Account"
            if row_data['website_url'] != "NA":
                origin, category = lookup_origin_and_category_from_master(row_data['website_url'])
                row_data['origin'] = origin
                row_data['category_of_website'] = category
            else:
                row_data['origin'] = "NA"; row_data['category_of_website'] = "NA"

        elif sheet_type == 'investment':
            row_data.update({
                'customer': "Mystery Shopping", 'package_name': "com.mysteryshopping",
                'channel_name': "Organic Search", 'status': "Active", 'priority': "High",
                'flag': "1", 'cessation': "Open", 'reviewed_status': "1",
                'reported_earlier': "No", 'approvd_status': "1",
                'feature_type': "BS Investment Scam", 'platform': "NA",
                'neft_imps': "NA", 'bank_branch_details': "NA"
            })
            row_data['upi_bank_account_wallet'] = "UPI" if row_data['upi_vpa'] != "NA" else "Bank Account"
            if row_data['scam_type'] != "NA" and row_data['category_of_website'] == "NA":
                row_data['category_of_website'] = row_data['scam_type']
            if row_data['website_url'] != "NA":
                origin, _ = lookup_origin_and_category_from_master(row_data['website_url'])
                row_data['origin'] = origin
            else:
                row_data['origin'] = "NA"

        elif sheet_type == 'messaging':
            row_data.update({
                'customer': "Mystery Shopping", 'package_name': "com.mysteryshopping",
                'channel_name': "Messaging Channel Platforms", 'status': "Active",
                'priority': "High", 'flag': "1", 'cessation': "Open",
                'reviewed_status': "1", 'reported_earlier': "No", 'approvd_status': "1",
                'feature_type': "BS Money Laundering", 'platform': "NA",
                'neft_imps': "NA", 'bank_branch_details': "NA", 'scam_type': "NA",
                'origin': "India"
            })
            row_data['upi_bank_account_wallet'] = "UPI" if row_data['upi_vpa'] != "NA" else "Bank Account"

        handle = extract_handle(row_data['upi_vpa'])
        row_data['handle'] = handle
        row_data['bank_name'] = get_bank_name_from_handle(handle, row_data['ifsc_code'])
        row_data['search_for'] = extract_search_for_from_url(row_data['website_url'])
        if sheet_type != 'messaging' and row_data['category_of_website'] == "NA":
            if row_data['website_url'] != "NA":
                _, category = lookup_origin_and_category_from_master(row_data['website_url'])
                row_data['category_of_website'] = category
        row_data['screenshot_case_report_link'] = row_data.get('screenshot', "NA")
        row_data.pop('_original_screenshot', None)
        payment_gateway_url = row_data.get('payment_gateway_url', "NA")
        if payment_gateway_url != "NA":
            row_data['payment_gateway_intermediate_url'] = payment_gateway_url
            row_data['upi_url'] = payment_gateway_url
            row_data['payment_gateway_name'] = extract_payment_gateway_name(
                row_data['upi_url'], row_data['website_url']
            )
        else:
            row_data['payment_gateway_intermediate_url'] = "NA"
            row_data['upi_url'] = "NA"
            row_data['payment_gateway_name'] = "NA"
        result_df.loc[idx] = [row_data.get(col, "NA") for col in REQUIRED_COLUMNS]

    return result_df, {
        'total_values': len(result_df),
        'unique_upi_ids': len(unique_upi_ids),
        'unique_bank_accounts': len(unique_bank_accounts)
    }


# ============================================================
# LOGIN / LOGOUT ROUTES
# ============================================================
@app.route("/login", methods=["GET", "POST"])
def login():
    if "user_id" in session:
        return redirect("/")

    error = None
    prefill_email = ""

    if request.method == "POST":
        email    = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")
        prefill_email = email

        if not email or not password:
            error = "Please enter both email and password."
        else:
            user = fetch_user_by_email(email)
            if user and user.get("password") == password:
                session.permanent = False
                session["user_id"]      = user["id"]
                session["email"]        = user["email"]
                session["display_name"] = user["display_name"]
                session["allowed_pages"]= user.get("allowed_pages") or []
                session["is_admin"]     = bool(user.get("is_admin", False))

                allowed = session["allowed_pages"]
                first_page = allowed[0] if allowed else "scraping"
                return redirect(f"/?page={first_page}")
            else:
                error = "Invalid email or password. Please try again."

    return render_template("login.html", error=error, prefill_email=prefill_email)


@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login")


# ============================================================
# ADMIN — USER MANAGEMENT PANEL
# ============================================================
@app.route("/admin/users", methods=["GET"])
@admin_required
def admin_users():
    try:
        client = get_auth_supabase()
        res = client.table("dashboard_users").select("*").order("id", desc=False).execute()
        users = res.data or []
    except Exception as e:
        flash(f"Error loading users: {e}", "error")
        users = []
    return render_template(
        "admin.html",
        users=users,
        display_name=session.get("display_name", "Admin")
    )


@app.route("/admin/users", methods=["POST"])
@admin_required
def admin_add_user():
    try:
        data = request.get_json()
        if not data:
            return jsonify({"success": False, "error": "No data provided"})
        email    = (data.get("email") or "").strip().lower()
        password = (data.get("password") or "").strip()
        name     = (data.get("display_name") or "").strip()
        pages    = data.get("allowed_pages", [])
        is_admin = bool(data.get("is_admin", False))
        is_active= bool(data.get("is_active", True))
        if not email or not password or not name:
            return jsonify({"success": False, "error": "Email, password, and name are required."})
        if not pages:
            return jsonify({"success": False, "error": "Select at least one page for this user."})
        client = get_auth_supabase()
        dup = client.table("dashboard_users").select("id").eq("email", email).execute()
        if dup.data:
            return jsonify({"success": False, "error": f"A user with email {email} already exists."})
        client.table("dashboard_users").insert({
            "email": email, "password": password, "display_name": name,
            "allowed_pages": pages, "is_admin": is_admin, "is_active": is_active
        }).execute()
        return jsonify({"success": True, "message": "User added successfully."})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route("/admin/users/<int:user_id>", methods=["PUT"])
@admin_required
def admin_edit_user(user_id):
    try:
        data = request.get_json()
        if not data:
            return jsonify({"success": False, "error": "No data provided"})
        update_payload = {}
        name = (data.get("display_name") or "").strip()
        if name: update_payload["display_name"] = name
        email = (data.get("email") or "").strip().lower()
        if email:
            client = get_auth_supabase()
            dup = client.table("dashboard_users").select("id").eq("email", email).neq("id", user_id).execute()
            if dup.data:
                return jsonify({"success": False, "error": f"Email {email} is already used by another user."})
            update_payload["email"] = email
        password = (data.get("password") or "").strip()
        if password: update_payload["password"] = password
        pages = data.get("allowed_pages")
        if pages is not None:
            if not pages:
                return jsonify({"success": False, "error": "Select at least one page."})
            update_payload["allowed_pages"] = pages
        if "is_admin" in data: update_payload["is_admin"] = bool(data["is_admin"])
        if "is_active" in data: update_payload["is_active"] = bool(data["is_active"])
        if not update_payload:
            return jsonify({"success": False, "error": "Nothing to update."})
        client = get_auth_supabase()
        client.table("dashboard_users").update(update_payload).eq("id", user_id).execute()
        if session.get("user_id") == user_id:
            if "display_name" in update_payload: session["display_name"] = update_payload["display_name"]
            if "allowed_pages" in update_payload: session["allowed_pages"] = update_payload["allowed_pages"]
            if "is_admin" in update_payload: session["is_admin"] = update_payload["is_admin"]
            if "email" in update_payload: session["email"] = update_payload["email"]
        return jsonify({"success": True, "message": "User updated successfully."})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route("/admin/users/<int:user_id>", methods=["DELETE"])
@admin_required
def admin_delete_user(user_id):
    try:
        if session.get("user_id") == user_id:
            return jsonify({"success": False, "error": "You cannot delete your own account."})
        client = get_auth_supabase()
        client.table("dashboard_users").delete().eq("id", user_id).execute()
        return jsonify({"success": True, "message": "User deleted."})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


# ============================================================
# SCRAPING TRACKER STATS — UPDATED WITH PLATFORM BREAKDOWN
# ============================================================
@app.route("/scraping-tracker-stats", methods=["GET"])
@login_required
def scraping_tracker_stats():
    """
    Return scam type counts, platform counts, and a scam_type × platform
    breakdown matrix for the scraping tracker.
    """
    try:
        # Supabase caps each request at 1000 rows — paginate to get all data
        CHUNK = 1000
        rows = []
        offset = 0
        while True:
            resp = supabase.table("scrapping_data") \
                .select("scam_type,platform") \
                .order("id", desc=False) \
                .range(offset, offset + CHUNK - 1) \
                .execute()
            chunk = resp.data or []
            rows.extend(chunk)
            if len(chunk) < CHUNK:
                break
            offset += CHUNK

        scam_counts = {}
        platform_counts = {}
        # nested dict: scam_type -> platform -> count
        scam_platform_breakdown = {}

        for row in rows:
            st = (row.get("scam_type") or "Unknown").strip()
            if not st or st in ("NA", "N/A", "nan", ""):
                st = "Unknown"

            p = (row.get("platform") or "Unknown").strip()
            if not p or p in ("NA", "N/A", "nan", ""):
                p = "Unknown"

            scam_counts[st] = scam_counts.get(st, 0) + 1
            platform_counts[p] = platform_counts.get(p, 0) + 1
            if st not in scam_platform_breakdown:
                scam_platform_breakdown[st] = {}
            scam_platform_breakdown[st][p] = scam_platform_breakdown[st].get(p, 0) + 1

        total = len(rows)

        return jsonify({
            "success": True,
            "stats": {
                "scam_counts": scam_counts,
                "platform_counts": platform_counts,
                "scam_platform_breakdown": scam_platform_breakdown,
                "total": total
            }
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


# ============================================================
# MAIN DASHBOARD ROUTE
# ============================================================
@app.route("/", methods=["GET"])
@login_required
def index():
    user = get_current_user()
    allowed_pages = session.get("allowed_pages", [])

    page_type = request.args.get("page", "").strip()
    if not page_type or page_type not in allowed_pages:
        page_type = allowed_pages[0] if allowed_pages else "scraping"

    search_query = request.args.get("search", "").strip()
    scam_filter = request.args.get("scam_type", "").strip()
    platform_filter = request.args.get("platform", "").strip()
    date_from = request.args.get("date_from", "").strip()
    date_to = request.args.get("date_to", "").strip()
    date_filter = request.args.get("date_filter", "").strip()
    page = int(request.args.get("page_num", 1))

    social_search = request.args.get("social_search", "").strip()
    social_platform = request.args.get("social_platform", "").strip()
    social_update_data = request.args.get("update_data", "").strip()
    social_activity_log = request.args.get("activity_log", "").strip()
    social_permanent_block = request.args.get("permanent_block", "").strip()
    social_status_filter = request.args.get("social_status", "").strip()

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
        if date_from:
            query = query.gte("inserted_date", date_from)
        if date_to:
            query = query.lte("inserted_date", date_to)
        if date_filter and not date_from and not date_to:
            query = query.eq("inserted_date", date_filter)
        query = query.order("id", desc=True)
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
                f"login_user.ilike.{like_term},"
                f"number.ilike.{like_term},"
                f"full_name.ilike.{like_term},"
                f"page_name.ilike.{like_term},"
                f"platform.ilike.{like_term},"
                f"account_status.ilike.{like_term},"
                f"review_status.ilike.{like_term},"
                f"login_device.ilike.{like_term}"
            )
        if social_platform and social_platform != "":
            query = query.eq("platform", social_platform)

        # FIX: Account status filter logic
        # "Block" in the DB could be stored as "Block" or "Blocked" — use ilike for safety
        if social_permanent_block == "true":
            # Show only permanent block accounts
            query = query.eq("account_status", "Permanent Block")
        else:
            if social_status_filter:
                # Filter by the specific status using ilike to catch case/value variations
                # e.g. "Block" matches "Block", "Blocked", etc.
                if social_status_filter.lower() == "block":
                    # Match both "Block" and "Blocked" but not "Permanent Block"
                    query = query.ilike("account_status", "block%").neq("account_status", "Permanent Block")
                else:
                    query = query.eq("account_status", social_status_filter)
            else:
                # No status filter — exclude permanent block from main view
                query = query.neq("account_status", "Permanent Block")

        query = query.order("id", desc=False)
        offset = (page - 1) * PER_PAGE
        query = query.range(offset, offset + PER_PAGE - 1)
        try:
            response = query.execute()
            items = response.data
            total_rows = response.count
            total_pages = max(1, math.ceil(total_rows / PER_PAGE)) if total_rows else 1
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
        date_filter=date_filter,
        date_from=date_from,
        date_to=date_to,
        social_search=social_search,
        social_platform=social_platform,
        social_update_data=social_update_data,
        social_activity_log=social_activity_log,
        social_permanent_block=social_permanent_block,
        social_status_filter=social_status_filter,
        page_num=page,
        total_pages=total_pages,
        total_rows=total_rows,
        platform_options=PLATFORM_OPTIONS,
        scam_type_options=SCAM_TYPE_OPTIONS,
        social_platform_options=SOCIAL_PLATFORM_OPTIONS,
        current_user=user,
        allowed_pages=allowed_pages,
        display_name=session.get("display_name", "User")
    )


# ============================================================
# TRACKER STATS
# ============================================================
@app.route("/tracker-stats", methods=["GET"])
@login_required
def tracker_stats():
    try:
        platforms = ["Facebook", "Amazon", "Instagram", "Telegram", "WhatsApp"]
        platform_counts = {}
        platform_status_counts = {}
        perm_block_counts = {}
        perm_block_total = 0

        CHUNK = 1000

        for platform in platforms:
            try:
                # Paginate to fetch ALL rows for this platform (bypass 1000-row Supabase cap)
                all_rows = []
                offset = 0
                total_count = 0
                while True:
                    resp = social_supabase.table("social_media_accounts") \
                        .select("account_status", count='exact') \
                        .eq("platform", platform) \
                        .range(offset, offset + CHUNK - 1) \
                        .execute()
                    if offset == 0:
                        total_count = resp.count or 0
                    chunk = resp.data or []
                    all_rows.extend(chunk)
                    if len(chunk) < CHUNK:
                        break
                    offset += CHUNK

                platform_counts[platform] = total_count

                # Build status map — exclude Permanent Block from main counts
                status_map = {}
                pb_count = 0
                for item in all_rows:
                    status = (item.get('account_status') or 'Active').strip()
                    if status == 'Permanent Block':
                        pb_count += 1
                    else:
                        status_map[status] = status_map.get(status, 0) + 1

                platform_status_counts[platform] = status_map
                perm_block_counts[platform] = pb_count
                perm_block_total += pb_count

            except Exception as e:
                print(f"[tracker_stats] error for {platform}: {e}")
                platform_counts[platform] = 0
                platform_status_counts[platform] = {}
                perm_block_counts[platform] = 0

        try:
            total_response = social_supabase.table("social_media_accounts") \
                .select("id", count='exact').execute()
            total_accounts = total_response.count or 0
        except Exception:
            total_accounts = sum(platform_counts.values())

        return jsonify({
            "success": True,
            "stats": {
                "platform_counts": platform_counts,
                "platform_status_counts": platform_status_counts,
                "total_accounts": total_accounts,
                "perm_block_counts": perm_block_counts,
                "perm_block_total": perm_block_total
            }
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route("/get-platform-counts", methods=["GET"])
@login_required
def get_platform_counts():
    try:
        platforms = ["Facebook", "Amazon", "Instagram", "Telegram", "WhatsApp"]
        platform_counts = {}
        status_counts = {}
        for platform in platforms:
            try:
                response = social_supabase.table("social_media_accounts").select("*", count='exact').eq("platform", platform).execute()
                platform_counts[platform] = response.count or 0
                status_response = social_supabase.table("social_media_accounts").select("account_status", count='exact').eq("platform", platform).execute()
                status_counts[platform] = {"Active": 0, "Block": 0, "Restricted": 0, "Frozen": 0, "Permanent Block": 0}
                if hasattr(status_response, 'data'):
                    for item in status_response.data:
                        status = (item.get('account_status') or '').lower()
                        if 'active' in status:
                            status_counts[platform]["Active"] += 1
                        elif 'block' in status and 'permanent' not in status:
                            status_counts[platform]["Block"] += 1
                        elif 'restricted' in status:
                            status_counts[platform]["Restricted"] += 1
                        elif 'frozen' in status:
                            status_counts[platform]["Frozen"] += 1
                        elif 'permanent' in status:
                            status_counts[platform]["Permanent Block"] += 1
            except Exception as e:
                platform_counts[platform] = 0
                status_counts[platform] = {"Active": 0, "Block": 0, "Restricted": 0, "Frozen": 0, "Permanent Block": 0}
        total_response = social_supabase.table("social_media_accounts").select("*", count='exact').execute()
        platform_counts["Total"] = total_response.count or 0
        return jsonify({"success": True, "platform_counts": platform_counts, "status_counts": status_counts})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route("/update-social-data", methods=["POST"])
@login_required
def update_social_data():
    try:
        data = request.get_json()
        if not data:
            return jsonify({"success": False, "error": "No data provided"})
        account_id = data.get('id')
        field = data.get('field')
        value = data.get('value')
        if not account_id or not field:
            return jsonify({"success": False, "error": "Missing required fields: id and field"})
        valid_fields = ['login_user', 'number', 'login_device', 'account_status',
                        'review_status', 'blocked_date', 'unblock_date', 'recharge_date']
        if field not in valid_fields:
            return jsonify({"success": False, "error": f"Invalid field: {field}"})
        response = social_supabase.table("social_media_accounts").update({field: value}).eq("id", account_id).execute()
        if hasattr(response, 'data') and response.data:
            return jsonify({"success": True, "message": "Data updated successfully"})
        return jsonify({"success": False, "error": "No data was updated"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route("/social-import", methods=["POST"])
@login_required
def social_import():
    try:
        file = request.files.get("file")
        if not file or file.filename == '':
            flash("No file selected", "error")
            return redirect("/?page=social")
        if not is_allowed_file(file.filename):
            flash(f"Only {', '.join(get_allowed_extensions())} files are allowed.", "error")
            return redirect("/?page=social")
        filename = secure_filename(file.filename)
        temp_path = os.path.join(tempfile.gettempdir(), filename)
        file.save(temp_path)
        file_ext = filename.rsplit('.', 1)[1].lower()
        df = read_data_file(temp_path, file_ext)
        df.columns = df.columns.astype(str).str.strip()
        df = df.fillna('')
        ALL_SOCIAL_COLUMNS = [
            'owned_by', 'login_user', 'number', 'login_device', 'sim_inserted_device',
            'account_status', 'review_status', 'number_type', 'blocked_date', 'unblock_date',
            'account_create_date', 'sim_operator', 'full_name', 'recharge_date', 'sim_buy_date',
            'account_type', 'mail_id', 'account_id', 'password', 'page_name', 'platform',
        ]
        file_columns = list(df.columns)
        matched_columns = [col for col in file_columns if col in ALL_SOCIAL_COLUMNS and col != 'id']
        if not matched_columns:
            flash("Import Error: No matching column names found.", "error")
            os.remove(temp_path)
            return redirect("/?page=social")
        try:
            max_id_response = social_supabase.table("social_media_accounts").select("id").order("id", desc=True).limit(1).execute()
            next_id = int(max_id_response.data[0]['id']) + 1 if max_id_response.data else 1
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
        social_supabase.table("social_media_accounts").insert(records).execute()
        flash(f"File Imported Successfully! {len(records)} records added.", "success")
        os.remove(temp_path)
    except Exception as e:
        flash(f"Import Error: {str(e)}", "error")
    return redirect("/?page=social")


@app.route("/social-export", methods=["GET"])
@login_required
def social_export():
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
        query = query.order("id", desc=False)
        response = query.execute()
        df = pd.DataFrame(response.data)
        output = io.StringIO()
        df.to_csv(output, index=False, encoding='utf-8-sig')
        output.seek(0)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return send_file(
            io.BytesIO(output.getvalue().encode('utf-8-sig')),
            download_name=f"social_media_accounts_{timestamp}.csv",
            as_attachment=True, mimetype="text/csv"
        )
    except Exception as e:
        flash(f"Export Error: {str(e)}", "error")
        return redirect("/?page=social")


@app.route("/get-sheet-headers/<sheet_type>", methods=["GET"])
@login_required
def get_sheet_headers_route(sheet_type):
    try:
        headers = get_sheet_headers(sheet_type)
        config = load_config()
        sheet_name = config['sheet_mappings'][sheet_type]['name'] if config else sheet_type
        return jsonify({"success": True, "sheet_name": sheet_name, "headers": headers, "headers_count": len(headers)})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route("/download-template/<sheet_type>", methods=["GET"])
@login_required
def download_template(sheet_type):
    try:
        headers = get_sheet_headers(sheet_type)
        if not headers:
            flash("No headers found for this sheet type", "error")
            return redirect("/?page=sheet")
        output = io.StringIO()
        csv.writer(output).writerow(headers)
        output.seek(0)
        config = load_config()
        sheet_name = config['sheet_mappings'][sheet_type]['name'].replace(' ', '_') if config else sheet_type
        return send_file(
            io.BytesIO(output.getvalue().encode('utf-8-sig')),
            download_name=f"{sheet_name}_Input_Template.csv",
            as_attachment=True, mimetype="text/csv"
        )
    except Exception as e:
        flash(f"Error generating template: {str(e)}", "error")
        return redirect("/?page=sheet")


@app.route("/preview-sheet", methods=["POST"])
@login_required
def preview_sheet():
    try:
        sheet_type = request.form.get("sheet_type")
        file = request.files.get("file")
        if not sheet_type:
            return jsonify({"success": False, "error": "Please select a sheet type"})
        if not file or file.filename == '':
            return jsonify({"success": False, "error": "Please select a file"})
        if not is_allowed_file(file.filename):
            return jsonify({"success": False, "error": f"Only {', '.join(get_allowed_extensions())} files are allowed."})
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
            return jsonify({
                "success": True,
                "sheet_name": sheet_config['name'],
                "total_values": preview_metrics['total_values'],
                "unique_upi_ids": preview_metrics['unique_upi_ids'],
                "unique_bank_accounts": preview_metrics['unique_bank_accounts'],
                "total_columns": len(result_df.columns),
                "columns": list(result_df.columns),
                "preview_rows": result_df.fillna('').head(50).to_dict(orient='records'),
                "input_headers": list(df.columns),
                "output_headers": list(result_df.columns)
            })
        except Exception as e:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            return jsonify({"success": False, "error": f"Error processing file: {str(e)}"})
    except Exception as e:
        return jsonify({"success": False, "error": f"Error previewing sheet: {str(e)}"})


@app.route("/generate-sheet", methods=["POST"])
@login_required
def generate_sheet():
    try:
        sheet_type = request.form.get("sheet_type")
        file = request.files.get("file")
        if not sheet_type:
            flash("Please select a sheet type", "error"); return redirect("/?page=sheet")
        if not file or file.filename == '':
            flash("Please select a file", "error"); return redirect("/?page=sheet")
        if not is_allowed_file(file.filename):
            flash(f"Only {', '.join(get_allowed_extensions())} files are allowed.", "error"); return redirect("/?page=sheet")
        filename = secure_filename(file.filename)
        temp_path = os.path.join(tempfile.gettempdir(), filename)
        file.save(temp_path)
        try:
            file_ext = filename.rsplit('.', 1)[1].lower()
            df = read_data_file(temp_path, file_ext)
            if df.empty:
                flash("The uploaded file is empty", "error"); return redirect("/?page=sheet")
            result_df, _ = process_sheet_data(df, sheet_type)
            output = io.StringIO()
            result_df.to_csv(output, index=False, encoding='utf-8-sig')
            today_date = datetime.now().strftime("%Y-%m-%d")
            sheet_name_clean = SHEET_TYPES.get(sheet_type, sheet_type)
            os.remove(temp_path)
            return send_file(
                io.BytesIO(output.getvalue().encode('utf-8-sig')),
                download_name=f"{sheet_name_clean}_{today_date}.csv",
                as_attachment=True, mimetype="text/csv"
            )
        except Exception as e:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            flash(f"Error processing file: {str(e)}", "error"); return redirect("/?page=sheet")
    except Exception as e:
        flash(f"Error generating sheet: {str(e)}", "error"); return redirect("/?page=sheet")


@app.route("/get-excel-headers", methods=["GET"])
@login_required
def get_excel_headers():
    try:
        master_headers = list(pd.read_excel(MASTER_URL_DATA_PATH).columns) if MASTER_URL_DATA_PATH.exists() else []
        bank_headers = list(pd.read_excel(BANK_NAME_MAPPING_PATH).columns) if BANK_NAME_MAPPING_PATH.exists() else []
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
@login_required
def get_ifsc_headers():
    try:
        ifsc_headers = list(pd.read_excel(IFSC_MAPPING_PATH).columns) if IFSC_MAPPING_PATH.exists() else []
        return jsonify({"success": True, "ifsc_mapping_headers": ifsc_headers, "ifsc_mapping_count": len(IFSC_MAPPING)})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route("/reload-data", methods=["POST"])
@login_required
def reload_data():
    try:
        load_excel_data()
        return jsonify({"success": True, "message": f"Data reloaded! URLs: {len(MASTER_URL_DATA)}, Bank: {len(BANK_NAME_MAPPING)}, IFSC: {len(IFSC_MAPPING)}"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route("/upload", methods=["POST"])
@login_required
def upload():
    if "file" not in request.files:
        flash("No file uploaded", "error"); return redirect("/?page=scraping")
    file = request.files["file"]
    if not file or file.filename == '':
        flash("No file selected", "error"); return redirect("/?page=scraping")
    if not is_allowed_file(file.filename):
        flash(f"Only {', '.join(get_allowed_extensions())} files are allowed.", "error"); return redirect("/?page=scraping")
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
        supabase.table("scrapping_data").insert(records).execute()
        flash(f"File Imported Successfully! {len(records)} records added.", "success")
        os.remove(temp_path)
    except Exception as e:
        flash(f"Import Error: {str(e)}", "error")
    return redirect("/?page=scraping")


@app.route("/export")
@login_required
def export():
    try:
        search_query = request.args.get("search", "").strip()
        scam_filter = request.args.get("scam_type", "").strip()
        platform_filter = request.args.get("platform", "").strip()
        date_filter = request.args.get("date_filter", "").strip()
        date_from = request.args.get("date_from", "").strip()
        date_to = request.args.get("date_to", "").strip()

        # ── Supabase caps each request at 1000 rows by default.
        # ── We paginate in chunks of 1000 until no more data is returned.
        CHUNK = 1000
        all_rows = []
        offset = 0

        while True:
            def _build_query():
                q = supabase.table("scrapping_data").select("*")
                if search_query:
                    like_term = f"%{search_query}%"
                    q = q.or_(
                        f"name.ilike.{like_term},"
                        f"platform.ilike.{like_term},"
                        f"post_url.ilike.{like_term},"
                        f"chat_number.ilike.{like_term},"
                        f"group_name.ilike.{like_term},"
                        f"chat_link.ilike.{like_term},"
                        f"scam_type.ilike.{like_term}"
                    )
                if scam_filter:
                    q = q.eq("scam_type", scam_filter)
                if platform_filter:
                    q = q.eq("platform", platform_filter)
                if date_from:
                    q = q.gte("inserted_date", date_from)
                if date_to:
                    q = q.lte("inserted_date", date_to)
                if date_filter and not date_from and not date_to:
                    q = q.eq("inserted_date", date_filter)
                return q

            chunk_resp = _build_query().order("id", desc=False).range(offset, offset + CHUNK - 1).execute()
            rows = chunk_resp.data or []
            all_rows.extend(rows)
            if len(rows) < CHUNK:
                break          # last page reached
            offset += CHUNK

        df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame()
        output = io.StringIO()
        df.to_csv(output, index=False, encoding='utf-8-sig')
        output.seek(0)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return send_file(
            io.BytesIO(output.getvalue().encode('utf-8-sig')),
            download_name=f"scam_reports_{timestamp}.csv",
            as_attachment=True, mimetype="text/csv"
        )
    except Exception as e:
        flash(f"Export Error: {str(e)}", "error")
        return redirect("/?page=scraping")


@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "excel_data_loaded": {
            "master_url_data": len(MASTER_URL_DATA),
            "bank_name_mapping": len(BANK_NAME_MAPPING),
            "ifsc_mapping": len(IFSC_MAPPING)
        }
    })


@app.route("/update-social-accounts", methods=["GET"])
@login_required
def update_social_accounts():
    search = request.args.get("search", "").strip()
    platform = request.args.get("platform", "").strip()
    page = int(request.args.get("page_num", 1))

    query = social_supabase.table("social_media_accounts").select(
        "id,login_user,number,login_device,account_status,review_status,blocked_date,unblock_date,recharge_date,platform,account_create_date,full_name",
        count='exact'
    )
    query = query.neq("account_status", "Permanent Block")
    if search:
        like_term = f"%{search}%"
        query = query.or_(f"login_user.ilike.{like_term},number.ilike.{like_term},platform.ilike.{like_term},account_status.ilike.{like_term}")
    if platform:
        query = query.eq("platform", platform)
    query = query.order("id", desc=False)
    offset = (page - 1) * PER_PAGE
    query = query.range(offset, offset + PER_PAGE - 1)
    try:
        response = query.execute()
        items = response.data
        total_rows = response.count
        total_pages = max(1, math.ceil(total_rows / PER_PAGE)) if total_rows else 1
    except Exception as e:
        items = []; total_rows = 0; total_pages = 1

    return render_template(
        "update_social.html",
        items=items,
        search=search,
        platform=platform,
        page_num=page,
        total_pages=total_pages,
        total_rows=total_rows,
        social_platform_options=SOCIAL_PLATFORM_OPTIONS,
        platform_account_status=PLATFORM_ACCOUNT_STATUS
    )


@app.route("/save-social-field", methods=["POST"])
@login_required
def save_social_field():
    try:
        data = request.get_json()
        if not data:
            return jsonify({"success": False, "error": "No data provided"})
        account_id = data.get('id')
        field = data.get('field')
        value = data.get('value', '').strip()
        if not account_id or not field:
            return jsonify({"success": False, "error": "Missing id or field"})
        EDITABLE_FIELDS = ['login_user', 'number', 'login_device', 'account_status',
                           'review_status', 'blocked_date', 'unblock_date', 'recharge_date']
        if field not in EDITABLE_FIELDS:
            return jsonify({"success": False, "error": f"Field '{field}' is not editable"})
        update_payload = {field: value if value else "NA"}
        if field == 'account_status' and value == 'Permanent Block':
            update_payload['blocked_date'] = datetime.now().strftime("%Y-%m-%d")
        response = social_supabase.table("social_media_accounts").update(update_payload).eq("id", account_id).execute()
        if hasattr(response, 'data'):
            if response.data:
                return jsonify({"success": True, "message": "Saved successfully", "updated_row": response.data[0]})
            verify = social_supabase.table("social_media_accounts").select("id").eq("id", account_id).execute()
            if verify.data:
                return jsonify({"success": False, "error": "Update failed — check Supabase API key permissions"})
            return jsonify({"success": False, "error": f"Row {account_id} not found"})
        return jsonify({"success": False, "error": "No response from Supabase"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route("/get-permanent-block-accounts", methods=["GET"])
@login_required
def get_permanent_block_accounts():
    try:
        search = request.args.get("search", "").strip()
        query = social_supabase.table("social_media_accounts") \
            .select("id,owned_by,number,login_device,blocked_date,account_create_date,platform") \
            .eq("account_status", "Permanent Block")
        if search:
            like_term = f"%{search}%"
            query = query.or_(f"owned_by.ilike.{like_term},number.ilike.{like_term},login_device.ilike.{like_term},platform.ilike.{like_term}")
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
                            days = (datetime.strptime(b_date_str[:10], fmt) - datetime.strptime(create_date_str[:10], fmt)).days
                            active_duration = f"{days} days" if days >= 0 else 'N/A'; break
                        except ValueError:
                            continue
                except Exception:
                    pass
            accounts.append({
                'id': item.get('id'), 'owned_by': item.get('owned_by') or 'N/A',
                'number': item.get('number') or 'N/A', 'login_device': item.get('login_device') or 'N/A',
                'platform': item.get('platform') or 'N/A', 'blocked_date': b_date_str or 'N/A',
                'active_duration': active_duration
            })
        return jsonify({"success": True, "accounts": accounts, "count": len(accounts)})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route("/get-activity-log", methods=["GET"])
@login_required
def get_activity_log():
    return jsonify({"success": True, "placeholder": True, "message": "Implement Soon Have Some Patience"})


if __name__ == "__main__":
    EXCEL_FOLDER_PATH.mkdir(exist_ok=True)
    load_config()
    load_excel_data()
    port = int(os.environ.get("PORT", 5000))
    debug_mode = os.environ.get("FLASK_DEBUG", "False").lower() == "true"
    app.run(debug=debug_mode, host='0.0.0.0', port=port)