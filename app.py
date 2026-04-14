from flask import Flask, render_template, render_template_string, request, redirect, flash, send_file, jsonify, session
import pandas as pd
import io
import math
import urllib.parse
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
# AUTH HELPERS
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
    "allowed_pages": ["scraping", "sheet", "social", "investment"],
    "is_admin": True,
    "is_active": True,
    "can_view_activity_log": True,
    "allowed_departments": ["ITC","AML", "Investment Scam", "Infringement", "Chargeback"],
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

def get_current_user():
    if "user_id" not in session:
        return None
    return {
        "id": session.get("user_id"),
        "email": session.get("email"),
        "display_name": session.get("display_name"),
        "allowed_pages": session.get("allowed_pages", []),
        "is_admin": session.get("is_admin", False),
        "can_view_activity_log": session.get("can_view_activity_log", False),
        "allowed_departments": session.get("allowed_departments"),
    }


# ============================================================
# ACTIVITY LOG HELPER
# ============================================================

def log_activity(action_type, target_table=None, target_record_id=None,
                 field_name=None, old_value=None, new_value=None, extra_info=None):
    try:
        client = get_auth_supabase()
        client.table("activity_logs").insert({
            "user_id":          session.get("user_id"),
            "user_email":       session.get("email"),
            "display_name":     session.get("display_name"),
            "action_type":      action_type,
            "target_table":     target_table,
            "target_record_id": target_record_id,
            "field_name":       field_name,
            "old_value":        str(old_value) if old_value is not None else None,
            "new_value":        str(new_value) if new_value is not None else None,
            "extra_info":       extra_info,
        }).execute()
    except Exception as e:
        print(f"[ACTIVITY LOG] Failed to log activity: {e}")


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
    "Facebook", "Amazon", "Instagram", "Telegram", "WhatsApp",
    "Gmail Accounts", "Total Numbers"
]

DEPARTMENT_OPTIONS = [
    "AML", "Investment Scam", "ITC", "Infringement", "Chargeback"
]

PLATFORM_ACCOUNT_STATUS = {
    "Facebook": ["Active", "Block", "Restricted", "Permanent Block"],
    "Instagram": ["Active", "Block", "Permanent Block"],
    "Telegram": ["Active", "Frozen", "Permanent Block"],
    "WhatsApp": ["Active", "Block", "Permanent Block", "Restricted"],
    "Amazon": ["Active", "Block", "Permanent Block"],
    "Gmail Accounts": ["Active", "Block", "Permanent Block"],
    "Total Numbers": ["Active", "Block", "Permanent Block"],
}

BS_INVESTMENT_COLUMNS = [
    "id", "bank_account_number", "bank_name", "upi_vpa", "screenshot",
    "search_for", "upi_bank_account_wallet", "handle", "payment_gateway_name",
    "scam_type", "ifsc_code", "upi_url", "website_url", "inserted_date",
    "input_user", "web_contact_no"
]

BS_INVESTMENT_SCAM_TYPE_OPTIONS = [
    "Investment Scam", "Loan Scam", "Subscription Scam", "Carding Scam",
    "Fake Website Scam", "Currency Exchange Scam", "Job Scam", "Shopping Scam"
]

BS_INVESTMENT_SEARCH_FOR_OPTIONS = [
    "Web", "Telegram", "WhatsApp", "Facebook",
    "Instagram", "YouTube", "X", "Thread"
]

BS_INVESTMENT_WALLET_OPTIONS = [
    "UPI", "Bank Account", "Wallet"
]

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

ALLOWED_IMPORT_EXTENSIONS = {
    'csv', 'tsv', 'txt',
    'xlsx', 'xlsm', 'xlsb', 'xltx', 'xltm',
    'xls', 'xla', 'xlam',
    'ods', 'ots',
}


def is_allowed_file(filename):
    if not filename:
        return False
    ext = filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''
    return ext in ALLOWED_IMPORT_EXTENSIONS


def read_data_file(file_path, file_ext):
    try:
        ext = file_ext.lower().lstrip('.')
        if ext == 'csv':
            for encoding in ['utf-8-sig', 'utf-8', 'latin-1', 'iso-8859-1', 'cp1252']:
                try:
                    return pd.read_csv(file_path, encoding=encoding)
                except UnicodeDecodeError:
                    continue
            return pd.read_csv(file_path, encoding='latin-1', engine='python')
        if ext == 'tsv':
            for encoding in ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252']:
                try:
                    return pd.read_csv(file_path, sep='\t', encoding=encoding)
                except UnicodeDecodeError:
                    continue
            return pd.read_csv(file_path, sep='\t', encoding='latin-1')
        if ext == 'txt':
            for sep in ['\t', ',', ';', '|']:
                for encoding in ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252']:
                    try:
                        df = pd.read_csv(file_path, sep=sep, encoding=encoding)
                        if len(df.columns) > 1:
                            return df
                    except Exception:
                        continue
            return pd.read_csv(file_path, encoding='latin-1', engine='python')
        if ext in ('xlsx', 'xlsm', 'xltx', 'xltm'):
            return pd.read_excel(file_path, engine='openpyxl')
        if ext == 'xlsb':
            return pd.read_excel(file_path, engine='pyxlsb')
        if ext in ('xls', 'xla', 'xlam'):
            try:
                return pd.read_excel(file_path, engine='xlrd')
            except Exception:
                return pd.read_excel(file_path, engine='openpyxl')
        if ext in ('ods', 'ots'):
            return pd.read_excel(file_path, engine='odf')
        try:
            return pd.read_excel(file_path)
        except Exception:
            return pd.read_csv(file_path, encoding='latin-1')
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        raise


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
            key_col = next((c for c in df_bank.columns if any(k in str(c).lower() for k in ['key', 'handle', 'upi'])), df_bank.columns[0] if len(df_bank.columns) > 0 else None)
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
            prefix_col = next((c for c in df_ifsc.columns if any(k in str(c).lower() for k in ['ifsc', 'prefix', 'code'])), df_ifsc.columns[0] if len(df_ifsc.columns) > 0 else None)
            bank_col2 = next((c for c in df_ifsc.columns if 'bank' in str(c).lower() and 'name' in str(c).lower()), df_ifsc.columns[1] if len(df_ifsc.columns) > 1 else None)
            if prefix_col and bank_col2:
                for _, row in df_ifsc.iterrows():
                    k = str(row.get(prefix_col, '')).strip().upper()
                    v = str(row.get(bank_col2, 'NA')).strip()
                    if k and k.lower() not in ['na', 'nan', '']:
                        IFSC_MAPPING[k] = v
    except Exception as e:
        print(f"Error loading Excel data: {e}")
        MASTER_URL_DATA = {}
        BANK_NAME_MAPPING = {}
        IFSC_MAPPING = {}


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
            "allowed_extensions": list(ALLOWED_IMPORT_EXTENSIONS),
            "max_file_size_mb": 50
        }
    }
    with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
        json.dump(default_config, f, indent=2)
    return default_config


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
            'instagram.com': 'Instagram', 'telegram.org': 'Telegram','web.telegram.org': 'Telegram',
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
        return result_df, {'total_values': 0, 'unique_upi_ids': 0, 'unique_bank_accounts': 0, 'unique_websites': 0}
    input_headers = list(df.columns)
    standardized_headers = standardize_headers(input_headers, sheet_type)
    df.columns = standardized_headers
    unique_upi_ids = set()
    unique_bank_accounts = set()
    unique_websites = set()
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
                if cleaned_value != "NA":
                    unique_websites.add(cleaned_value)
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
                row_data['origin'] = "NA"
                row_data['category_of_website'] = "NA"

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
        'unique_bank_accounts': len(unique_bank_accounts),
        'unique_websites': len(unique_websites)
    }


# ============================================================
# Helper function to extract clean display name
# ============================================================
def get_clean_display_name(display_name):
    """Extract display name without parentheses content"""
    if not display_name:
        return "User"
    clean_name = re.sub(r'\s*\([^)]*\)', '', display_name).strip()
    return clean_name if clean_name else display_name


# ============================================================
# LOGIN / LOGOUT
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
                session["user_id"]               = user["id"]
                session["email"]                 = user["email"]
                session["display_name"]          = user["display_name"]
                session["allowed_pages"]         = user.get("allowed_pages") or []
                session["is_admin"]              = bool(user.get("is_admin", False))
                session["can_view_activity_log"] = bool(user.get("can_view_activity_log", False))
                session["allowed_departments"] = user.get("allowed_departments") or None
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
# USER ACTIVITY LOG ROUTES
# ============================================================
@app.route("/get-user-activity-log", methods=["GET"])
@login_required
def get_user_activity_log():
    if not session.get("can_view_activity_log"):
        return jsonify({"success": False, "error": "Access denied."})
    try:
        client = get_auth_supabase()
        resp = client.table("activity_logs") \
            .select("*") \
            .order("created_at", desc=True) \
            .limit(500) \
            .execute()
        logs = resp.data or []
        return jsonify({"success": True, "logs": logs})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route("/export-user-activity-log", methods=["GET"])
@login_required
def export_user_activity_log():
    """Export user activity log as CSV"""
    if not session.get("can_view_activity_log"):
        flash("Access denied.", "error")
        return redirect("/")
    try:
        client = get_auth_supabase()
        resp = client.table("activity_logs") \
            .select("*") \
            .order("created_at", desc=True) \
            .execute()
        logs = resp.data or []
        if not logs:
            flash("No activity logs to export.", "error")
            return redirect("/")
        df = pd.DataFrame(logs)
        column_mapping = {
            'id': 'ID',
            'user_id': 'User ID',
            'user_email': 'User Email',
            'display_name': 'Login User Name',
            'action_type': 'Action Type',
            'target_table': 'Target Table',
            'target_record_id': 'Target Record ID',
            'field_name': 'Field Name',
            'old_value': 'Previous Value',
            'new_value': 'Updated Value',
            'extra_info': 'Extra Info',
            'created_at': 'Timestamp'
        }
        available_columns = [col for col in column_mapping.keys() if col in df.columns]
        df = df[available_columns]
        df = df.rename(columns=column_mapping)
        if 'Timestamp' in df.columns:
            df['Timestamp'] = pd.to_datetime(df['Timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
        output = io.StringIO()
        df.to_csv(output, index=False, encoding='utf-8-sig')
        output.seek(0)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"user_activity_log_{timestamp}.csv"
        return send_file(
            io.BytesIO(output.getvalue().encode('utf-8-sig')),
            download_name=filename,
            as_attachment=True,
            mimetype="text/csv"
        )
    except Exception as e:
        flash(f"Export Error: {str(e)}", "error")
        return redirect("/")


# ============================================================
# SCRAPING TRACKER STATS
# ============================================================
@app.route("/scraping-tracker-stats", methods=["GET"])
@login_required
def scraping_tracker_stats():
    try:
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
    social_department_filter = request.args.get("social_department", "").strip()

    inv_search = request.args.get("inv_search", "").strip()
    inv_scam_type = request.args.get("inv_scam_type", "").strip()
    inv_search_for = request.args.get("inv_search_for", "").strip()
    inv_wallet = request.args.get("inv_wallet", "").strip()
    inv_date_from = request.args.get("inv_date_from", "").strip()
    inv_date_to = request.args.get("inv_date_to", "").strip()

    items = []
    total_rows = 0
    total_pages = 1

    if page_type == "scraping":
        try:
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
            share_status_filter = request.args.get("share_status", "").strip()
            if share_status_filter:
                query = query.eq("share_status", share_status_filter)
            query = query.order("id", desc=True)
            offset = (page - 1) * PER_PAGE
            query = query.range(offset, offset + PER_PAGE - 1)
            response = query.execute()
            items = response.data or []
            total_rows = response.count or 0
            total_pages = max(1, math.ceil(total_rows / PER_PAGE)) if total_rows else 1
        except Exception as e:
            print(f"[DEBUG] Scraping error: {e}")
            items = []
            total_rows = 0
            total_pages = 1
            flash(f"Error fetching scraping data: {str(e)}", "error")

    elif page_type == "social":
        try:
            query = social_supabase.table("social_media_accounts").select("*", count='exact')
            allowed_depts = session.get("allowed_departments")
            if allowed_depts:  # None = see all, list = restricted
                if len(allowed_depts) == 1:
                    query = query.eq("department", allowed_depts[0])
                else:
                    query = query.in_("department", allowed_depts)
            if social_search:
                like_term = f"%{social_search}%"
                query = query.or_(
                    f"login_user.ilike.{like_term},"
                    f"number.ilike.{like_term},"
                    f"full_name.ilike.{like_term},"
                    f"page_name.ilike.{like_term},"
                    f"platform.ilike.{like_term},"
                    f"account_status.ilike.{like_term}"
                )
            if social_platform and social_platform != "":
                query = query.eq("platform", social_platform)
            if social_department_filter:
                query = query.eq("department", social_department_filter)
            if social_permanent_block == "true":
                query = query.eq("account_status", "Permanent Block")
            else:
                if social_status_filter:
                    query = query.eq("account_status", social_status_filter)
                else:
                    query = query.neq("account_status", "Permanent Block")
            query = query.order("id", desc=False)
            offset = (page - 1) * PER_PAGE
            query = query.range(offset, offset + PER_PAGE - 1)
            response = query.execute()
            items = [dict(row) for row in (response.data or [])]
            total_rows = response.count or 0
            total_pages = max(1, math.ceil(total_rows / PER_PAGE)) if total_rows else 1
            print(f"[DEBUG] Social items: {len(items)}, total: {total_rows}")
        except Exception as e:
            print(f"[DEBUG] Social error: {e}")
            items = []
            total_rows = 0
            total_pages = 1
            flash(f"Error fetching social media data: {str(e)}", "error")

    elif page_type == "investment":
        try:
            query = supabase.table("BS_Investment_Scam").select("*", count='exact')
            if inv_search:
                like_term = f"%{inv_search}%"
                query = query.or_(
                    f"Bank_account_number.ilike.{like_term},"
                    f"Upi_vpa.ilike.{like_term},"
                    f"Handle.ilike.{like_term},"
                    f"Website_url.ilike.{like_term},"
                    f"Web_contact_no.ilike.{like_term},"
                    f"Input_user.ilike.{like_term}"
                )
            if inv_scam_type:
                query = query.eq("Scam_type", inv_scam_type)
            if inv_search_for:
                query = query.eq("Search_for", inv_search_for)
            if inv_wallet:
                query = query.eq("Upi_bank_account_wallet", inv_wallet)
            if inv_date_from:
                query = query.gte("Inserted_date", inv_date_from)
            if inv_date_to:
                query = query.lte("Inserted_date", inv_date_to)
            query = query.order("Id", desc=True)
            offset = (page - 1) * PER_PAGE
            query = query.range(offset, offset + PER_PAGE - 1)
            response = query.execute()
            raw = response.data or []
            items = [{k.lower(): v for k, v in row.items()} for row in raw]
            total_rows = response.count or 0
            total_pages = max(1, math.ceil(total_rows / PER_PAGE)) if total_rows else 1
            print(f"[DEBUG] Investment items: {len(items)}, total: {total_rows}")
        except Exception as e:
            print(f"[DEBUG] Investment error: {e}")
            items = []
            total_rows = 0
            total_pages = 1
            flash(f"Error fetching BS Investment Scam data: {str(e)}", "error")

    # Get clean display name for template
    clean_display_name = get_clean_display_name(session.get("display_name", "User"))

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
        social_department_filter=social_department_filter,
        inv_search=inv_search,
        inv_scam_type=inv_scam_type,
        inv_search_for=inv_search_for,
        inv_wallet=inv_wallet,
        inv_date_from=inv_date_from,
        inv_date_to=inv_date_to,
        page_num=page,
        total_pages=total_pages,
        total_rows=total_rows,
        platform_options=PLATFORM_OPTIONS,
        scam_type_options=SCAM_TYPE_OPTIONS,
        social_platform_options=SOCIAL_PLATFORM_OPTIONS,
        bs_investment_scam_type_options=BS_INVESTMENT_SCAM_TYPE_OPTIONS,
        bs_investment_search_for_options=BS_INVESTMENT_SEARCH_FOR_OPTIONS,
        bs_investment_wallet_options=BS_INVESTMENT_WALLET_OPTIONS,
        department_options=DEPARTMENT_OPTIONS,
        current_user=user,
        allowed_pages=allowed_pages,
        display_name=session.get("display_name", "User"),
        clean_display_name=clean_display_name,
        can_view_activity_log=session.get("can_view_activity_log", False),
    )


# ============================================================
# BS Investment Scam Tracker Stats
# ============================================================
@app.route("/investment-tracker-stats", methods=["GET"])
@login_required
def investment_tracker_stats():
    try:
        date_from = request.args.get("date_from", "").strip()
        date_to = request.args.get("date_to", "").strip()
        CHUNK = 1000
        all_rows = []
        offset = 0
        while True:
            q = supabase.table("BS_Investment_Scam").select("Input_user,Search_for,Scam_type,Inserted_date,Upi_vpa,Bank_account_number,Upi_bank_account_wallet")
            if date_from: q = q.gte("Inserted_date", date_from)
            if date_to: q = q.lte("Inserted_date", date_to)
            resp = q.order("Id", desc=False).range(offset, offset + CHUNK - 1).execute()
            chunk = resp.data or []
            all_rows.extend(chunk)
            if len(chunk) < CHUNK:
                break
            offset += CHUNK
        rows = [{k.lower(): v for k, v in r.items()} for r in all_rows]
        upi_set = set()
        bank_set = set()
        for r in rows:
            wallet = (r.get("upi_bank_account_wallet") or "").strip()
            upi_vpa = (r.get("upi_vpa") or "").strip()
            bank_acc = (r.get("bank_account_number") or "").strip()
            if wallet == "UPI" and upi_vpa and upi_vpa.upper() not in ("NA", "N/A", ""):
                upi_set.add(upi_vpa)
            if wallet == "Bank Account" and bank_acc and bank_acc.upper() not in ("NA", "N/A", ""):
                bank_set.add(bank_acc)
        users_count = {}
        for r in rows:
            user = (r.get("input_user") or "Unknown").strip()
            sf = (r.get("search_for") or "Unknown").strip()
            if user not in users_count: users_count[user] = {}
            users_count[user][sf] = users_count[user].get(sf, 0) + 1
        scam_type_counts = {}
        for r in rows:
            user = (r.get("input_user") or "Unknown").strip()
            st = (r.get("scam_type") or "Unknown").strip()
            if user not in scam_type_counts: scam_type_counts[user] = {}
            scam_type_counts[user][st] = scam_type_counts[user].get(st, 0) + 1
        total_counts = {}
        for r in rows:
            st = (r.get("scam_type") or "Unknown").strip()
            sf = (r.get("search_for") or "Unknown").strip()
            if st not in total_counts: total_counts[st] = {}
            total_counts[st][sf] = total_counts[st].get(sf, 0) + 1
        return jsonify({
            "success": True,
            "total_rows": len(rows),
            "unique_upi_count": len(upi_set),
            "unique_bank_count": len(bank_set),
            "users_count": users_count,
            "scam_type_counts": scam_type_counts,
            "total_counts": total_counts
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route("/investment-last-date", methods=["GET"])
@login_required
def investment_last_date():
    try:
        resp = supabase.table("BS_Investment_Scam") \
            .select("Inserted_date") \
            .order("Inserted_date", desc=True) \
            .limit(1) \
            .execute()
        if resp.data:
            raw = resp.data[0].get("Inserted_date") or ""
            # Normalise to YYYY-MM-DD
            date_str = str(raw).split("T")[0].strip()
            return jsonify({"success": True, "last_date": date_str})
        return jsonify({"success": True, "last_date": None})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

# ============================================================
# BS Investment Scam Export
# ============================================================
@app.route("/investment-export", methods=["GET"])
@login_required
def investment_export():
    try:
        inv_search = request.args.get("inv_search", "").strip()
        inv_scam_type = request.args.get("inv_scam_type", "").strip()
        inv_search_for = request.args.get("inv_search_for", "").strip()
        inv_wallet = request.args.get("inv_wallet", "").strip()
        inv_date_from = request.args.get("inv_date_from", "").strip()
        inv_date_to = request.args.get("inv_date_to", "").strip()
        CHUNK = 1000
        all_rows = []
        offset = 0
        while True:
            def _build_inv_query():
                q = supabase.table("BS_Investment_Scam").select("*")
                if inv_search:
                    like_term = f"%{inv_search}%"
                    q = q.or_(f"Bank_account_number.ilike.{like_term},Upi_vpa.ilike.{like_term},Handle.ilike.{like_term},Website_url.ilike.{like_term},Web_contact_no.ilike.{like_term},Input_user.ilike.{like_term}")
                if inv_scam_type: q = q.eq("Scam_type", inv_scam_type)
                if inv_search_for: q = q.eq("Search_for", inv_search_for)
                if inv_wallet: q = q.eq("Upi_bank_account_wallet", inv_wallet)
                if inv_date_from: q = q.gte("Inserted_date", inv_date_from)
                if inv_date_to: q = q.lte("Inserted_date", inv_date_to)
                return q
            chunk_resp = _build_inv_query().order("Id", desc=False).range(offset, offset + CHUNK - 1).execute()
            rows = chunk_resp.data or []
            all_rows.extend(rows)
            if len(rows) < CHUNK:
                break
            offset += CHUNK
        df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame()
        output = io.StringIO()
        df.to_csv(output, index=False, encoding='utf-8-sig')
        output.seek(0)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return send_file(
            io.BytesIO(output.getvalue().encode('utf-8-sig')),
            download_name=f"bs_investment_scam_{timestamp}.csv",
            as_attachment=True, mimetype="text/csv"
        )
    except Exception as e:
        flash(f"Export Error: {str(e)}", "error")
        return redirect("/?page=investment")


# ============================================================
# TRACKER STATS
# ============================================================
@app.route("/tracker-stats", methods=["GET"])
@login_required
def tracker_stats():
    try:
        platforms = ["Facebook", "Amazon", "Instagram", "Telegram", "WhatsApp", "Gmail Accounts", "Total Numbers"]
        platform_counts = {}
        platform_status_counts = {}
        perm_block_counts = {}
        perm_block_total = 0
        platform_dept_counts = {}
        CHUNK = 1000
        for platform in platforms:
            try:
                all_rows = []
                offset = 0
                total_count = 0
                while True:
                    _q = social_supabase.table("social_media_accounts") \
                        .select("account_status,department", count='exact') \
                        .eq("platform", platform)
                    allowed_depts = session.get("allowed_departments")
                    if allowed_depts:
                        if len(allowed_depts) == 1:
                            _q = _q.eq("department", allowed_depts[0])
                        else:
                            _q = _q.in_("department", allowed_depts)
                    resp = _q.range(offset, offset + CHUNK - 1).execute()
                    if offset == 0: total_count = resp.count or 0
                    chunk = resp.data or []
                    all_rows.extend(chunk)
                    if len(chunk) < CHUNK: break
                    offset += CHUNK
                platform_counts[platform] = total_count
                status_map = {}
                pb_count = 0
                dept_map = {}
                for item in all_rows:
                    status = (item.get('account_status') or 'Active').strip()
                    dept = (item.get('department') or 'Unknown').strip()
                    if not dept or dept in ('NA', 'N/A', 'nan', ''):
                        dept = 'Unknown'
                    if status == 'Permanent Block':
                        pb_count += 1
                    else:
                        status_map[status] = status_map.get(status, 0) + 1
                    if status != 'Permanent Block':
                        dept_map[dept] = dept_map.get(dept, 0) + 1
                platform_status_counts[platform] = status_map
                perm_block_counts[platform] = pb_count
                perm_block_total += pb_count
                platform_dept_counts[platform] = dept_map
            except Exception as e:
                print(f"[tracker_stats] error for {platform}: {e}")
                platform_counts[platform] = 0
                platform_status_counts[platform] = {}
                perm_block_counts[platform] = 0
        try:
            total_response = social_supabase.table("social_media_accounts").select("id", count='exact').execute()
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
                "perm_block_total": perm_block_total,
                "platform_dept_counts": platform_dept_counts,
            }
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route("/get-platform-counts", methods=["GET"])
@login_required
def get_platform_counts():
    try:
        platforms = ["Facebook", "Amazon", "Instagram", "Telegram", "WhatsApp", "Gmail Accounts", "Total Numbers"]
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
                        if 'active' in status: status_counts[platform]["Active"] += 1
                        elif 'block' in status and 'permanent' not in status: status_counts[platform]["Block"] += 1
                        elif 'restricted' in status: status_counts[platform]["Restricted"] += 1
                        elif 'frozen' in status: status_counts[platform]["Frozen"] += 1
                        elif 'permanent' in status: status_counts[platform]["Permanent Block"] += 1
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


# ============================================================
# SOCIAL IMPORT — with activity logging
# ============================================================
@app.route("/social-import", methods=["POST"])
@login_required
def social_import():
    try:
        file = request.files.get("file")
        if not file or file.filename == '':
            flash("No file selected", "error")
            return redirect("/?page=social")
        if not is_allowed_file(file.filename):
            flash(f"Unsupported file type.", "error")
            return redirect("/?page=social")
        filename = secure_filename(file.filename)
        temp_path = os.path.join(tempfile.gettempdir(), filename)
        file.save(temp_path)
        file_ext = filename.rsplit('.', 1)[1].lower() if '.' in filename else 'csv'
        df = read_data_file(temp_path, file_ext)
        df.columns = df.columns.astype(str).str.strip()
        df = df.fillna('')
        ALL_SOCIAL_COLUMNS = [
            'owned_by', 'login_user', 'number', 'login_device', 'sim_inserted_device',
            'account_status', 'review_status', 'number_type', 'blocked_date', 'unblock_date',
            'account_create_date', 'sim_operator', 'full_name', 'recharge_date', 'sim_buy_date',
            'account_type', 'mail_id', 'account_id', 'password', 'page_name', 'platform', 'department',
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
        DATE_COLUMNS = {'blocked_date', 'unblock_date', 'account_create_date', 'recharge_date', 'sim_buy_date'}
        def sanitize_value(col, value):
            if value is None:
                return None if col in DATE_COLUMNS else "NA"
            try:
                if pd.isna(value):
                    return None if col in DATE_COLUMNS else "NA"
            except (TypeError, ValueError):
                pass
            v = str(value).strip()
            if col in DATE_COLUMNS:
                if not v or v.upper() in ('NA', 'N/A', 'NAN', 'NAT', 'NONE', 'NULL', 'UNDEFINED', '-', 'N.A', 'N.A.', ''):
                    return None
                if ' ' in v: v = v.split(' ')[0]
                if 'T' in v: v = v.split('T')[0]
                return v
            else:
                return v if v else "NA"
        records = []
        for i, (_, row) in enumerate(df.iterrows()):
            record = {}
            if next_id is not None:
                record['id'] = next_id + i
            for col in matched_columns:
                record[col] = sanitize_value(col, row[col])
            records.append(record)
        social_supabase.table("social_media_accounts").insert(records).execute()
        log_activity(
            action_type="import",
            target_table="social_media_accounts",
            extra_info={"file_name": filename, "records_count": len(records)}
        )
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
        allowed_depts = session.get("allowed_departments")
        if allowed_depts:
            if len(allowed_depts) == 1:
                query = query.eq("department", allowed_depts[0])
            else:
                query = query.in_("department", allowed_depts)
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
            return jsonify({"success": False, "error": "Unsupported file type."})
        filename = secure_filename(file.filename)
        temp_path = os.path.join(tempfile.gettempdir(), filename)
        file.save(temp_path)
        try:
            file_ext = filename.rsplit('.', 1)[1].lower() if '.' in filename else 'csv'
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
                "unique_websites": preview_metrics['unique_websites'],
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
            flash("Please select a sheet type", "error")
            return redirect("/?page=sheet")
        if not file or file.filename == '':
            flash("Please select a file", "error")
            return redirect("/?page=sheet")
        if not is_allowed_file(file.filename):
            flash("Unsupported file type.", "error")
            return redirect("/?page=sheet")
        filename = secure_filename(file.filename)
        temp_path = os.path.join(tempfile.gettempdir(), filename)
        file.save(temp_path)
        try:
            file_ext = filename.rsplit('.', 1)[1].lower() if '.' in filename else 'csv'
            df = read_data_file(temp_path, file_ext)
            if df.empty:
                flash("The uploaded file is empty", "error")
                return redirect("/?page=sheet")
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
            flash(f"Error processing file: {str(e)}", "error")
            return redirect("/?page=sheet")
    except Exception as e:
        flash(f"Error generating sheet: {str(e)}", "error")
        return redirect("/?page=sheet")


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


# ============================================================
# SCRAPING DATA IMPORT — with activity logging
# ============================================================
@app.route("/upload", methods=["POST"])
@login_required
def upload():
    if "file" not in request.files:
        flash("No file uploaded", "error")
        return redirect("/?page=scraping")
    file = request.files["file"]
    if not file or file.filename == '':
        flash("No file selected", "error")
        return redirect("/?page=scraping")
    if not is_allowed_file(file.filename):
        flash("Unsupported file type.", "error")
        return redirect("/?page=scraping")
    try:
        filename = secure_filename(file.filename)
        temp_path = os.path.join(tempfile.gettempdir(), filename)
        file.save(temp_path)
        file_ext = filename.rsplit('.', 1)[1].lower() if '.' in filename else 'csv'
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
        log_activity(
            action_type="import",
            target_table="scrapping_data",
            extra_info={"file_name": filename, "records_count": len(records)}
        )
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
        share_status_filter = request.args.get("share_status", "").strip()
        CHUNK = 1000
        all_rows = []
        offset = 0
        while True:
            def _build_query():
                q = supabase.table("scrapping_data").select("*")
                if search_query:
                    like_term = f"%{search_query}%"
                    q = q.or_(f"name.ilike.{like_term},platform.ilike.{like_term},post_url.ilike.{like_term},chat_number.ilike.{like_term},group_name.ilike.{like_term},chat_link.ilike.{like_term},scam_type.ilike.{like_term}")
                if scam_filter: q = q.eq("scam_type", scam_filter)
                if platform_filter: q = q.eq("platform", platform_filter)
                if date_from: q = q.gte("inserted_date", date_from)
                if date_to: q = q.lte("inserted_date", date_to)
                if date_filter and not date_from and not date_to: q = q.eq("inserted_date", date_filter)
                if share_status_filter: q = q.eq("share_status", share_status_filter)
                return q
            chunk_resp = _build_query().order("id", desc=False).range(offset, offset + CHUNK - 1).execute()
            rows = chunk_resp.data or []
            all_rows.extend(rows)
            if len(rows) < CHUNK:
                break
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


@app.route("/parse-raw-file", methods=["POST"])
@login_required
def parse_raw_file():
    try:
        file = request.files.get("file")
        if not file or file.filename == '':
            return jsonify({"success": False, "error": "No file"})
        filename = secure_filename(file.filename)
        temp_path = os.path.join(tempfile.gettempdir(), filename)
        file.save(temp_path)
        file_ext = filename.rsplit('.', 1)[1].lower() if '.' in filename else 'csv'
        df = read_data_file(temp_path, file_ext)
        df = df.fillna('')
        os.remove(temp_path)
        headers = list(df.columns)
        rows = df.head(5000).to_dict(orient='records')
        return jsonify({
            "success": True,
            "headers": headers,
            "rows": rows,
            "total_rows": len(df)
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


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


# ============================================================
# UPDATE SOCIAL ACCOUNTS PAGE
# ============================================================
@app.route("/update-social-accounts", methods=["GET"])
@login_required
def update_social_accounts():
    search = request.args.get("search", "").strip()
    platform = request.args.get("platform", "").strip()
    account_status_filter = request.args.get("account_status_filter", "").strip()
    department_filter = request.args.get("department_filter", "").strip()
    page = int(request.args.get("page_num", 1))
    query = social_supabase.table("social_media_accounts").select(
        "id,login_user,number,login_device,account_status,review_status,blocked_date,unblock_date,recharge_date,platform,account_create_date,full_name,department",
        count='exact'
    )
    query = query.neq("account_status", "Permanent Block")
    allowed_depts = session.get("allowed_departments")
    if allowed_depts:
        if len(allowed_depts) == 1:
            query = query.eq("department", allowed_depts[0])
        else:
            query = query.in_("department", allowed_depts)
    if search:
        like_term = f"%{search}%"
        query = query.or_(f"login_user.ilike.{like_term},number.ilike.{like_term},platform.ilike.{like_term},account_status.ilike.{like_term}")
    if platform: query = query.eq("platform", platform)
    if department_filter: query = query.eq("department", department_filter)
    if account_status_filter:
        if account_status_filter == "Block": query = query.eq("account_status", "Block")
        else: query = query.eq("account_status", account_status_filter)
    query = query.order("id", desc=False)
    offset = (page - 1) * PER_PAGE
    query = query.range(offset, offset + PER_PAGE - 1)
    try:
        response = query.execute()
        items = response.data or []
        total_rows = response.count or 0
        total_pages = max(1, math.ceil(total_rows / PER_PAGE)) if total_rows else 1
    except Exception as e:
        print(f"[DEBUG] update_social_accounts error: {e}")
        items = []
        total_rows = 0
        total_pages = 1
    return render_template(
        "update_social.html",
        items=items,
        search=search,
        platform=platform,
        account_status_filter=account_status_filter,
        department_filter=department_filter,
        page_num=page,
        total_pages=total_pages,
        total_rows=total_rows,
        social_platform_options=SOCIAL_PLATFORM_OPTIONS,
        platform_account_status=PLATFORM_ACCOUNT_STATUS,
        department_options=DEPARTMENT_OPTIONS
    )


# ============================================================
# SAVE SOCIAL FIELD — with old value fetch + activity logging
# ============================================================
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
                           'review_status', 'blocked_date', 'unblock_date', 'recharge_date',
                           'full_name', 'account_create_date']
        if field not in EDITABLE_FIELDS:
            return jsonify({"success": False, "error": f"Field '{field}' is not editable"})
        old_value = None
        platform = None
        try:
            old_resp = social_supabase.table("social_media_accounts") \
                .select(f"{field},platform").eq("id", account_id).limit(1).execute()
            if old_resp.data:
                old_value = old_resp.data[0].get(field)
                platform = old_resp.data[0].get('platform')
        except Exception as e:
            print(f"[ACTIVITY LOG] Could not fetch old value: {e}")
        DATE_FIELDS = {'blocked_date', 'unblock_date', 'recharge_date', 'account_create_date'}
        if field in DATE_FIELDS:
            save_value = None if (not value or value.upper() in ('NA', 'N/A', 'NONE', 'NULL', '')) else value
        else:
            save_value = value if value else "NA"
        update_payload = {field: save_value}
        if field == 'account_status' and value == 'Permanent Block':
            update_payload['blocked_date'] = datetime.now().strftime("%Y-%m-%d")
        response = social_supabase.table("social_media_accounts").update(update_payload).eq("id", account_id).execute()
        if hasattr(response, 'data'):
            if response.data:
                extra_info = {}
                if platform:
                    extra_info['platform'] = platform
                log_activity(
                    action_type="field_update",
                    target_table="social_media_accounts",
                    target_record_id=account_id,
                    field_name=field,
                    old_value=old_value,
                    new_value=save_value,
                    extra_info=extra_info if extra_info else None
                )
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
        platform = request.args.get("platform", "").strip()
        query = social_supabase.table("social_media_accounts") \
            .select("id,owned_by,number,login_device,blocked_date,account_create_date,platform") \
            .eq("account_status", "Permanent Block")
        if platform: query = query.eq("platform", platform)
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
                            active_duration = f"{days} days" if days >= 0 else 'N/A'
                            break
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

@app.route("/check-duplicates", methods=["POST"])
@login_required
def check_duplicates():
    try:
        data = request.get_json()
        entries = data.get("entries", [])
        if not entries:
            return jsonify({"success": False, "error": "No entries provided"})

        results = []
        for entry in entries:
            val = str(entry.get("value", "")).strip()
            typ = entry.get("type", "upi")
            if not val or val.upper() in ("NA", "N/A", "", "NONE"):
                continue
            try:
                if typ == "upi":
                    res = supabase.table("BS_Investment_Scam")\
                        .select("Id, Upi_vpa, Inserted_date, Scam_type, Input_user")\
                        .ilike("Upi_vpa", val)\
                        .limit(10).execute()
                else:
                    res = supabase.table("BS_Investment_Scam")\
                        .select("Id, Bank_account_number, Inserted_date, Scam_type, Input_user")\
                        .ilike("Bank_account_number", val)\
                        .limit(10).execute()

                found = res.data or []
                results.append({
                    "value": val,
                    "type": typ,
                    "status": "DUPLICATE" if found else "NEW",
                    "count": len(found),
                    "earliest_date": found[0].get("Inserted_date") if found else None,
                    "latest_date": found[-1].get("Inserted_date") if len(found) > 1 else None,
                    "scam_type": found[0].get("Scam_type") if found else None,
                    "input_user": found[0].get("Input_user") if found else None,
                    "record_ids": [str(r.get("Id")) for r in found]
                })
            except Exception as e:
                results.append({
                    "value": val, "type": typ,
                    "status": "ERROR", "count": 0,
                    "error": str(e)
                })

        total = len(results)
        duplicates = sum(1 for r in results if r["status"] == "DUPLICATE")
        new_entries = sum(1 for r in results if r["status"] == "NEW")

        return jsonify({
            "success": True,
            "results": results,
            "summary": {
                "total": total,
                "duplicates": duplicates,
                "new": new_entries,
                "errors": total - duplicates - new_entries
            }
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

import urllib.request

@app.route("/getDepartmentData", methods=["GET"])
@login_required  
def get_department_data_proxy():
    """Proxy for external MIS API to avoid CORS issues"""
    try:
        user_mail = request.args.get("user_mail", "")
        department = request.args.get("department", "")
        role = request.args.get("role", "")
        
        external_url = (
            f"https://mis-iw3m.onrender.com/getDepartmentData"
            f"?user_mail={urllib.parse.quote(user_mail)}"
            f"&department={urllib.parse.quote(department)}"
            f"&role={urllib.parse.quote(role)}"
        )
        
        req = urllib.request.Request(external_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        
        return jsonify(data)
    except Exception as e:
        print(f"[MIS PROXY] Error: {e}")
        return jsonify([])

@app.route("/insert-social-record", methods=["POST"])
@login_required
def insert_social_record():
    try:
        data = request.get_json()
        if not data:
            return jsonify({"success": False, "error": "No data provided"})

        DATE_FIELDS = {'blocked_date', 'unblock_date', 'account_create_date', 'recharge_date', 'sim_buy_date'}
        ALLOWED_FIELDS = [
            'platform', 'department', 'owned_by', 'login_user', 'number',
            'login_device', 'sim_inserted_device', 'account_status', 'review_status',
            'number_type', 'blocked_date', 'unblock_date', 'account_create_date',
            'sim_operator', 'full_name', 'recharge_date', 'sim_buy_date',
            'account_type', 'mail_id', 'account_id', 'password', 'page_name'
        ]

        record = {}
        for field in ALLOWED_FIELDS:
            val = str(data.get(field, '')).strip()
            if field in DATE_FIELDS:
                record[field] = val if val and val.upper() not in ('NA', 'N/A', 'NONE', 'NULL', '') else None
            else:
                record[field] = val if val else "NA"

        # platform is required
        if not record.get('platform') or record['platform'] == 'NA':
            return jsonify({"success": False, "error": "Platform is required"})

        # get next id
        try:
            max_id_resp = social_supabase.table("social_media_accounts") \
                .select("id").order("id", desc=True).limit(1).execute()
            record['id'] = int(max_id_resp.data[0]['id']) + 1 if max_id_resp.data else 1
        except Exception:
            pass

        resp = social_supabase.table("social_media_accounts").insert(record).execute()
        if resp.data:
            inserted = resp.data[0]
            log_activity(
                action_type="import",
                target_table="social_media_accounts",
                extra_info={"file_name": "manual_insert", "records_count": 1}
            )
            return jsonify({"success": True, "record": inserted})
        return jsonify({"success": False, "error": "Insert failed"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

# ============================================================
# ADD THIS ROUTE TO app.py  (paste before the if __name__ == "__main__": block)
# ============================================================

@app.route("/insert-scraping-record", methods=["POST"])
@login_required
def insert_scraping_record():
    try:
        data = request.get_json()
        if not data:
            return jsonify({"success": False, "error": "No data provided"})

        rows = data.get("rows", [])
        if not rows:
            return jsonify({"success": False, "error": "No rows provided"})

        ALLOWED_FIELDS = [
            "name", "platform", "post_url", "chat_number", "group_name",
            "scam_type", "share_status", "screenshot",
            "chat_status", "assigned_to", "assigned_at_datetime",
            "inserted_datetime", "priority", "inserted_date",
            "extra_field_1", "extra_field_2", "extra_field_3",
            "extra_field_4", "extra_field_5"
        ]

        records = []
        today = datetime.now().strftime("%Y-%m-%d")
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for row in rows:
            record = {}
            for field in ALLOWED_FIELDS:
                val = str(row.get(field, "")).strip()
                record[field] = val if val else "NA"

            # defaults
            if record.get("inserted_date") in ("", "NA"):
                record["inserted_date"] = today
            if record.get("inserted_datetime") in ("", "NA"):
                record["inserted_datetime"] = now_str
            if record.get("screenshot") in ("", "NA"):
                record["screenshot"] = "NA"
            if record.get("share_status") in ("", "NA"):
                record["share_status"] = "Pending"
            if record.get("chat_status") in ("", "NA"):
                record["chat_status"] = "NA"
            if record.get("priority") in ("", "NA"):
                record["priority"] = "NA"
            for ef in ["extra_field_1","extra_field_2","extra_field_3","extra_field_4","extra_field_5"]:
                if record.get(ef) in ("", "NA"):
                    record[ef] = "NA"

            records.append(record)

        resp = supabase.table("scrapping_data").insert(records).execute()

        if resp.data:
            log_activity(
                action_type="import",
                target_table="scrapping_data",
                extra_info={"file_name": "manual_insert", "records_count": len(records)}
            )
            return jsonify({"success": True, "records": resp.data, "count": len(resp.data)})

        return jsonify({"success": False, "error": "Insert returned no data"})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)})
    
@app.route("/check-scraping-duplicates", methods=["POST"])
@login_required
def check_scraping_duplicates():
    try:
        data = request.get_json()
        entries = data.get("entries", [])
        if not entries:
            return jsonify({"success": True, "results": []})

        results = []
        for entry in entries:
            gn = str(entry.get("group_name", "")).strip()
            cn = str(entry.get("chat_number", "")).strip()

            # Dono NA hain toh skip
            gn_empty = not gn or gn.upper() in ("NA", "N/A", "")
            cn_empty = not cn or cn.upper() in ("NA", "N/A", "")

            if gn_empty and cn_empty:
                results.append({"status": "NEW", "count": 0})
                continue

            try:
                found = []

                if not gn_empty and not cn_empty:
                    # Dono available — AND match
                    res = supabase.table("scrapping_data") \
                        .select("id, group_name, chat_number, inserted_date") \
                        .ilike("group_name", gn) \
                        .ilike("chat_number", cn) \
                        .limit(10).execute()
                    found = res.data or []

                elif not gn_empty:
                    # Sirf group_name
                    res = supabase.table("scrapping_data") \
                        .select("id, group_name, chat_number, inserted_date") \
                        .ilike("group_name", gn) \
                        .limit(10).execute()
                    found = res.data or []

                elif not cn_empty:
                    # Sirf chat_number
                    res = supabase.table("scrapping_data") \
                        .select("id, group_name, chat_number, inserted_date") \
                        .ilike("chat_number", cn) \
                        .limit(10).execute()
                    found = res.data or []

                results.append({
                    "status": "DUPLICATE" if found else "NEW",
                    "count": len(found),
                    "earliest_date": found[0].get("inserted_date") if found else None,
                })

            except Exception as e:
                results.append({"status": "ERROR", "count": 0, "error": str(e)})

        return jsonify({"success": True, "results": results})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)})
    
@app.route("/check-chat-number", methods=["POST"])
@login_required
def check_chat_number():
    try:
        data = request.get_json()
        chat_number = str(data.get("chat_number", "")).strip()

        if not chat_number or chat_number.upper() in ("NA", "N/A", ""):
            return jsonify({"exists": False})

        res = supabase.table("scrapping_data") \
            .select("id, inserted_date, name") \
            .ilike("chat_number", chat_number) \
            .limit(5).execute()

        found = res.data or []
        return jsonify({
            "exists": len(found) > 0,
            "count": len(found),
            "first_seen": found[0].get("inserted_date") if found else None,
            "inserted_by": found[0].get("name") if found else None,
        })

    except Exception as e:
        return jsonify({"exists": False, "error": str(e)})
    
@app.route("/scrapping-summary-data", methods=["GET"])
@login_required
def scrapping_summary_data():
    """Fetch scrapping data for summary generation (filtered by date)"""
    try:
        date_from = request.args.get("date_from", "").strip()
        date_to   = request.args.get("date_to",   "").strip()
        date_on   = request.args.get("date_on",   "").strip()   # single date shortcut

        CHUNK = 1000
        all_rows = []
        offset = 0
        while True:
            q = supabase.table("scrapping_data") \
                .select("name,platform,chat_number,group_name,scam_type,inserted_date")
            if date_on:
                q = q.eq("inserted_date", date_on)
            else:
                if date_from:
                    q = q.gte("inserted_date", date_from)
                if date_to:
                    q = q.lte("inserted_date", date_to)
            resp = q.order("id", desc=False).range(offset, offset + CHUNK - 1).execute()
            chunk = resp.data or []
            all_rows.extend(chunk)
            if len(chunk) < CHUNK:
                break
            offset += CHUNK

        return jsonify({"success": True, "rows": all_rows, "total": len(all_rows)})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})
    
if __name__ == "__main__":
    EXCEL_FOLDER_PATH.mkdir(exist_ok=True)
    load_config()
    load_excel_data()
    port = int(os.environ.get("PORT", 5000))
    debug_mode = os.environ.get("FLASK_DEBUG", "False").lower() == "true"
    app.run(debug=debug_mode, host='0.0.0.0', port=port)