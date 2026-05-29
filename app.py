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
app.permanent_session_lifetime = timedelta(hours=8)

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
    "Thread", "YouTube", "X"
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

WEBSITE_DIRECTORY_CATEGORY_OPTIONS = [
    "Job Scam", "Subscription Scam", "Fake Website Scam",
    "Loan Scam", "Government Scheme Scam", "Investment Scam",
    "LPG Booking Scam", "IPL Tickets Scam", "ChaarDham Booking Scam"
]

WEBSITE_DIRECTORY_SEARCH_FOR_OPTIONS = ["Web", "App"]

WEBSITE_DIRECTORY_COLUMNS = [
    "id", "date", "name", "url", "final_url", "invitation_code",
    "search_for", "group_app_name", "number", "email", "login_id",
    "password", "remark", "origin", "category",
    "automated_website", "payment_gateway", "inserted_at"
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
BS_INVESTMENT_SM_SEARCH_FOR_VALUES = [
    "Telegram", "WhatsApp", "Facebook", "Instagram",
    "YouTube", "X", "Twitter", "Thread", "Threads",
    "Snapchat", "TikTok", "LinkedIn", "Pinterest", "Reddit"
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
    global BANK_NAME_MAPPING, IFSC_MAPPING
    try:
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
            'instagram.com': 'Instagram', 'telegram.org': 'Telegram','web.telegram.org': 'Telegram','telegram.me': 'Telegram',
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
    """
    Legacy single-row lookup — kept for compatibility.
    Queries website_directory table in Supabase.
    For bulk processing use bulk_lookup_origin_category() instead.
    """
    url_value = clean_value(url)
    if url_value == "NA":
        return "NA", "NA"
    url_clean = url_value.strip()
    try:
        # Try exact match first (case-insensitive via ilike)
        resp = supabase.table("website_directory") \
            .select("origin,category") \
            .or_(f"url.ilike.{url_clean},final_url.ilike.{url_clean}") \
            .limit(1).execute()
        if resp.data:
            row = resp.data[0]
            origin   = (row.get("origin")   or "NA").strip() or "NA"
            category = (row.get("category") or "NA").strip() or "NA"
            return origin, category
        # Try domain-level match
        try:
            domain = urlparse(url_clean).netloc
            if domain:
                domain_clean = domain[4:] if domain.startswith("www.") else domain
                like_term = f"%{domain_clean}%"
                resp2 = supabase.table("website_directory") \
                    .select("origin,category") \
                    .or_(f"url.ilike.{like_term},final_url.ilike.{like_term}") \
                    .limit(1).execute()
                if resp2.data:
                    row = resp2.data[0]
                    origin   = (row.get("origin")   or "NA").strip() or "NA"
                    category = (row.get("category") or "NA").strip() or "NA"
                    return origin, category
        except Exception:
            pass
    except Exception as e:
        print(f"[WD Lookup] Error for {url}: {e}")
    return "NA", "NA"


def bulk_lookup_origin_category(urls: list) -> dict:
    """
    Fetch origin+category for a list of URLs from website_directory in one pass.
    Returns dict: { url_lower: (origin, category) }
    Matching priority: exact url/final_url → domain fallback.
    """
    result = {}
    unique_urls = [u for u in set(urls) if u and u.upper() not in ("NA", "N/A", "")]
    if not unique_urls:
        return result

    # Build domain index for fallback
    domain_map = {}
    for u in unique_urls:
        try:
            netloc = urlparse(u).netloc
            if netloc:
                domain = netloc[4:] if netloc.startswith("www.") else netloc
                if domain not in domain_map:
                    domain_map[domain] = u
        except Exception:
            pass

    CHUNK = 200
    try:
        all_rows = []
        offset = 0
        while True:
            resp = supabase.table("website_directory") \
                .select("url,final_url,origin,category") \
                .order("id", desc=False) \
                .range(offset, offset + CHUNK - 1).execute()
            rows = resp.data or []
            all_rows.extend(rows)
            if len(rows) < CHUNK:
                break
            offset += CHUNK

        # Build lookup maps from DB
        url_exact   = {}   # url_lower → (origin, category)
        domain_db   = {}   # domain    → (origin, category)

        for row in all_rows:
            origin   = (row.get("origin")   or "NA").strip() or "NA"
            category = (row.get("category") or "NA").strip() or "NA"
            for col in ("url", "final_url"):
                val = (row.get(col) or "").strip().lower()
                if val and val not in ("na", "n/a", ""):
                    url_exact[val] = (origin, category)
                    try:
                        netloc = urlparse(val).netloc
                        if netloc:
                            dom = netloc[4:] if netloc.startswith("www.") else netloc
                            if dom not in domain_db:
                                domain_db[dom] = (origin, category)
                    except Exception:
                        pass

        # Match each requested URL
        for u in unique_urls:
            u_lower = u.lower().strip()
            if u_lower in url_exact:
                result[u_lower] = url_exact[u_lower]
                continue
            # Try http/https swap
            for old, new in [("https://", "http://"), ("http://", "https://")]:
                if u_lower.startswith(old):
                    alt = new + u_lower[len(old):]
                    if alt in url_exact:
                        result[u_lower] = url_exact[alt]
                        break
            if u_lower in result:
                continue
            # Domain fallback
            try:
                netloc = urlparse(u_lower).netloc
                if netloc:
                    dom = netloc[4:] if netloc.startswith("www.") else netloc
                    if dom in domain_db:
                        result[u_lower] = domain_db[dom]
            except Exception:
                pass

    except Exception as e:
        print(f"[bulk_lookup_origin_category] Error: {e}")

    return result


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

    # ── Pre-fetch origin/category for all website URLs in one bulk call ──
    _website_url_col = None
    for h in standardized_headers:
        if h == "Website URL":
            _website_url_col = h
            break
    _wd_cache = {}
    if _website_url_col and _website_url_col in df.columns:
        _all_urls = [
            clean_value(df.iloc[i][_website_url_col])
            for i in range(len(df))
        ]
        _valid_urls = [u for u in _all_urls if u != "NA"]
        if _valid_urls:
            _wd_cache = bulk_lookup_origin_category(_valid_urls)

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
                _key = row_data['website_url'].lower().strip()
                origin, category = _wd_cache.get(_key, ("NA", "NA"))
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
                _key = row_data['website_url'].lower().strip()
                origin, _ = _wd_cache.get(_key, ("NA", "NA"))
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
                _key = row_data['website_url'].lower().strip()
                _, category = _wd_cache.get(_key, ("NA", "NA"))
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
                session.permanent = True
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
        is_admin = session.get("is_admin", False)
        allowed_depts = session.get("allowed_departments")  # None = see all, list = restricted
 
        resp = client.table("activity_logs") \
            .select("*") \
            .order("created_at", desc=True) \
            .limit(500) \
            .execute()
        all_logs = resp.data or []
 
        # Admins OR users with no dept restriction see everything
        if is_admin or not allowed_depts:
            logs = all_logs
        else:
            current_email = session.get("email", "")
 
            def _log_allowed(log):
                # Always show the current user's own activity
                if log.get("user_email") == current_email:
                    return True
 
                target_table = log.get("target_table", "")
                action_type  = log.get("action_type", "")
 
                # ── social_media_accounts logs ──────────────────────────
                if target_table == "social_media_accounts":
 
                    # Other users' import logs → always hide
                    if action_type == "import":
                        return False
 
                    if action_type == "field_update":
                        extra = log.get("extra_info") or {}
                        # supabase-py returns JSONB as dict, guard against raw string
                        if isinstance(extra, str):
                            try:
                                import json as _j
                                extra = _j.loads(extra)
                            except Exception:
                                extra = {}
 
                        dept = extra.get("department", "")
                        if not dept:
                            return False
 
                        return dept in allowed_depts
 
                    # Any other action type on social table → hide from restricted users
                    return False
                return True
 
            logs = [l for l in all_logs if _log_allowed(l)]
 
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
    if not page_type or (page_type not in allowed_pages and page_type != 'insights'):
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
        platform_number_type_counts = {}
        CHUNK = 1000
        for platform in platforms:
            try:
                all_rows = []
                offset = 0
                total_count = 0
                while True:
                    _q = social_supabase.table("social_media_accounts") \
                        .select("account_status,department,number_type", count='exact') \
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
                num_type_map = {}
                for item in all_rows:
                    status = (item.get('account_status') or 'Active').strip()
                    dept = (item.get('department') or 'Unknown').strip()
                    num_type = (item.get('number_type') or 'Unknown').strip()
                    if not dept or dept in ('NA', 'N/A', 'nan', ''):
                        dept = 'Unknown'
                    if not num_type or num_type in ('NA', 'N/A', 'nan', ''):
                        num_type = 'Unknown'
                    if status == 'Permanent Block':
                        pb_count += 1
                    else:
                        status_map[status] = status_map.get(status, 0) + 1
                    if status != 'Permanent Block':
                        dept_map[dept] = dept_map.get(dept, 0) + 1
                        num_type_map[num_type] = num_type_map.get(num_type, 0) + 1
                platform_status_counts[platform] = status_map
                perm_block_counts[platform] = pb_count
                perm_block_total += pb_count
                platform_dept_counts[platform] = dept_map
                platform_number_type_counts[platform] = num_type_map
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
                "platform_number_type_counts": platform_number_type_counts,
            }
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route("/get-number-type-counts", methods=["GET"])
@login_required
def get_number_type_counts():
    try:
        is_admin = session.get("is_admin", False)
        allowed_depts = session.get("allowed_departments")
        number_types = ["Prepaid", "Postpaid", "Disposable Number"]
        counts = {}
        for nt in number_types:
            q = social_supabase.table("social_media_accounts") \
                .select("id", count='exact') \
                .eq("number_type", nt) \
                .neq("account_status", "Permanent Block")
            if not is_admin and allowed_depts:
                if len(allowed_depts) == 1:
                    q = q.eq("department", allowed_depts[0])
                else:
                    q = q.in_("department", allowed_depts)
            resp = q.execute()
            counts[nt] = resp.count or 0
        return jsonify({"success": True, "counts": counts})
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
        bank_headers = list(pd.read_excel(BANK_NAME_MAPPING_PATH).columns) if BANK_NAME_MAPPING_PATH.exists() else []
        return jsonify({
            "success": True,
            "bank_name_mapping_headers": bank_headers,
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
        return jsonify({"success": True, "message": f"Data reloaded! Bank: {len(BANK_NAME_MAPPING)}, IFSC: {len(IFSC_MAPPING)}"})
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
            "bank_name_mapping": len(BANK_NAME_MAPPING),
            "ifsc_mapping": len(IFSC_MAPPING)
        }
    })
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
                # Also store department so activity log can be filtered
                try:
                    dept_resp = social_supabase.table("social_media_accounts") \
                        .select("department").eq("id", account_id).limit(1).execute()
                    if dept_resp.data and dept_resp.data[0].get("department"):
                        extra_info['department'] = dept_resp.data[0]['department']
                except Exception:
                    pass
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
        search   = request.args.get("search",   "").strip()
        platform = request.args.get("platform", "").strip()
 
        query = social_supabase.table("social_media_accounts") \
            .select("id,owned_by,number,login_device,blocked_date,account_create_date,platform,department") \
            .eq("account_status", "Permanent Block")
 
        # ── Department filter ───────────────────────────────────────────
        # Always enforce for non-admins; admins see all
        is_admin     = session.get("is_admin", False)
        allowed_depts = session.get("allowed_departments")  # None = unrestricted
 
        if not is_admin and allowed_depts:
            if len(allowed_depts) == 1:
                query = query.eq("department", allowed_depts[0])
            else:
                query = query.in_("department", allowed_depts)
        # If is_admin OR allowed_depts is None → no department filter applied
 
        # ── Platform filter ────────────────────────────────────────────
        if platform:
            query = query.eq("platform", platform)
 
        # ── Search filter ──────────────────────────────────────────────
        if search:
            like_term = f"%{search}%"
            query = query.or_(
                f"owned_by.ilike.{like_term},"
                f"number.ilike.{like_term},"
                f"login_device.ilike.{like_term},"
                f"platform.ilike.{like_term}"
            )
 
        query = query.order("id", desc=False)
        response = query.execute()
 
        accounts = []
        for item in (response.data or []):
            b_date_str      = item.get("blocked_date")        or ""
            create_date_str = item.get("account_create_date") or ""
            active_duration = "N/A"
 
            if b_date_str and create_date_str:
                try:
                    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d"):
                        try:
                            days = (
                                datetime.strptime(b_date_str[:10], fmt) -
                                datetime.strptime(create_date_str[:10], fmt)
                            ).days
                            active_duration = f"{days} days" if days >= 0 else "N/A"
                            break
                        except ValueError:
                            continue
                except Exception:
                    pass
 
            accounts.append({
                "id":              item.get("id"),
                "owned_by":        item.get("owned_by")    or "N/A",
                "number":          item.get("number")      or "N/A",
                "login_device":    item.get("login_device") or "N/A",
                "platform":        item.get("platform")    or "N/A",
                "department":      item.get("department")  or "N/A",
                "blocked_date":    b_date_str              or "N/A",
                "active_duration": active_duration,
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
    
@app.route("/update-share-status", methods=["POST"])
@login_required
def update_share_status():
    try:
        data = request.get_json()
        if not data:
            return jsonify({"success": False, "error": "No data provided"})
        
        raw_ids = data.get("ids", [])
        new_status = data.get("status", "").strip()
        
        if not raw_ids:
            return jsonify({"success": False, "error": "No IDs provided"})
        if new_status not in ["Pending", "Shared"]:
            return jsonify({"success": False, "error": "Invalid status"})
        
        # Clean & parse IDs — accept int or string
        clean_ids = []
        for item in raw_ids:
            try:
                clean_ids.append(int(str(item).strip()))
            except (ValueError, TypeError):
                continue
        
        if not clean_ids:
            return jsonify({"success": False, "error": "No valid numeric IDs found"})
        
        # Bulk update in Supabase
        resp = supabase.table("scrapping_data") \
            .update({"share_status": new_status}) \
            .in_("id", clean_ids) \
            .execute()
        
        updated_count = len(resp.data) if resp.data else 0
        
        log_activity(
            action_type="field_update",
            target_table="scrapping_data",
            field_name="share_status",
            old_value="(bulk)",
            new_value=new_status,
            extra_info={"ids": clean_ids, "count": updated_count}
        )
        
        return jsonify({
            "success": True,
            "updated": updated_count,
            "ids_submitted": len(clean_ids)
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})
    
@app.route("/delete-social-record", methods=["POST"])
@login_required
def delete_social_record():
    try:
        data = request.get_json()
        record_id = data.get("id")
        if not record_id:
            return jsonify({"success": False, "error": "No ID provided"})
        # Fetch info before deleting for logging
        try:
            old_resp = social_supabase.table("social_media_accounts") \
                .select("platform,login_user,number").eq("id", record_id).limit(1).execute()
            old_info = old_resp.data[0] if old_resp.data else {}
        except Exception:
            old_info = {}
        social_supabase.table("social_media_accounts") \
            .delete().eq("id", record_id).execute()
        log_activity(
            action_type="field_update",
            target_table="social_media_accounts",
            target_record_id=record_id,
            field_name="DELETE",
            old_value=f"platform={old_info.get('platform','?')}, number={old_info.get('number','?')}",
            new_value="DELETED",
            extra_info={"platform": old_info.get("platform")}
        )
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})
@app.route("/get-scraping-record/<int:record_id>", methods=["GET"])
@login_required
def get_scraping_record(record_id):
    try:
        resp = supabase.table("scrapping_data") \
            .select("id,name,platform,post_url,chat_number,group_name,scam_type,share_status,inserted_date,screenshot") \
            .eq("id", record_id).limit(1).execute()
        if resp.data:
            return jsonify({"success": True, "record": resp.data[0]})
        return jsonify({"success": False, "error": "Record not found"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})
@app.route("/update-scraping-record", methods=["POST"])
@login_required
def update_scraping_record():
    try:
        data = request.get_json()
        record_id = data.get("id")
        if not record_id:
            return jsonify({"success": False, "error": "No ID"})
        ALLOWED = ["platform", "post_url", "chat_number", "group_name", "scam_type", "share_status"]
        updates = {k: v for k, v in data.items() if k in ALLOWED}
        if not updates:
            return jsonify({"success": False, "error": "No valid fields to update"})
        resp = supabase.table("scrapping_data").update(updates).eq("id", record_id).execute()
        log_activity(
            action_type="field_update",
            target_table="scrapping_data",
            target_record_id=record_id,
            field_name=",".join(updates.keys()),
            new_value=str(updates),
        )
        return jsonify({"success": True, "record": resp.data[0] if resp.data else {}})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})
@app.route("/delete-scraping-record", methods=["POST"])
@login_required
def delete_scraping_record():
    try:
        data = request.get_json()
        record_id = data.get("id")
        if not record_id:
            return jsonify({"success": False, "error": "No ID"})
        supabase.table("scrapping_data").delete().eq("id", record_id).execute()
        log_activity(
            action_type="field_update",
            target_table="scrapping_data",
            target_record_id=record_id,
            field_name="DELETE",
            old_value="EXISTS",
            new_value="DELETED"
        )
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})
@app.route("/my-scraping-count", methods=["GET"])
@login_required
def my_scraping_count():
    try:
        display_name = session.get("display_name", "")
        clean_name = re.sub(r'\s*\(.*?\)\s*', '', display_name).strip()
        if not clean_name:
            return jsonify({"success": True, "count": 0})
        date_from = request.args.get("date_from", "").strip()
        date_to   = request.args.get("date_to",   "").strip()
        q = supabase.table("scrapping_data") \
            .select("id", count='exact') \
            .ilike("name", f"%{clean_name}%")
        if date_from:
            q = q.gte("inserted_date", date_from)
        if date_to:
            q = q.lte("inserted_date", date_to)
        resp = q.execute()
        return jsonify({"success": True, "count": resp.count or 0})
    except Exception as e:
        return jsonify({"success": False, "count": 0, "error": str(e)})
    
# ============================================================
# INSIGHTS — BS Investment Scam Analytics
# ============================================================
@app.route("/investment-insights-data", methods=["GET"])
@login_required
def investment_insights_data():
    try:
        date_from   = request.args.get("date_from",   "").strip()
        date_to     = request.args.get("date_to",     "").strip()
        search_for  = request.args.get("search_for",  "").strip()
        scam_type   = request.args.get("scam_type",   "").strip()
        wallet      = request.args.get("wallet",      "").strip()
        input_user  = request.args.get("input_user",  "").strip()
        is_sm_search = search_for in ("__SM__", "SM Counts")

        CHUNK = 1000
        all_rows, offset = [], 0
        while True:
            q = supabase.table("BS_Investment_Scam").select(
                "Inserted_date,Input_user,Search_for,Scam_type,"
                "Upi_bank_account_wallet,Upi_vpa,Bank_account_number,Web_contact_no"
            )
            if date_from:  q = q.gte("Inserted_date", date_from)
            if date_to:    q = q.lte("Inserted_date", date_to)
            if is_sm_search:
                q = q.in_("Search_for", BS_INVESTMENT_SM_SEARCH_FOR_VALUES)
            elif search_for:
                q = q.eq("Search_for", search_for)
            if scam_type:  q = q.eq("Scam_type",  scam_type)
            if wallet:     q = q.eq("Upi_bank_account_wallet", wallet)
            if input_user: q = q.eq("Input_user", input_user)
            resp  = q.order("Id", desc=False).range(offset, offset + CHUNK - 1).execute()
            chunk = resp.data or []
            all_rows.extend(chunk)
            if len(chunk) < CHUNK:
                break
            offset += CHUNK

        rows = [{k.lower(): v for k, v in r.items()} for r in all_rows]

        # ---------- unique UPI per date ----------
        upi_by_date = {}
        upi_set = set()
        bank_set = set()
        dated_rows = []
        for r in rows:
            d      = (r.get("inserted_date") or "")[:10]
            wallet_val = (r.get("upi_bank_account_wallet") or "").strip()
            upi    = (r.get("upi_vpa") or "").strip()
            bank_acc = (r.get("bank_account_number") or "").strip()
            try:
                parsed_date = datetime.strptime(d, "%Y-%m-%d").date() if d else None
            except ValueError:
                parsed_date = None
            if parsed_date:
                dated_rows.append((r, parsed_date))
            if not d: continue
            if wallet_val == "UPI" and upi and upi.upper() not in ("NA", "N/A", ""):
                if d not in upi_by_date:
                    upi_by_date[d] = set()
                upi_by_date[d].add(upi)
                upi_set.add(upi)
            if wallet_val == "Bank Account" and bank_acc and bank_acc.upper() not in ("NA", "N/A", ""):
                bank_set.add(bank_acc)
        # upi_series: cumulative unique UPIs seen up to each date
        # This ensures chart total matches the card total
        seen_upis = set()
        upi_series = {}
        for d in sorted(upi_by_date.keys()):
            seen_upis.update(upi_by_date[d])
            upi_series[d] = len(upi_by_date[d])

        def _trend_bucket(start_date, end_date):
            case_count = 0
            upis = set()
            banks = set()
            for row, parsed_date in dated_rows:
                if not (start_date <= parsed_date <= end_date):
                    continue
                case_count += 1
                wallet_val = (row.get("upi_bank_account_wallet") or "").strip()
                upi_val = (row.get("upi_vpa") or "").strip()
                bank_val = (row.get("bank_account_number") or "").strip()
                if wallet_val == "UPI" and upi_val and upi_val.upper() not in ("NA", "N/A", ""):
                    upis.add(upi_val)
                if wallet_val == "Bank Account" and bank_val and bank_val.upper() not in ("NA", "N/A", ""):
                    banks.add(bank_val)
            return {
                "cases": case_count,
                "upi": len(upis),
                "bank": len(banks),
            }

        def _trend_series(start_date, end_date):
            buckets = {}
            current_date = start_date
            while current_date <= end_date:
                buckets[current_date] = {"cases": 0, "upis": set(), "banks": set()}
                current_date += timedelta(days=1)
            for row, parsed_date in dated_rows:
                if parsed_date not in buckets:
                    continue
                buckets[parsed_date]["cases"] += 1
                wallet_val = (row.get("upi_bank_account_wallet") or "").strip()
                upi_val = (row.get("upi_vpa") or "").strip()
                bank_val = (row.get("bank_account_number") or "").strip()
                if wallet_val == "UPI" and upi_val and upi_val.upper() not in ("NA", "N/A", ""):
                    buckets[parsed_date]["upis"].add(upi_val)
                if wallet_val == "Bank Account" and bank_val and bank_val.upper() not in ("NA", "N/A", ""):
                    buckets[parsed_date]["banks"].add(bank_val)
            ordered_dates = sorted(buckets)
            return {
                "labels": [d.isoformat() for d in ordered_dates],
                "cases": [buckets[d]["cases"] for d in ordered_dates],
                "upi": [len(buckets[d]["upis"]) for d in ordered_dates],
                "bank": [len(buckets[d]["banks"]) for d in ordered_dates],
            }

        def _trend_metric(current_value, previous_value):
            delta = current_value - previous_value
            pct = None if previous_value == 0 else round((delta / previous_value) * 100, 1)
            return {"current": current_value, "previous": previous_value, "delta": delta, "percent": pct}

        if dated_rows:
            trend_end = max(parsed_date for _, parsed_date in dated_rows)
            trend_start = trend_end - timedelta(days=29)
            prev_end = trend_start - timedelta(days=1)
            prev_start = prev_end - timedelta(days=29)
            current_trend = _trend_bucket(trend_start, trend_end)
            previous_trend = _trend_bucket(prev_start, prev_end)
        else:
            trend_end = datetime.utcnow().date()
            trend_start = trend_end - timedelta(days=29)
            prev_end = trend_start - timedelta(days=1)
            prev_start = prev_end - timedelta(days=29)
            current_trend = {"cases": 0, "upi": 0, "bank": 0}
            previous_trend = {"cases": 0, "upi": 0, "bank": 0}
        trend_30d = {
            "period_start": trend_start.isoformat(),
            "period_end": trend_end.isoformat(),
            "previous_start": prev_start.isoformat(),
            "previous_end": prev_end.isoformat(),
            "cases": _trend_metric(current_trend["cases"], previous_trend["cases"]),
            "upi": _trend_metric(current_trend["upi"], previous_trend["upi"]),
            "bank": _trend_metric(current_trend["bank"], previous_trend["bank"]),
            "series": _trend_series(trend_start, trend_end),
        }

        # ---------- user counts per date ----------
        user_by_date = {}
        for r in rows:
            d    = (r.get("inserted_date") or "")[:10]
            user = (r.get("input_user") or "Unknown").strip()
            if not d: continue
            if d not in user_by_date:
                user_by_date[d] = {}
            user_by_date[d][user] = user_by_date[d].get(user, 0) + 1
        all_users = sorted({u for dmap in user_by_date.values() for u in dmap})

        # ---------- scam type counts ----------
        scam_counts = {}
        for r in rows:
            st = (r.get("scam_type") or "Unknown").strip() or "Unknown"
            scam_counts[st] = scam_counts.get(st, 0) + 1

        # ---------- search_for counts ----------
        sf_counts = {}
        for r in rows:
            sf = (r.get("search_for") or "Unknown").strip() or "Unknown"
            wc = (r.get("web_contact_no") or "").strip()
            has_contact = wc and wc.upper() not in ("NA", "N/A", "", "NONE", "NULL")
            if has_contact:
                sf_counts["WhatsApp"] = sf_counts.get("WhatsApp", 0) + 1
            else:
                # No contact number → original sf mein count karo
                sf_counts[sf] = sf_counts.get(sf, 0) + 1
        # Normalize WhatsApp variants (case fix)
        for key in list(sf_counts.keys()):
            if key.lower() == "whatsapp" and key != "WhatsApp":
                sf_counts["WhatsApp"] = sf_counts.get("WhatsApp", 0) + sf_counts.pop(key)
        # ── Average daily cases ──────────────────────────────
        active_dates = [d for d in user_by_date.keys() if d]
        if active_dates:
            if input_user:
                user_daily = {
                    d: user_by_date[d].get(input_user, 0)
                    for d in active_dates
                    if user_by_date[d].get(input_user, 0) > 0
                }
                avg_cases = round(sum(user_daily.values()) / len(user_daily), 1) if user_daily else 0
            else:
                total_daily = {d: sum(user_by_date[d].values()) for d in active_dates}
                avg_cases = round(sum(total_daily.values()) / len(total_daily), 1) if total_daily else 0
        else:
            avg_cases = 0
        # ── Per-user stats for comparison ───────────────────
        user_stats = {}
        for u in all_users:
            u_upi_set  = set()
            u_bank_set = set()
            u_dates    = set()
            u_total    = 0
            for r in rows:
                ru = (r.get("input_user") or "Unknown").strip()
                if ru != u:
                    continue
                u_total += 1
                d = (r.get("inserted_date") or "")[:10]
                if d:
                    u_dates.add(d)
                wallet_val = (r.get("upi_bank_account_wallet") or "").strip()
                upi_val    = (r.get("upi_vpa") or "").strip()
                bank_val   = (r.get("bank_account_number") or "").strip()
                if wallet_val == "UPI" and upi_val and upi_val.upper() not in ("NA", "N/A", ""):
                    u_upi_set.add(upi_val)
                if wallet_val == "Bank Account" and bank_val and bank_val.upper() not in ("NA", "N/A", ""):
                    u_bank_set.add(bank_val)
            u_avg = round(u_total / len(u_dates), 1) if u_dates else 0
            user_stats[u] = {
                "total":       u_total,
                "avg":         u_avg,
                "unique_upi":  len(u_upi_set),
                "unique_bank": len(u_bank_set),
            }

        return jsonify({
            "success": True,
            "total_rows": len(rows),
            "unique_upi_count": len(upi_set),
            "unique_bank_count": len(bank_set),
            "avg_cases": avg_cases,
            "trend_30d": trend_30d,
            "upi_series":   upi_series,
            "user_by_date": {d: user_by_date[d] for d in sorted(user_by_date)},
            "all_users":    all_users,
            "scam_counts":  scam_counts,
            "sf_counts":    sf_counts,
            "user_stats":   user_stats,
            "all_input_users": sorted(list({(r.get("input_user") or "Unknown").strip() for r in rows if r.get("input_user")})),
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})
    
@app.route("/investment-bank-data", methods=["GET"])
@login_required
def investment_bank_data():
    try:
        date_from  = request.args.get("date_from",  "").strip()
        date_to    = request.args.get("date_to",    "").strip()
        search_for = request.args.get("search_for", "").strip()
        scam_type  = request.args.get("scam_type",  "").strip()
        wallet     = request.args.get("wallet",     "").strip()
        input_user = request.args.get("input_user", "").strip()
        is_sm_search = search_for in ("__SM__", "SM Counts")

        CHUNK = 1000
        all_rows, offset = [], 0
        while True:
            q = supabase.table("BS_Investment_Scam").select(
                "Bank_name,Inserted_date,Search_for,Input_user"
            )
            if date_from:  q = q.gte("Inserted_date", date_from)
            if date_to:    q = q.lte("Inserted_date", date_to)
            if is_sm_search:
                q = q.in_("Search_for", BS_INVESTMENT_SM_SEARCH_FOR_VALUES)
            elif search_for:
                q = q.eq("Search_for", search_for)
            if scam_type:  q = q.eq("Scam_type", scam_type)
            if wallet:     q = q.eq("Upi_bank_account_wallet", wallet)
            if input_user: q = q.eq("Input_user", input_user)
            resp  = q.order("Id", desc=False).range(offset, offset + CHUNK - 1).execute()
            chunk = resp.data or []
            all_rows.extend(chunk)
            if len(chunk) < CHUNK:
                break
            offset += CHUNK

        rows = [{k.lower(): v for k, v in r.items()} for r in all_rows]
        bank_counts = {}
        for r in rows:
            bn = (r.get("bank_name") or "").strip()
            if not bn or bn.upper() in ("NA", "N/A", "") or bn.lower() == "unknown":
                continue
            bank_counts[bn] = bank_counts.get(bn, 0) + 1

        sorted_banks = sorted(bank_counts.items(), key=lambda x: x[1], reverse=True)[:10]

        monthly_counts = {}
        for r in rows:
            d = (r.get("inserted_date") or "")[:7]
            if not d:
                continue
            monthly_counts[d] = monthly_counts.get(d, 0) + 1

        return jsonify({
            "success": True,
            "bank_counts": dict(sorted_banks),
            "monthly_counts": {k: monthly_counts[k] for k in sorted(monthly_counts)},
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})
    
# ============================================================
# WEBSITE DIRECTORY — LIST / FILTER
# ============================================================
@app.route("/website-directory", methods=["GET"])
@login_required
def website_directory():
    user = get_current_user()
    allowed_pages = session.get("allowed_pages", [])

    wd_search    = request.args.get("wd_search",   "").strip()
    wd_remark    = request.args.get("wd_remark",   "").strip()
    wd_category  = request.args.get("wd_category", "").strip()
    wd_search_for= request.args.get("wd_search_for","").strip()
    wd_date_from = request.args.get("wd_date_from","").strip()
    wd_date_to   = request.args.get("wd_date_to",  "").strip()
    page = int(request.args.get("page_num", 1))

    items = []
    total_rows = 0
    total_pages = 1

    try:
        query = supabase.table("website_directory").select("*", count="exact")
        query = query.or_("remark.is.null,remark.eq.NA,remark.eq.,remark.eq.IPG")
        if wd_search:
            lt = f"%{wd_search}%"
            query = query.or_(
                f"url.ilike.{lt},"
                f"final_url.ilike.{lt},"
                f"name.ilike.{lt},"
                f"group_app_name.ilike.{lt},"
                f"login_id.ilike.{lt},"
                f"number.ilike.{lt},"
                f"email.ilike.{lt},"
                f"invitation_code.ilike.{lt},"
                f"password.ilike.{lt},"
                f"remark.ilike.{lt},"
                f"origin.ilike.{lt},"
                f"category.ilike.{lt},"
                f"search_for.ilike.{lt},"
                f"automated_website.ilike.{lt},"
                f"payment_gateway.ilike.{lt}"
            )         
        if wd_remark:
            query = query.eq("remark", wd_remark)
        if wd_category:
            query = query.eq("category", wd_category)
        if wd_search_for:
            query = query.eq("search_for", wd_search_for)
        if wd_date_from:
            query = query.gte("date", wd_date_from)
        if wd_date_to:
            query = query.lte("date", wd_date_to)

        query = query.order("id", desc=True)
        offset = (page - 1) * PER_PAGE
        query = query.range(offset, offset + PER_PAGE - 1)
        resp = query.execute()
        items = resp.data or []
        total_rows = resp.count or 0
        total_pages = max(1, math.ceil(total_rows / PER_PAGE))
    except Exception as e:
        flash(f"Error fetching website directory: {e}", "error")

    clean_display_name = get_clean_display_name(session.get("display_name", "User"))
    return render_template(
        "website_directory.html",
        items=items,
        wd_search=wd_search,
        wd_remark=wd_remark,
        wd_category=wd_category,
        wd_search_for=wd_search_for,
        wd_date_from=wd_date_from,
        wd_date_to=wd_date_to,
        page_num=page,
        total_pages=total_pages,
        total_rows=total_rows,
        wd_category_options=WEBSITE_DIRECTORY_CATEGORY_OPTIONS,
        wd_search_for_options=WEBSITE_DIRECTORY_SEARCH_FOR_OPTIONS,
        current_user=user,
        allowed_pages=allowed_pages,
        display_name=session.get("display_name", "User"),
        clean_display_name=clean_display_name,
        can_view_activity_log=session.get("can_view_activity_log", False),
    )
# ============================================================
# WEBSITE DIRECTORY — IMPORT
# ============================================================
@app.route("/website-directory-import", methods=["POST"])
@login_required
def website_directory_import():
    file = request.files.get("file")
    if not file or file.filename == "":
        flash("No file selected", "error")
        return redirect("/website-directory")
    if not is_allowed_file(file.filename):
        flash("Unsupported file type.", "error")
        return redirect("/website-directory")

    try:
        filename = secure_filename(file.filename)
        temp_path = os.path.join(tempfile.gettempdir(), filename)
        file.save(temp_path)
        file_ext = filename.rsplit(".", 1)[1].lower() if "." in filename else "csv"
        df = read_data_file(temp_path, file_ext)
        df.columns = df.columns.astype(str).str.strip()
        df = df.fillna("")

        COL_MAP = {
            "date":             ["date", "Date", "DATE"],
            "name":             ["name", "Name", "NAME"],
            "url":              ["url", "URL", "Url", "website_url", "Website URL"],
            "final_url":        ["final_url", "Final URL", "FinalURL", "final url"],
            "invitation_code":  ["invitation_code", "Invitation Code", "InvitationCode", "invite_code"],
            "search_for":       ["search_for", "Search For", "SearchFor", "search for"],
            "group_app_name":   ["group_app_name", "Group/App Name", "Group App Name", "GroupAppName", "group_name", "app_name"],
            "number":           ["number", "Number", "Phone", "phone", "Mobile", "mobile"],
            "email":            ["email", "Email", "EMAIL", "mail"],
            "login_id":         ["login_id", "Login ID", "LoginID", "login id", "Login Id"],
            "password":         ["password", "Password", "PASSWORD", "pass"],
            "remark":           ["remark", "Remark", "REMARK", "remarks", "Remarks"],
            "origin":           ["origin", "Origin", "ORIGIN"],
            "category":         ["category", "Category", "CATEGORY", "scam_type", "Scam Type"],
            "automated_website":["automated_website", "Automated Website", "AutomatedWebsite", "automated"],
            "payment_gateway":  ["payment_gateway", "Payment Gateway", "PaymentGateway", "gateway"],
        }

        def resolve_col(target_cols):
            for c in target_cols:
                if c in df.columns:
                    return c
            for c in target_cols:
                for col in df.columns:
                    if c.lower() == col.lower():
                        return col
            return None

        DATE_FIELDS = {"date"}
        records = []
        for _, row in df.iterrows():
            rec = {}
            for db_col, candidates in COL_MAP.items():
                src = resolve_col(candidates)
                val = str(row[src]).strip() if src else ""
                if db_col in DATE_FIELDS:
                    rec[db_col] = val if val and val.upper() not in ("NA","N/A","NAN","","NONE","NULL") else None
                else:
                    rec[db_col] = val if val else "NA"
            records.append(rec)

        supabase.table("website_directory").insert(records).execute()
        log_activity(
            action_type="import",
            target_table="website_directory",
            extra_info={"file_name": filename, "records_count": len(records)}
        )
        flash(f"Imported successfully! {len(records)} records added.", "success")
        os.remove(temp_path)
    except Exception as e:
        flash(f"Import Error: {e}", "error")
    return redirect("/website-directory")

# ============================================================
# WEBSITE DIRECTORY — EXPORT
# ============================================================
@app.route("/website-directory-export", methods=["GET"])
@login_required
def website_directory_export():
    try:
        wd_search     = request.args.get("wd_search",    "").strip()
        wd_remark     = request.args.get("wd_remark",    "").strip()
        wd_category   = request.args.get("wd_category",  "").strip()
        wd_search_for = request.args.get("wd_search_for","").strip()
        wd_date_from  = request.args.get("wd_date_from", "").strip()
        wd_date_to    = request.args.get("wd_date_to",   "").strip()

        CHUNK = 1000
        all_rows, offset = [], 0
        while True:
            q = supabase.table("website_directory").select("*")
            if wd_search:
                lt = f"%{wd_search}%"
                q = q.or_(
                    f"url.ilike.{lt},"
                    f"final_url.ilike.{lt},"
                    f"name.ilike.{lt},"
                    f"group_app_name.ilike.{lt},"
                    f"login_id.ilike.{lt},"
                    f"number.ilike.{lt},"
                    f"email.ilike.{lt},"
                    f"invitation_code.ilike.{lt},"
                    f"password.ilike.{lt},"
                    f"remark.ilike.{lt},"
                    f"origin.ilike.{lt},"
                    f"category.ilike.{lt},"
                    f"search_for.ilike.{lt},"
                    f"automated_website.ilike.{lt},"
                    f"payment_gateway.ilike.{lt}"
                )
            if wd_remark:    q = q.eq("remark", wd_remark)
            if wd_category:  q = q.eq("category",      wd_category)
            if wd_search_for:q = q.eq("search_for",    wd_search_for)
            if wd_date_from: q = q.gte("date",          wd_date_from)
            if wd_date_to:   q = q.lte("date",          wd_date_to)
            chunk = q.order("id", desc=False).range(offset, offset + CHUNK - 1).execute()
            rows = chunk.data or []
            all_rows.extend(rows)
            if len(rows) < CHUNK:
                break
            offset += CHUNK

        df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame()
        output = io.StringIO()
        df.to_csv(output, index=False, encoding="utf-8-sig")
        output.seek(0)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        return send_file(
            io.BytesIO(output.getvalue().encode("utf-8-sig")),
            download_name=f"website_directory_{ts}.csv",
            as_attachment=True,
            mimetype="text/csv"
        )
    except Exception as e:
        flash(f"Export Error: {e}", "error")
        return redirect("/website-directory")
# ============================================================
# WEBSITE DIRECTORY — TRACKER STATS
# ============================================================
@app.route("/website-directory-tracker-stats", methods=["GET"])
@login_required
def website_directory_tracker_stats():
    try:
        CHUNK = 1000
        all_rows, offset = [], 0

        # ── Fetch ALL rows first ──
        while True:
            resp = supabase.table("website_directory") \
                .select("category,search_for,remark,date,name") \
                .order("id", desc=False) \
                .range(offset, offset + CHUNK - 1).execute()
            chunk = resp.data or []
            all_rows.extend(chunk)
            if len(chunk) < CHUNK:
                break
            offset += CHUNK

        # ── Aggregation AFTER loop ──
        cat_counts        = {}
        sf_counts         = {}
        remark_counts     = {}
        daily_counts      = {}
        user_total_counts = {}
        user_cat_counts   = {}

        for row in all_rows:
            cat  = (row.get("category")   or "Unknown").strip() or "Unknown"
            sf   = (row.get("search_for") or "Unknown").strip() or "Unknown"
            rem  = (row.get("remark")     or "").strip()
            dt   = (row.get("date")       or "")[:10]
            user = (row.get("name")       or "Unknown").strip() or "Unknown"
            if user.upper() in ("NA", "N/A", ""):
                user = "Unknown"

            cat_counts[cat] = cat_counts.get(cat, 0) + 1
            sf_counts[sf]   = sf_counts.get(sf,  0) + 1

            if rem and rem.upper() not in ("NA", "N/A", ""):
                remark_counts[rem] = remark_counts.get(rem, 0) + 1

            if dt:
                daily_counts[dt] = daily_counts.get(dt, 0) + 1

            user_total_counts[user] = user_total_counts.get(user, 0) + 1
            if user not in user_cat_counts:
                user_cat_counts[user] = {}
            user_cat_counts[user][cat] = user_cat_counts[user].get(cat, 0) + 1

        return jsonify({
            "success": True,
            "total": len(all_rows),
            "cat_counts":   cat_counts,
            "sf_counts":    sf_counts,
            "remark_counts": dict(
                sorted(remark_counts.items(), key=lambda x: x[1], reverse=True)[:20]
            ),
            "daily_counts": {k: daily_counts[k] for k in sorted(daily_counts)},
            "user_total_counts": dict(
                sorted(user_total_counts.items(), key=lambda x: x[1], reverse=True)
            ),
            "user_cat_counts": user_cat_counts,
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)})
# ============================================================
# WEBSITE DIRECTORY — INSERT SINGLE RECORD
# ============================================================
@app.route("/website-directory-insert", methods=["POST"])
@login_required
def website_directory_insert():
    try:
        data = request.get_json()
        if not data:
            return jsonify({"success": False, "error": "No data"})

        ALLOWED = [
            "date","name","url","final_url","invitation_code","search_for",
            "group_app_name","number","email","login_id","password",
            "remark","origin","category","automated_website","payment_gateway"
        ]
        record = {}
        for f in ALLOWED:
            val = str(data.get(f, "")).strip()
            if f == "date":
                record[f] = val if val and val.upper() not in ("NA","N/A","") else None
            else:
                record[f] = val if val else "NA"

        resp = supabase.table("website_directory").insert(record).execute()
        if resp.data:
            log_activity(
                action_type="import",
                target_table="website_directory",
                extra_info={"file_name": "manual_insert", "records_count": 1}
            )
            return jsonify({"success": True, "record": resp.data[0]})
        return jsonify({"success": False, "error": "Insert failed"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})
    
@app.route("/website-directory-update", methods=["POST"])
@login_required
def website_directory_update():
    try:
        data = request.get_json()
        if not data:
            return jsonify({"success": False, "error": "No data"})
        rid = data.get("id")
        if not rid:
            return jsonify({"success": False, "error": "No ID"})

        ALLOWED = [
            "date","name","url","final_url","invitation_code","search_for",
            "group_app_name","number","email","login_id","password",
            "remark","origin","category","automated_website","payment_gateway"
        ]
        record = {}
        for f in ALLOWED:
            if f not in data:
                continue  # Only update fields that are sent
            val = str(data.get(f, "")).strip()
            if f == "date":
                record[f] = val if val and val.upper() not in ("NA","N/A","") else None
            elif f == "remark":
                record[f] = val  # Allow empty string for remark
            else:
                record[f] = val if val else "NA"

        resp = supabase.table("website_directory").update(record).eq("id", rid).execute()
        if resp.data:
            log_activity(
                action_type="field_update",
                target_table="website_directory",
                target_record_id=rid,
                field_name="edit",
                new_value=str(record)
            )
            return jsonify({"success": True, "record": resp.data[0]})
        return jsonify({"success": False, "error": "Update failed"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route("/website-directory-get-record", methods=["GET"])
@login_required
def website_directory_get_record():
    try:
        rid = request.args.get("id")
        if not rid:
            return jsonify({"success": False, "error": "No ID"})
        resp = supabase.table("website_directory").select("*").eq("id", rid).limit(1).execute()
        if resp.data:
            return jsonify({"success": True, "record": resp.data[0]})
        return jsonify({"success": False, "error": "Record not found"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})
# ============================================================
# WEBSITE DIRECTORY — DELETE SINGLE RECORD
# ============================================================
@app.route("/website-directory-delete", methods=["POST"])
@login_required
def website_directory_delete():
    try:
        data = request.get_json()
        rid = data.get("id")
        if not rid:
            return jsonify({"success": False, "error": "No ID"})
        supabase.table("website_directory").delete().eq("id", rid).execute()
        log_activity(
            action_type="field_update",
            target_table="website_directory",
            target_record_id=rid,
            field_name="DELETE",
            old_value="EXISTS",
            new_value="DELETED"
        )
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})
@app.route("/website-directory-delete-bulk", methods=["POST"])
@login_required
def website_directory_delete_bulk():
    try:
        data = request.get_json()
        ids = data.get("ids", [])
        if not ids:
            return jsonify({"success": False, "error": "No IDs provided"})
        clean_ids = []
        for item in ids:
            try:
                clean_ids.append(int(str(item).strip()))
            except (ValueError, TypeError):
                continue
        if not clean_ids:
            return jsonify({"success": False, "error": "No valid IDs"})
        supabase.table("website_directory").delete().in_("id", clean_ids).execute()
        log_activity(
            action_type="field_update",
            target_table="website_directory",
            field_name="DELETE_BULK",
            old_value="EXISTS",
            new_value="DELETED",
            extra_info={"ids": clean_ids, "count": len(clean_ids)}
        )
        return jsonify({"success": True, "deleted": len(clean_ids)})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route("/website-directory-search-api", methods=["GET"])
@login_required
def website_directory_search_api():
    try:
        q = request.args.get("q", "").strip()
        if not q:
            return jsonify({"success": True, "items": []})
        like_term = f"%{q}%"
        resp = supabase.table("website_directory") \
            .select("id,url,final_url,search_for,login_id,password,origin,category,invitation_code") \
            .or_(f"url.ilike.{like_term},final_url.ilike.{like_term}") \
            .order("id", desc=True) \
            .limit(5) \
            .execute()
        items = resp.data or []
        return jsonify({"success": True, "items": items})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})
# ============================================================
# WEBSITE DIRECTORY — DOWNLOAD TEMPLATE
# ============================================================
@app.route("/website-directory-template", methods=["GET"])
@login_required
def website_directory_template():
    headers = [
        "date","name","url","final_url","invitation_code","search_for",
        "group_app_name","number","email","login_id","password",
        "remark","origin","category","automated_website","payment_gateway"
    ]
    out = io.StringIO()
    csv.writer(out).writerow(headers)
    out.seek(0)
    return send_file(
        io.BytesIO(out.getvalue().encode("utf-8-sig")),
        download_name="Website_Directory_Template.csv",
        as_attachment=True,
        mimetype="text/csv"
    )

@app.route("/website-directory-user-summary", methods=["GET"])
@login_required
def website_directory_user_summary():
    """
    Returns count of website_directory records grouped by name (user),
    optionally filtered by date range.
    """
    try:
        date_from = request.args.get("date_from", "").strip()
        date_to   = request.args.get("date_to",   "").strip()
        date_on   = request.args.get("date_on",   "").strip()

        CHUNK = 1000
        all_rows, offset = [], 0
        while True:
            q = supabase.table("website_directory").select("name")
            if date_on:
                q = q.eq("date", date_on)
            else:
                if date_from:
                    q = q.gte("date", date_from)
                if date_to:
                    q = q.lte("date", date_to)
            resp = q.order("id", desc=False).range(offset, offset + CHUNK - 1).execute()
            chunk = resp.data or []
            all_rows.extend(chunk)
            if len(chunk) < CHUNK:
                break
            offset += CHUNK

        user_counts = {}
        for row in all_rows:
            name = (row.get("name") or "Unknown").strip() or "Unknown"
            if name.upper() in ("NA", "N/A", ""):
                name = "Unknown"
            user_counts[name] = user_counts.get(name, 0) + 1

        return jsonify({"success": True, "user_counts": user_counts, "total": len(all_rows)})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})
    
@app.route("/website-directory-summary-stats", methods=["GET"])
@login_required
def website_directory_summary_stats():
    """
    Returns count of website_directory records grouped by category,
    optionally filtered by date range (date column).
    Used by Scrapping Summary → Category/Platform wise summary.
    """
    try:
        date_from = request.args.get("date_from", "").strip()
        date_to   = request.args.get("date_to",   "").strip()
        date_on   = request.args.get("date_on",   "").strip()

        CHUNK = 1000
        all_rows, offset = [], 0
        while True:
            q = supabase.table("website_directory").select("category")
            if date_on:
                q = q.eq("date", date_on)
            else:
                if date_from:
                    q = q.gte("date", date_from)
                if date_to:
                    q = q.lte("date", date_to)
            resp = q.order("id", desc=False).range(offset, offset + CHUNK - 1).execute()
            chunk = resp.data or []
            all_rows.extend(chunk)
            if len(chunk) < CHUNK:
                break
            offset += CHUNK

        cat_counts = {}
        for row in all_rows:
            cat = (row.get("category") or "Unknown").strip() or "Unknown"
            if cat.upper() in ("NA", "N/A", ""):
                cat = "Unknown"
            cat_counts[cat] = cat_counts.get(cat, 0) + 1

        return jsonify({"success": True, "cat_counts": cat_counts, "total": len(all_rows)})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})
    
@app.route("/website-directory-inoperable", methods=["GET"])
@login_required
def website_directory_inoperable():
    try:
        offset = int(request.args.get("offset", 0))
        limit  = int(request.args.get("limit", 1000))
        resp = supabase.table("website_directory") \
            .select("id,date,name,url,final_url,search_for,login_id,password,remark,origin,category,group_app_name") \
            .not_.eq("remark", "IPG") \
            .not_.is_("remark", "null") \
            .order("id", desc=True) \
            .range(offset, offset + limit - 1) \
            .execute()
        return jsonify({"success": True, "items": resp.data or []})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})
@app.route("/social-search-ajax", methods=["GET"])
@login_required
def social_search_ajax():
    try:
        query = request.args.get("q", "").strip()
        platform = request.args.get("platform", "").strip()
        status = request.args.get("status", "").strip()
        department = request.args.get("department", "").strip()
        permanent_block = request.args.get("permanent_block", "").strip()

        q = social_supabase.table("social_media_accounts").select("*", count='exact')

        allowed_depts = session.get("allowed_departments")
        if allowed_depts:
            if len(allowed_depts) == 1:
                q = q.eq("department", allowed_depts[0])
            else:
                q = q.in_("department", allowed_depts)

        if query:
            like_term = f"%{query}%"
            q = q.or_(
                f"login_user.ilike.{like_term},"
                f"number.ilike.{like_term},"
                f"full_name.ilike.{like_term},"
                f"page_name.ilike.{like_term},"
                f"platform.ilike.{like_term},"
                f"account_status.ilike.{like_term},"
                f"owned_by.ilike.{like_term},"
                f"department.ilike.{like_term},"
                f"sim_operator.ilike.{like_term},"
                f"mail_id.ilike.{like_term},"
                f"account_id.ilike.{like_term},"
                f"number_type.ilike.{like_term},"
                f"login_device.ilike.{like_term}"
            )

        if platform:
            q = q.eq("platform", platform)
        if department:
            q = q.eq("department", department)

        if permanent_block == "true":
            q = q.eq("account_status", "Permanent Block")
        else:
            if status:
                q = q.eq("account_status", status)
            else:
                q = q.neq("account_status", "Permanent Block")

        q = q.order("id", desc=False).range(0, 999)
        resp = q.execute()
        items = [dict(row) for row in (resp.data or [])]
        total = resp.count or len(items)

        return jsonify({"success": True, "items": items, "total": total})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})
# ============================================================
# TOTAL NUMBERS — Public Read-Only API
# ============================================================

TN_COLUMNS = "id,department,owned_by,number,sim_inserted_device,account_status,number_type,sim_operator"


@app.route("/api/total-numbers", methods=["GET"])
def api_total_numbers_list():
    """
    GET /api/total-numbers
    Public read-only — no login required.
    Returns all Total Numbers records (7 columns only).
    Query params: department, account_status, number_type, sim_operator, search, page, per_page
    """
    try:
        department     = request.args.get("department",     "").strip()
        account_status = request.args.get("account_status", "").strip()
        number_type    = request.args.get("number_type",    "").strip()
        sim_operator   = request.args.get("sim_operator",   "").strip()
        search         = request.args.get("search",         "").strip()
        page           = max(1, int(request.args.get("page",     1)))
        per_page       = min(500, max(1, int(request.args.get("per_page", 100))))

        query = social_supabase.table("social_media_accounts") \
            .select(TN_COLUMNS, count="exact") \
            .eq("platform", "Total Numbers")

        if department:
            query = query.eq("department", department)
        if account_status:
            query = query.eq("account_status", account_status)
        if number_type:
            query = query.eq("number_type", number_type)
        if sim_operator:
            query = query.ilike("sim_operator", f"%{sim_operator}%")
        if search:
            lt = f"%{search}%"
            query = query.or_(
                f"owned_by.ilike.{lt},"
                f"number.ilike.{lt},"
                f"sim_inserted_device.ilike.{lt},"
                f"account_status.ilike.{lt},"
                f"number_type.ilike.{lt},"
                f"sim_operator.ilike.{lt},"
                f"department.ilike.{lt}"
            )

        offset = (page - 1) * per_page
        resp   = query.order("id", desc=False) \
                      .range(offset, offset + per_page - 1) \
                      .execute()

        items      = resp.data or []
        total_rows = resp.count or 0

        return jsonify({
            "success":     True,
            "total":       total_rows,
            "page":        page,
            "per_page":    per_page,
            "total_pages": max(1, math.ceil(total_rows / per_page)),
            "items":       items
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/total-numbers/<int:record_id>", methods=["GET"])
def api_total_numbers_get(record_id):
    """
    GET /api/total-numbers/<id>
    Public read-only — fetch single record by ID.
    """
    try:
        resp = social_supabase.table("social_media_accounts") \
            .select(TN_COLUMNS) \
            .eq("id", record_id) \
            .eq("platform", "Total Numbers") \
            .limit(1) \
            .execute()

        if not resp.data:
            return jsonify({"success": False, "error": "Record not found"}), 404

        return jsonify({"success": True, "record": resp.data[0]})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/total-numbers/stats", methods=["GET"])
def api_total_numbers_stats():
    """
    GET /api/total-numbers/stats
    Public read-only — summary counts by status, number_type, department, sim_operator.
    """
    try:
        resp = social_supabase.table("social_media_accounts") \
            .select(TN_COLUMNS) \
            .eq("platform", "Total Numbers") \
            .execute()

        rows = resp.data or []

        status_counts   = {}
        num_type_counts = {}
        dept_counts     = {}
        sim_op_counts   = {}

        for r in rows:
            s  = (r.get("account_status") or "Unknown").strip()
            nt = (r.get("number_type")    or "Unknown").strip()
            d  = (r.get("department")     or "Unknown").strip()
            so = (r.get("sim_operator")   or "Unknown").strip()

            status_counts[s]   = status_counts.get(s,   0) + 1
            num_type_counts[nt] = num_type_counts.get(nt, 0) + 1
            dept_counts[d]     = dept_counts.get(d,     0) + 1
            sim_op_counts[so]  = sim_op_counts.get(so,  0) + 1

        return jsonify({
            "success":        True,
            "total":          len(rows),
            "by_status":      status_counts,
            "by_number_type": num_type_counts,
            "by_department":  dept_counts,
            "by_sim_operator": dict(
                sorted(sim_op_counts.items(), key=lambda x: x[1], reverse=True)
            )
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

if __name__ == "__main__":
    EXCEL_FOLDER_PATH.mkdir(exist_ok=True)
    load_config()
    load_excel_data()
    port = int(os.environ.get("PORT", 5000))
    debug_mode = os.environ.get("FLASK_DEBUG", "False").lower() == "true"
    app.run(debug=debug_mode, host='0.0.0.0', port=port)