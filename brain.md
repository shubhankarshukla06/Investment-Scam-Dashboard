# brain.md — Project Map

## 1. Project Overview

- Flask monolith for Scam Intelligence workflows: scraping records, social media account inventory, investment-scam data, website directory/allotment, QC, lunch tracker, and case-report PDF generation.
- Stack: Flask/Jinja2, Supabase/PostgREST, pandas/openpyxl/xlrd CSV/XLSX processing, ReportLab/PyPDF/PyMuPDF/Pillow/Tesseract, Selenium/browser automation, boto3/S3 + CloudFront, gunicorn on Render/Docker.
- Main backend is `app.py`; templates are standalone Jinja HTML files with no shared base/includes detected.

## 2. Folder/File Map

- `.env` — local environment variables; used for Flask secret, Supabase, AWS/S3/CloudFront credentials.
- `.git/` — Git repository metadata.
- `.gitignore` — Git ignore rules.
- `.venv/` — local Python virtual environment; not app logic.
- `.vscode/` — editor settings.
- `__pycache__/` — generated Python bytecode cache.
- `app.py` — main Flask monolith; routes, helpers, Supabase clients, imports, background jobs, and most business logic.
- `config.py` — centralized env var config for Flask, Supabase, AWS, S3/CloudFront, upload size.
- `desktop.ini` — Windows/OneDrive metadata.
- `Dockerfile` — container build/runtime; Python 3.11 slim + gunicorn on port 8080.
- `excel_data/` — local Excel reference/mapping data such as `bank_name.xlsx` and `ifsc_mapping.xlsx`.
- `generated_reports/` — generated report artifacts / temporary output area.
- `migrate_block_status.py` — one-time Supabase migration: social status `Block` → `Temporarily Block`.
- `Procfile` — Render/Heroku web command: gunicorn bound to `$PORT`.
- `requirements.txt` — Python dependencies.
- `runtime.txt` — deployment Python version: `python-3.11.9`.
- `sheet_mapping_config.json` — sheet conversion mappings for `upi` and `investment` output formats.
- `static/` — static assets; currently contains image asset(s).
- `templates/` — standalone Jinja templates for dashboard modules/pages.
- `uploads/` — uploaded files / temporary user-upload staging.
- `utils/` — helper modules for AWS upload, filename generation, PDF generation.

## 3. Module Index

### Auth / Session / Access Control

- Helpers: `get_auth_supabase()` L67, `parse_bool()` L86, `fetch_user_by_email()` L95, `login_required()` L112, `get_current_user()` L120, `get_clean_display_name()` L926, `redirect_to_allowed_page()` L933.
- Routes:
  - `/login` GET/POST — `login()` L955-L984 — renders `templates/login.html`.
  - `/logout` GET — `logout()` L987-L989.
  - `/get-session-info` GET — `get_session_info()` L3297-L3305.
- Data: `dashboard_users`; fallback `DEMO_ADMIN` hardcoded in `app.py` L72-L84.
- Behavior: stores `user_id`, `email`, `display_name`, `allowed_pages`, `is_admin`, `role`, `can_view_activity_log`, `allowed_departments` in Flask session.
- Quirk: `login()` redirects to `/?page={first_page}` directly; `redirect_to_allowed_page()` has better dedicated-page mapping but is not used there.

### Activity Log

- Helpers: `log_activity()` L136-L153.
- Routes:
  - `/get-user-activity-log` GET — `get_user_activity_log()` L995-L1084.
  - `/export-user-activity-log` GET — `export_user_activity_log()` L1088-L1136.
- Data: `activity_logs`.
- Template: consumed inside `templates/index.html` dashboard.
- Behavior: requires `can_view_activity_log`; non-superadmins are filtered by `allowed_pages`, target tables, user email, and `allowed_departments` for social-account activity.

### Main Dashboard Shell

- Route: `/` GET — `index()` L1194-L1404 — renders `templates/index.html`.
- Modules inside same template: `scraping`, `social`, `investment`, `sheet`, activity log, insights widgets.
- Data: `scrapping_data`, `social_media_accounts`, `BS_Investment_Scam` depending on `page` query param.
- Behavior: checks `page` against `allowed_pages`; redirects dedicated pages for `qc`, `website_directory`, `case_report`, `allotment`, `allotment_admin`.
- Quirk: `index()` checks `page_type == "lunch_break"`, but lunch access helper uses allowed page key `lunch`; this can affect `/lunch-break` redirect from `/?page=lunch`.

### Scrapping Data

- Main UI: `/?page=scraping` via `index()` L1194-L1404; template `templates/index.html`.
- Routes:
  - `/scraping-tracker-stats` GET — `scraping_tracker_stats()` L1142-L1188.
  - `/upload` POST — `upload()` L2304-L2348.
  - `/export` GET — `export()` L2352-L2382.
  - `/parse-raw-file` POST — `parse_raw_file()` L2386-L2407.
  - `/insert-scraping-record` POST — `insert_scraping_record()` L3393-L3447.
  - `/check-scraping-duplicates` POST — `check_scraping_duplicates()` L3530-L3632.
  - `/check-chat-number` POST — `check_chat_number()` L3636-L3654.
  - `/scrapping-summary-data` GET — `scrapping_summary_data()` L3658-L3685.
  - `/get-scraping-record/<int:record_id>` GET — `get_scraping_record()` L3718-L3727.
  - `/update-scraping-record` POST — `update_scraping_record()` L3730-L3750.
  - `/delete-scraping-record` POST — `delete_scraping_record()` L3753-L3770.
  - `/my-scraping-count` GET — `my_scraping_count()` L3773-L3791.
- Helpers: `_extract_facebook_profile_id()` L3449, `_normalize_scraping_duplicate_url()` L3470, `_scraping_url_lookup_term()` L3487, `_normalize_chat_number()` L3503, `_chat_numbers_match()` L3515.
- Data: `scrapping_data`.
- Behavior: non-superadmins see only rows where `name` equals clean display name; duplicate checks normalize URLs and chat numbers; import/export use pandas/CSV.

### Social Media Accounts

- Main UI: `/?page=social` via `index()` L1194-L1404; template `templates/index.html`.
- Routes:
  - `/tracker-stats` GET — `tracker_stats()` L1577-L1668.
  - `/get-number-type-counts` GET — `get_number_type_counts()` L1672-L1692.
  - `/social-import` POST — `social_import()` L1698-L1768.
  - `/social-export` GET — `social_export()` L1772-L1815.
  - `/save-social-field` POST — `save_social_field()` L2799-L2877.
  - `/get-permanent-block-accounts` GET — `get_permanent_block_accounts()` L2881-L2945.
  - `/insert-social-record` POST — `insert_social_record()` L3331-L3387.
  - `/delete-social-record` POST — `delete_social_record()` L3689-L3715.
  - `/social-search-ajax` GET — `social_search_ajax()` L5442-L5493.
  - `/social-download-template` GET — `social_download_template()` L6903-L7039.
  - `/api/total-numbers` GET — `api_total_numbers_list()` L6804-L6850.
  - `/api/total-numbers/<int:record_id>` GET — `api_total_numbers_get()` L6853-L6865.
  - `/api/total-numbers/stats` GET — `api_total_numbers_stats()` L6868-L6899.
- Helpers: `normalize_social_account_status()` L263, `normalize_social_account_row()` L276, `apply_social_status_filter()` L282.
- Data: `social_media_accounts`.
- Behavior: department filtering via `allowed_departments`; permanent-block rows hidden by default in several queries; status rename handled by migration script.

### Sheet Conversion / AML GUI Import

- UI: `/?page=sheet` via `templates/index.html`.
- Routes:
  - `/get-sheet-headers/<sheet_type>` GET — `get_sheet_headers_route()` L1819-L1826.
  - `/download-template/<sheet_type>` GET — `download_template()` L1830-L1848.
  - `/preview-sheet` POST — `preview_sheet()` L1852-L1907.
  - `/generate-sheet` POST — `generate_sheet()` L1911-L1965.
  - `/sheet-import-to-aml-gui` POST — `sheet_import_to_aml_gui()` L2015-L2046.
  - `/get-excel-headers` GET — `get_excel_headers()` L2271-L2280.
  - `/get-ifsc-headers` GET — `get_ifsc_headers()` L2284-L2289.
  - `/reload-data` POST — `reload_data()` L2293-L2298.
  - `/summary-source-from-gui` POST — `summary_source_from_gui()` L2755-L2782.
  - `/getDepartmentData` GET — `get_department_data_proxy()` L3309-L3327.
- Helpers: `read_data_file()` L345, `load_excel_data()` L390, `load_config()` L420, `get_sheet_headers()` L472, `validate_input_columns()` L743, `process_sheet_data()` L789, `_build_sheet_csv_response()` L1967, `_import_sheet_csv_to_aml_gui()` L2623, `_import_sheet_csv_to_aml_gui_headed()` L2652.
- Config: `sheet_mapping_config.json` with `upi` and `investment` mappings.
- Data/files: `excel_data/bank_name.xlsx`, `excel_data/ifsc_mapping.xlsx`, uploaded CSV/XLSX files.
- Behavior: validates exact input headers, standardizes columns, derives UPI/bank/account fields, generates CSV downloads, optionally imports to AML GUI using HTTP/Selenium helper paths.

### UPI Validity Checker

- UI: invoked from `templates/index.html`.
- Routes:
  - `/start-upi-check` POST — `start_upi_check()` L3103-L3140.
  - `/upi-check-status/<job_id>` GET — `upi_check_status()` L3144-L3157.
  - `/stop-upi-check/<job_id>` POST — `stop_upi_check()` L3161-L3166.
  - `/export-upi-check/<job_id>` GET — `export_upi_check()` L3170-L3232.
- Helpers: `build_upi_candidates()` L2966, `verify_upi_external()` L2980, `_run_upi_check_worker()` L3010, `_run_upi_check_job()` L3083.
- Behavior: background-thread job model with in-memory job state; exports checked results for a given job id.

### Investment Scam

- Main UI: `/?page=investment` via `index()` L1194-L1404; template `templates/index.html`.
- Routes:
  - `/investment-tracker-stats` GET — `investment_tracker_stats()` L1410-L1466.
  - `/investment-last-date` GET — `investment_last_date()` L1470-L1484.
  - `/investment-export` GET — `investment_export()` L1536-L1571.
  - `/investment-import` POST — `investment_import()` L2149-L2267.
  - `/check-duplicates` POST — `check_duplicates()` L3236-L3291.
  - `/investment-insights-data` GET — `investment_insights_data()` L3797-L4039.
  - `/investment-bank-data` GET — `investment_bank_data()` L4047-L4097.
- Helpers: `normalize_import_header()` L2111, `find_import_column()` L2114, `normalize_import_date()` L2123, `normalize_import_datetime()` L2135.
- Data: `BS_Investment_Scam`.
- Behavior: search/filter by UPI, bank account, handle, website URL, contact, input user; imports and exports in CSV; insights aggregate users/scam/search/wallet/bank stats.

### Website Directory

- Main UI: `/website-directory` GET — `website_directory()` L4103-L4182 — renders `templates/website_directory.html`.
- Routes:
  - `/website-directory-import` POST — `website_directory_import()` L4188-L4253.
  - `/website-directory-export` GET — `website_directory_export()` L4259-L4301.
  - `/website-directory-tracker-stats` GET — `website_directory_tracker_stats()` L4307-L4364.
  - `/website-directory-insert` POST — `website_directory_insert()` L4370-L4399.
  - `/website-directory-update` POST — `website_directory_update()` L4403-L4440.
  - `/website-directory-get-record` GET — `website_directory_get_record()` L4444-L4454.
  - `/website-directory-delete` POST — `website_directory_delete()` L4460-L4477.
  - `/website-directory-delete-bulk` POST — `website_directory_delete_bulk()` L4480-L4505.
  - `/website-directory-search-api` GET — `website_directory_search_api()` L4509-L4524.
  - `/website-directory-template` GET — `website_directory_template()` L4530-L4544.
  - `/website-directory-user-summary` GET — `website_directory_user_summary()` L4548-L4579.
  - `/website-directory-summary-stats` GET — `website_directory_summary_stats()` L4583-L4657.
  - `/website-directory-inoperable` GET — `website_directory_inoperable()` L4661-L4675.
  - `/website-directory-operable` GET — `website_directory_operable()` L4805-L4871.
- Data: `website_directory`.
- Behavior: filter/search by website, category, search_for, remark, date; supports CSV/XLSX import/export, CRUD, stats, operable/inoperable lists.

### Scam++ Website Allotment

- Main UI: `/scam-website-allotment` GET — `scam_website_allotment()` L4878-L5095 — renders `templates/scam_website_allotment.html`.
- Routes:
  - `/scam-website-allotment-bulk-match` POST — `scam_website_allotment_bulk_match()` L4679-L4800.
  - `/scam-website-allotment-users` GET — `scam_website_allotment_users()` L5100-L5120.
  - `/scam-website-allotment-allot` POST — `scam_website_allotment_allot()` L5125-L5223.
  - `/scam-website-allotment-reassign` POST — `scam_website_allotment_reassign()` L5227-L5258.
  - `/scam-website-allotment-update-remark` POST — `scam_website_allotment_update_remark()` L5263-L5279.
  - `/scam-website-allotment-counts` GET — `scam_website_allotment_counts()` L5283-L5313.
  - `/scam-website-allotment-check-target` GET — `scam_website_allotment_check_target()` L5317-L5354.
  - `/scam-website-allotment-delete-row` POST — `scam_website_allotment_delete_row()` L5359-L5372.
  - `/scam-website-allotment-export` GET — `scam_website_allotment_export()` L5377-L5438.
- Data: `website_directory`, `website_allotment`, `dashboard_users`.
- Access: `can_access_allotment()` accepts `allotment` or `allotment_admin`; `is_allotment_admin()` gates admin assignment actions.
- Behavior: allotment admins can assign/reassign and see broader rows; regular users see assigned rows and update remarks.

### Case Report Generator / AML Regenerate

- Main UI: `/case-report` GET — `case_report_page()` L6514-L6526 — renders `templates/case_report.html`.
- Routes:
  - `/generate-case-report` POST — `generate_case_report()` L6530-L6634.
  - `/case-reports-list` GET — `case_reports_list()` L6638-L6677.
  - `/delete-case-report/<report_id>` DELETE — `delete_case_report()` L6681-L6699.
  - `/bulk-regenerate-cases` POST — `bulk_regenerate_cases()` L6706-L6749.
  - `/download-regenerate-file/<job_id>/<file_type>` GET — `download_regenerate_file()` L6753-L6769.
  - `/regenerate-job-status/<job_id>` GET — `regenerate_job_status()` L6774-L6782.
  - `/stop-regenerate-job/<job_id>` POST — `stop_regenerate_job()` L6787-L6801.
- Helpers: `configure_tesseract_cmd()` L5518, `get_aml_credentials_for_user()` L5560, `case_report_allowed_file()` L5642, `extract_screenshots_from_pdf()` L5677, `download_and_extract_report_images()` L5720, AML captcha/login/submit helpers L5738-L6307, `_run_bulk_regenerate_job()` L6371-L6510.
- Utils: `utils/pdf_generator.py`, `utils/aws_upload.py`, `utils/filename_generator.py`.
- Data/files: Supabase `reports`; PDFs in `generated_reports/`; S3 prefix `case-reports/`.
- Behavior: creates ReportLab PDFs from screenshots/source URL, uploads to S3/CloudFront, lists/deletes reports, and runs bulk regeneration jobs with snapshot files.

### Lunch Break Tracker

- Main UI: `/lunch-break` GET — `lunch_break()` L7046-L7102 — renders `templates/lunch_break.html`.
- Routes:
  - `/lunch-break/insert` POST — `lunch_break_insert()` L7107-L7160.
  - `/lunch-break/update` POST — `lunch_break_update()` L7165-L7203.
  - `/lunch-break/delete` POST — `lunch_break_delete()` L7208-L7222.
  - `/lunch-break/export` GET — `lunch_break_export()` L7227-L7263.
- Helpers: `can_access_lunch()` L215, `is_own_lunch_record()` L7403.
- Data: `lunch_breaks`.
- Behavior: access requires `lunch` in `allowed_pages`; users can fill lunch entries for others; current user's own lunch records are hidden; duration is calculated from start/end times.
- Quirk: main dashboard redirect checks `lunch_break`, not `lunch`.

### Dashboard Management

- Main UI: `/dashboard-management` GET — `dashboard_management()` L7423-L7435 — renders `templates/dashboard_management.html`.
- Routes:
  - `/dashboard-management/api/users` GET — `dashboard_management_users()` L7550-L7557.
  - `/dashboard-management/api/gui-status` GET — `dashboard_management_gui_status()` L7563-L7586.
  - `/dashboard-management/api/investment-scam-users` GET — `dashboard_management_investment_scam_users()` L7592-L7598.
  - `/dashboard-management/api/investment-scam-users` POST — `dashboard_management_create_investment_scam_user()` L7635-L7676.
  - `/dashboard-management/api/investment-scam-users/<int:user_id>` PUT — `dashboard_management_update_investment_scam_user()` L7682-L7728.
  - `/dashboard-management/api/investment-scam-users/<int:user_id>` DELETE — `dashboard_management_delete_investment_scam_user()` L7734-L7748.
  - `/dashboard-management/api/users` POST — `dashboard_management_create_user()` L7754-L7796.
  - `/dashboard-management/api/users/<int:user_id>` PUT — `dashboard_management_update_user()` L7802-L7851.
  - `/dashboard-management/api/users/<int:user_id>` DELETE — `dashboard_management_delete_user()` L7857-L7869.
  - `/dashboard-management/api/users/<int:user_id>/toggle-active` POST — `dashboard_management_toggle_active()` L7875-L7890.
- Helpers: `can_manage_dashboard()` L7352, `dashboard_management_required()` L7361, `dashboard_management_required_json()` L7371, `_serialize_dashboard_user()` L7438, `_table_status_row()` L7492.
- Data: `dashboard_users`, `investment_scam_users`/investment scam user APIs, status queries across feature tables.
- Behavior: requires login plus superadmin/dashboard-management permission; JSON APIs use separate JSON access decorator.

### GUI QC

- Main UI: `/qc-gui` GET — `qc_gui()` L8163-L8300 — renders `templates/qc_gui.html`.
- Routes:
  - `/qc-gui-import` POST — `qc_gui_import()` L8305-L8348.
  - `/qc-gui-users` GET — `qc_gui_users()` L8353-L8385.
  - `/qc-gui-allotment` POST — `qc_gui_allotment()` L8390-L8491.
  - `/qc-gui-export` GET — `qc_gui_export()` L8496-L8555.
  - `/qc-gui-template` GET — `qc_gui_template()` L8560-L8576.
  - `/qc-gui-get-record` GET — `qc_gui_get_record()` L8581-L8593.
  - `/qc-gui-update` POST — `qc_gui_update()` L8598-L8659.
  - `/qc-gui-delete` POST — `qc_gui_delete()` L8664-L8683.
  - `/qc-gui-tracker-stats` GET — `qc_gui_tracker_stats()` L8688-L8771.
- Helpers: `_qc_normalize_header()` L8006, `_qc_find_import_column()` L8010, `_qc_normalize_int_value()` L8020, `_qc_build_records_from_df()` L8030, `_auto_allot_qc_for_user()` L8073.
- Data: `qc_table`, `dashboard_users`.
- Behavior: access requires `qc` in `allowed_pages`; auto-allotment gives up to `AUTO_QC_DAILY_LIMIT = 70` pending rows/user/day; non-admins see their own pending/completed records; admins can view broader statuses.

### Health / Misc

- Route: `/health` GET — `health_check()` L2785-L2793.
- Behavior: simple app health endpoint; no login decorator.

## 4. Templates Index

- `templates/login.html` — rendered by `/login` (`login()`); standalone login UI; no extends/includes detected.
- `templates/index.html` — rendered by `/` (`index()`); dashboard shell for scraping, social, investment, sheet conversion, activity log, insight widgets; no extends/includes detected; very large template.
- `templates/website_directory.html` — rendered by `/website-directory`; directory list/filter/import/export/manual CRUD UI; no extends/includes detected.
- `templates/scam_website_allotment.html` — rendered by `/scam-website-allotment`; website allotment/assignment UI; no extends/includes detected.
- `templates/case_report.html` — rendered by `/case-report`; case report PDF generation, list, delete, and bulk regenerate UI; no extends/includes detected.
- `templates/lunch_break.html` — rendered by `/lunch-break`; lunch tracker list/filter/insert/update/delete/export UI; no extends/includes detected.
- `templates/dashboard_management.html` — rendered by `/dashboard-management`; dashboard user/access management and GUI/table status UI; no extends/includes detected.
- `templates/qc_gui.html` — rendered by `/qc-gui`; QC import/allotment/review/export/template/stats UI; no extends/includes detected.

## 5. Cross-Cutting Concerns

### Authentication and sessions

- `login_required()` checks `session["user_id"]`; unauthenticated users redirect to `/login`.
- Login loads users from Supabase `dashboard_users` except hardcoded `DEMO_ADMIN`.
- Access keys in `allowed_pages`: `scraping`, `sheet`, `social`, `investment`, `qc`, `website_directory`, `allotment`, `allotment_admin`, `insights`, `case_report`, `lunch`, `dashboard_management`.
- Role helpers: `is_superadmin()`, `is_admin_or_above()`, `can_access_lunch()`, `can_access_allotment()`, `is_allotment_admin()`.
- `/get-session-info` returns current session details for frontend use.

### Supabase/PostgREST

- Main client: `supabase = create_client(SUPABASE_URL, SUPABASE_KEY, options=_SUPABASE_CLIENT_OPTS)` in `app.py` L165-L169.
- `social_supabase` aliases the same client.
- Auth helper creates separate client via `get_auth_supabase()`.
- Client timeouts capped at 30 seconds to avoid Cloudflare ~60s timeout issues.
- Main tables: `dashboard_users`, `activity_logs`, `scrapping_data`, `social_media_accounts`, `BS_Investment_Scam`, `website_directory`, `website_allotment`, `reports`, `lunch_breaks`, `qc_table`, `investment_scam_users`.
- Many exports use chunked Supabase reads via `_stream_supabase_csv()`.

### Import/export and file handling

- Allowed input extensions come from helpers/config: CSV, XLSX, XLS, XLSM, XLSB, ODS.
- `read_data_file()` centralizes pandas file reads.
- CSV responses use UTF-8 BOM in several helpers for Excel compatibility.
- Temporary uploads are written under system temp via `tempfile.gettempdir()` and cleaned up after processing.

### PDF, OCR, AML GUI, and AWS

- Report PDFs are generated by `utils/pdf_generator.py::generate_pdf()` using ReportLab and IST timestamps.
- Filenames come from `utils/filename_generator.py::generate_filename()` with source-hostname suffix.
- Upload/delete is in `utils/aws_upload.py`, using S3 + CloudFront domain from `Config`.
- AML workflows use requests/Selenium, captcha OCR helpers, Tesseract/ddddocr/PyMuPDF/Pillow.

### Deployment and environment

- `Dockerfile`: Python 3.11 slim, installs `requirements.txt`, runs gunicorn on `0.0.0.0:8080`, 1 worker, 600s timeout.
- `Procfile`: `web: gunicorn -b 0.0.0.0:$PORT app:app --workers 1 --timeout 600 --graceful-timeout 30`.
- `runtime.txt`: `python-3.11.9`.
- `config.py` env vars: `SECRET_KEY`, `FLASK_DEBUG`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `S3_BUCKET_NAME`, `CLOUDFRONT_DOMAIN`, `SUPABASE_URL`, `SUPABASE_KEY`.
- `Config.validate()` requires AWS + Supabase env vars but is not necessarily called in every path.

## 6. Quick Task Lookup Table

| If the task involves... | Look at... |
|---|---|
| Login/logout/session bugs | `app.py` L67-L132, L933-L989; `templates/login.html` |
| Page permission / allowed page redirects | `app.py` L215-L238, L933-L950, L1194-L1210; route decorators near each feature |
| Activity log visibility/export | `app.py` L136-L153, L995-L1136; `templates/index.html` |
| Main dashboard tab behavior | `app.py` L1194-L1404; `templates/index.html` |
| Scrapping data list/filter/import/export | `app.py` L1142-L1188, L1194-L1267, L2304-L2407; `templates/index.html` |
| Scrapping duplicate/chat-number checks | `app.py` L3393-L3654; `templates/index.html` |
| Social account filters/status/import/export | `app.py` L263-L286, L1268-L1318, L1577-L1815; `templates/index.html` |
| Social inline update/delete/search/template | `app.py` L2799-L2945, L3331-L3387, L3689-L3715, L5442-L5493, L6903-L7039; `templates/index.html` |
| Total Numbers API/cards | `app.py` L6804-L6899; `templates/index.html` |
| Sheet generation/conversion | `app.py` L340-L922, L1819-L2046; `sheet_mapping_config.json`; `templates/index.html` |
| AML GUI sheet import/proxy | `app.py` L2417-L2782, L3309-L3327; `templates/index.html` |
| UPI validity background checks | `app.py` L2966-L3232; `templates/index.html` |
| Investment scam list/import/export | `app.py` L1319-L1363, L1410-L1571, L2149-L2267; `templates/index.html` |
| Investment duplicates/insights/bank data | `app.py` L3236-L3291, L3797-L4097; `templates/index.html` |
| Website Directory CRUD/import/export | `app.py` L4103-L4675, L4805-L4871; `templates/website_directory.html` |
| Scam++ website allotment | `app.py` L4679-L5438; `templates/scam_website_allotment.html` |
| Case report PDF generation | `app.py` L5518-L6634; `utils/pdf_generator.py`; `utils/aws_upload.py`; `utils/filename_generator.py`; `templates/case_report.html` |
| Bulk case regeneration jobs | `app.py` L6315-L6510, L6706-L6801; `templates/case_report.html` |
| Lunch break tracker | `app.py` L215-L218, L7046-L7263, L7403-L7417; `templates/lunch_break.html` |
| Dashboard user/access management | `app.py` L7352-L7890; `templates/dashboard_management.html` |
| GUI QC import/allotment/review/export | `app.py` L8006-L8771; `templates/qc_gui.html` |
| AWS/S3/CloudFront report uploads | `config.py`; `utils/aws_upload.py`; `app.py` L6530-L6699 |
| Deployment/runtime issues | `Dockerfile`; `Procfile`; `runtime.txt`; `requirements.txt`; `config.py` |
| Status migration `Block` → `Temporarily Block` | `migrate_block_status.py`; `app.py` L250-L260; social routes/templates |

## 7. Notes for Future AI Sessions

- Start with this file, then inspect only the listed route ranges/templates for the target feature.
- For frontend bugs, search within the relevant standalone template; there are no shared Jinja partials/base templates detected.
- For backend bugs, most feature code is grouped by line ranges in `app.py`; keep changes localized.
- Avoid broad reads of entire `app.py` unless a task crosses modules; it is a large monolith.
