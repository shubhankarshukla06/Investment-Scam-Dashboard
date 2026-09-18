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

## 8. UI Layout Standardization (2026-09-12)

Task: make every page's layout/dimensions consistent with the **Data Scraping page** (`templates/index.html` → `scraping-page`), fix filter scrolling, and slim the filter footprint. Implemented across all standalone templates.

### Layout system (now uniform)
- **Container**: every page uses `width:100%; max-width:none; margin:0; box-sizing:border-box; padding:10px 15px 30px; padding-top:var(--fixed-header-space);` (lunch_break & dashboard_management keep their `height:100vh; overflow:hidden` bounded flex model, which already locks the filter in place).
- **Sticky filter**: all *data* pages now keep the filter fixed while only the data area scrolls:
  - `index.html` — `scraping-page` and `investment-page` `.filters` get `position:sticky; top:var(--fixed-header-space); z-index:900; flex-shrink:0`. The investment page was switched from `display:block` to the scraping flex-column model (the `.social-page.active{display:block}` rule now applies to Social Media only).
  - `website_directory.html`, `qc_gui.html` — `.filters` made sticky + slimmed.
  - `scam_website_allotment.html` — `.filters` already sticky; aligned its `top` to `var(--fixed-header-space)` and slimmed.
  - `dashboard_management.html` — `.filter-bar` was already sticky; `top:0` corrected to `var(--fixed-header-space)` and slimmed.
  - `lunch_break.html` — filter locked by the bounded `100vh` flex container (no `.filters` class); standardized container + slimmed inputs.
- **Social Media page** (`social-page` in `index.html`) was intentionally left **unchanged** (no sticky, `display:block`) per the requirement.
- **Data area** continues to scroll internally (`table-container`/`table-wrap` with `max-height` + `overflow-y:auto`); the window therefore does not normally scroll, and the sticky filter is a safety net if it ever does.

### Slimmed filter footprint (consistent everywhere)
- `.filters` padding `12px → 10px`, `margin-bottom 12px → 10px`.
- `.filters-header` margin-bottom `12px → 10px`.
- `.filters-grid` gap `12px → 10px`; `.filter-group` gap `5px → 4px`.
- Filter `input`/`select` heights `34px → 32px` (qc_gui & scam already ~30px, left as-is).
- Maintained readability/usability; not cramped. Social Media kept at original 12px/34px.

### Notes for future edits
- Sticky `top` must equal each page's `--fixed-header-space` (72px default; 92/100/136/156px on some pages via media queries) so the filter sits flush under the fixed header and never overlaps it.
- Keep changes localized to the page-prefixed selectors; do NOT introduce a shared base template (none exists and the app relies on standalone templates).
- `agent-browser` is unavailable on Windows in this environment, so scrolling/overlap behavior was verified by CSS reasoning, not live browser testing.

## 9. Header Flattening — Fixed Top Nav Bar (2026-09-12)

Task: remove the **rounded/carded container styling** from the Scam Intelligence GUI header and make it read as a flat, full-width **fixed top navigation bar** (not a floating card), on every application page.

### Architecture (unchanged wrapper, flattened inner card)
- Every GUI template already has an outer `.fixed-header` (`position:fixed; left:0; right:0; top:0`) that is already edge-to-edge of the viewport, plus an inner `.header` `<div>` that previously rendered as a white floating card (`background:white; border-radius:8px; box-shadow:0 1px 8px ...`).
- Fix: strip `background`, `border-radius`, and `box-shadow` from the inner `.header` so it blends into the full-width `.fixed-header` bar. Kept `padding:12px 15px`, `display:flex`, `justify-content:space-between`, `align-items:center`, and the existing `gap` so header content position is **unchanged**. The outer bar's own `border-bottom` + subtle `box-shadow` remain, giving the nav bar its clean bottom edge.

### Files changed (CSS only; no HTML, no component changes)
- `index.html`, `case_report.html`, `dashboard_management.html`, `qc_gui.html`, `lunch_break.html`, `scam_website_allotment.html`, `website_directory.html`.
- `login.html` has no `.header`/`.fixed-header` (standalone auth screen) — intentionally left untouched.

### Dark-mode handling
- Added two `!important` overrides right after the base `.header` rule in each file so the header stays flat/transparent in dark mode too:
  - `body.theme-dark .header { background:transparent !important; border:none !important; border-radius:0 !important; box-shadow:none !important; }`
  - `body.theme-dark .header:hover { transform:none !important; box-shadow:none !important; }`
- The grouped `body.theme-dark .header, body.theme-dark .card/.filters/.table-wrap/...` selectors were **left untouched** — only `.header` is neutralized by the override, so every other component keeps its dark styling. The header text color (light) is inherited from those groups and stays readable on the dark bar.

### Verification notes
- Confirmed via grep that no base `.header` rule contains `background`/`border-radius`/`box-shadow` anymore, and the two override lines are present in all 7 files.
- Responsive `.header { flex-direction:column; align-items:flex-start }` media-query overrides remain valid (alignment only).
- `agent-browser` unavailable on Windows → verified by CSS reasoning, not live browser.

## 10. Header Darkening, Username/Page Visibility, Dimension Standardization & Filter Compaction (2026-09-12)

Task (Task 3): (1) make the header background slightly darker and ensure the logged-in username + page name are clearly visible; (2) standardize header length/width/dimensions to the Social Media Accounts page (inside `index.html`) across all pages with consistent alignment/spacing; (3) make the filter section on all pages slightly more compact while keeping fields/buttons/labels visible/usable.

### Header background (slightly darker)
- `.fixed-header` background changed `#f5f7fa` → `#e9edf3` on all 7 GUI templates. The page `body` background stays `#f5f7fa`, so the header now reads as a distinctly darker fixed top nav bar.
- Pitfall handled: a naive global `#f5f7fa`→`#e9edf3` replace also darkened `body` backgrounds, the inline QC tracker filter bar (`qc_gui.html` `#qcTrackerFilterBar` div), and `.btn-outline-secondary:hover` (`case_report.html`). Those were reverted back to `#f5f7fa` so ONLY `.fixed-header` carries `#e9edf3`. Verified by grep: `#e9edf3` appears exclusively inside the `.fixed-header` rule in every file.

### Username & page-name visibility
- `.header-username` (logged-in user) and `.logo p` (page-name subtitle) color changed `#7f8c8d` → `#44525e` (darker, higher contrast) on all pages. The `<h1>` app name stays `#2c3e50`.

### Header dimension standardization (to Social Media page)
- The Social Media Accounts header lives inside `index.html` and already uses the same outer `.fixed-header` wrapper + inner `.header` flex row as every other page, so length/width were already identical edge-to-edge.
- The only real dimension inconsistency was the inner `.header` `gap`: `website_directory.html` and `qc_gui.html` used `gap:8px` while the rest (and the Social Media reference) used `16px`. Normalized the inner `.header` `gap` to `16px` across all 7 pages for consistent alignment/spacing.
- Padding (`12px 15px`), flex/justify/align, and `border-bottom` edge unchanged.

### Filter compaction (all pages, slightly more compact)
- `.filters`/`.filter-bar` padding `10px → 8px`; `margin-bottom` `10px → 8px` (social & lunch `12 → 8`).
- `.filters-grid`/`.filter-group` gaps `10→8`, `12→8`, `4→3`, `5→3`; small `margin-bottom:4px → 3px`.
- Filter `input`/`select` heights `32px → 30px`, `34px → 30px`.
- All fields, buttons, labels remain clearly visible/usable; readability preserved.

### Files changed
- `index.html`, `website_directory.html`, `scam_website_allotment.html`, `qc_gui.html`, `lunch_break.html`, `dashboard_management.html`, `case_report.html` (CSS only).
- `login.html` has no header (standalone auth) — untouched.

### Verification
- grep confirmed `#e9edf3` appears ONLY in the `.fixed-header` rule in all 7 files; `body` is `#f5f7fa`; `.header-username`/`.logo p` are `#44525e`; inner `.header` `gap:16px` uniform (note: ripgrep is line-based, so multi-line `.header` rules in `case_report`/`dashboard_management` still contain `gap:16px` — confirmed by direct read); compacted filter values (`30px` inputs, `8px/3px` padding/gaps) present.
- `agent-browser` unavailable on Windows → verified by CSS reasoning + targeted grep, not live browser.

## 11. Header-to-Filter Spacing Standardization (2026-09-12)

Task (Task 4): standardize the header-to-filter spacing across **all pages** to match the **Data Scraping page** (reference: `templates/index.html` → `scraping-page`). The Website Case Assignment page (`scam_website_allotment.html`) was visibly looser than the reference. Header design/styling was already uniform from Task 2+3.

### Root cause
The `--fixed-header-space` CSS custom property (drives `padding-top` on `.container`) and the JS formula that updates it at runtime were inconsistent across templates:

| Template | CSS `:root` value (was) | JS formula (was) | Container `padding-top` |
|---|---|---|---|
| `index.html` (reference) | `72px` | `h + 14` | `var(--fixed-header-space)` |
| `scam_website_allotment.html` | `92px` | `h + 6` | `calc(var(--fixed-header-space) - 2px)` |
| `qc_gui.html` | `92px` | `h + 14` | `var(--fixed-header-space)` |
| `dashboard_management.html` | `100px` | `h + 14` | `var(--fixed-header-space)` |
| `lunch_break.html` | `72px` | `h + 14` | `var(--fixed-header-space)` |
| `website_directory.html` | `72px` | `h + 14` | `var(--fixed-header-space)` |

### Fix applied
- All seven templates now declare `--fixed-header-space: 72px` as the base `:root` value (matches Data Scraping).
- `scam_website_allotment.html` JS formula changed from `h + 6` to `h + 14` (matches Data Scraping's non-compact formula).
- `scam_website_allotment.html` `.container` `padding-top` changed from `calc(var(--fixed-header-space) - 2px)` to `var(--fixed-header-space)` (matches every other page).
- Responsive media-query overrides kept as-is:
  - `qc_gui.html` `156px`, `dashboard_management.html` `136px`, `website_directory.html` `136px` — these activate when the header switches to a column layout on smaller screens and the header itself becomes taller; the larger spacing ensures content clears the taller header.
- Header design itself (`.fixed-header` background `#e9edf3`, `.header` flex layout, `gap:16px`, `.header-username`/`.logo p` color `#44525e`) was already uniform from Tasks 2 and 3 — no changes needed for the "standardize header design" requirement.

### Files changed
- `scam_website_allotment.html` — CSS `:root` (line 19), `.container` `padding-top` (line 43), JS formula (line 1203).
- `qc_gui.html` — CSS `:root` (line 46).
- `dashboard_management.html` — CSS `:root` (line 36).

### Verification
- grep confirmed all seven `:root` declarations of `--fixed-header-space` are now `72px` (the three responsive overrides at `136px`/`156px` remain for mobile/tablet, which is correct).
- `scam_website_allotment.html` container uses `padding-top: var(--fixed-header-space)` (no more `calc(-2px)`).
- `scam_website_allotment.html` JS uses `Math.ceil(h + 14)`, identical to the Data Scraping reference.
- `agent-browser` unavailable on Windows → verified by CSS reasoning + grep, not live browser.

## 12. Header Design Standardization — Scam Intelligence GUI Branding (2026-09-12)

Task (Task 5): apply the same **Scam Intelligence GUI** header design used on the Data Scraping and Social Media Accounts pages to **all other pages**. Maintain exact same font, spacing, gaps, alignment, sizing, and overall header styling.

### Root cause
After Tasks 2–4, the CSS (`.fixed-header` background `#e9edf3`, `.header` flex layout, `gap:16px`, `.header-username`/`.logo p` color `#44525e`, h1 font size 18px/color `#2c3e50`) was already uniform across all 7 GUI templates. But the **h1 text content** was inconsistent on two pages:

| Template | h1 text (was) | Issue |
|---|---|---|
| `index.html` | `Scam Intelligence GUI` ✓ | reference |
| `lunch_break.html` | `Scam Intelligence GUI` ✓ | already correct |
| `qc_gui.html` | `Scam Intelligence GUI` ✓ | already correct |
| `website_directory.html` | `Scam Intelligence GUI` ✓ | already correct |
| `scam_website_allotment.html` | `Scam Intelligence GUI` ✓ | already correct |
| `dashboard_management.html` | `<i class="fas fa-user-cog">User & Access Management</i>` | page-specific icon + page name as h1 |
| `case_report.html` | `Case Report Generator` (inside a wrapper div with a `fa-file-pdf` header icon) | page name as h1 + extra icon wrapper |

### Fix applied
- **`dashboard_management.html`**: removed the `fa-user-cog` icon and the page-specific "User & Access Management" text from the h1; set h1 to `Scam Intelligence GUI`. Kept the existing `.logo-title-row` wrapper (already styled correctly in this file's CSS).
- **`case_report.html`**: removed the outer wrapper div and the `fa-file-pdf` header icon; changed h1 from `Case Report Generator` to `Scam Intelligence GUI`. Restructured to use the same inline-style nested flex divs pattern as `lunch_break.html`/`qc_gui.html`/`website_directory.html` (three nested divs with `display:flex;align-items:center;gap:10px` wrapping the h1+username row, then the `<p>` subtitle below) so the layout renders identically.

### Files changed
- `dashboard_management.html` — header `.logo` block (lines 947–959).
- `case_report.html` — header `.logo` block (lines 1000–1016).

### Verification
- grep confirmed all 7 GUI templates now have `<h1>Scam Intelligence GUI</h1>` (or `<h1 style="white-space:nowrap;">Scam Intelligence GUI</h1>` for pages that already had the nowrap attribute).
- `case_report.html` header structure now matches the inline-flex nested-div pattern used by `lunch_break.html`/`qc_gui.html`/`website_directory.html`, so the rendered output is identical (h1 + username on one line, subtitle below).
- `dashboard_management.html` retained its `.logo-title-row` wrapper pattern (matches `index.html`/`scam_website_allotment.html`) — both patterns produce the same visual result; keeping the existing structure avoids unnecessary change.
- `login.html` was already correct (standalone auth screen with `<h1>Scam Intelligence GUI</h1>`) — untouched.
- `agent-browser` unavailable on Windows → verified by grep + HTML structure comparison, not live browser.

## 13. Header White + Button Lightening (Task 6, 2026-09-12)

Task (Task 6): across **all pages**, (1) keep the Scam Intelligence GUI header **white**, (2) ensure all fonts display clearly and consistently, (3) **slightly lighten** the colors of all buttons while maintaining good readability and visual consistency.

### Changes applied (all 8 templates: 7 GUI + login.html)
- **Header white**: the only `#e9edf3` token (the `.fixed-header` background, introduced in Task 3) was replaced with `#ffffff` across all 7 GUI templates. `login.html` had no `#e9edf3` (already white). grep confirms `#e9edf3` now appears **0 times** anywhere in `templates/`.
  - Dark-mode header overrides (`body.theme-dark .fixed-header { background:#202938 }` / `rgba(15,23,42,.82)`) were left untouched — they use separate hexes, so dark mode is unaffected.
- **Buttons lightened** (~18% toward white) via a single token-replace pass. Full PALETTE map:

  | Original | Lightened | Used by |
  |---|---|---|
  | `#3498db` / `#2980b9` | `#5aa9e1` | `.btn-primary`, header buttons (UPI Checker `#8e44ad`→`#a266bc`, User's Activity Log `#6366f1`→`#7f82f4`), login gradient |
  | `#2ecc71` / `#27ae60` | `#54d58b` / `#4ebd7d` | `.btn-success` (+ hover) |
  | `#1abc9c` / `#16a085` / `#10b981` | `#43c8ae` / `#40b19b` / `#3bc698` | teal accents |
  | `#f39c12` / `#e67e22` | `#f5ae3d` / `#eb954a` | `.btn-warning` (+ hover) |
  | `#e74c3c` / `#ef4444` | `#eb6c5f` / `#f26666` | `.btn-danger` |
  | `#8e44ad` | `#a266bc` | `.btn-purple` |
  | `#6366f1` / `#5c6bc0` | `#7f82f4` / `#7986cb` | indigo accents |
  | `#95a5a6` | `#a8b5b6` | neutral/clear buttons (`.clear-btn`) |
  | `#6c757d` / `#495057` | `#878e94` / `#6a7075` | `.btn-refresh` gradient |

  - Lightened hexes land in **both** CSS classes (`.btn-*`) and inline `style="background:#..."` button styles, so the change is uniform. grep confirms **0 occurrences** of any original saturated accent hex remain in `templates/`.
- **Fonts / readability**: header text colors were intentionally **not** in the palette map, so they stay as before — `.header-username` + `.logo p` = `#44525e`, h1 `Scam Intelligence GUI` = `#2c3e50` (18px). On the now-white `.fixed-header` these remain clearly legible.
- **Hover states**: kept their existing pattern — `.btn-primary:hover` = base (`#5aa9e1`), while `.btn-success/.btn-warning/.btn-danger/.btn-purple:hover` keep a slightly deeper shade for affordance.

### Deliberately excluded from the lightening
- **Dark-mode overrides** (`#2563EB`, `#16A34A`, `#F59E0B`, `#DC2626`, `#202938`) — separate palette; left as-is so dark mode contrast is preserved.
- **Already-light neutrals** used as surfaces/tints (`#f8f9fa`, `#f0fff4`, `#e8f8f0`, `#f3e5f5`, `#ecf0f1` for `.btn-outline`, `#eafaf1` flash, `#e1e5eb` borders) — not accent buttons, so untouched.

### Helper script
- `lighten_header_buttons.py` created to apply the header→white + palette swap reliably under OneDrive file-locking (one pass, not per-file edits). **Deleted after verification.**

### Verification
- grep: `#e9edf3` → 0 matches (header fully white); all original saturated accent hexes → 0 matches (lightening uniform); lightened hexes present in all 7 GUI templates (index 53, website_directory 30, qc_gui 12, scam_website_allotment 10, dashboard_management 7, lunch_break 9, case_report 5) + login gradient.
- Header text colors `#44525e` / `#2c3e50` confirmed unchanged on white → good contrast.
- **Readability caveat**: white label text on the lightest buttons (notably `.btn-success` `#54d58b`, contrast ≈1.9:1; `.btn-primary` `#5aa9e1` ≈2.6:1) is softer than before. These matched the prior saturated design (which also used white text), but if stronger contrast is wanted, switch button label text to a dark tone (e.g. `#1f2d3d`) instead of white. Flagged, not auto-changed (user only asked to lighten buttons).
- `agent-browser` unavailable on Windows → verified by grep/contrast reasoning, not live browser.

### REVERTED (Task 6 follow-up, 2026-09-12)
User asked to **revert only the button-lightening** changes — restore original saturated button/accent colors while keeping the header white (`#ffffff`) and font clarity (`#44525e`/`#2c3e50`) intact.

- Ran `revert_button_lightening.py` — reverse token-replace of the PALETTE map (all 8 templates). Reverted counts: index 327, website_directory 151, qc_gui 56, lunch_break 45, case_report 54, dashboard_management 36, scam_website_allotment 30, login 11.
- grep confirms: **0** lightened hexes remain; original saturated accents (`#3498db`, `#2ecc71`, `#f39c12`, `#e74c3c`, `#8e44ad`, `#6366f1`, `#95a5a6`, etc.) restored.
- Header: `#ffffff` preserved on all 7 `.fixed-header` rules; `#e9edf3` still 0 → the header-white change was NOT reverted (only buttons).
- Font colors and dark-mode/neutral hexes untouched.
- Helper `revert_button_lightening.py` deleted after verification.

## 14. Filter Section Flattening + Full-Width (Task 7, 2026-09-12)

Task (Task 7): across **all pages**, (1) remove the rounded/card-style container styling from the filter section, (2) make it full-width, extending edge-to-edge across the viewport, (3) keep all content/buttons/dropdowns/icons/text/spacing/functionality unchanged, (4) don't modify any other part of the UI.

### Files changed
All 7 GUI templates + one inline filter bar in `index.html`.

- `index.html` — 3 class filters (`.scraping-page .filters`, `.investment-page .filters`, `.social-page .filters`) + 1 inline sticky filter bar on the Insights page (line ~2537).
- `dashboard_management.html` — `.filter-bar` (lines 227–241).
- `qc_gui.html` — `.filters` (line 52).
- `scam_website_allotment.html` — `.filters` (line 46).
- `website_directory.html` — `.filters` (line 55).
- `lunch_break.html` — `.filter-bar` (line 51).
- `case_report.html` / `login.html` — **no filter section** → no change.

### Changes applied
- **Removed card styling** (background, border-radius, box-shadow) from every `.filters`/`.filter-bar` rule and from the inline Insights filter bar. No background/border/radius/shadow remains on any filter.
- **Full-width edge-to-edge**: used negative horizontal margins equal to the sum of ancestor left-padding, plus internal horizontal padding equal to the same offset so the filter *content* stays aligned with the page/table (not flush to the screen edge). Vertical padding kept at `8px`; control gaps / grid-gap / spacing untouched.
  - Offset **P = 15px** (container horizontal padding): dashboard_management `.filter-bar`, qc_gui `.filters`, scam_website_allotment `.filters`, website_directory `.filters`, lunch_break `.filter-bar`, index `.scraping-page .filters`, index inline Insights filter.
  - Offset **P = 30px**: index `.investment-page .filters` and `.social-page .filters` — these pages add their own `padding: 0 15px 30px` on top of the 15px container padding, so total inset = 30px.
  - Pattern: `margin-left:-Ppx; margin-right:-Ppx; padding: 8px Ppx;`.
  - For index class filters, **removed `width:100%`** so the block fills symmetrically to the viewport edges under the negative margins (`width:100%` + negative margins would make the right side asymmetric).
  - No `100vw` used → no horizontal scrollbar introduced (the 100vw scrollbar pitfall is avoided).
- **Dark mode**: added an `!important` override resetting `.filters`/`.filter-bar` to `background:transparent; border:none; border-radius:0; box-shadow:none` in all 7 files, so the grouped dark card selectors (which still set `background:#2b3546/#1E293B; border; border-radius:12px; box-shadow`) no longer re-apply a card to the filter. The light text color from those grouped rules is intentionally preserved.
- **Untouched**: all other UI — `.table-container`/`.table-wrap` cards, `.pagination`, `.stat-card`, `.modal-box`, header, fonts, button colors, spacing between controls. The earlier "social-page left unchanged" convention from Task 1 is superseded by the explicit "across all pages" instruction here.

### Verification
- grep: 0 `.filters`/`.filter-bar` rules retain `background`/`border-radius:8px`/`box-shadow`; 0 inline card filter remains; full-bleed margins present in all 8 filter locations (7 class + 1 inline), correct P value per page.
- Box-model reasoning confirms symmetric edge-to-edge with content aligned to page; no 100vw → no horizontal scroll.
- `agent-browser` unavailable on Windows → verified by grep + box-model reasoning, not live browser.

### Gotcha (tooling)
- When 2+ `Edit` calls target the **same file** in one message, the 2nd+ are applied against a stale snapshot and **silently fail** (observed: index.html investment-page + inline Insights edits dropped in batch). Fix: apply same-file edits one at a time (or re-run and re-verify). Re-verified after fixing.

## 15. Pagination Section Flattening + Full-Width (Task 8, 2026-09-12)

Task (Task 8): across **all pages**, (1) remove the rounded/carded container styling from the bottom pagination section (Previous / Page / Next buttons), (2) make it full-width, edge-to-edge across the viewport, (3) keep all buttons, text, icons, alignment, and functionality unchanged, (4) do not modify pagination behavior or page navigation logic.

### Files changed
4 templates with pagination: `index.html`, `qc_gui.html`, `scam_website_allotment.html`, `website_directory.html`.
`dashboard_management.html`, `lunch_break.html`, `case_report.html`, `login.html` — no pagination section → no change.

### Changes applied
- **Removed card styling** (background, border-radius, box-shadow) from every `.pagination` rule. No background/border/radius/shadow remains on any pagination container.
- **Full-width edge-to-edge**: used the same negative-margin technique as Task 7 (`margin-left:-Ppx; margin-right:-Ppx; padding: ... Ppx;`), with P matching the filter offset because pagination is a sibling of filters inside the same page-content.
  - **P = 15px**: `index.html` scraping-page, `qc_gui.html`, `scam_website_allotment.html`, `website_directory.html`.
  - **P = 30px**: `index.html` investment-page and social-page (these page-content containers add `padding: 0 15px 30px` on top of the 15px outer container).
  - For `index.html`, the single global `.pagination` rule was split into three page-scoped rules (`.scraping-page .pagination`, `.investment-page .pagination`, `.social-page .pagination`) so each gets the correct offset.
  - `width:100%` removed where present so the block fills symmetrically under the negative margins (avoids asymmetric right edge).
  - No `100vw` used → no horizontal scrollbar.
- **Grouped border-radius cleanup**: removed `.pagination` from light-mode grouped `border-radius:12px` selectors in `index.html`, `scam_website_allotment.html`, and `website_directory.html`.
- **Dark-mode cleanup**:
  - `index.html`: standalone dark `.pagination` card rule (`background:#1E293B; border; border-radius; box-shadow`) replaced with `background:transparent !important; border:none !important; border-radius:0 !important; box-shadow:none !important`.
  - `qc_gui.html`: removed `.pagination` from the grouped dark card selector (`body.theme-dark .header, ... .pagination, .modal-box`). The base `.pagination` is already flat, so dark mode stays flat.
  - `scam_website_allotment.html`: removed `.pagination` from the grouped dark card selector (`body.theme-dark .header, ... .modal-content, .pagination`). Base `.pagination` is already flat.
  - `website_directory.html`: three dark `.pagination` references found — (a) grouped dark #1 (`background:#2b3546`), (b) standalone dark (`background:#2b3546`), (c) grouped dark #2 (`background:#1E293B; border; border-radius; box-shadow`). Removed `.pagination` from both grouped selectors; replaced the standalone with the `!important` transparent override.
- **Untouched**: all pagination buttons (`.pagination-btn`/`.pag-btn`), text (`.pagination-info`/`.pag-info`), icons (`fa-chevron-left`/`fa-chevron-right`), alignment (`justify-content:center`), disabled states, and `goToPage()` logic. `.table-container`/`.table-wrap` cards, filter bars, stat cards, modals, header all unchanged.

### Verification
- grep: 0 `.pagination` rules retain `background` + `border-radius` card styling; 0 grouped dark/light selectors still include `.pagination` (except harmless `html.theme-switching .pagination` transition rule in qc_gui).
- Full-bleed margins present in all 4 files with correct P value per page.
- Box-model reasoning confirms symmetric edge-to-edge with content aligned to page; no 100vw → no horizontal scroll.
- `agent-browser` unavailable on Windows → verified by grep + box-model reasoning, not live browser.
