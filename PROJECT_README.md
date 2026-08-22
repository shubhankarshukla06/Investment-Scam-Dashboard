# Testing_Code — Scam Intelligence Unit Dashboard

A Flask-based internal web application used by a Scam Intelligence Unit (cyber-crime reporting workflow).
It combines record tracking (scraping, social media, investment scams, websites), Excel sheet
generation/import, automated browser checks (UPI / AML), PDF case-report generation, cloud storage
(AWS S3 + CloudFront), and a Supabase database backend — all behind a session-based login with
role/department permissions.

---

## Table of Contents

- [Project Structure](#project-structure)
- [Tech Stack](#tech-stack)
- [Configuration (Environment Variables)](#configuration-environment-variables)
- [Core Modules](#core-modules)
- [Features / Route Groups](#features--route-groups)
- [Templates (Pages)](#templates-pages)
- [Data Files](#data-files)
- [Running Locally](#running-locally)
- [Deployment](#deployment)

---

## Project Structure

```
Testing_Code/
├── app.py                      # Main Flask application (~8,700 lines, all routes & business logic)
├── config.py                   # Centralised environment-variable loading & startup validation
├── requirements.txt            # Python dependencies (pinned)
├── runtime.txt                 # Python version pin for Heroku (3.11.9)
├── Procfile                    # Heroku launch command (gunicorn)
├── Dockerfile                  # Container build (python:3.11-slim + gunicorn on :8080)
├── sheet_mapping_config.json   # Column mappings & fixed values for sheet generation (UPI / Investment)
├── .env                        # Local secrets (NOT committed; listed in .gitignore)
├── .gitignore
│
├── utils/                      # Helper modules
│   ├── aws_upload.py           # Upload/delete PDFs on AWS S3, returns CloudFront URL
│   ├── filename_generator.py   # Unique PDF filename generator (npci-<rand>_<uuid>--<host>.pdf)
│   └── pdf_generator.py        # ReportLab-based "Case Report" PDF builder (2 screenshots per page)
│
├── templates/                  # Jinja2 HTML pages
│   ├── index.html              # Main dashboard / working console
│   ├── login.html              # Login page
│   ├── case_report.html        # Case report generation & listing
│   ├── dashboard_management.html  # Admin: users, GUI status, investment-scam users
│   ├── lunch_break.html        # Lunch-break tracking
│   ├── qc_gui.html             # QC (quality-check) worklist GUI
│   ├── scam_website_allotment.html  # Allotting scam websites to analysts
│   └── website_directory.html  # Website directory management
│
├── static/
│   └── favicon.png             # Site icon
│
├── excel_data/                 # Reference data (read at runtime)
│   ├── bank_name.xlsx          # Bank name lookup
│   └── ifsc_mapping.xlsx       # IFSC → bank/branch mapping
│
├── uploads/                    # Runtime upload destination (user files)
├── .venv/                      # Local Python virtual environment (not committed)
└── .vscode/                    # Editor settings
```

---

## Tech Stack

| Layer            | Technology                                                        |
|------------------|-------------------------------------------------------------------|
| Web framework    | Flask 3.0 (Jinja2 templates, session auth, 8-hour sessions)       |
| Database         | Supabase (PostgreSQL via `supabase-py` client)                    |
| File storage     | AWS S3 + CloudFront CDN (case-report PDFs)                        |
| Data processing  | pandas, openpyxl, xlrd (Excel import/export)                      |
| PDF              | ReportLab (generation), pypdf / PyMuPDF (page counts, extraction) |
| Browser automation| Selenium + webdriver-manager (UPI/AML checks), Playwright         |
| OCR              | pytesseract, Pillow, ddddocr                                      |
| HTTP             | requests, httpx                                                   |
| Server           | gunicorn (1 worker, 600 s timeout)                                |
| Python           | 3.11                                                              |

---

## Configuration (Environment Variables)

All variables are loaded once in `config.py` (`Config` class). `Config.validate()` raises at
startup if any required variable is missing.

| Variable                | Required | Purpose                                        |
|-------------------------|----------|------------------------------------------------|
| `SECRET_KEY`            | yes*     | Flask session signing                          |
| `AWS_ACCESS_KEY_ID`     | yes      | AWS credentials for S3                         |
| `AWS_SECRET_ACCESS_KEY` | yes      | AWS credentials for S3                         |
| `AWS_REGION`            | no       | Default `ap-south-1`                           |
| `S3_BUCKET_NAME`        | yes      | Target bucket for case-report PDFs             |
| `CLOUDFRONT_DOMAIN`     | yes      | CDN domain used to build public PDF URLs       |
| `SUPABASE_URL`          | yes      | Supabase project URL                           |
| `SUPABASE_KEY`          | yes      | Supabase service key                           |
| `FLASK_DEBUG`           | no       | `"true"`/`"false"` debug flag                  |
| `AML_HEADLESS`          | no       | Headless flag for AML browser automation       |

Other constants: Supabase table `reports`, S3 key prefix `case-reports/`, max upload 25 MB.

---

## Core Modules

### `app.py`
Single-file Flask app containing every route and helper (~110 routes). Notable internals:

- **Auth** — `/login`, `/logout`, session-backed login with a hardcoded demo admin user
  (`test123@gmail.com` / `test123`, role `admin`). Users carry `allowed_pages`,
  `allowed_departments` (ITC, AML, Investment Scam, dashboard_management, Infringement, Chargeback)
  and activity logging.
- **Background jobs** — `ThreadPoolExecutor` used for UPI checks, bulk case generation and bulk
  regeneration; each job gets a `job_id` with status/stop/download endpoints.
- **Excel engine** — import, preview, header mapping (driven by `sheet_mapping_config.json`),
  duplicate checking and templated exports.

### `config.py`
Centralised `Config` class; all other modules import settings from here instead of calling
`os.getenv()` directly. `Config.validate()` enforces required variables at startup.

### `utils/aws_upload.py`
- `upload_pdf(local_file_path)` — uploads a PDF to `s3://<bucket>/case-reports/<filename>` with
  inline `Content-Disposition`, returns a CloudFront URL.
- `delete_from_s3(filename)` — removes the object by bare filename.

### `utils/filename_generator.py`
`generate_filename(source_url)` — produces collision-safe names like
`npci-1782707868_698c756c_6a41f69c98915--instagram.com.pdf`
(10 random digits + 8-char UUID + 14-char UUID + hostname of the source URL).

### `utils/pdf_generator.py`
`generate_pdf(source_url, image_paths, output_path)` — builds an A4 "Case Report" PDF with
ReportLab: IST timestamp top-right, source URL in red at top/bottom, two screenshots per page;
returns the page count (via pypdf).

---

## Features / Route Groups

| Group                       | Representative routes                                                | What it does |
|-----------------------------|----------------------------------------------------------------------|--------------|
| **Auth & sessions**         | `/login`, `/logout`, `/get-session-info`                              | Session login (8 h lifetime), department/page permissions |
| **User activity log**       | `/get-user-activity-log`, `/export-user-activity-log`                 | View/export per-user activity |
| **Scraping tracker**        | `/tracker-stats`, `/insert-scraping-record`, `/check-scraping-duplicates`, `/my-scraping-count` | Manual scraping record CRUD + stats |
| **Social media records**    | `/social-import`, `/social-export`, `/social-search-ajax`, `/save-social-field`, `/insert-social-record` | Import/export/search/update social-platform scam records; permanent-block account list |
| **Investment scam tracker** | `/investment-tracker-stats`, `/investment-import`/`-export`, `/investment-insights-data`, `/investment-bank-data` | Investment-scam records, bank insights, last-date tracking |
| **Sheet generation engine** | `/get-sheet-headers`, `/download-template`, `/preview-sheet`, `/generate-sheet`, `/sheet-import-to-aml-gui` | Map Excel columns via `sheet_mapping_config.json`, generate UPI (AML) / Investment sheets |
| **Excel reference data**    | `/get-excel-headers`, `/get-ifsc-headers`, `/reload-data`, `/upload`, `/export`, `/parse-raw-file` | Upload raw files, parse (incl. OCR via PyMuPDF/pytesseract), export |
| **UPI check (AML)**         | `/start-upi-check`, `/upi-check-status/<job_id>`, `/stop-upi-check/<job_id>`, `/export-upi-check/<job_id>` | Background Selenium job that checks UPI IDs (bank/wallet detection) with live progress |
| **Case reports**            | `/case-report`, `/generate-case-report`, `/case-reports-list`, `/delete-case-report/<id>`, `/bulk-generate-cases`, `/bulk-regenerate-cases` | Generate PDF case reports (screenshots + URL), bulk jobs, upload to S3/CloudFront |
| **Website directory**       | `/website-directory`, `-import`, `-export`, `-insert/-update/-delete(-bulk)`, `-search-api`, `-template`, `-tracker-stats`, `-operable/-inoperable` | Full CRUD directory of scam/legit websites with per-user summaries |
| **Scam website allotment**  | `/scam-website-allotment`, `-users`, `-allot`, `-reassign`, `-update-remark`, `-counts`, `-check-target`, `-delete-row`, `-export`, `-bulk-match` | Allot/reassign websites to analysts, remarks, counts, exports |
| **QC GUI**                  | `/qc-gui`, `-import`, `-users`, `-allotment`, `-export`, `-template`, `-get-record`, `-update`, `-delete`, `-tracker-stats` | Quality-check worklist with allotment to QC users |
| **Lunch break**             | `/lunch-break`, `/lunch-break/insert|update|delete|export`            | Track lunch-break entries |
| **Dashboard management**    | `/dashboard-management` + `/api/users`, `/api/gui-status`, `/api/investment-scam-users` (GET/POST/PUT/DELETE, toggle-active) | Admin console for user accounts and GUI status |
| **Misc**                    | `/health`, `/api/total-numbers(/stats)`, `/get-number-type-counts`, `/getDepartmentData`, `/summary-source-from-gui` | Health check, number analytics, department data |

---

## Templates (Pages)

| Template                     | Purpose |
|------------------------------|---------|
| `login.html`                 | Login screen |
| `index.html`                 | Main working console (scraping / social / investment / sheet tabs) |
| `case_report.html`           | Case-report generation & management |
| `website_directory.html`     | Website directory GUI |
| `scam_website_allotment.html`| Website allotment GUI |
| `qc_gui.html`                | QC worklist GUI |
| `lunch_break.html`           | Lunch-break tracker |
| `dashboard_management.html`  | Admin dashboard management |

---

## Data Files

- `excel_data/bank_name.xlsx` — bank name lookup used during sheet generation.
- `excel_data/ifsc_mapping.xlsx` — IFSC code → bank/branch mapping.
- `sheet_mapping_config.json` — declarative sheet definitions:
  - `upi` → "UPI (AML)": required headers (`UPI`, `Screenshot`, `Website URL`, `Payment Gateway URL`),
    column mapping to DB fields, fixed columns (e.g. `feature_type: "BS Money Laundering"`),
    and conditional columns (e.g. `upi_bank_account_wallet` decided by a `upi_check` condition).
  - `investment` → "Investment Scam" sheet with bank-account and contact fields.

---

## Running Locally

```bash
# 1. Create/activate a virtual environment (Python 3.11)
python -m venv .venv
.venv\Scripts\activate            # Windows

# 2. Install dependencies
pip install -r requirements.txt

# 3. Create a .env file with the required variables (see table above)

# 4. Run the dev server
flask --app app run --debug       # or: python app.py
```

> Note: browser-automation features (UPI check / AML) need Chrome installed; Selenium
> fetches the matching driver via `webdriver-manager`. OCR features need Tesseract installed
> on the system for `pytesseract`.

Demo login (hardcoded in `app.py`): `test123@gmail.com` / `test123`.

---

## Deployment

- **Docker** — `Dockerfile` builds `python:3.11-slim`, installs system libs, runs
  `gunicorn -b 0.0.0.0:8080 --workers 1 --timeout 600 app:app`.
- **Heroku** — `Procfile` (`web: gunicorn -b 0.0.0.0:$PORT app:app --workers 1 --timeout 600`)
  plus `runtime.txt` (`python-3.11.9`).
- **Health endpoint** — `GET /health` for uptime monitoring.

Set all required environment variables (AWS, Supabase, `SECRET_KEY`) on the host platform;
`Config.validate()` fails fast at boot if anything is missing.
