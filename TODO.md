# GUI QC Page — Implementation Steps

## Task

Add a new "GUI QC" standalone page in `Testing_Code/templates/` backed by the `qc_table` Postgres/Supabase table. Requires:

- Header with **Import** and **Export** buttons + a page-selector dropdown
- **Filter section**
- **Data table** (subset of columns) with pagination
- **Edit modal** for updating records (QC remarks, approved_by, etc.)

## Confirmed requirements

- `id` auto-assigned by Supabase (`generated always as identity`) — not set on import
- Display a **subset** of columns (QC-focused)
- Edit via **modal**
- Main `supabase` client (SUPABASE_URL/SUPABASE_KEY env vars) for `qc_table`

## Steps

- [x] 1. Create `templates/qc_gui.html` — NEW standalone template
  - Fixed header: logo + username, page-selector dropdown, Refresh, Theme toggle, Logout, **Import**, **Export**
  - Filter section: Search, Scam Type, Search For, UPI/Bank/Wallet, Input User, Date Range, Approved By, Clear Filters
  - Data table: QC subset columns with copy buttons, URL links, screenshot links, badges, edit action
  - Pagination
  - Edit modal (sync via modal single entry)

- [ ] 2. Edit `app.py` — add backend routes:
  - [ ] `GET /qc-gui` — page render with filters + pagination
  - [ ] `POST /qc-gui-import` — import CSV/Excel into `qc_table`
  - [ ] `GET /qc-gui-export` — export filtered data (CSV)
  - [ ] `GET /qc-gui-template` — download import template
  - [ ] `GET /qc-gui-get-record` — fetch single record
  - [ ] `POST /qc-gui-update` — update record (edit modal save)
  - [ ] `POST /qc-gui-delete` — delete record
  - [ ] `GET /qc-gui-tracker-stats` — optional stats mini-panel

- [ ] 3. Edit `templates/website_directory.html` — add `qc` option to page-selector dropdown + navigation handler

- [ ] 4. Edit `templates/index.html` — add `qc` option to page-selector dropdown + navigation handler

## Verification

- [ ] Python syntax check on app.py
- [ ] Run Flask app and open /qc-gui page
