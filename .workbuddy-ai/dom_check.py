import sys
sys.path.insert(0, ".")
from app import app
from flask import session
import re, os

os.makedirs(".workbuddy-ai", exist_ok=True)
client = app.test_client()

# Logged-in session simulation
with client.session_transaction() as sess:
    sess["user_id"] = "test-admin"
    sess["email"] = "admin@test.com"
    sess["display_name"] = "Admin Test"
    sess["role"] = "superadmin"
    sess["is_admin"] = True
    sess["allowed_pages"] = ["scraping","sheet","social","investment","insights","website_directory","allotment","case_report","qc","dashboard_management","lunch"]
    sess["allowed_departments"] = ["ALL"]
    sess["can_view_activity_log"] = True
    sess["logged_in"] = True

pages = ["social","investment","insights"]
for p in pages:
    r = client.get(f"/?page={p}")
    html = r.get_data(as_text=True)
    # find body class and container + page-content parent chain via regex-ish:
    # Simpler: write full html to file for Playwright
    with open(f".workbuddy-ai/test_{p}.html","w",encoding="utf-8") as f:
        f.write(html)
    print(p, "status", r.status_code, "bytes", len(html))
print("done")
