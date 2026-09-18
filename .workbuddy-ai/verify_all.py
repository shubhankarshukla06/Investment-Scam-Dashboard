import sys
sys.path.insert(0, ".")
from app import app
import os
os.makedirs(".workbuddy-ai", exist_ok=True)
client = app.test_client()
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

# Render scraping and sheet to verify they still work
for p in ["scraping", "sheet"]:
    r = client.get(f"/?page={p}")
    html = r.get_data(as_text=True)
    with open(f".workbuddy-ai/test_{p}.html", "w", encoding="utf-8") as f:
        f.write(html)
    print(p, "status", r.status_code, "bytes", len(html))
print("done")