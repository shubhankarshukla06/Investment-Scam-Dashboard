import sys
sys.path.insert(0, ".")
from app import app

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

pages = {
    "scraping": "/?page=scraping",
    "sheet": "/?page=sheet",
    "social": "/?page=social",
    "investment": "/?page=investment",
    "insights": "/?page=insights",
    "qc": "/qc-gui",
    "website_directory": "/website-directory",
    "allotment": "/scam-website-allotment",
    "case_report": "/case-report",
    "dashboard_management": "/dashboard-management",
    "lunch": "/lunch-break",
}
for name, url in pages.items():
    try:
        r = client.get(url)
        html = r.get_data(as_text=True)
        with open(f".workbuddy-ai/audit_{name}.html", "w", encoding="utf-8") as f:
            f.write(html)
        print(name, r.status_code, len(html))
    except Exception as e:
        print(name, "ERROR", e)
print("done")
