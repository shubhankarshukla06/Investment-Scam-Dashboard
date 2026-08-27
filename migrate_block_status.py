"""
One-time migration: rename social-account status 'Block' -> 'Temporarily Block'.

The UI/backend rename (app.py PLATFORM_ACCOUNT_STATUS / PLATFORM_STATUS_OPTIONS and
templates/index.html) only affects NEW inserts/updates. Existing rows in the
`social_media_accounts` table that still hold the old value 'Block' must be updated
so filters, dropdowns, and the status pill colours line up.

Run ONCE from the project root (same environment where SUPABASE_URL / SUPABASE_KEY
are available, i.e. the same place you run `python app.py`):

    python migrate_block_status.py

The script is idempotent: running it again finds 0 rows to change.
"""

import os
import sys

try:
    from supabase import create_client
except ImportError:
    print("ERROR: supabase-py is not installed. Install it with: pip install supabase")
    sys.exit(1)

OLD_STATUS = "Block"
NEW_STATUS = "Temporarily Block"
TABLE = "social_media_accounts"  # must match the table used in app.py


def main():
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        print("ERROR: SUPABASE_URL and SUPABASE_KEY environment variables must be set.")
        sys.exit(1)

    supabase = create_client(url, key)

    # 1) Count rows that still use the old value
    count_resp = (
        supabase.table(TABLE).select("*", count="exact").eq("account_status", OLD_STATUS).execute()
    )
    total = count_resp.count or 0
    print(f"Found {total} row(s) with account_status == '{OLD_STATUS}'.")

    if total == 0:
        print("Nothing to migrate. Existing data is already up to date.")
        sys.exit(0)

    # 2) Update them all in a single server-side update
    upd = supabase.table(TABLE).update({"account_status": NEW_STATUS}).eq("account_status", OLD_STATUS).execute()
    updated = len(upd.data or [])
    print(f"Update call returned {updated} row(s).")

    # 3) Verify
    remaining = (
        supabase.table(TABLE).select("*", count="exact").eq("account_status", OLD_STATUS).execute().count or 0
    )
    now_new = (
        supabase.table(TABLE).select("*", count="exact").eq("account_status", NEW_STATUS).execute().count or 0
    )
    print(f"Remaining rows with '{OLD_STATUS}': {remaining}")
    print(f"Rows now with '{NEW_STATUS}': {now_new}")

    if remaining == 0:
        print("Migration complete — all 'Block' rows are now 'Temporarily Block'.")
    else:
        print("WARNING: some rows still hold the old value; re-run the script.")


if __name__ == "__main__":
    main()
