"""Intern feedback reporting backed by existing investment and lunch data."""
from datetime import date, datetime, timedelta
from io import BytesIO
import calendar
from concurrent.futures import ThreadPoolExecutor
import os

import pandas as pd
from flask import Blueprint, current_app, jsonify, make_response, render_template, request, send_file, session
from supabase import create_client, ClientOptions

intern_feedback_bp = Blueprint("intern_feedback", __name__)
EXCESS_BREAK_MINUTES = 30


def _client():
    opts = ClientOptions(postgrest_client_timeout=30, storage_client_timeout=30, function_client_timeout=30)
    return create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"), options=opts)


def _allowed(manager=False):
    pages = session.get("allowed_pages", [])
    if "intern_feedback" not in pages:
        return False
    return not manager or session.get("role") in ("admin", "superadmin")


def _date(value, fallback):
    try:
        return datetime.strptime(value, "%Y-%m-%d").date() if value else fallback
    except (TypeError, ValueError):
        return fallback


def _rows(table, cols, start, end, date_col, client=None, ilike=None):
    client = client or _client()
    rows, offset = [], 0
    while True:
        q = client.table(table).select(cols).gte(date_col, start.isoformat()).lte(date_col, end.isoformat()).order(date_col).range(offset, offset + 999)
        for column, pattern in (ilike or []):
            q = q.ilike(column, pattern)
        batch = q.execute().data or []
        rows.extend(batch)
        if len(batch) < 1000:
            return rows
        offset += 1000


def _break_duration_minutes(record):
    start, end = record.get("lunch_start"), record.get("lunch_end")
    if not start or not end:
        return None
    try:
        start_time = datetime.strptime(str(start)[:5], "%H:%M")
        end_time = datetime.strptime(str(end)[:5], "%H:%M")
        minutes = int((end_time-start_time).total_seconds() // 60)
        return minutes if minutes >= 0 else None
    except (TypeError, ValueError):
        return None


def _intern_name(source_user):
    import re
    source_user = (source_user or "").strip()
    if not re.match(r"^INT\s*", source_user, flags=re.I):
        return ""
    name = re.sub(r"^INT\s*|\s*icuser\s*$", "", source_user, flags=re.I).strip()
    name = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", name)
    return re.sub(r"\s+", " ", name).title()


def _report_payload(start, end):
    # The page needs both live source tables; fetch them at the same time and
    # restrict Investment Data rows at the source to intern-tagged records.
    with ThreadPoolExecutor(max_workers=2) as pool:
        cases_job = pool.submit(_rows, "BS_Investment_Scam", "input_user,inserted_date", start, end, "inserted_date", ilike=[("input_user", "INT%")])
        breaks_job = pool.submit(_rows, "lunch_breaks", "employee_name,date,lunch_start,lunch_end", start, end, "date")
        cases = cases_job.result()
        breaks = breaks_job.result()
    counts, daily_counts = {}, {}
    for row in cases:
        name = _intern_name(row.get("input_user"))
        day = str(row.get("inserted_date") or "")[:10]
        if name and day:
            counts[name] = counts.get(name, 0) + 1
            daily_counts.setdefault(name, {})[day] = daily_counts.setdefault(name, {}).get(day, 0) + 1
    # Use only actual intern-attributed Investment Data for team membership;
    # the existing GUI's user-role field does not distinguish interns.
    interns = sorted(counts)
    import re
    intern_keys = {re.sub(r"[^a-z0-9]", "", name.lower()) for name in interns}
    intern_breaks = [b for b in breaks if re.sub(r"[^a-z0-9]", "", str(b.get("employee_name") or "").lower()) in intern_keys]
    work_dates = [start + timedelta(days=n) for n in range((end-start).days + 1) if (start + timedelta(days=n)).weekday() < 5]
    workdays = len(work_dates)
    items = []
    for name in interns:
        by_date = {d.isoformat(): daily_counts.get(name, {}).get(d.isoformat(), 0) for d in work_dates}
        n = sum(by_date.values())
        intern_lunch = [b for b in intern_breaks if re.sub(r"[^a-z0-9]", "", str(b.get("employee_name") or "").lower()) == re.sub(r"[^a-z0-9]", "", name.lower())]
        intern_lunch = [{**b, "duration_minutes": _break_duration_minutes(b)} for b in intern_lunch]
        excess_breaks = [b for b in intern_lunch if b["duration_minutes"] is not None and b["duration_minutes"] > EXCESS_BREAK_MINUTES]
        excess_dates = sorted({str(b.get("date") or "") for b in excess_breaks if b.get("date")})
        excess_minutes = sum(b["duration_minutes"] - EXCESS_BREAK_MINUTES for b in excess_breaks)
        items.append({"name": name, "cases": n, "days": sum(v > 0 for v in by_date.values()), "average": round(n / workdays, 2) if workdays else 0,
                      "consistency": round(sum(v > 0 for v in by_date.values()) / workdays * 100, 1) if workdays else 0,
                      "highest": max(by_date.values(), default=0), "lowest": min((v for v in by_date.values() if v > 0), default=0),
                      "daily_rows": [{"date": d, "cases": v} for d, v in by_date.items()],
                      "feedback": {}, "excess_breaks": excess_breaks,
                      "excess_days": len(excess_dates), "excess_minutes": excess_minutes,
                      "excess_break_count": len(excess_breaks)})
    all_excess = [b for i in items for b in i["excess_breaks"]]
    return {"interns": items, "total": len(interns), "active": len(interns), "cases": sum(counts.values()),
            "workdays": workdays, "start": start.isoformat(), "end": end.isoformat(),
            "break_count": len(intern_breaks), "excess_break_count": len(all_excess),
            "excess_days": len({str(b.get("date") or "") for b in all_excess if b.get("date")}),
            "excess_minutes": sum(b["duration_minutes"] - EXCESS_BREAK_MINUTES for b in all_excess),
            "daily_rows": [row for i in items for row in i["daily_rows"]]}


def _period():
    today = date.today()
    start = _date(request.args.get("date_from"), today.replace(day=1))
    end = _date(request.args.get("date_to"), today)
    if request.args.get("month"):
        try:
            y, m = map(int, request.args["month"].split("-"))
            start, end = date(y, m, 1), date(y, m, calendar.monthrange(y, m)[1])
        except ValueError:
            pass
    return start, max(start, end)


@intern_feedback_bp.get("/intern-feedback")
def page():
    if "user_id" not in session:
        from flask import redirect
        return redirect("/login")
    if not _allowed():
        return make_response("Intern Feedback Report permission is required.", 403)
    start, end = _period()
    try:
        data = _report_payload(start, end)
        data["error"] = None
    except Exception as exc:
        current_app.logger.exception("Intern feedback source fetch failed")
        data = {"interns": [], "total": 0, "active": 0, "break_count": 0,
                "workdays": 0,
                "start": start.isoformat(), "end": end.isoformat(), "error": str(exc)}
    selected_name = request.args.get("intern", "")
    selected = next((i for i in data.get("interns", []) if i["name"] == selected_name), None)
    if selected is None and data.get("interns"):
        selected = data["interns"][0]
    return render_template("intern_feedback.html", data=data, selected=selected, can_edit=session.get("role") in ("admin", "superadmin"), allowed_pages=session.get("allowed_pages", []), display_name=session.get("display_name", "User"), role=session.get("role", "user"))


@intern_feedback_bp.get("/intern-feedback/team.xlsx")
def export_team():
    if not _allowed(manager=True): return make_response("Manager access required", 403)
    start, end = _period(); payload = _report_payload(start, end)
    df = pd.DataFrame([{k: i.get(k) for k in ("name", "cases", "average", "highest", "lowest", "consistency", "excess_days", "excess_break_count", "excess_minutes")} for i in payload["interns"]])
    df.rename(columns={"name":"Intern", "cases":"Total cases", "average":"Average cases per working day", "highest":"Highest daily cases", "lowest":"Lowest daily cases", "consistency":"Working day activity %", "excess_days":"Days with breaks over 30 min", "excess_break_count":"Excess break count", "excess_minutes":"Total excess minutes"}, inplace=True)
    out = BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer: df.to_excel(writer, index=False, sheet_name="Team Performance")
    out.seek(0)
    return send_file(out, as_attachment=True, download_name=f"intern_team_{start}_{end}.xlsx", mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


@intern_feedback_bp.get("/intern-feedback/report/<path:name>")
def report(name):
    if not _allowed(): return make_response("Access denied", 403)
    start, end = _period(); payload = _report_payload(start, end)
    intern = next((x for x in payload["interns"] if x["name"] == name), None)
    if not intern: return make_response("Intern not found", 404)
    return render_template("intern_feedback_report.html", intern=intern, data=payload)
