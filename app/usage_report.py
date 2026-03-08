"""
usage_report.py

Reads the master usage log CSV and generates a PDF report for a date range.

Run (inside container):
  python3 -m app.usage_report --start 2026-01-01 --end 2026-01-31

Output:
  /app/data/reports/usage_report_2026-01-01_to_2026-01-31.pdf
"""

from __future__ import annotations

import argparse
import csv
import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from app.tenant_config import get_tenant_config

DATA_DIR = Path("/app/data")
USAGE_LOG_PATH = DATA_DIR / "usage_log.csv"
REPORTS_DIR = DATA_DIR / "reports"
BOISE_TZ = ZoneInfo("America/Boise")


def _parse_date_boise(s: str) -> datetime:
    # YYYY-MM-DD interpreted as Boise local day boundary, converted to UTC for comparisons
    local = datetime.strptime(s.strip(), "%Y-%m-%d").replace(tzinfo=BOISE_TZ)
    return local.astimezone(timezone.utc)


def _parse_iso(ts: str) -> Optional[datetime]:
    ts = (ts or "").strip()
    if not ts:
        return None
    try:
        # stored like 2026-01-09T23:25:01Z
        if ts.endswith("Z"):
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return datetime.fromisoformat(ts)
    except Exception:
        return None


def _load_rows() -> List[Dict[str, str]]:
    if not USAGE_LOG_PATH.exists():
        return []
    with USAGE_LOG_PATH.open("r", encoding="utf-8", newline="") as f:
        rdr = csv.DictReader(f)
        return [row for row in rdr]


def _filter_rows(rows: List[Dict[str, str]], start: datetime, end: datetime) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for r in rows:
        finished_iso = _parse_iso(r.get("finished_at_utc", "") or "")
        created_iso = _parse_iso(r.get("created_at_utc", "") or "")
        # Prefer finished time; fallback to created
        t = finished_iso or created_iso
        if not t:
            continue
        if start <= t < end:
            out.append(r)
    return out


def _wrap_line(s: str, max_chars: int) -> List[str]:
    s = s or ""
    if len(s) <= max_chars:
        return [s]
    out = []
    while len(s) > max_chars:
        out.append(s[:max_chars])
        s = s[max_chars:]
    if s:
        out.append(s)
    return out


def _render_pdf(title: str, subtitle: str, lines: List[str], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    c = canvas.Canvas(str(out_path), pagesize=letter)
    width, height = letter
    left = 54
    right = 54
    top = 54
    bottom = 54
    y = height - top

    c.setFont("Helvetica-Bold", 14)
    c.drawString(left, y, title[:140])
    y -= 18

    c.setFont("Helvetica", 10)
    c.drawString(left, y, subtitle[:180])
    y -= 18

    c.setFont("Courier", 8.8)
    line_h = 11
    max_chars = int((width - left - right) / 5.2)  # rough width for Courier 8.8

    for raw in lines:
        for ln in _wrap_line(raw, max_chars):
            if y < bottom:
                c.showPage()
                c.setFont("Courier", 8.8)
                y = height - top
            c.drawString(left, y, ln)
            y -= line_h

    c.showPage()
    c.save()


def generate_report(start_ymd: str, end_ymd: str) -> Tuple[Path, int]:
    start = _parse_date_boise(start_ymd)
    # end is exclusive; include the whole end day by adding 1 day
    end = _parse_date_boise(end_ymd)
    end = end.replace(hour=0, minute=0, second=0, microsecond=0)
    end = end + (datetime(1970, 1, 2, tzinfo=timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc))  # +1 day

    rows = _load_rows()
    rows = _filter_rows(rows, start, end)

    # Basic totals
    total = len(rows)
    done = sum(1 for r in rows if (r.get("status") or "").lower() == "done")
    failed = sum(1 for r in rows if (r.get("status") or "").lower() == "failed")

    # Build printable lines
    lines: List[str] = []
    lines.append(f"Total requests: {total}   Done: {done}   Failed: {failed}")
    lines.append("")

    # Header row (fixed-width-ish)
    header = "finished_boise | requestor | bill | status | bill_id | text_chars | ocr_used | model"
    lines.append(header)
    lines.append("-" * len(header))

    # Sort by finished_at
    def sort_key(r: Dict[str, str]) -> str:
        return r.get("finished_at_utc") or r.get("created_at_utc") or ""

    rows_sorted = sorted(rows, key=sort_key)

    # Limit rows to avoid insane PDFs
    max_rows = int(os.getenv("REPORT_MAX_ROWS", "500"))
    if len(rows_sorted) > max_rows:
        rows_sorted = rows_sorted[:max_rows]
        lines.append(f"[NOTE] Report truncated to first {max_rows} rows. Set REPORT_MAX_ROWS to change.")
        lines.append("")

    for r in rows_sorted:
        finished = r.get("finished_at_boise") or r.get("created_at_boise") or r.get("finished_at_utc") or r.get("created_at_utc") or ""
        email = r.get("requestor") or ""
        bill = r.get("bill_resolved") or r.get("bill_input") or ""
        status = r.get("status") or ""
        bill_id = r.get("bill_id") or ""
        text_chars = r.get("bill_text_sent_to_ai_chars") or r.get("bill_text_chars") or ""
        ocr_used = r.get("bill_text_ocr_used") or ""
        model = r.get("openai_model") or ""
        lines.append(f"{finished} | {email} | {bill} | {status} | {bill_id} | {text_chars} | {ocr_used} | {model}")

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = REPORTS_DIR / f"usage_report_{start_ymd}_to_{end_ymd}.pdf"
    title = f"{get_tenant_config()['org_name']} — Usage Report"
    subtitle = f"Period: {start_ymd} to {end_ymd} (Boise time). Source: /app/data/usage_log.csv"
    _render_pdf(title, subtitle, lines, out_path)

    return out_path, total


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="Start date YYYY-MM-DD (inclusive)")
    ap.add_argument("--end", required=True, help="End date YYYY-MM-DD (inclusive)")
    args = ap.parse_args()

    out_path, total = generate_report(args.start, args.end)
    print(f"OK: {total} row(s) in range")
    print(f"PDF: {out_path}")


if __name__ == "__main__":
    main()
