"""
GSE Daily API Ingestion Script — Cloud Version
================================================
Fetches live market data from kwayisi.org and upserts it into
the daily_trading table in Supabase (PostgreSQL).

Run this once at end of market day (GSE closes ~15:00 GMT).
Safe to re-run — uses upsert so duplicate calls won't create
duplicate rows. If run again on the same day, it UPDATES the
existing row with the latest figures.

Usage:
    python fetch_daily.py              # uses today's date
    python fetch_daily.py --date 2026-05-05   # backfill a specific date

Scheduling:
    GitHub Actions: .github/workflows/daily_fetch.yml (runs at 15:30 GMT)

Dependencies:
    pip install requests psycopg2-binary python-dotenv
"""

import argparse
import os
import re
import sys
from datetime import date, datetime
from pathlib import Path

import psycopg2
import requests
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Load environment variables from .env
# ---------------------------------------------------------------------------

load_dotenv(Path(__file__).parent / ".env")

DATABASE_URL = os.environ.get("DATABASE_URL")
API_URL      = os.environ.get("GSE_API_URL", "https://dev.kwayisi.org/apis/gse/live")

if not DATABASE_URL:
    print("ERROR: DATABASE_URL not set. Check your .env file.")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Source-of-truth stock codes
# ---------------------------------------------------------------------------

KNOWN_STOCKS = {
    "ACCESS", "ADB", "AGA", "ALW", "ASG", "ALLGH", "BOPP", "CAL", "CLYD",
    "CMLT", "CPC", "DASPHARMA", "DIGICUT", "EGH", "EGL", "ETI", "FAB",
    "FML", "GCB", "GGBL", "GLD", "GOIL", "HORDS", "ILL", "KAS", "MAC",
    "MMH", "MTNGH", "PBC", "RBGH", "SAMBA", "SCB", "SCBPREF", "SIC",
    "SOGEGH", "SWL", "TBL", "TLW", "TOTAL", "UNIL", "ZEN",
}


def clean_share_code(raw: str) -> str:
    """Strip asterisks, uppercase, collapse spaces. e.g. **MTNGH** → MTNGH"""
    cleaned = re.sub(r"\*+", "", raw).strip().upper()
    return cleaned.replace(" ", "")   # "SCB PREF" → "SCBPREF"


# ---------------------------------------------------------------------------
# Upsert SQL
# Rule: API data should NEVER overwrite rows that came from a monthly CSV.
#       Only insert fresh rows OR update rows that are also API-sourced.
#       CSV rows are authoritative — identified by source_file NOT starting with 'api:'.
# ---------------------------------------------------------------------------

UPSERT_SQL = """
INSERT INTO daily_trading (
    date, share_code,
    closing_vwap, price_change,
    total_shares_traded, total_value_traded,
    source_file
)
VALUES (
    %(date)s, %(share_code)s,
    %(closing_vwap)s, %(price_change)s,
    %(total_shares_traded)s, %(total_value_traded)s,
    %(source_file)s
)
ON CONFLICT (date, share_code) DO UPDATE SET
    closing_vwap         = EXCLUDED.closing_vwap,
    price_change         = EXCLUDED.price_change,
    total_shares_traded  = EXCLUDED.total_shares_traded,
    total_value_traded   = EXCLUDED.total_value_traded,
    source_file          = EXCLUDED.source_file,
    loaded_at            = NOW()
WHERE daily_trading.source_file LIKE 'api:%%';
-- ^^^ Only update if the existing row is also API-sourced.
-- If a CSV row already exists for this date+stock, DO NOTHING.
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def fetch_live_data() -> list[dict]:
    """Pull all stocks from the kwayisi.org live endpoint."""
    print(f"  Fetching: {API_URL} ...", end=" ")
    try:
        resp = requests.get(API_URL, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        print(f"{len(data)} stocks received")
        return data
    except requests.exceptions.RequestException as e:
        print(f"\n  ERROR: Could not reach API — {e}")
        sys.exit(1)
    except ValueError:
        print(f"\n  ERROR: API did not return valid JSON")
        sys.exit(1)


def map_record(raw: dict, trade_date: str) -> dict | None:
    """Map one API record to a daily_trading row."""
    share_code = clean_share_code(str(raw.get("name", "")))
    if not share_code:
        return None

    price  = raw.get("price")
    change = raw.get("change")
    volume = raw.get("volume")

    def to_float(v):
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    closing_vwap        = to_float(price)
    price_change        = to_float(change)
    total_shares_traded = to_float(volume)

    # Derive total value traded: price × volume (best estimate from API)
    if closing_vwap is not None and total_shares_traded is not None:
        total_value_traded = round(closing_vwap * total_shares_traded, 4)
    else:
        total_value_traded = None

    return {
        "date":                 trade_date,
        "share_code":           share_code,
        "closing_vwap":         closing_vwap,
        "price_change":         price_change,
        "total_shares_traded":  total_shares_traded,
        "total_value_traded":   total_value_traded,
        "source_file":          f"api:kwayisi.org:{trade_date}",
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(trade_date: str):
    print(f"\n{'='*60}")
    print(f"  GSE Daily API Ingestion — Cloud (Supabase)")
    print(f"{'='*60}")
    print(f"  Trade date : {trade_date}")
    print(f"  Database   : Supabase\n")

    # 1. Fetch
    print("[ Step 1 ] Fetching live data ...")
    raw_records = fetch_live_data()

    # 2. Map
    print("\n[ Step 2 ] Mapping records ...")
    rows = [r for raw in raw_records if (r := map_record(raw, trade_date)) is not None]
    skipped = len(raw_records) - len(rows)
    print(f"  Mapped: {len(rows)} rows  |  Skipped (no share code): {skipped}")

    unknown = {r["share_code"] for r in rows} - KNOWN_STOCKS
    if unknown:
        print(f"  [WARN] Unknown share codes (not in KNOWN_STOCKS): {sorted(unknown)}")

    if not rows:
        print("  Nothing to insert.")
        return

    # 3. Preview
    print(f"\n  Sample (first 3):")
    for r in rows[:3]:
        print(f"    {r['share_code']:<12}  price={r['closing_vwap']}  "
              f"change={r['price_change']}  vol={r['total_shares_traded']}")

    # 4. Connect to Supabase & upsert
    print(f"\n[ Step 3 ] Connecting to Supabase ...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
    except psycopg2.OperationalError as e:
        print(f"  ERROR: Could not connect to Supabase — {e}")
        sys.exit(1)

    print(f"  Connected. Upserting {len(rows)} rows ...")
    cur = conn.cursor()

    for row in rows:
        cur.execute(UPSERT_SQL, row)

    conn.commit()

    # Count how many API rows exist for this date
    cur.execute(
        "SELECT COUNT(*) FROM daily_trading WHERE date = %s AND source_file LIKE 'api:%%'",
        (trade_date,)
    )
    api_rows_in_db = cur.fetchone()[0]

    cur.close()
    conn.close()

    # Summary
    print(f"\n{'─'*60}")
    print(f"  Stocks upserted      : {len(rows)}")
    print(f"  Total API rows in DB : {api_rows_in_db}  (date={trade_date})")
    print(f"\n  ✓ Done — Supabase daily_trading updated for {trade_date}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch GSE daily data from kwayisi.org API.")
    parser.add_argument(
        "--date",
        type=str,
        default=date.today().strftime("%Y-%m-%d"),
        help="Trade date in YYYY-MM-DD format (default: today)",
    )
    args = parser.parse_args()

    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        print(f"Error: Invalid date format '{args.date}'. Use YYYY-MM-DD.")
        sys.exit(1)

    run(args.date)
