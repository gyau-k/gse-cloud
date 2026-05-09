"""
GSE Data — Supabase Loader (Cloud Version)
==========================================
Downloads complete_data.csv from S3, then loads it into
the daily_trading table in Supabase (PostgreSQL).

Safe to re-run: existing rows are upserted (date + share_code unique key).
CSV rows always win — they overwrite any previously loaded API rows.

Usage:
    python load_to_postgres.py

    Optionally, override the S3 key:
    python load_to_postgres.py --s3-key historical/complete_data.csv

Dependencies:
    pip install psycopg2-binary pandas boto3 python-dotenv
"""

import argparse
import io
import os
import re
import sys
from pathlib import Path

import boto3
import pandas as pd
import psycopg2
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Load environment variables
# ---------------------------------------------------------------------------

load_dotenv(Path(__file__).parent / ".env")

DATABASE_URL     = os.environ.get("DATABASE_URL")
AWS_REGION       = os.environ.get("AWS_REGION", "eu-west-1")
S3_BUCKET_NAME   = os.environ.get("S3_BUCKET_NAME")
S3_CSV_PREFIX    = os.environ.get("S3_CSV_PREFIX", "historical/")

COMPLETE_DATA_KEY = f"{S3_CSV_PREFIX}complete_data.csv"

if not DATABASE_URL:
    print("ERROR: DATABASE_URL not set. Check your .env file.")
    sys.exit(1)

if not S3_BUCKET_NAME:
    print("ERROR: S3_BUCKET_NAME not set. Check your .env file.")
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


def clean_share_code(code: str) -> str:
    """Strip asterisks, uppercase, collapse spaces. e.g. **MTNGH** → MTNGH"""
    cleaned = re.sub(r"\*+", "", str(code)).strip().upper()
    return cleaned.replace(" ", "")


# ---------------------------------------------------------------------------
# Upsert SQL — CSV always wins
# ---------------------------------------------------------------------------

UPSERT_SQL = """
INSERT INTO daily_trading (
    date, share_code,
    year_high, year_low,
    prev_closing_vwap, opening_price, last_transaction_price, closing_vwap,
    price_change, closing_bid, closing_offer,
    total_shares_traded, total_value_traded,
    source_file
)
VALUES (
    %(date)s, %(share_code)s,
    %(year_high)s, %(year_low)s,
    %(prev_closing_vwap)s, %(opening_price)s, %(last_transaction_price)s, %(closing_vwap)s,
    %(price_change)s, %(closing_bid)s, %(closing_offer)s,
    %(total_shares_traded)s, %(total_value_traded)s,
    %(source_file)s
)
ON CONFLICT (date, share_code) DO UPDATE SET
    year_high               = EXCLUDED.year_high,
    year_low                = EXCLUDED.year_low,
    prev_closing_vwap       = EXCLUDED.prev_closing_vwap,
    opening_price           = EXCLUDED.opening_price,
    last_transaction_price  = EXCLUDED.last_transaction_price,
    closing_vwap            = EXCLUDED.closing_vwap,
    price_change            = EXCLUDED.price_change,
    closing_bid             = EXCLUDED.closing_bid,
    closing_offer           = EXCLUDED.closing_offer,
    total_shares_traded     = EXCLUDED.total_shares_traded,
    total_value_traded      = EXCLUDED.total_value_traded,
    source_file             = EXCLUDED.source_file,
    loaded_at               = NOW();
-- ^^^ CSV always wins — overwrites API rows AND corrects previously loaded CSVs.
"""


# ---------------------------------------------------------------------------
# Step 1 — Download complete_data.csv from S3
# ---------------------------------------------------------------------------

def download_csv_from_s3(s3_key: str) -> pd.DataFrame:
    print(f"  Downloading s3://{S3_BUCKET_NAME}/{s3_key} ...")
    s3 = boto3.client("s3", region_name=AWS_REGION)
    try:
        obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
        content = obj["Body"].read()
        df = pd.read_csv(io.BytesIO(content))
        print(f"  Downloaded: {len(df):,} rows")
        return df
    except s3.exceptions.NoSuchKey:
        print(f"  ERROR: {s3_key} not found in S3 bucket '{S3_BUCKET_NAME}'.")
        print("  Run combine_historical.py first to generate and upload complete_data.csv.")
        sys.exit(1)
    except Exception as e:
        print(f"  ERROR: Failed to download from S3 — {e}")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Step 2 — Clean and prepare the DataFrame
# ---------------------------------------------------------------------------

def prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # Drop rows with null/empty share_code
    df = df[df["share_code"].notna() & (df["share_code"].astype(str).str.strip() != "")]

    # Clean share codes (safety net)
    df["share_code"] = df["share_code"].astype(str).apply(clean_share_code)
    df = df[df["share_code"].str.len() > 0]

    unknown = set(df["share_code"].unique()) - KNOWN_STOCKS
    if unknown:
        print(f"  [WARN] Unknown share codes (not in KNOWN_STOCKS): {sorted(unknown)}")

    # Parse dates
    df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce").dt.strftime("%Y-%m-%d")
    df = df[df["date"].notna()]

    # Clean numeric columns
    numeric_cols = [
        "year_high", "year_low", "prev_closing_vwap", "opening_price",
        "last_transaction_price", "closing_vwap", "price_change",
        "closing_bid", "closing_offer", "total_shares_traded", "total_value_traded",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].where(df[col].notna(), other=None)

    df = df.where(pd.notna(df), other=None)
    print(f"  Rows ready to load: {len(df):,}")
    return df


# ---------------------------------------------------------------------------
# Step 3 — Load into Supabase
# ---------------------------------------------------------------------------

def load_data(conn, df: pd.DataFrame):
    cur = conn.cursor()
    inserted = 0
    skipped  = 0
    batch_size = 500

    records = df.to_dict("records")
    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        for row in batch:
            cur.execute(UPSERT_SQL, row)
            if cur.rowcount == 1:
                inserted += 1
            else:
                skipped += 1

        conn.commit()
        done = min(i + batch_size, len(records))
        pct  = done / len(records) * 100
        print(f"    Progress: {done:>6,} / {len(records):,}  ({pct:.0f}%)", end="\r")

    cur.close()
    print(f"\n  Done — Inserted/Updated: {inserted:,}   Unchanged: {skipped:,}")


# ---------------------------------------------------------------------------
# Step 4 — Sanity check
# ---------------------------------------------------------------------------

def sanity_check(conn):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM daily_trading")
    total = cur.fetchone()[0]

    cur.execute("""
        SELECT
            EXTRACT(YEAR FROM date)::INT AS yr,
            COUNT(DISTINCT date)          AS trading_days,
            COUNT(DISTINCT share_code)    AS stocks,
            SUM(total_shares_traded)      AS volume,
            SUM(total_value_traded)       AS value
        FROM daily_trading
        GROUP BY yr
        ORDER BY yr
    """)
    rows = cur.fetchall()
    cur.close()

    print(f"\n  {'='*72}")
    print(f"  Sanity check — daily_trading (Supabase)")
    print(f"  {'='*72}")
    print(f"  Total rows in table: {total:,}\n")
    print(f"  {'Year':>6} {'Days':>6} {'Stocks':>8} {'Volume':>18} {'Value (GHS)':>20}")
    print(f"  {'-'*64}")
    for yr, days, stocks, vol, val in rows:
        print(f"  {yr:>6} {days:>6} {stocks:>8} {vol:>18,.0f} {val:>20,.2f}")
    print(f"  {'='*72}\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load GSE historical data from S3 into Supabase.")
    parser.add_argument(
        "--s3-key",
        type=str,
        default=COMPLETE_DATA_KEY,
        help=f"S3 key of the complete_data.csv file (default: {COMPLETE_DATA_KEY})",
    )
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"  GSE Data — Supabase Loader (Cloud)")
    print(f"{'='*60}")
    print(f"  Source  : s3://{S3_BUCKET_NAME}/{args.s3_key}")
    print(f"  Target  : Supabase → daily_trading\n")

    # 1. Download from S3
    print("[ Step 1 ] Downloading complete_data.csv from S3 ...")
    df = download_csv_from_s3(args.s3_key)

    # 2. Prepare data
    print("\n[ Step 2 ] Preparing data ...")
    df = prepare_dataframe(df)

    # 3. Connect to Supabase
    print("\n[ Step 3 ] Connecting to Supabase ...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        print("  Connected.")
    except psycopg2.OperationalError as e:
        print(f"  ERROR: Could not connect to Supabase — {e}")
        sys.exit(1)

    # 4. Load data
    print("\n[ Step 4 ] Loading data ...")
    load_data(conn, df)

    # 5. Sanity check
    print("\n[ Step 5 ] Sanity check ...")
    sanity_check(conn)

    conn.close()
    print("  All done. Supabase daily_trading is up to date.\n")
