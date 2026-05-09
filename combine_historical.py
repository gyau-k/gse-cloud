"""
GSE Historical Data Combiner — Cloud Version (S3)
==================================================
Scans an S3 bucket for new/changed GSE CSV or Excel files,
combines them into complete_data.csv, and uploads it back to S3.

S3 structure expected:
    s3://gse-financial-data/
        historical/
            2023/
                jan.csv
                feb.csv
                ...
            2024/
                ...
            2025/
                ...
            2026/
                ...
            complete_data.csv          ← created/updated by this script
            .processed_manifest.json   ← tracks already-processed files (auto-managed)

Usage:
    python combine_historical.py

Dependencies:
    pip install pandas boto3 openpyxl python-dotenv
"""

import io
import json
import os
import re
import sys
from datetime import datetime
from hashlib import md5
from pathlib import Path

import boto3
import pandas as pd
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Load environment variables
# ---------------------------------------------------------------------------

load_dotenv(Path(__file__).parent / ".env")

AWS_REGION     = os.environ.get("AWS_REGION", "eu-west-1")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")
S3_CSV_PREFIX  = os.environ.get("S3_CSV_PREFIX", "historical/")

if not S3_BUCKET_NAME:
    print("ERROR: S3_BUCKET_NAME not set. Check your .env file.")
    sys.exit(1)

OUTPUT_KEY   = f"{S3_CSV_PREFIX}complete_data.csv"
MANIFEST_KEY = f"{S3_CSV_PREFIX}.processed_manifest.json"

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

COLUMN_RENAME = {
    "Daily Date":                             "date",
    "Share Code":                             "share_code",
    "Year High (GH¢)":                        "year_high",
    "Year Low (GH¢)":                         "year_low",
    "Previous Closing Price - VWAP (GH¢)":   "prev_closing_vwap",
    "Opening Price (GH¢)":                    "opening_price",
    "Last Transaction Price (GH¢)":           "last_transaction_price",
    "Closing Price - VWAP (GH¢)":            "closing_vwap",
    "Price Change (GH¢)":                     "price_change",
    "Closing Bid Price (GH¢)":               "closing_bid",
    "Closing Offer Price (GH¢)":             "closing_offer",
    "Total Shares Traded":                    "total_shares_traded",
    "Total Value Traded (GH¢)":              "total_value_traded",
}

FINAL_COLUMNS = list(COLUMN_RENAME.values()) + ["source_file"]


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------

def get_s3_client():
    return boto3.client("s3", region_name=AWS_REGION)


def list_s3_files(s3, prefix: str) -> list[dict]:
    """List all CSV/Excel objects under the given prefix (excluding output/manifest)."""
    paginator = s3.get_paginator("list_objects_v2")
    files = []
    for page in paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            name = key.split("/")[-1]
            # Skip the output file, manifest, and dotfiles
            if name in ("complete_data.csv", ".processed_manifest.json") or name.startswith("."):
                continue
            if key.lower().endswith((".csv", ".xlsx", ".xls")):
                files.append({"key": key, "etag": obj["ETag"].strip('"'), "size": obj["Size"]})
    return files


def download_s3_bytes(s3, key: str) -> bytes:
    obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=key)
    return obj["Body"].read()


def upload_to_s3(s3, key: str, content: bytes, content_type: str = "text/csv"):
    s3.put_object(Bucket=S3_BUCKET_NAME, Key=key, Body=content, ContentType=content_type)
    print(f"  ✓ Uploaded → s3://{S3_BUCKET_NAME}/{key}")


# ---------------------------------------------------------------------------
# Manifest — stored in S3
# ---------------------------------------------------------------------------

def load_manifest(s3) -> dict:
    try:
        data = download_s3_bytes(s3, MANIFEST_KEY)
        return json.loads(data.decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        return {}
    except Exception:
        return {}


def save_manifest(s3, manifest: dict):
    content = json.dumps(manifest, indent=2).encode("utf-8")
    upload_to_s3(s3, MANIFEST_KEY, content, content_type="application/json")


# ---------------------------------------------------------------------------
# File reading & cleaning
# ---------------------------------------------------------------------------

def read_file_bytes(raw_bytes: bytes, key: str) -> pd.DataFrame | None:
    """Parse CSV or Excel bytes into a raw DataFrame."""
    suffix = key.split(".")[-1].lower()
    try:
        if suffix == "csv":
            df = pd.read_csv(io.BytesIO(raw_bytes), dtype=str)
        elif suffix in ("xlsx", "xls"):
            df = pd.read_excel(io.BytesIO(raw_bytes), dtype=str)
        else:
            return None

        df.columns = [c.strip() for c in df.columns]

        if "Daily Date" not in df.columns or "Share Code" not in df.columns:
            print(f"    [SKIP] Not a GSE file (missing expected columns): {key.split('/')[-1]}")
            return None

        df["source_file"] = key
        return df

    except Exception as e:
        print(f"    [ERROR] Could not read {key.split('/')[-1]}: {e}")
        return None


def clean_share_code(code) -> str:
    if code is None or (isinstance(code, float) and pd.isna(code)):
        return ""
    raw = str(code).strip()
    if raw.lower() in ("nan", "none", "n/a", "-", ""):
        return ""
    cleaned = re.sub(r"\*+", "", raw).strip().upper()
    return cleaned.replace(" ", "")


def clean_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(
        series.astype(str).str.replace(",", "").str.strip(), errors="coerce"
    )


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df["Share Code"] = df["Share Code"].apply(clean_share_code)
    df = df[df["Share Code"].str.len() > 0]

    unknown = set(df["Share Code"].unique()) - KNOWN_STOCKS
    if unknown:
        print(f"    [WARN] Unknown share codes (not in KNOWN_STOCKS): {sorted(unknown)}")

    df["Daily Date"] = pd.to_datetime(df["Daily Date"], dayfirst=True, errors="coerce")
    df = df.dropna(subset=["Daily Date"])

    numeric_cols = [c for c in COLUMN_RENAME if c not in ("Daily Date", "Share Code") and c in df.columns]
    for col in numeric_cols:
        df[col] = clean_numeric(df[col])

    # Remove intra-file duplicates — keep row with highest value traded
    df = (
        df.sort_values("Total Value Traded (GH¢)", ascending=False, na_position="last")
          .drop_duplicates(subset=["Daily Date", "Share Code"], keep="first")
    )

    rename_map = {k: v for k, v in COLUMN_RENAME.items() if k in df.columns}
    df = df.rename(columns=rename_map)
    df["date"] = df["date"].dt.strftime("%d/%m/%Y")

    final = [c for c in FINAL_COLUMNS if c in df.columns]
    return df[final]


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def run():
    print(f"\n{'='*60}")
    print(f"  GSE Historical Data Combiner — Cloud (S3)")
    print(f"{'='*60}")
    print(f"  Bucket  : s3://{S3_BUCKET_NAME}")
    print(f"  Prefix  : {S3_CSV_PREFIX}\n")

    s3 = get_s3_client()

    # 1. Load manifest
    manifest = load_manifest(s3)

    # 2. List all files in S3
    all_files = list_s3_files(s3, S3_CSV_PREFIX)
    print(f"  Files found in S3 : {len(all_files)}")

    # 3. Filter to new/changed files (by ETag)
    new_files = []
    skipped   = []
    for f in all_files:
        key  = f["key"]
        etag = f["etag"]
        if key in manifest and manifest[key]["etag"] == etag:
            skipped.append(key)
        else:
            new_files.append(f)

    print(f"  Already processed : {len(skipped)} file(s)")
    print(f"  New / changed     : {len(new_files)} file(s)\n")

    if not new_files:
        print("  Nothing to do — complete_data.csv is already up to date.")
        print(f"{'='*60}\n")
        return

    # 4. Load existing complete_data.csv from S3 if it exists
    try:
        existing_bytes = download_s3_bytes(s3, OUTPUT_KEY)
        existing = pd.read_csv(io.BytesIO(existing_bytes), dtype=str)
        print(f"  Loaded existing complete_data.csv : {len(existing):,} rows")
    except Exception:
        existing = pd.DataFrame(columns=FINAL_COLUMNS)
        print("  No existing complete_data.csv — will create fresh.")

    # 5. Process each new file
    new_frames = []
    for f in new_files:
        key  = f["key"]
        etag = f["etag"]
        name = key.split("/")[-1]
        print(f"\n  Processing: {name} ...", end=" ")
        raw_bytes = download_s3_bytes(s3, key)
        raw_df    = read_file_bytes(raw_bytes, key)
        if raw_df is None:
            continue
        cleaned = clean_dataframe(raw_df)
        print(f"{len(cleaned):,} rows")
        new_frames.append({"key": key, "etag": etag, "df": cleaned, "rows": len(cleaned)})

    if not new_frames:
        print("\n  No valid data in new files.")
        return

    # 6. Combine existing + new
    new_data = pd.concat([f["df"] for f in new_frames], ignore_index=True)
    combined = pd.concat([existing, new_data], ignore_index=True)

    # 7. Remove cross-file duplicates — keep highest value traded
    before = len(combined)
    combined["total_value_traded"] = pd.to_numeric(combined["total_value_traded"], errors="coerce")
    combined = (
        combined
        .sort_values("total_value_traded", ascending=False, na_position="last")
        .drop_duplicates(subset=["date", "share_code"], keep="first")
        .sort_values(["date", "share_code"])
        .reset_index(drop=True)
    )
    dupes = before - len(combined)

    # 8. Upload complete_data.csv to S3
    combined["total_value_traded"] = combined["total_value_traded"].astype(str)
    csv_bytes = combined.to_csv(index=False).encode("utf-8")
    upload_to_s3(s3, OUTPUT_KEY, csv_bytes)

    # 9. Update manifest
    for f in new_frames:
        manifest[f["key"]] = {
            "etag":         f["etag"],
            "rows":         f["rows"],
            "processed_at": datetime.now().isoformat(timespec="seconds"),
        }
    save_manifest(s3, manifest)

    # 10. Summary
    print(f"\n{'─'*60}")
    print(f"  New rows added           : {len(new_data):,}")
    print(f"  Cross-file dupes removed : {dupes:,}")
    print(f"  Total rows in output     : {len(combined):,}")
    print(f"  Date range               : {combined['date'].min()} → {combined['date'].max()}")
    print(f"  Unique stocks            : {combined['share_code'].nunique()}")
    print(f"  Unique dates             : {combined['date'].nunique()}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    run()
