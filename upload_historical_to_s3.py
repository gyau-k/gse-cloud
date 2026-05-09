"""
Upload Historical CSVs to S3
==============================
One-time script to mirror your local sample/ folder into S3.

Uploads all CSV/Excel files from sample/YYYY/ subfolders into:
    s3://YOUR-BUCKET/historical/YYYY/filename.csv

Skips files already in S3 with matching size (safe to re-run).

Usage:
    python upload_historical_to_s3.py
    python upload_historical_to_s3.py --sample-dir C:\\path\\to\\sample

Dependencies:
    pip install boto3 python-dotenv
"""

import argparse
import os
import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Load environment
# ---------------------------------------------------------------------------

load_dotenv(Path(__file__).parent / ".env")

AWS_REGION     = os.environ.get("AWS_REGION", "eu-west-1")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")
S3_CSV_PREFIX  = os.environ.get("S3_CSV_PREFIX", "historical/")

if not S3_BUCKET_NAME:
    print("ERROR: S3_BUCKET_NAME not set in .env")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def upload(sample_dir: Path):
    print(f"\n{'='*60}")
    print(f"  GSE — Upload Historical Files to S3")
    print(f"{'='*60}")
    print(f"  Source  : {sample_dir}")
    print(f"  Target  : s3://{S3_BUCKET_NAME}/{S3_CSV_PREFIX}\n")

    s3 = boto3.client("s3", region_name=AWS_REGION)

    # Gather all CSV/Excel files inside YYYY/ subfolders
    candidates = []
    for ext in ("*.csv", "*.xlsx", "*.xls"):
        candidates.extend(sample_dir.rglob(ext))

    # Exclude complete_data.csv and dotfiles
    candidates = sorted([
        f for f in candidates
        if f.name != "complete_data.csv" and not f.name.startswith(".")
    ])

    if not candidates:
        print("  No CSV/Excel files found in sample/ subfolders.")
        return

    print(f"  Files found locally : {len(candidates)}\n")

    uploaded = 0
    skipped  = 0
    failed   = 0

    for filepath in candidates:
        # Build the S3 key: historical/YYYY/filename.csv
        # filepath is like sample/2023/jan.csv → relative = 2023/jan.csv
        try:
            rel = filepath.relative_to(sample_dir)
        except ValueError:
            rel = Path(filepath.name)

        s3_key = f"{S3_CSV_PREFIX}{rel.as_posix()}"
        local_size = filepath.stat().st_size

        # Check if already in S3 with same size
        try:
            head = s3.head_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
            if head["ContentLength"] == local_size:
                print(f"  [SKIP]   {s3_key}  (already uploaded, same size)")
                skipped += 1
                continue
        except ClientError as e:
            if e.response["Error"]["Code"] != "404":
                print(f"  [ERROR]  {s3_key} — {e}")
                failed += 1
                continue
            # 404 = not in S3 yet, proceed with upload

        # Upload
        try:
            s3.upload_file(str(filepath), S3_BUCKET_NAME, s3_key)
            print(f"  [OK]     {s3_key}  ({local_size / 1024:.1f} KB)")
            uploaded += 1
        except Exception as e:
            print(f"  [ERROR]  {s3_key} — {e}")
            failed += 1

    print(f"\n{'─'*60}")
    print(f"  Uploaded : {uploaded}")
    print(f"  Skipped  : {skipped}  (already in S3)")
    print(f"  Failed   : {failed}")
    print(f"\n  ✓ Done. Now run combine_historical.py to generate complete_data.csv")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload local GSE CSVs to S3.")
    parser.add_argument(
        "--sample-dir",
        type=Path,
        default=Path(__file__).parent.parent / "sample",
        help="Path to local sample/ folder (default: ../sample relative to this script)",
    )
    args = parser.parse_args()

    if not args.sample_dir.exists():
        print(f"ERROR: sample directory not found: {args.sample_dir}")
        print("Use --sample-dir to specify the correct path.")
        sys.exit(1)

    upload(args.sample_dir)
