"""
Connection Test — Supabase + S3
================================
Run this once to verify your .env credentials are correct
before uploading any data.

Usage:
    python test_connections.py

Dependencies:
    pip install psycopg2-binary boto3 python-dotenv
"""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

DATABASE_URL   = os.environ.get("DATABASE_URL")
AWS_REGION     = os.environ.get("AWS_REGION", "eu-west-1")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")

print("\n" + "="*55)
print("  GSE Cloud — Connection Test")
print("="*55)

# ── 1. Supabase (PostgreSQL) ─────────────────────────────────
print("\n[ 1 ] Testing Supabase connection ...")
if not DATABASE_URL:
    print("  ✗ DATABASE_URL not set in .env")
else:
    try:
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
        cur  = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM daily_trading;")
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        print(f"  ✓ Connected to Supabase — daily_trading has {count:,} rows")
    except Exception as e:
        print(f"  ✗ Supabase connection failed: {e}")

# ── 2. AWS S3 ────────────────────────────────────────────────
print("\n[ 2 ] Testing S3 connection ...")
if not S3_BUCKET_NAME:
    print("  ✗ S3_BUCKET_NAME not set in .env")
else:
    try:
        import boto3
        from botocore.exceptions import ClientError, NoCredentialsError

        s3 = boto3.client("s3", region_name=AWS_REGION)
        s3.head_bucket(Bucket=S3_BUCKET_NAME)
        print(f"  ✓ Connected to S3 — bucket '{S3_BUCKET_NAME}' found and accessible")

        # List top-level objects
        resp = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, MaxKeys=5)
        keys = [o["Key"] for o in resp.get("Contents", [])]
        if keys:
            print(f"  ✓ Objects in bucket (first 5): {keys}")
        else:
            print("  ✓ Bucket is empty — ready to receive files")

    except NoCredentialsError:
        print("  ✗ AWS credentials not found — check AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY in .env")
    except ClientError as e:
        code = e.response["Error"]["Code"]
        print(f"  ✗ S3 error ({code}): {e}")
    except Exception as e:
        print(f"  ✗ S3 connection failed: {e}")

print("\n" + "="*55 + "\n")
