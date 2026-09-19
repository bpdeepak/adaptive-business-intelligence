"""Phase 0 loader: raw Olist CSVs -> MinIO (bronze, immutable) -> Postgres bronze.* tables.

Design note: bronze keeps the raw bytes verbatim — every column lands as TEXT so
nothing is type-cast or cleaned at this stage. All typing/cleaning happens in the
dbt silver layer. Each bronze table additionally carries ingestion lineage
columns ``_loaded_at``, ``_source_file`` and ``_batch_id`` so "when was this
landed, from which file, in which run" is always answerable.
"""

from __future__ import annotations

import csv
import os
import time
import uuid
from pathlib import Path

import boto3
import botocore.exceptions
import psycopg

REPO_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = REPO_ROOT / "data" / "raw"

MINIO_ENDPOINT = os.getenv("ABI_MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS = os.getenv("ABI_MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET = os.getenv("ABI_MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("ABI_MINIO_BUCKET", "bronze")

PG_HOST = os.getenv("ABI_PG_HOST", "localhost")
PG_PORT = os.getenv("ABI_PG_PORT", "5432")
PG_DB = os.getenv("ABI_PG_DB", "abi")
PG_USER = os.getenv("ABI_PG_USER", "abi")
PG_PASS = os.getenv("ABI_PG_PASSWORD", "abi")
PG_DSN = os.getenv(
    "DATABASE_URL",
    f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}",
)

# bronze table name -> source CSV filename. The names must match dbt source names.
TABLES: dict[str, str] = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "product_category_name_translation": "product_category_name_translation.csv",
    "geolocation": "olist_geolocation_dataset.csv",
}

# Ingestion lineage columns stamped on every bronze table. They answer "when was
# this landed, from which file, in which pipeline run?" — a question that gets
# real once Phase 1 adds a second (streaming) ingestion path.
LINEAGE_COLS = """\
"_loaded_at" timestamptz not null default now(),
"_source_file" text not null default '' ,
"_batch_id" text not null default ''
"""


def new_batch_id() -> str:
    """Readable per-run batch id: 20260919-143301-1a2b3c."""
    return f"{time.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"


def ensure_bucket(s3) -> None:
    try:
        s3.create_bucket(Bucket=MINIO_BUCKET)
    except botocore.exceptions.ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        # Both are fine: the bucket already exists (re-run of the loader).
        if code not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            raise


def upload_to_minio() -> None:
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        region_name="us-east-1",
    )
    ensure_bucket(s3)
    for table, filename in TABLES.items():
        path = RAW_DIR / filename
        if not path.exists():
            raise FileNotFoundError(
                f"missing {path} — run scripts/download_olist.py first"
            )
        key = f"olist/{filename}"
        s3.upload_file(str(path), MINIO_BUCKET, key)
        head = s3.head_object(Bucket=MINIO_BUCKET, Key=key)
        expected = path.stat().st_size
        if head["ContentLength"] != expected:
            raise RuntimeError(
                f"minio size mismatch for {key}: {head['ContentLength']} != {expected}"
            )
        print(f"  minio  {MINIO_BUCKET}/{key} ({head['ContentLength']:,} bytes)")


def csv_columns(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8", newline="") as fh:
        header = next(csv.reader(fh))
    if not header:
        raise ValueError(f"empty header in {path}")
    # Strip whitespace and a UTF-8 BOM that some source files carry on the
    # first header cell (e.g. product_category_name_translation.csv).
    return [h.strip().lstrip("\ufeff") for h in header]


def load_to_postgres(batch_id: str) -> None:
    with psycopg.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS bronze")
            for table, filename in TABLES.items():
                path = RAW_DIR / filename
                if not path.exists():
                    raise FileNotFoundError(f"missing {path}")
                cols = csv_columns(path)
                cols_sql = ", ".join(f'"{c}"' for c in cols)
                cur.execute(f'DROP TABLE IF EXISTS bronze."{table}" CASCADE')
                coldefs = ", ".join(f'"{c}" text' for c in cols)
                cur.execute(
                    f'CREATE TABLE bronze."{table}" ({coldefs}, {LINEAGE_COLS})'
                )
                copy_sql = (
                    f'COPY bronze."{table}" ({cols_sql}) FROM STDIN '
                    "WITH (FORMAT csv, HEADER true)"
                )
                with path.open("rb") as fh, cur.copy(copy_sql) as copy:
                    while chunk := fh.read(1 << 20):
                        copy.write(chunk)
                # COPY fills the lineage columns with their DDL defaults; stamp
                # the real run + source file afterwards.
                cur.execute(
                    f'UPDATE bronze."{table}" '
                    "SET _source_file = %s, _batch_id = %s, _loaded_at = now()",
                    (filename, batch_id),
                )
                row_count = cur.execute(
                    f'SELECT count(*) FROM bronze."{table}"'
                ).fetchone()[0]
                print(
                    f"  pg     bronze.{table} ({row_count:,} rows) "
                    f"from {filename}  batch={batch_id}"
                )
        conn.commit()


def main() -> None:
    missing = [p for _, f in TABLES.items() if not (RAW_DIR / f).exists()]
    if missing:
        raise SystemExit(
            f"missing source files under {RAW_DIR}: {missing} — "
            "run scripts/download_olist.py first"
        )
    print("== 1/2 uploading raw files to MinIO (bronze bucket)")
    upload_to_minio()
    print("== 2/2 loading raw CSV text into Postgres bronze schema")
    batch_id = new_batch_id()
    print(f"  batch {batch_id}")
    load_to_postgres(batch_id)
    print("\nBronze layer ready.")


if __name__ == "__main__":
    main()