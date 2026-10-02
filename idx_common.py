"""Shared config & helpers untuk semua stage ETL IDX (datalake -> curated -> dwh -> dq).

Aturan: modul ini TIDAK punya side-effect saat di-import (nggak ada basicConfig, nggak ada client global).
Panggil setup_logging() sekali di entry point (blok __main__) tiap stage.
"""
import json
import logging
import os
import re
from datetime import date, datetime
from functools import lru_cache
from io import BytesIO
from typing import Callable, Iterator, Optional
from zoneinfo import ZoneInfo

import boto3
import pandas as pd
from botocore.exceptions import ClientError

BUCKET_NAME = os.getenv("IDX_BUCKET", "afdal-idx-stock-data-s3")
METADATA_KEY = "metadata/etl_control.json"
TZ = ZoneInfo("Asia/Jakarta")  # IDX = WIB; jangan bergantung ke timezone server
DEFAULT_START_DATE = date(2020, 1, 1)

logger = logging.getLogger(__name__)


def setup_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(level=level, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    logging.getLogger().setLevel(level)


# --------------------------------------------------------------------------- time
def now() -> datetime:
    return datetime.now(TZ)


def parse_ts(value: str) -> datetime:
    """Parse timestamp dari metadata. Nilai lama yang masih naive dianggap WIB."""
    dt = datetime.fromisoformat(value)
    return dt if dt.tzinfo else dt.replace(tzinfo=TZ)


def get_month_list(start_date: date, end_date: date) -> list[int]:
    """Daftar bulan (yyyymm) dari start_date s.d. end_date, inklusif. Kosong kalau start > end."""
    months, y, m = [], start_date.year, start_date.month
    while (y, m) <= (end_date.year, end_date.month):
        months.append(y * 100 + m)
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return months


# --------------------------------------------------------------------------- S3
@lru_cache(maxsize=1)
def s3_client():
    return boto3.client("s3")


def _is_missing(e: ClientError) -> bool:
    return e.response["Error"]["Code"] in ("NoSuchKey", "404", "NotFound")


def object_exists(bucket_name: str, key: str) -> bool:
    try:
        s3_client().head_object(Bucket=bucket_name, Key=key)
        return True
    except ClientError as e:
        if _is_missing(e):
            return False
        raise


def read_df_from_s3(bucket_name: str, key: str) -> Optional[pd.DataFrame]:
    """Return None kalau key nggak ada (caller WAJIB handle None)."""
    try:
        body = s3_client().get_object(Bucket=bucket_name, Key=key)["Body"].read()
    except ClientError as e:
        if _is_missing(e):
            return None
        raise
    return pd.read_parquet(BytesIO(body))


def upload_df_to_s3(df: pd.DataFrame, bucket_name: str, key: str) -> None:
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    s3_client().put_object(Bucket=bucket_name, Key=key, Body=buffer.getvalue())


def list_keys(bucket_name: str, prefix: str, suffix: str = "") -> Iterator[str]:
    """List key dengan paginator (list_objects_v2 biasa cuma max 1000 object)."""
    for page in s3_client().get_paginator("list_objects_v2").paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(suffix):
                yield obj["Key"]


def delete_prefix(bucket_name: str, prefix: str) -> int:
    keys = list(list_keys(bucket_name, prefix))
    for i in range(0, len(keys), 1000):
        s3_client().delete_objects(
            Bucket=bucket_name,
            Delete={"Objects": [{"Key": k} for k in keys[i:i + 1000]]},
        )
    return len(keys)


def list_layer_keys(layer: str, start_date: date, end_date: date, full_month_daily: bool = False) -> list[str]:
    """
    full_month_daily=True : ambil SEMUA daily di bulan itu (s.d. end_date), bukan cuma yang >= start_date.
                            Dipakai stage yang rebuild satu bulan penuh (fact_stock_daily).
    """
    keys: list[str] = []
    for ym in get_month_list(start_date, end_date):
        yyyy, mm = f"{ym // 100:04d}", f"{ym % 100:02d}"
        base = f"{layer}/idx/year={yyyy}/month={mm}"

        month_key = f"{base}/data.parquet"
        if object_exists(BUCKET_NAME, month_key):
            keys.append(month_key)
            continue

        found = 0
        for key in sorted(list_keys(BUCKET_NAME, f"{base}/daily/", "data.parquet")):
            d = date.fromisoformat(re.search(r"date=(\d{4}-\d{2}-\d{2})", key).group(1))
            if d <= end_date and (full_month_daily or d >= start_date):
                keys.append(key)
                found += 1
        if found == 0:
            logger.warning("Tidak ada data %s untuk %s-%s (skip)", layer, yyyy, mm)
    return keys


def read_json_s3(bucket_name: str, key: str) -> dict:
    body = s3_client().get_object(Bucket=bucket_name, Key=key)["Body"].read()
    return json.loads(body.decode("utf-8"))


def upload_json_to_s3(data: dict, bucket_name: str, key: str) -> None:
    s3_client().put_object(
        Bucket=bucket_name, Key=key,
        Body=json.dumps(data, indent=4), ContentType="application/json",
    )


# --------------------------------------------------------------------------- metadata
def update_metadata(bucket_name: str, key: str, etl_name: str, **kwargs) -> None:
    # NOTE: read-modify-write seluruh file -> jangan jalankan 2 stage bersamaan.
    metadata = read_json_s3(bucket_name, key)
    if etl_name not in metadata:
        raise ValueError(f"ETL '{etl_name}' tidak ditemukan di metadata")

    etl_metadata = metadata[etl_name]
    invalid_keys = set(kwargs) - set(etl_metadata)
    if invalid_keys:
        raise ValueError(f"Key tidak valid: {invalid_keys}")

    etl_metadata.update(kwargs)
    upload_json_to_s3(metadata, bucket_name, key)


def run_stage(
    etl_name: str,
    fn: Callable[[date, date, bool], None],
    prev_etl_name: Optional[str] = None,
    default_start: date = DEFAULT_START_DATE,
) -> None:
    """Lifecycle standar satu stage ETL.

    fn(start_date, end_date, first_run) dijalankan untuk rentang [start_date, end_date].
    - start_date  : tanggal last_processed (di-reprocess lagi, karena datanya bisa parsial), atau default_start kalau first run
    - end_date    : hari ini (WIB), dibatasi last_processed stage sebelumnya (prev_etl_name)
    - last_processed baru dicatat kalau fn sukses; kalau gagal, error dicatat di `message` lalu di-raise ulang
    """
    started = now().isoformat()
    metadata = read_json_s3(BUCKET_NAME, METADATA_KEY)

    if etl_name not in metadata:
        raise ValueError(f"ETL '{etl_name}' belum terdaftar. Jalankan init_metadata_etl.py dulu.")
    cfg = metadata[etl_name]
    if not cfg.get("is_active", 1):
        logger.info("%s nonaktif (is_active=0), skip", etl_name)
        return

    first_run = cfg["last_processed"] is None
    start_date = default_start if first_run else parse_ts(cfg["last_processed"]).date()

    upper = now()
    if prev_etl_name:
        prev_ts = metadata[prev_etl_name]["last_processed"]
        if prev_ts is None:
            raise RuntimeError(f"{prev_etl_name} belum pernah sukses, {etl_name} tidak bisa jalan")
        upper = min(upper, parse_ts(prev_ts))  # tidak boleh melewati stage sebelumnya
    end_date = upper.date()

    update_metadata(BUCKET_NAME, METADATA_KEY, etl_name, last_run=started, message="RUNNING")
    logger.info("%s: %s -> %s (first_run=%s)", etl_name, start_date, end_date, first_run)

    try:
        fn(start_date, end_date, first_run)
    except Exception as e:
        update_metadata(
            BUCKET_NAME, METADATA_KEY, etl_name,
            start_time=started, end_time=now().isoformat(),
            message=f"FAILED: {type(e).__name__}: {e}"[:500],
        )
        raise

    update_metadata(
        BUCKET_NAME, METADATA_KEY, etl_name,
        last_processed=upper.isoformat(),
        start_time=started, end_time=now().isoformat(),
        message="SUCCESS",
    )
