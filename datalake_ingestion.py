import logging
import random
import time
from datetime import date, datetime, timedelta
import os

import pandas as pd
from dateutil.relativedelta import relativedelta

from idx_common import (
    BUCKET_NAME,
    delete_prefix,
    get_month_list,
    list_keys,
    now,
    read_df_from_s3,
    run_stage,
    setup_logging,
    upload_df_to_s3,
    get_zapi_key,
)

logger = logging.getLogger(__name__)

IDX_URL = "https://www.idx.co.id/primary/TradingSummary/GetStockSummary"
ZAPI_URL = "https://api.zapi.ink/v1/finance:idx/stock-summary"
MAX_IDX_RETRY = 2
IMPERSONATE = os.getenv("IDX_IMPERSONATE", "safari")
IDX_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.idx.co.id/",
}
RAW_GRAIN = ["Date", "StockCode"]
 
 
def create_session():
    """Session HTTP yang meniru browser (TLS/HTTP2 fingerprint), supaya nggak kena bot-block Cloudflare IDX."""
    try:
        from curl_cffi import requests as curl_requests
 
        return curl_requests.Session(impersonate=IMPERSONATE)
    except ImportError:
        logger.warning("curl_cffi belum terinstall, pakai cloudscraper (rawan 403). pip install curl_cffi")
        import cloudscraper
 
        return cloudscraper.create_scraper()
 
 
def fetch_from_idx(target_date: date, session) -> pd.DataFrame:
    """Mencoba fetch langsung ke endpoint internal IDX."""
    params = {
        "length": 9999,
        "start": 0,
        "date": target_date.strftime("%Y-%m-%d"),
    }
    
    for attempt in range(MAX_IDX_RETRY):
        try:
            response = session.get(IDX_URL, params=params, headers=IDX_HEADERS, timeout=7)
            
            if response.status_code == 200:
                raw_data = response.json().get("data", [])
                df = pd.DataFrame(raw_data)
                logger.info(f"[IDX Direct] {target_date} - OK ({len(df)} rows)")
                return df
            
            logger.warning(f"[IDX Direct] {target_date} - HTTP {response.status_code} (Attempt {attempt+1}/{MAX_IDX_RETRY})")
            
            if response.status_code == 403:
                # IP AWS kemungkinan besar langsung kena Cloudflare block
                break
                
        except Exception as e:
            logger.warning(f"[IDX Direct] {target_date} - Error: {e}")
            
        if attempt < MAX_IDX_RETRY - 1:
            time.sleep(1) # Delay singkat sebelum retry
            
    return pd.DataFrame()

def fetch_from_zapi(target_date: date) -> pd.DataFrame:
    """Fallback fetch menggunakan ZAPI."""
    params = {
        "length": "5000",
        "start": "0",
        "date": target_date.strftime("%Y%m%d"), # ZAPI format YYYYMMDD
        "code": ""
    }
    # Ambil ZAPI Key dari SSM / Cache
    zapi_key = get_zapi_key()
    headers = {"x-api-key": zapi_key}
    
    try:
        import requests
        response = requests.get(ZAPI_URL, params=params, headers=headers, timeout=15)
        
        if response.status_code == 200:
            res_json = response.json()
            raw_data = res_json.get("data", {}).get("data", [])
            df = pd.DataFrame(raw_data)
            logger.info(f"[ZAPI Fallback] {target_date} - OK ({len(df)} rows)")
            return df
        else:
            logger.error(f"[ZAPI Fallback] {target_date} - HTTP {response.status_code}: {response.text[:100]}")
    except Exception as e:
        logger.error(f"[ZAPI Fallback] {target_date} - Error: {e}")
        
    return pd.DataFrame()

def get_idx_data(start_date: date, end_date: date = None, session=None) -> pd.DataFrame:
    """Ambil trading summary IDX. 
    Menggunakan IDX direct sebagai sumber utama, dan otomatis fallback ke ZAPI jika gagal/terblokir.
    """
    if end_date is None:
        end_date = start_date

    session = session or create_session()
    dates = pd.date_range(start=start_date, end=end_date)
    all_data = []

    for current_date in dates:
        dt = current_date.date()
        
        # 1. Coba fetch dari IDX Direct
        df = fetch_from_idx(dt, session)
        
        # 2. Fallback ke ZAPI jika IDX Direct gagal/kosong dan bukan weekend
        if df.empty and dt.weekday() < 5:
            logger.info(f"{dt} - Switching to ZAPI fallback...")
            df = fetch_from_zapi(dt)

        if not df.empty:
            all_data.append(df)
        else:
            logger.warning(f"{dt} - Tidak ada data yang berhasil ditarik.")

        # Jeda antar request agar ramah API
        time.sleep(random.uniform(0.5, 1.5))

    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()


def compact_daily(yyyy, mm):
    """Gabungkan semua daily bulan (yyyy, mm). Kalau data.parquet bulan itu sudah ada, digabung (bukan ditimpa)."""
    prefix = f"raw/idx/year={yyyy}/month={mm}/daily/"
    keys = sorted(list_keys(BUCKET_NAME, prefix, ".parquet"))
    if not keys:
        logger.info(f"Data daily {yyyy}-{mm} tidak ditemukan")
        return None

    frames = [df for df in (read_df_from_s3(BUCKET_NAME, k) for k in keys) if df is not None]
    if not frames:
        return None
    df = pd.concat(frames, ignore_index=True)

    month_key = f"raw/idx/year={yyyy}/month={mm}/data.parquet"
    existing = read_df_from_s3(BUCKET_NAME, month_key)
    if existing is not None:
        df = pd.concat([existing, df], ignore_index=True).drop_duplicates(subset=RAW_GRAIN, keep="last")

    logger.info(f"Compact done for {yyyy}-{mm}: {len(df)} rows")
    return df


def delete_daily(yyyy, mm):
    # dipanggil setelah compact berhasil di-upload
    n = delete_prefix(BUCKET_NAME, f"raw/idx/year={yyyy}/month={mm}/daily/")
    logger.info(f"Data daily untuk {yyyy}-{mm} telah terhapus ({n} file)")


def auto_compact(start_date, end_date):
    """Compact semua bulan yang sudah lewat (bulan terakhir = bulan berjalan, tetap daily)."""
    month_list = get_month_list(start_date, end_date)
    for year_month in month_list[:-1]:
        mm = f"{year_month % 100:02d}"
        yyyy = f"{year_month // 100}"
        df = compact_daily(yyyy, mm)
        if df is not None and not df.empty:
            upload_df_to_s3(df, BUCKET_NAME, f"raw/idx/year={yyyy}/month={mm}/data.parquet")
            delete_daily(yyyy, mm)


def daily_load(start_date, end_date):
    for i in pd.date_range(start_date, end_date):
        d = i.date()
        df = get_idx_data(d)
        key = f"raw/idx/year={d.year:04d}/month={d.month:02d}/daily/date={d}/data.parquet"
        logger.info(f"{key} - jumlah: {len(df)}")

        # Jika weekday (Senin-Jumat) tapi data kosong dan data bukan hari ini, raise RuntimeError agar ETL FAILED & metadata TIDAK ter-update!
        today = date.today()
        if len(df) == 0 and d < today and d.weekday() < 5:
            raise RuntimeError(f"Gagal mengambil data IDX/ZAPI untuk tanggal {d} (0 rows). ETL dihentikan.")
        if len(df) > 0:
            upload_df_to_s3(df, BUCKET_NAME, key)


def initial_load(start_date, end_date):
    # Backfill historis: bulan yang sudah lewat diambil sekaligus per bulan (efisien,
    # langsung disimpan compacted tanpa lewat tahap daily/compact/delete).
    # Bulan terakhir (bulan berjalan) diambil harian lewat daily_load, biar nanti
    # otomatis ke-compact sama auto_compact() di run main_etl berikutnya.
    months = get_month_list(start_date, end_date)

    for i, month in enumerate(months):
        mm = f"{month % 100:02d}"
        yyyy = f"{month // 100}"
        month_start = datetime.strptime(f"{yyyy}-{mm}-01", "%Y-%m-%d").date()

        if i != len(months) - 1:
            month_end = month_start + relativedelta(months=1) - relativedelta(days=1)
            df = get_idx_data(month_start, month_end)
            key = f"raw/idx/year={yyyy}/month={mm}/data.parquet"
            logger.info(f"{key} - jumlah: {len(df)}")
            if len(df) > 0:
                upload_df_to_s3(df, BUCKET_NAME, key)
        else:
            daily_load(month_start, end_date)


def ingest(start_date, end_date, first_run):
    daily_load(start_date, end_date)
    auto_compact(start_date, end_date)


def main_etl():
    # first run: mulai dari kemarin (backfill historis pakai initial_load, manual)
    run_stage("datalake_idx", ingest, default_start=now().date() - timedelta(days=1))


if __name__ == "__main__":
    setup_logging()
    main_etl()
