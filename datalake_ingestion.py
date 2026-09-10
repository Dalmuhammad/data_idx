import pandas as pd
import cloudscraper
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
import boto3
from io import BytesIO
import time
import random
import json
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

BUCKET_NAME = "afdal-idx-stock-data-s3"

def get_idx_data(start_date: date, end_date: date = None):
    if end_date is None:
        end_date = start_date

    scraper = cloudscraper.create_scraper()
    url = "https://www.idx.co.id/primary/TradingSummary/GetStockSummary"

    dates = pd.date_range(
        start=start_date,
        end=end_date
    )

    all_data = []
    for current_date in dates:
        params = {
            "length": 9999,
            "start": 0,
            "date": current_date.strftime("%Y-%m-%d")
        }
        fetched_ok = False

        # Retry maksimal 5 kali
        for attempt in range(5):
            try:
                response = scraper.get(
                    url,
                    params=params,
                    timeout=30
                )
                if response.status_code == 200:
                    data_json = response.json()
                    df_idx = pd.DataFrame(data_json["data"])
                    if not df_idx.empty:
                        all_data.append(df_idx)
                    print(
                        f"{current_date.date()} - "
                        f"OK - {len(df_idx)} rows"
                    )
                    fetched_ok = True
                    break
                elif response.status_code in [429, 503]:
                    # Exponential backoff + random jitter
                    wait = min(60, 3 * (2 ** attempt))
                    wait += random.uniform(0, 2)
                    print(
                        f"{current_date.date()} - "
                        f"HTTP {response.status_code}. "
                        f"Retry dalam {wait:.1f} detik..."
                    )
                    time.sleep(wait)
                else:
                    print(
                        f"{current_date.date()} - "
                        f"Request gagal: {response.status_code}"
                    )
                    break

            except Exception as e:
                wait = min(60, 3 * (2 ** attempt))
                wait += random.uniform(0, 2)
                print(
                    f"{current_date.date()} - "
                    f"Error: {e}. "
                    f"Retry dalam {wait:.1f} detik..."
                )
                time.sleep(wait)

        if not fetched_ok:
            # Semua retry gagal (bukan sekadar libur/weekend dengan data kosong) -> stop ETL
            raise RuntimeError(
                f"Gagal fetch data IDX untuk {current_date.date()} setelah 5x percobaan"
            )

        # Jeda normal antar request
        wait = random.uniform(2, 4)
        time.sleep(wait)

    if all_data:
        df = pd.concat(all_data, ignore_index=True)
    else:
        df = pd.DataFrame()

    return df


def get_month_list(start_date, end_date):
    months = set()
    current_date = start_date
    while current_date <= end_date:
        year = current_date.year
        month = current_date.month
        year_month = year * 100 + month
        months.add(year_month)
        current_date += timedelta(days=1)
    return sorted(months)


def upload_df_to_s3(df, bucket_name, key):
    s3 = boto3.client("s3")
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=buffer.getvalue()
    )


def read_df_from_s3(bucket_name, key):
    s3 = boto3.client("s3")
    response = s3.get_object(
        Bucket=bucket_name,
        Key=key
    )
    buffer = BytesIO(response["Body"].read())
    return pd.read_parquet(buffer)


def compact_daily(year, month):
    s3 = boto3.client("s3")
    prefix = f"raw/idx/year={year}/month={month}/daily"
    response = s3.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=prefix
    )

    if "Contents" not in response:
        print("data daily tidak ditemukan")
        return None

    keys = [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".parquet")]
    if not keys:
        print("data daily tidak ditemukan")
        return None

    df_compact = []
    for key in keys:
        df_daily = read_df_from_s3(BUCKET_NAME, key)
        df_compact.append(df_daily)
    print(f'Compact done for {year}-{month}')

    return pd.concat(df_compact, ignore_index=True)


def delete_daily(yyyy, mm):
    #ini digunakan untuk menghapus data daily jika auto compact berhasil ketrigger
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(BUCKET_NAME)
    bucket.objects.filter(
        Prefix=f"raw/idx/year={yyyy}/month={mm}/daily/"
    ).delete()
    print(f"Data daily untuk {yyyy}-{mm} telah terhapus")
    

def auto_compact(lastrun_prev, current):
    month_list = get_month_list(lastrun_prev, current)
    if len(month_list) > 1:
        for year_month in month_list[:-1]:
            mm = f'{year_month % 100:02d}'
            yyyy = f'{year_month // 100}'
            df = compact_daily(yyyy, mm)
            if df is not None and not df.empty:
                key = f"raw/idx/year={yyyy}/month={mm}/data.parquet"
                upload_df_to_s3(df, BUCKET_NAME, key)
                delete_daily(yyyy, mm)


def upload_json_to_s3(data, bucket_name, key):
    s3 = boto3.client("s3")

    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(data, indent=4),
        ContentType="application/json"
    )


def read_json_s3(bucket_name, key):
    s3 = boto3.client("s3")

    response = s3.get_object(
        Bucket=bucket_name,
        Key=key
    )

    metadata = json.loads(response["Body"].read().decode("utf-8"))
    return metadata


def update_metadata(bucket_name, key, etl_name, **kwargs):
    metadata = read_json_s3(bucket_name, key)

    if etl_name not in metadata:
        raise ValueError(
            f"ETL '{etl_name}' tidak ditemukan di metadata"
        )
    etl_metadata = metadata[etl_name]

    invalid_keys = set(kwargs) - set(etl_metadata)

    if invalid_keys:
        raise ValueError(
            f"Key tidak valid: {invalid_keys}"
        )

    etl_metadata.update(kwargs)

    upload_json_to_s3(metadata, bucket_name, key)


def daily_load(start_date, end_date):
    list_date = pd.date_range(start_date, end_date)
    for i in list_date:
        d = i.date()
        mm = f"{d.month:02d}"
        yyyy = f"{d.year:04d}"
        df = get_idx_data(d)
        key = f'raw/idx/year={yyyy}/month={mm}/daily/date={d}/data.parquet'
        print(f'{key} - jumlah: {len(df)}')
        if len(df) > 0:
            upload_df_to_s3(df, BUCKET_NAME, key)


def initial_load(start_date, end_date):
    # Backfill historis: bulan yang sudah lewat diambil sekaligus per bulan (efisien,
    # langsung disimpan compacted tanpa lewat tahap daily/compact/delete).
    # Bulan terakhir (bulan berjalan) diambil harian lewat daily_load, biar nanti
    # otomatis ke-compact sama auto_compact() di run main_etl berikutnya.
    months = get_month_list(start_date, end_date)

    for i, month in enumerate(months):
        mm = f'{month % 100:02d}'
        yyyy = f'{month // 100}'
        month_start = datetime.strptime(f'{yyyy}-{mm}-01', '%Y-%m-%d').date()

        if i != len(months) - 1:
            month_end = month_start + relativedelta(months=1) - relativedelta(days=1)
            df = get_idx_data(month_start, month_end)
            key = f'raw/idx/year={yyyy}/month={mm}/data.parquet'
            print(f'{key} - jumlah: {len(df)}')
            if len(df) > 0:
                upload_df_to_s3(df, BUCKET_NAME, key)
        else:
            daily_load(month_start, end_date)


def main_etl():
    METADATA_KEY = "metadata/etl_control.json"
    etl_name = "datalake_idx"

    start_time = datetime.now().isoformat()

    # get metadata ETL
    metadata_etl = read_json_s3(BUCKET_NAME, METADATA_KEY)[etl_name]

    # get last_success and today (date)
    last_success = datetime.fromisoformat(metadata_etl["last_success"]).date() \
        if metadata_etl["last_success"] is not None \
        else (datetime.today() - timedelta(days=1)).date()

    today = datetime.today().date()

    # update last_run
    update_metadata(BUCKET_NAME, METADATA_KEY, etl_name,
                     last_run=datetime.now().isoformat())

    # daily load to s3
    daily_load(last_success, today)

    # auto compact
    auto_compact(last_success, today)

    # update last_success
    update_metadata(BUCKET_NAME, METADATA_KEY, etl_name,
                     last_success=datetime.now().isoformat(),
                     start_time=start_time,
                     end_time=datetime.now().isoformat())


if __name__ == "__main__":
    main_etl()