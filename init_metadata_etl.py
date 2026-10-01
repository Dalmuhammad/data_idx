import json
import boto3
from botocore.exceptions import ClientError

BUCKET_NAME = "afdal-idx-stock-data-s3"
METADATA_KEY = "metadata/etl_control.json"

# Definisikan dictionary daftar semua ETL
ALL_ETL_CONFIGS = {
    "datalake_idx": {"is_active": 1},
    "curated_idx": {"is_active": 1},
    "dwh_idx": {"is_active": 1},
    "dataquality_idx": {"is_active": 1},
    # Nanti kalau mau nambah ETL name, tinggal tambahkan di sini
}

def initialize_etl_control():
    s3 = boto3.client("s3")
    
    try:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=METADATA_KEY)
        metadata = json.loads(response["Body"].read().decode("utf-8"))
        print(f"File {METADATA_KEY} ditemukan di S3. Memeriksa konfigurasi ETL...")
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            print(f"File {METADATA_KEY} belum ada. Membuat file baru...")
            metadata = {}
        else:
            raise e


    updated = False
    for etl_name, config in ALL_ETL_CONFIGS.items():
        if etl_name not in metadata:
            # Hitung auto-increment etl_id berdasarkan ID maksimum yang sudah ada di S3
            existing_ids = [
                val.get("etl_id", 0) 
                for val in metadata.values() 
                if isinstance(val, dict) and "etl_id" in val
            ]
            next_id = max(existing_ids, default=0) + 1

            # Masukkan struktur default lengkap dengan auto increment id
            metadata[etl_name] = {
                "etl_id": next_id,
                "last_run": None,
                "is_active": config.get("is_active", 1),
                "start_time": None,
                "end_time": None,
                "message": None,
                "last_processed": None
            }
            print(f"-> Menambahkan ETL baru: '{etl_name}' dengan etl_id = {next_id}")
            updated = True
        else:
            print(f"-> ETL '{etl_name}' sudah terdaftar, dilewati (aman dari overwrite).")

    # Upload kembali ke S3 hanya jika ada penambahan ETL baru
    if updated:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=METADATA_KEY,
            Body=json.dumps(metadata, indent=4),
            ContentType="application/json"
        )
        print(f"Berhasil memperbarui {METADATA_KEY} di S3.")
    else:
        print("Semua ETL sudah terdaftar. Tidak ada perubahan yang disimpan.")

if __name__ == "__main__":
    initialize_etl_control()