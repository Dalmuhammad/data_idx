import logging

import pandas as pd

from idx_common import (
    BUCKET_NAME,
    delete_prefix,
    list_layer_keys,
    now,
    read_df_from_s3,
    run_stage,
    setup_logging,
    upload_df_to_s3,
)

logger = logging.getLogger(__name__)

# Mapping column
STANDARDIZE_COLUMN_MAP = {
    "IDStockSummary": "stock_summary_id",
    "Date": "trade_date",
    "StockCode": "stock_code",
    "StockName": "stock_name",
    "Remarks": "remarks",
    "DelistingDate": "delisting_date",

    "Previous": "previous_price",
    "OpenPrice": "open_price",
    "FirstTrade": "first_trade_price",
    "High": "high_price",
    "Low": "low_price",
    "Close": "close_price",
    "Change": "price_change",

    "Volume": "volume",
    "Value": "trading_value",
    "Frequency": "frequency",

    "IndexIndividual": "individual_index",

    "Offer": "offer_price",
    "OfferVolume": "offer_volume",
    "Bid": "bid_price",
    "BidVolume": "bid_volume",

    "ListedShares": "listed_shares",
    "TradebleShares": "tradeable_shares",
    "WeightForIndex": "index_weight",

    "ForeignSell": "foreign_sell",
    "ForeignBuy": "foreign_buy",

    "NonRegularVolume": "non_regular_volume",
    "NonRegularValue": "non_regular_value",
    "NonRegularFrequency": "non_regular_frequency",
}

EXPECTED_SOURCE_COLUMNS = set(STANDARDIZE_COLUMN_MAP.keys())
EXPECTED_CURATED_COLUMNS = list(STANDARDIZE_COLUMN_MAP.values())

# Data type
INTEGER_COLUMNS = [
    "stock_summary_id", "volume", "frequency", "offer_volume", "bid_volume",
    "listed_shares", "tradeable_shares", "foreign_sell", "foreign_buy",
    "non_regular_volume", "non_regular_frequency",
]

FLOAT_COLUMNS = [
    "previous_price", "open_price", "first_trade_price", "high_price", "low_price",
    "close_price", "price_change", "trading_value", "individual_index",
    "offer_price", "bid_price", "index_weight", "non_regular_value",
]

STRING_COLUMNS = ["stock_code", "stock_name", "remarks"]
DATE_COLUMNS = ["trade_date", "delisting_date"]

# IDX pakai sentinel negatif (mis. -9999999) untuk "tidak ada data" -> jadikan NULL
NON_NEGATIVE_COLUMNS = [
    "previous_price", "open_price", "first_trade_price", "high_price", "low_price", "close_price",
    "volume", "trading_value", "frequency",
    "offer_price", "offer_volume", "bid_price", "bid_volume",
    "listed_shares", "tradeable_shares",
    "foreign_sell", "foreign_buy",
    "non_regular_volume", "non_regular_value", "non_regular_frequency",
]


# Validation
_warned_unexpected = set()  # kolom sumber tak dikenal cukup di-warning sekali per run, bukan tiap file


def validate_source_schema(dataframe):
    actual_columns = set(dataframe.columns)
    missing_columns = EXPECTED_SOURCE_COLUMNS - actual_columns
    unexpected_columns = actual_columns - EXPECTED_SOURCE_COLUMNS

    if missing_columns:
        raise ValueError(f"Source schema validation failed. Missing columns: {sorted(missing_columns)}")

    new_unexpected = unexpected_columns - _warned_unexpected
    if new_unexpected:
        logger.warning(f"Unexpected source columns detected (di-drop): {sorted(new_unexpected)}")
        _warned_unexpected.update(new_unexpected)

    return dataframe


# Rename
def rename_columns(dataframe):
    return dataframe.rename(columns=STANDARDIZE_COLUMN_MAP)


# Data type casting
def cast_numeric_column(dataframe, column, dtype):
    original = dataframe[column]
    original_null = original.isna()
    converted = pd.to_numeric(original, errors="coerce")

    conversion_error_mask = ~original_null & converted.isna()
    conversion_error_count = conversion_error_mask.sum()

    if conversion_error_count > 0:
        bad_values = original[conversion_error_mask].unique()
        raise ValueError(
            f"Datatype conversion failed for column '{column}'. "
            f"{conversion_error_count} invalid values. Examples: {bad_values[:5]}"
        )

    dataframe[column] = converted.astype(dtype)
    return dataframe


def cast_datatypes(dataframe):
    """Apply the curated data type contract."""
    for column in INTEGER_COLUMNS:
        dataframe = cast_numeric_column(dataframe, column, "Int64")

    for column in FLOAT_COLUMNS:
        dataframe = cast_numeric_column(dataframe, column, "float64")

    for column in STRING_COLUMNS:
        dataframe[column] = dataframe[column].astype("string").str.strip()

    for column in DATE_COLUMNS:
        dataframe[column] = pd.to_datetime(dataframe[column], errors="coerce").dt.date

    return dataframe


def clean_sentinels(dataframe):
    """Nilai negatif di kolom yang seharusnya non-negatif dianggap sentinel IDX -> NULL."""
    for column in NON_NEGATIVE_COLUMNS:
        invalid_mask = (dataframe[column] < 0).fillna(False).astype(bool)
        n_invalid = int(invalid_mask.sum())
        if n_invalid:
            logger.warning(f"Negative sentinel values in '{column}': {n_invalid} rows. Converting to NULL.")
            dataframe[column] = dataframe[column].mask(invalid_mask)
    return dataframe


def quality_check(dataframe):
    """Hanya memvalidasi (nggak mengubah data). Raise kalau ada pelanggaran keras."""
    errors = []

    # 1. Required columns must not be null
    for column in ["trade_date", "stock_code"]:
        n_null = int(dataframe[column].isna().sum())
        if n_null:
            errors.append(f"NULL found in required column '{column}': {n_null} rows")

    # 2. Required string values must not be empty
    empty_stock_code = dataframe["stock_code"].fillna("").str.strip().eq("")
    if empty_stock_code.any():
        errors.append(f"Empty stock_code: {int(empty_stock_code.sum())} rows")

    # 3. Duplicate grain
    grain_columns = ["trade_date", "stock_code"]
    duplicate_mask = dataframe.duplicated(subset=grain_columns, keep=False)
    if duplicate_mask.any():
        errors.append(f"Duplicate grain detected for ({', '.join(grain_columns)}): {int(duplicate_mask.sum())} rows")

    # 4. OHLC consistency (log only, not hard failures)
    ohlc_validations = {
        "High < Low": dataframe["high_price"] < dataframe["low_price"],
        "Open < Low": dataframe["open_price"] < dataframe["low_price"],
        "Open > High": dataframe["open_price"] > dataframe["high_price"],
        "Close < Low": dataframe["close_price"] < dataframe["low_price"],
        "Close > High": dataframe["close_price"] > dataframe["high_price"],
    }
    for rule, invalid_mask in ohlc_validations.items():
        n_invalid = int(invalid_mask.sum())
        if n_invalid:
            logger.warning(f"OHLC anomaly - {rule}: {n_invalid} rows")

    # 5. Future trade date (WIB), NaT diabaikan (sudah ditangkap di cek #1)
    n_future = int((dataframe["trade_date"].dropna() > now().date()).sum())
    if n_future:
        errors.append(f"trade_date in the future: {n_future} rows")

    if errors:
        raise ValueError("Quality check failed:\n- " + "\n- ".join(errors))

    return dataframe


def transform_one(dataframe):
    """RAW -> CURATED untuk satu file."""
    dataframe = validate_source_schema(dataframe)
    dataframe = rename_columns(dataframe)
    dataframe = dataframe[EXPECTED_CURATED_COLUMNS].copy()  # drops source-only fields (No, persen, etc.)
    dataframe = cast_datatypes(dataframe)
    dataframe = clean_sentinels(dataframe)
    dataframe = quality_check(dataframe)
    return dataframe


def save_curated(raw_key, dataframe):
    curated_key = raw_key.replace("raw/", "curated/", 1)
    logger.info(f"Uploading curated: {curated_key} ({len(dataframe):,} rows)")
    upload_df_to_s3(dataframe, BUCKET_NAME, curated_key)

    # Bulan sudah jadi file bulanan (raw sudah di-compact) -> curated daily bulan itu nggak dipakai lagi
    if "/daily/" not in curated_key:
        daily_prefix = curated_key.rsplit("/", 1)[0] + "/daily/"
        n_deleted = delete_prefix(BUCKET_NAME, daily_prefix)
        if n_deleted:
            logger.info(f"Curated daily terhapus: {daily_prefix} ({n_deleted} file)")


def curate(start_date, end_date, first_run):
    """RAW -> CURATED, diproses satu file per iterasi supaya memory nggak numpuk."""
    keys = list_layer_keys("raw", start_date, end_date)
    logger.info(f"Starting RAW -> CURATED: {len(keys)} file")

    for key in keys:
        raw_df = read_df_from_s3(BUCKET_NAME, key)
        if raw_df is None:
            logger.warning(f"Raw file hilang, skip: {key}")
            continue
        curated_df = transform_one(raw_df)
        logger.info(f"Key: {key} | RAW -> CURATED completed. Rows: {len(curated_df):,}")
        save_curated(key, curated_df)


def main_curated():
    run_stage("curated_idx", curate, prev_etl_name="datalake_idx")


if __name__ == "__main__":
    setup_logging()
    main_curated()
