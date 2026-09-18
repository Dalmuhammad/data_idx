import logging
from io import BytesIO
from datetime import datetime, timedelta, date

import boto3
import pandas as pd
from botocore.exceptions import ClientError

BUCKET_NAME = "afdal-idx-stock-data-s3"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
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


# S3
def read_df_from_s3(bucket_name, key, s3_client=None):
    s3_client = s3_client or boto3.client("s3")
    try:
        response = s3_client.get_object(
            Bucket=bucket_name,
            Key=key,
        )
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return None
        raise

    return pd.read_parquet(BytesIO(response["Body"].read()))



def upload_df_to_s3(df, bucket_name, key, s3_client=None):
    s3_client = s3_client or boto3.client("s3")
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    s3_client.put_object(Bucket=bucket_name, Key=key, Body=buffer.getvalue())


def save_curated_to_s3(dfs:dict):
    s3_client = boto3.client("s3")
    for key,value in dfs.items():
        curated_key = key.replace("raw/", "curated/", 1)
        logger.info(f"Uploading curated: {curated_key} ({len(value)} rows)")
        upload_df_to_s3(value, BUCKET_NAME, curated_key, s3_client)


def get_month_list(start_date, end_date):
    months = set()
    current_date = start_date
    while current_date <= end_date:
        months.add(current_date.year * 100 + current_date.month)
        current_date += timedelta(days=1)
    return sorted(months)


def load_raw(start_date, end_date):
    """Load raw data for a date range and return it as one concatenated DataFrame."""
    months = get_month_list(start_date, end_date)
    s3_client = boto3.client("s3")
    dfs = {}

    for i, year_month in enumerate(months):
        month, year = year_month % 100, year_month // 100
        mm, yyyy = f"{month:02d}", f"{year:04d}"

        if i != len(months) - 1:
            key = f"raw/idx/year={yyyy}/month={mm}/data.parquet"
            logger.info(f"Loading raw: {key}")
            dfs[key] = read_df_from_s3(BUCKET_NAME, key, s3_client)
        else:
            first_date = date(year, month, 1) if len(months) > 1 else start_date
            for current_date in pd.date_range(first_date, end_date):
                current_date = current_date.date()
                key = f"raw/idx/year={yyyy}/month={mm}/daily/date={current_date}/data.parquet"
                logger.info(f"Loading raw: {key}")

                df = read_df_from_s3(BUCKET_NAME, key, s3_client)
                if df is not None:
                    dfs[key] = df
                else:
                    logger.info(f"No data found: {key}")

    logger.info(f"Loaded {len(dfs)} raw files")
    return dfs


# Validation
def validate_source_schema(dataframe):
    actual_columns = set(dataframe.columns)
    missing_columns = EXPECTED_SOURCE_COLUMNS - actual_columns
    unexpected_columns = actual_columns - EXPECTED_SOURCE_COLUMNS

    if missing_columns:
        raise ValueError(f"Source schema validation failed. Missing columns: {sorted(missing_columns)}")

    if unexpected_columns:
        logger.warning(f"Unexpected source columns detected: {sorted(unexpected_columns)}")

    logger.info("Source schema validation passed")
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

    logger.info("Datatype casting passed")
    return dataframe


def quality_check(dataframe):
    errors = []

    # 1. Required columns must not be null
    for column in ["trade_date", "stock_code"]:
        if dataframe[column].isna().any():
            errors.append(f"NULL found in required column '{column}': {dataframe[column].isna().sum()} rows")

    # 2. Required string values must not be empty
    empty_stock_code = dataframe["stock_code"].fillna("").str.strip().eq("")
    if empty_stock_code.any():
        errors.append(f"Empty stock_code: {empty_stock_code.sum()} rows")

    # 3. Duplicate grain
    grain_columns = ["trade_date", "stock_code"]
    duplicate_mask = dataframe.duplicated(subset=grain_columns, keep=False)
    if duplicate_mask.any():
        errors.append(f"Duplicate grain detected for ({', '.join(grain_columns)}): {duplicate_mask.sum()} rows")

    # 4. Non-negative values - IDX uses sentinels like -9999999, treat as NULL
    non_negative_columns = [
        "previous_price", "open_price", "first_trade_price", "high_price", "low_price", "close_price",
        "volume", "trading_value", "frequency",
        "offer_price", "offer_volume", "bid_price", "bid_volume",
        "listed_shares", "tradeable_shares",
        "foreign_sell", "foreign_buy",
        "non_regular_volume", "non_regular_value", "non_regular_frequency",
    ]
    for column in non_negative_columns:
        invalid_mask = dataframe[column] < 0
        if invalid_mask.any():
            logger.warning(f"Negative sentinel values in '{column}': {invalid_mask.sum()} rows. Converting to NULL.")
            dataframe.loc[invalid_mask, column] = pd.NA

    # 5. OHLC consistency (log only, not hard failures)
    ohlc_validations = {
        "High < Low": dataframe["high_price"] < dataframe["low_price"],
        "Open < Low": dataframe["open_price"] < dataframe["low_price"],
        "Open > High": dataframe["open_price"] > dataframe["high_price"],
        "Close < Low": dataframe["close_price"] < dataframe["low_price"],
        "Close > High": dataframe["close_price"] > dataframe["high_price"],
    }
    for rule, invalid_mask in ohlc_validations.items():
        if invalid_mask.sum() > 0:
            logger.warning(f"OHLC anomaly - {rule}: {invalid_mask.sum()} rows")

    # 6. Future trade date
    today = date.today()
    future_date_mask = dataframe["trade_date"] > today
    if future_date_mask.any():
        errors.append(f"trade_date in the future: {future_date_mask.sum()} rows")

    if errors:
        raise ValueError("Quality check failed:\n- " + "\n- ".join(errors))

    logger.info("Quality check passed")
    return dataframe


def transform_to_curated(dfs:dict):
    """Complete RAW -> CURATED transformation."""
    dfs_curated = {}
    logger.info("Starting RAW -> CURATED transformation")
    for key,value in dfs.items():
        dataframe = validate_source_schema(value)
        dataframe = rename_columns(dataframe)
        dataframe = dataframe[EXPECTED_CURATED_COLUMNS].copy()  # drops source-only fields (No, persen, etc.)
        dataframe = cast_datatypes(dataframe)
        dataframe = quality_check(dataframe)

        logger.info(f"Key : {key} \n RAW -> CURATED completed. Rows: {len(dataframe):,}, Columns: {len(dataframe.columns)}")
        dfs_curated[key] = dataframe
    return dfs_curated


# if __name__ == "__main__":
#     df_raw = load_raw(
#         datetime.strptime("2026-08-30", "%Y-%m-%d").date(),
#         datetime.strptime("2026-09-01", "%Y-%m-%d").date(),
#     )
#     df_curated = transform_to_curated(df_raw)
#     save_curated_to_s3(df_curated)