import logging

from idx_athena_common import DATABASE, MODELED_PREFIX, run_query

log = logging.getLogger(__name__)

# Partition projection: sesuaikan dengan layout path di S3
YEAR_RANGE = "2020,2040"
MONTH_DIGITS = 2  

TABLES = {
    "dim_date": {
        "columns": {
            "date_key": "BIGINT",
            "`date`": "DATE",
            "`year`": "INT",
            "`month`": "INT",
            "quarter": "INT",
            "day_name": "STRING",
        },
    },
    "dim_sector_industry": {
        "columns": {
            "code": "STRING",
            "sector": "STRING",
            "sub_sector": "STRING",
            "industry": "STRING",
            "sub_industry": "STRING",
        },
    },
    "dim_stock": {
        "columns": {
            "stock_code": "STRING",
            "stock_name": "STRING",
            "valid_from": "DATE",
            "valid_to": "DATE",
            "is_active": "INT",
            "last_update_date": "DATE",
        },
    },
    "fact_stock_daily": {
        "columns": {
            "date_key": "INT",
            "stock_code": "STRING",
            "idx_ic_code": "STRING",
            "previous_price": "DOUBLE",
            "open_price": "DOUBLE",
            "first_trade_price": "DOUBLE",
            "high_price": "DOUBLE",
            "low_price": "DOUBLE",
            "close_price": "DOUBLE",
            "price_change": "DOUBLE",
            "volume": "BIGINT",
            "trading_value": "DOUBLE",
            "frequency": "BIGINT",
            "foreign_buy": "BIGINT",
            "foreign_sell": "BIGINT",
            "non_regular_volume": "BIGINT",
            "non_regular_value": "DOUBLE",
            "non_regular_frequency": "BIGINT",
        },
        "partitioned": True,
    },
}


def build_ddl(name: str, spec: dict) -> str:
    cols = ",\n    ".join(f"{c} {t}" for c, t in spec["columns"].items())
    location = f"{MODELED_PREFIX}/{name}/"

    partition_clause = ""
    props = ["'classification'='parquet'"]

    if spec.get("partitioned"):
        partition_clause = "PARTITIONED BY (`year` INT, `month` INT)"
        props += [
            "'projection.enabled'='true'",
            "'projection.year.type'='integer'",
            f"'projection.year.range'='{YEAR_RANGE}'",
            "'projection.month.type'='integer'",
            "'projection.month.range'='1,12'",
            f"'projection.month.digits'='{MONTH_DIGITS}'",
            f"'storage.location.template'='{location}year=${{year}}/month=${{month}}/'",
        ]

    return f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.{name} (
    {cols}
)
{partition_clause}
STORED AS PARQUET
LOCATION '{location}'
TBLPROPERTIES ({", ".join(props)})
""".strip()


def ensure_schema() -> None:
    run_query(f"CREATE DATABASE IF NOT EXISTS {DATABASE}", database="default")
    for name, spec in TABLES.items():
        log.info("Ensuring %s ...", name)
        run_query(build_ddl(name, spec))
    log.info("Schema ready.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    ensure_schema()