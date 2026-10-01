import logging
import calendar
from datetime import date

from idx_athena_common import DATABASE, parse_single_row, run_query
from idx_athena_ddl import ensure_schema
from idx_common import get_month_list, list_layer_keys, run_stage, setup_logging

log = logging.getLogger(__name__)

# nama check -> ekspresi SQL yang menghitung jumlah baris yang melanggar (harus 0)
DQ_CHECKS = {
    "null_stock_code": "COUNT_IF(stock_code IS NULL)",
    "null_date_key": "COUNT_IF(date_key IS NULL)",
    "duplicate_grain": "COUNT(*) - COUNT(DISTINCT (stock_code, date_key))",
    "negative_price": (
        "COUNT_IF(previous_price < 0 OR open_price < 0 OR high_price < 0 "
        "OR low_price < 0 OR close_price < 0)"
    ),
    "negative_volume_value": (
        "COUNT_IF(volume < 0 OR trading_value < 0 OR frequency < 0)"
    ),
    "high_lt_low": "COUNT_IF(high_price < low_price)",  # sanity check tambahan, murah
}


def run_dq(year: int, month: int) -> dict[str, int]:
    """Satu query Athena untuk semua check. Scan hanya partisi year/month (pruning)."""
    year, month = int(year), int(month)  # cegah SQL injection lewat f-string
    where = f"year = {year} AND month = {month}"

    select_list = ",\n    ".join(
        ["COUNT(*) AS total_rows"] + [f"{expr} AS {name}" for name, expr in DQ_CHECKS.items()]
    )
    sql = f"SELECT\n    {select_list}\nFROM {DATABASE}.fact_stock_daily\nWHERE {where}"

    row = parse_single_row(run_query(sql, fetch=True))
    return {k: int(v or 0) for k, v in row.items()}


def validate_dq(result: dict[str, int]) -> bool:
    passed = {"row_count": result["total_rows"] > 0}
    passed.update({name: result[name] == 0 for name in DQ_CHECKS})

    for name, ok in passed.items():
        detail = "" if ok else f" (violations: {result.get(name, result['total_rows'])})"
        log.info("%-22s %s%s", name, "PASS" if ok else "FAIL", detail)

    return all(passed.values())


def month_has_curated_data(yyyy: int, mm: int) -> bool:
    """Bulan 'diharapkan punya fact' hanya kalau curated-nya sudah ada datanya.
 
    Awal bulan / hari bursa belum close -> belum ada data sama sekali, itu bukan kegagalan DQ.
    Kalau curated ada tapi fact kosong, itu tetap FAIL (row_count).
    """
    last_day = calendar.monthrange(yyyy, mm)[1]
    return bool(list_layer_keys("curated", date(yyyy, mm, 1), date(yyyy, mm, last_day)))
 
 
def dq(start_date, end_date, first_run):
    # Init DDL tabel hanya untuk pertama kali
    if first_run:
        ensure_schema()
 
    for year_month in get_month_list(start_date, end_date):
        yyyy, mm = year_month // 100, year_month % 100
 
        if not month_has_curated_data(yyyy, mm):
            log.warning(f"Skip DQ {yyyy}-{mm:02d}: belum ada data curated (belum ada hari bursa yang ter-ingest)")
            continue
 
        if not validate_dq(run_dq(yyyy, mm)):
            raise RuntimeError(f"Data Quality check FAILED for {yyyy}-{mm:02d}")
        log.info(f"All Data Quality checks PASSED for {yyyy}-{mm:02d}")


def main_dq() -> None:
    run_stage("dataquality_idx", dq, prev_etl_name="dwh_idx")


if __name__ == "__main__":
    setup_logging()
    main_dq()
