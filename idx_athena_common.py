"""Shared Athena config & helpers untuk idx_athena_ddl.py dan idx_athena_dq.py."""
import time
from functools import lru_cache

import boto3

from idx_common import BUCKET_NAME

BUCKET = BUCKET_NAME
DATABASE = "idx_dw"
# WORKGROUP = "primary"
OUTPUT_LOCATION = f"s3://{BUCKET}/athena-results/"
MODELED_PREFIX = f"s3://{BUCKET}/modeled"


@lru_cache(maxsize=1)
def athena_client():
    # lazy: nggak bikin client (dan nggak butuh region/credential) saat modul cuma di-import
    return boto3.client("athena")


def run_query(sql: str, database: str = DATABASE, fetch: bool = False) -> str | dict:
    """Jalankan query & tunggu selesai. Return QueryExecutionId, atau result set kalau fetch=True."""
    athena = athena_client()
    qid = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        # WorkGroup=WORKGROUP,
        ResultConfiguration={"OutputLocation": OUTPUT_LOCATION},
    )["QueryExecutionId"]

    delay = 0.5
    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        state = status["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(delay)
        delay = min(delay * 1.5, 5)  # backoff, max 5 detik

    if state != "SUCCEEDED":
        raise RuntimeError(
            f"Athena {state}: {status.get('StateChangeReason', 'unknown')}\nSQL: {sql[:200]}"
        )

    if fetch:
        return athena.get_query_results(QueryExecutionId=qid)
    return qid


def parse_single_row(result: dict) -> dict[str, str | None]:
    """Row 0 = header, row 1 = nilai."""
    rows = result["ResultSet"]["Rows"]
    header = [c.get("VarCharValue") for c in rows[0]["Data"]]
    values = [c.get("VarCharValue") for c in rows[1]["Data"]]
    return dict(zip(header, values))
