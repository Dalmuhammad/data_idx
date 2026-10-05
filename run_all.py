"""Entrypoint ETL IDX: jalankan stage berurutan (datalake -> curated -> dwh -> dq).

Dipakai oleh Lambda (lewat lambda_handler.py), Fargate, maupun lokal:

    python run_all.py                    # semua stage
    python run_all.py curated dwh dq     # sebagian (urutan dependensi tetap dijaga)

Stage berhenti di kegagalan pertama (exception naik ke caller). Status tiap stage dicatat run_stage di etl_control.json.
"""
import logging
import sys

from curated_data import main_curated
from datalake_ingestion import main_etl
from dimentional import main_dimentional
from idx_athena_dq import main_dq
from idx_common import setup_logging

logger = logging.getLogger(__name__)

# urutan = urutan dependensi
STAGES = {
    "datalake": main_etl,
    "curated": main_curated,
    "dwh": main_dimentional,
    "dq": main_dq,
}


def main(stages=None) -> list[str]:
    requested = list(stages) if stages else list(STAGES)
    unknown = [name for name in requested if name not in STAGES]
    if unknown:
        raise ValueError(f"Stage tidak dikenal: {unknown}. Pilihan: {list(STAGES)}")

    ran = []
    for name, fn in STAGES.items():  # selalu urut sesuai dependensi, apa pun urutan input
        if name not in requested:
            continue
        logger.info("=== stage: %s ===", name)
        fn()
        ran.append(name)
    return ran


if __name__ == "__main__":
    setup_logging()
    main(sys.argv[1:])
