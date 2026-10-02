"""Handler AWS Lambda. Event opsional: {"stages": ["curated", "dwh", "dq"]} (default: semua stage)."""
import run_all
from idx_common import setup_logging


def handler(event, context):
    setup_logging()
    stages = event.get("stages") if isinstance(event, dict) else None
    ran = run_all.main(stages)
    return {"status": "ok", "stages": ran}
