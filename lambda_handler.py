"""Handler AWS Lambda (default: semua stage)."""
import os
 
import run_all
from idx_common import setup_logging
 
DEFAULT_STAGES = [
    s.strip() for s in os.getenv("IDX_LAMBDA_STAGES", "datalake,curated,dwh,dq").split(",") if s.strip()
]
 
 
def handler(event, context):
    setup_logging()
    stages = event.get("stages") if isinstance(event, dict) else None
    ran = run_all.main(stages or DEFAULT_STAGES)
    return {"status": "ok", "stages": ran}
