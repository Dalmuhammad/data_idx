# Image yang sama bisa dipakai Lambda (default CMD) maupun Fargate (override entryPoint, lihat bawah).
FROM public.ecr.aws/lambda/python:3.12

COPY requirements.txt ${LAMBDA_TASK_ROOT}/
RUN pip install --no-cache-dir -r requirements.txt

# Extension DuckDB (httpfs, aws) di-bake saat build. Runtime Lambda: filesystem read-only kecuali /tmp.
ENV DUCKDB_EXTENSION_DIR=/opt/duckdb_extensions \
    DUCKDB_HOME_DIR=/tmp
COPY bake_duckdb_extensions.py /tmp/
RUN python /tmp/bake_duckdb_extensions.py && rm /tmp/bake_duckdb_extensions.py

COPY idx_common.py datalake_ingestion.py curated_data.py dimentional.py \
     idx_athena_common.py idx_athena_ddl.py idx_athena_dq.py \
     run_all.py lambda_handler.py ${LAMBDA_TASK_ROOT}/

# Lambda: handler dipanggil per invokasi.
CMD ["lambda_handler.handler"]

# Fargate/ECS: pakai image yang sama, di task definition override:
#   entryPoint: ["python"]   command: ["run_all.py"]        (WORKDIR sudah /var/task)
