"""Dijalankan SAAT BUILD image: download extension DuckDB sekali, supaya runtime nggak perlu internet/write ke home dir."""
import os

import duckdb

ext_dir = os.environ["DUCKDB_EXTENSION_DIR"]
con = duckdb.connect()
con.execute(f"SET extension_directory='{ext_dir}'")
for ext in ("httpfs", "aws"):
    con.execute(f"INSTALL {ext}")
    con.execute(f"LOAD {ext}")  # gagal di sini = build gagal (lebih baik ketahuan saat build daripada saat run)
    print(f"baked: {ext}")
