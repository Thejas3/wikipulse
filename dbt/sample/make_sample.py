"""Regenerate the small Parquet samples dbt builds against in CI (no S3 lake there).

Takes two consecutive hours of Silver (so the windowed vandalism signals have context) and
the matching Gold rows. Wikipedia edit data is public. Run from the repo root:

    set -a && . ./.env && set +a && quality/.venv/bin/python dbt/sample/make_sample.py
"""

import os
from pathlib import Path

import duckdb

OUT = Path(__file__).resolve().parent
BUCKET = os.getenv("LAKE_BUCKET", "lakehouse")

con = duckdb.connect()
con.execute("LOAD delta; LOAD httpfs; SET TimeZone = 'UTC';")
con.execute(f"""CREATE SECRET lake (TYPE s3, KEY_ID '{os.environ["S3_ACCESS_KEY"]}',
    SECRET '{os.environ["S3_SECRET_KEY"]}', ENDPOINT '{os.getenv("S3_ENDPOINT_HOST", "127.0.0.1:8333")}',
    URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')""")

# Two complete hours, three hours back: settled in every layer.
con.execute("create temp table win as select date_trunc('hour', now()) - interval 3 hour as lo, "
            "date_trunc('hour', now()) - interval 1 hour as hi")
queries = {
    "silver_edits": f"""select * from delta_scan('s3://{BUCKET}/silver/edits')
        where event_date >= (select cast(lo as date) from win)
          and event_time >= (select lo from win) and event_time < (select hi from win)""",
    "gold_edits_per_minute": f"""select * from delta_scan('s3://{BUCKET}/gold/edits_per_minute')
        where window_hour >= (select lo from win) and window_hour < (select hi from win)""",
    "gold_trending_pages": f"""select * from delta_scan('s3://{BUCKET}/gold/trending_pages')
        where window_hour >= (select lo from win) and window_hour < (select hi from win) limit 2000""",
    "gold_spikes": f"""select * from delta_scan('s3://{BUCKET}/gold/spikes')
        where window_start >= (select lo from win) and window_start < (select hi from win)""",
}
for name, sql in queries.items():
    # Stage locally first: aggregating straight off delta_scan can hit a DuckDB 1.2 internal error.
    con.execute(f"create temp table {name} as {sql}")
    path = OUT / f"{name}.parquet"
    con.execute(f"copy {name} to '{path}' (format parquet, compression zstd)")
    print(f"{path.name}: {con.execute(f'select count(*) from {name}').fetchone()[0]} rows, {path.stat().st_size // 1024} KB")
