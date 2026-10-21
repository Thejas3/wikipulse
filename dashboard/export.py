"""Export the dashboard's data: small Parquet snapshots + manifest.json (PRD §7, §9).

One file per dashboard metric, read in the browser by DuckDB-WASM. Timestamps are written
as plain UTC TIMESTAMPs: the browser build of DuckDB has no time-zone (ICU) extension, so
TIMESTAMPTZ arithmetic fails there.
  edits_per_minute.parquet   last 6 h, human vs bot per minute         (Gold, streaming)
  trending.parquet           top 10 articles, last 15 minutes          (Gold, streaming)
  spikes.parquet             flagged spikes, last 24 h                 (Gold, streaming)
  language_share.parquet     edits per language, today (UTC)           (dbt)
  vandalism.parquet          highest vandalism scores, last hour       (dbt)
manifest.json carries the export time, data freshness, end-to-end latency, the last dbt test
result and row counts. Files are written to a temp folder and swapped in, manifest last.

    python export.py --out ../dashboard/data
"""

import argparse
import json
import os
import shutil
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
BUCKET = os.getenv("LAKE_BUCKET", "lakehouse")


def delta(name: str) -> str:
    return f"delta_scan('s3://{BUCKET}/{name}')"


QUERIES = {
    # Browser-friendly types: INTEGER/DOUBLE rather than BIGINT (BigInt in JS), plain UTC
    # TIMESTAMPs plus epoch-millisecond columns (the browser build of DuckDB has no ICU).
    "edits_per_minute": """
        select window_start::timestamp as window_start, epoch_ms(window_start)::double as t_ms,
               sum(human_count)::integer as human, sum(bot_count)::integer as bot, sum(edit_count)::integer as total
        from epm where window_start >= now() - interval 6 hour
        group by window_start order by window_start""",
    "trending": """
        -- 5-minute buckets aligned to :00/:05/:10, so the 15-minute total doesn't double count.
        select wiki, any_value(domain) as domain, title, sum(edit_count)::integer as edits,
               count(distinct editor)::integer as editors,
               'https://' || any_value(domain) || '/wiki/' || replace(title, ' ', '_') as url
        from (select wiki, domain, title, edit_count, unnest(editors) as editor
              from trending
              where window_start >= now() - interval 15 minute and minute(window_start) % 5 = 0)
        group by wiki, title order by edits desc, editors desc limit 10""",
    "spikes": """
        select detected_at::timestamp as detected_at, window_start::timestamp as window_start,
               epoch_ms(window_start)::double as t_ms, wiki, title,
               window_count::integer as window_count, unique_editors::integer as unique_editors,
               round(baseline_mean, 3)::double as baseline_mean, round(z_score, 1)::double as z_score,
               'https://' || domain || '/wiki/' || replace(title, ' ', '_') as url
        from spikes
        order by window_start desc""",
    "language_share": """
        select coalesce(w.language, a.wiki) as language, sum(a.edits)::integer as edits,
               round(sum(a.edits) / sum(sum(a.edits)) over (), 4)::double as share
        from wh.agg_daily_wiki a left join wh.dim_wiki w using (wiki)
        where a.event_date = current_date
        group by 1 order by edits desc""",
    "vandalism": """
        select event_time::timestamp as event_time, epoch_ms(event_time)::double as t_ms, wiki, title, user_name,
               byte_delta::integer as byte_delta, vandalism_score::integer as vandalism_score,
               is_anonymous, is_blanking, page_reverts_last_hour::integer as page_reverts_last_hour,
               user_edits_last_10min::integer as user_edits_last_10min,
               'https://' || domain || '/w/index.php?diff=' || revision_new || '&oldid=' || revision_old as diff_url
        from wh.fct_edits
        where event_time >= now() - interval 1 hour and vandalism_score > 0 and revision_old is not null
        order by vandalism_score desc, event_time desc limit 25""",
}


def connect(warehouse: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("LOAD delta; LOAD httpfs; SET TimeZone = 'UTC';")
    con.execute(f"""CREATE SECRET lake (TYPE s3, KEY_ID '{os.environ["S3_ACCESS_KEY"]}',
        SECRET '{os.environ["S3_SECRET_KEY"]}', ENDPOINT '{os.getenv("S3_ENDPOINT_HOST", "127.0.0.1:8333")}',
        URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')""")
    con.execute(f"ATTACH '{warehouse}' AS wh (READ_ONLY)")
    # Load the recent Gold rows into local tables first: aggregating directly over delta_scan
    # hit a DuckDB 1.2 internal error (dictionary vectors in the hash aggregate).
    for name, sql in {
        "epm": f"select * from {delta('gold/edits_per_minute')} where window_hour >= now() - interval 7 hour",
        "trending": f"select * from {delta('gold/trending_pages')} where window_hour >= now() - interval 2 hour",
        "spikes": f"select * from {delta('gold/spikes')} where window_start >= now() - interval 24 hour",
        "latency": f"select * from {delta('gold/latency_histogram')} where measured_at >= now() - interval 1 hour",
    }.items():
        con.execute(f"create temp table {name} as {sql}")
    return con


def health(con: duckdb.DuckDBPyConnection, dbt_target: Path) -> dict:
    # Formatted in SQL: returning a TIMESTAMPTZ to Python would need pytz.
    as_of = con.execute("select strftime(max(last_event_time), '%Y-%m-%dT%H:%M:%SZ') from epm").fetchone()[0]
    hist = con.execute("select le_seconds, sum(events) from latency group by 1 order by 1").fetchall()
    total, running, pct = sum(n for _, n in hist), 0, {}
    for le, n in hist:
        running += n
        for q in (0.5, 0.95, 0.99):
            if q not in pct and total and running >= q * total:
                pct[q] = le
    dbt = {"status": "unknown"}
    results = dbt_target / "run_results.json"
    if results.exists():
        rr = json.loads(results.read_text())
        statuses = [r["status"] for r in rr["results"]]
        dbt = {"generated_at": rr["metadata"]["generated_at"], "command": rr["args"].get("which"),
               "passed": statuses.count("pass") + statuses.count("success"),
               "failed": statuses.count("fail") + statuses.count("error"), "warned": statuses.count("warn")}
    return {
        "data_as_of": as_of,
        "latency_last_hour_seconds": {"p50": pct.get(0.5), "p95": pct.get(0.95), "p99": pct.get(0.99), "events": total},
        "last_dbt_run": dbt,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default=str(ROOT / "dashboard" / "data"))
    parser.add_argument("--warehouse", default=os.getenv("DBT_DUCKDB_PATH", str(ROOT / "warehouse" / "wikipulse.duckdb")))
    parser.add_argument("--dbt-target", default=os.getenv("DBT_TARGET_PATH", str(ROOT / "dbt" / "target")))
    args = parser.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    con = connect(args.warehouse)
    staging = Path(tempfile.mkdtemp(prefix=".export-", dir=out))
    files = {}
    try:
        for name, sql in QUERIES.items():
            path = staging / f"{name}.parquet"
            # snappy: the dashboard's Parquet reader (hyparquet) decodes it without extra codecs
            con.execute(f"COPY ({sql}) TO '{path}' (FORMAT parquet, COMPRESSION snappy)")
            files[name] = {"file": path.name, "rows": con.execute(f"select count(*) from '{path}'").fetchone()[0],
                           "bytes": path.stat().st_size}
        manifest = {"exported_at": datetime.now(UTC).isoformat(timespec="seconds"),
                    **health(con, Path(args.dbt_target)), "files": files}
        for name in files:  # swap the data files in, then the manifest that points at them
            os.replace(staging / f"{name}.parquet", out / f"{name}.parquet")
        (staging / "manifest.json").write_text(json.dumps(manifest, indent=2, default=str))
        os.replace(staging / "manifest.json", out / "manifest.json")
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    print(json.dumps(manifest, indent=2, default=str))


if __name__ == "__main__":
    main()
