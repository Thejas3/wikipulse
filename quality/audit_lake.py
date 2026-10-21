"""Hourly lake audit in DuckDB: 0 lost, 0 duplicates, Gold = recount of Silver.

A lightweight version of streaming/audit_lake.py (no Spark), run by the quality_report DAG.
Exits 1 if any check fails, so the Airflow task fails and alerts (incident 2026-10-20 follow-up).

Checks:
  kafka -> bronze   every Kafka offset from the earliest retained to Bronze's newest appears once
  bronze -> silver  every settled Wikipedia edit in Bronze is in Silver exactly once
  silver -> gold    gold_edits_per_minute equals a recount of Silver for every settled wiki-minute

Read order (Silver before Bronze, Gold before Silver) means a later snapshot never lacks rows
that an earlier one has, so in-flight rows show up as "in flight", not as losses.

    python audit_lake.py [--settle-minutes 10]
"""

import argparse
import json
import os
import sys

import duckdb

BUCKET = os.getenv("LAKE_BUCKET", "lakehouse")
WIKIPEDIA = r"^[a-z0-9-]+\.wikipedia\.org$"


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta; INSTALL httpfs; LOAD httpfs; SET TimeZone = 'UTC';")
    # Bounded memory with spill-to-disk: the audit runs inside Airflow's container, where an
    # unbounded load of 2.6M Bronze rows was OOM-killed.
    con.execute(f"SET memory_limit = '{os.getenv('AUDIT_MEMORY_LIMIT', '512MB')}'; SET threads = 2;"
                " SET temp_directory = '/tmp/duckdb-audit-spill'; SET preserve_insertion_order = false;")
    con.execute(f"""CREATE SECRET lake (TYPE s3, KEY_ID '{os.environ["S3_ACCESS_KEY"]}',
        SECRET '{os.environ["S3_SECRET_KEY"]}', ENDPOINT '{os.getenv("S3_ENDPOINT_HOST", "127.0.0.1:8333")}',
        URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')""")
    return con


def kafka_watermarks(bootstrap: str, topic: str) -> dict[int, tuple[int, int]]:
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "wikipulse-audit"})
    try:
        partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
        return {p: consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10) for p in partitions}
    finally:
        consumer.close()


def table(name: str) -> str:
    return f"delta_scan('s3://{BUCKET}/{name}')"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--settle-minutes", type=int, default=10)
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "127.0.0.1:29092"))
    parser.add_argument("--topic", default=os.getenv("TOPIC_RAW", "wiki.recentchange.raw"))
    args = parser.parse_args()
    settle = f"interval {args.settle_minutes} minute"
    con = connect()

    # Gold first, then Silver, then Bronze, then Kafka: each later read is a superset.
    con.execute(f"create temp table gold as select window_start, wiki, edit_count from {table('gold/edits_per_minute')}")
    con.execute(f"create temp table silver as select edit_id, event_time, wiki from {table('silver/edits')}")
    # Keep only offsets, plus the event id for the rows Silver should contain (Wikipedia edits).
    con.execute(f"""create temp table bronze as
        select kafka_partition, kafka_offset, ingested_at,
               case when json_extract_string(raw_json, '$.type') in ('edit', 'new')
                     and regexp_matches(json_extract_string(raw_json, '$.meta.domain'), '{WIKIPEDIA}')
                     and json_extract_string(raw_json, '$.title') is not null
                    then json_extract_string(raw_json, '$.meta.id') end as id
        from {table('bronze/recentchange')}""")
    marks = kafka_watermarks(args.bootstrap, args.topic)

    partitions = []
    for p, n, distinct, lo, hi in con.execute("""
            select kafka_partition, count(*), count(distinct kafka_offset), min(kafka_offset), max(kafka_offset)
            from bronze group by 1 order by 1""").fetchall():
        low, high = marks.get(p, (lo, hi + 1))
        partitions.append({"partition": p, "rows": n, "duplicates": n - distinct,
                           "missing": (lo - low) + ((hi - lo + 1) - distinct), "not_yet_ingested": (high - 1) - hi})

    silver_rows, silver_distinct = con.execute("select count(*), count(distinct edit_id) from silver").fetchone()
    expected, missing_settled, missing_in_flight = con.execute(f"""
        with expected as (
            select id, min(ingested_at) as first_ingested from bronze
            where id is not null
            group by id
        ),
        missing as (select e.* from expected e anti join silver s on s.edit_id = e.id)
        select (select count(*) from expected),
               count(*) filter (where first_ingested < now() - {settle}),
               count(*) filter (where first_ingested >= now() - {settle})
        from missing""").fetchone()

    gold = con.execute(f"""
        with bound as (select max(window_start) - {settle} as b from gold),
        recount as (
            select date_trunc('minute', event_time) as window_start, wiki, count(*) as n
            from silver group by 1, 2
        ),
        compared as (
            select coalesce(r.n, 0) as s, coalesce(g.edit_count, 0) as g
            from (select * from recount where window_start < (select b from bound)) r
            full join (select * from gold where window_start < (select b from bound)) g
              using (window_start, wiki)
        )
        select count(*), count(*) filter (where s <> g), sum(s), sum(g) from compared""").fetchone()

    report = {
        "bronze": {"lost": sum(p["missing"] for p in partitions),
                   "duplicates": sum(p["duplicates"] for p in partitions), "partitions": partitions},
        "silver": {"rows": silver_rows, "duplicates": silver_rows - silver_distinct, "expected": expected,
                   "missing_settled": missing_settled, "missing_in_flight": missing_in_flight},
        "gold": {"wiki_minutes": gold[0], "mismatched": gold[1], "silver_edits": gold[2], "gold_edits": gold[3]},
    }
    failures = [k for k, bad in {
        "bronze lost": report["bronze"]["lost"], "bronze duplicates": report["bronze"]["duplicates"],
        "silver duplicates": report["silver"]["duplicates"], "silver missing": missing_settled,
        "gold mismatched": gold[1],
    }.items() if bad]
    report["status"] = "ok" if not failures else "FAILED: " + ", ".join(failures)
    print(json.dumps(report, indent=2, default=str))
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
