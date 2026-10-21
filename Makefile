.PHONY: up down logs test test-streaming audit audit-lake audit-quick latency metrics restart-producer kill-spark reset-gold reset-silver dbt quality export

up:  ## start Kafka, the producer, the object store, and the Spark pipeline
	docker compose up -d --build

down:  ## stop everything (data volumes are kept)
	docker compose down

logs:
	docker compose logs -f producer

test:
	.venv/bin/pytest -q

audit:  ## volume, duplicates and event-time gaps in the raw topic
	.venv/bin/python producer/audit.py

metrics:
	@curl -s localhost:9108/metrics | grep '^wikipulse_'

restart-producer:  ## kill and restart the producer (resume test)
	docker compose kill producer && docker compose up -d producer

test-streaming:  ## PySpark unit tests, inside the streaming image
	docker compose run --rm spark-tools

audit-lake:  ## Kafka -> Bronze -> Silver: lost and duplicate events
	docker compose run --rm spark-tools python audit_lake.py

kill-spark:  ## SIGKILL the Spark app mid-run, then restart it (exactly-once test)
	docker compose kill -s SIGKILL streaming && docker compose up -d streaming

latency:  ## end-to-end latency percentiles over the last 30 minutes
	docker compose run --rm spark-tools python latency_report.py 30

reset-gold:  ## drop Gold and rebuild it from Silver (after Gold logic changes)
	docker compose stop streaming
	docker compose run --rm spark-tools python reset_tables.py gold
	docker compose up -d --build streaming

reset-silver:  ## drop Silver and Gold and rebuild both from Bronze (after Silver logic changes)
	docker compose stop streaming
	docker compose run --rm spark-tools python reset_tables.py silver
	docker compose up -d --build streaming

audit-quick:  ## DuckDB lake audit (no Spark); what the hourly lake_audit DAG runs
	set -a && . ./.env && set +a && quality/.venv/bin/python quality/audit_lake.py

dbt:  ## dbt build (models, snapshot, tests) against the local warehouse
	cd dbt && set -a && . ../.env && set +a && ../.venv/bin/dbt build --profiles-dir .

quality:  ## Great Expectations suites
	cd quality/great_expectations && ../.venv/bin/python run_checks.py

export:  ## dashboard Parquet snapshots + manifest.json into dashboard/data
	set -a && . ./.env && set +a && quality/.venv/bin/python dashboard/export.py --out dashboard/data
