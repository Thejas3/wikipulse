.PHONY: up down logs test test-streaming audit audit-lake metrics restart-producer kill-spark

up:  ## start Kafka, the producer, the object store, and the Bronze/Silver jobs
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

kill-spark:  ## SIGKILL both Spark jobs mid-run, then restart them (exactly-once test)
	docker compose kill -s SIGKILL bronze silver && docker compose up -d bronze silver
