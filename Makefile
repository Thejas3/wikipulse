.PHONY: up down logs test audit metrics restart-producer

up:  ## start Kafka and the producer
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
