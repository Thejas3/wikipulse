FROM python:3.12-slim

WORKDIR /app
COPY producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY contracts/ /contracts/
COPY producer/*.py ./

ENV SCHEMA_PATH=/contracts/recentchange.schema.json \
    CHECKPOINT_PATH=/state/last_event_id \
    PYTHONUNBUFFERED=1
# A new named volume copies this directory's ownership, so the non-root user can write checkpoints.
RUN mkdir /state && chown nobody:nogroup /state
USER nobody
CMD ["python", "producer.py"]
