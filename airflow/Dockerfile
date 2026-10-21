FROM apache/airflow:2.10.5-python3.11

# dbt and Great Expectations each get their own virtualenv: their dependency pins
# (protobuf, pydantic, ...) would otherwise fight with Airflow's constraints.
USER root
# git: dashboard/publish.py force-pushes snapshots to the `data` branch.
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
RUN python -m venv /opt/dbt-venv && python -m venv /opt/quality-venv \
    && chown -R airflow: /opt/dbt-venv /opt/quality-venv
USER airflow
RUN /opt/dbt-venv/bin/pip install --no-cache-dir "dbt-core==1.9.4" "dbt-duckdb==1.9.3" "duckdb==1.2.2" \
    && /opt/quality-venv/bin/pip install --no-cache-dir "great-expectations==1.23.2" "duckdb==1.2.2" \
       "confluent-kafka==2.8.0" "pyarrow==18.1.0" \
    && pip install --no-cache-dir "boto3==1.35.99"
# Bake DuckDB's extensions into the image so tasks don't download them at run time.
RUN /opt/dbt-venv/bin/python -c "import duckdb; c = duckdb.connect(); c.execute('INSTALL delta; INSTALL httpfs')" \
    && /opt/quality-venv/bin/python -c "import duckdb; c = duckdb.connect(); c.execute('INSTALL delta; INSTALL httpfs')"
