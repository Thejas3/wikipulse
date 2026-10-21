FROM python:3.11-slim-bookworm

# Spark 3.5 runs on Java 17 (bookworm's default JRE); procps provides `ps` for Spark's scripts.
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java \
    SPARK_IVY_DIR=/opt/ivy \
    PYTHONUNBUFFERED=1

WORKDIR /app
# pyspark and the Maven jars first, so adding a Python package doesn't re-download the jars.
RUN pip install --no-cache-dir pyspark==3.5.3
COPY streaming/packages.py streaming/warmup.py ./
RUN python warmup.py  # bake Delta, Kafka and S3A jars into the image so containers start offline

COPY streaming/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY streaming/ ./
CMD ["python", "pipeline.py"]
