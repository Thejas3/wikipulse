"""Maven packages for every Spark session. Separate file so the Docker layer that downloads
them (warmup.py) is only rebuilt when this list changes."""

PACKAGES = ",".join([
    "io.delta:delta-spark_2.12:3.2.1",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
    "org.apache.hadoop:hadoop-aws:3.3.4",  # matches the Hadoop version bundled with Spark 3.5.3
])
