"""Run at image build time: resolve the Spark Maven packages into the image's Ivy cache."""

import os

from pyspark.sql import SparkSession

from packages import PACKAGES

(SparkSession.builder.master("local[1]")
 .config("spark.jars.packages", PACKAGES)
 .config("spark.jars.ivy", os.getenv("SPARK_IVY_DIR", "/opt/ivy"))
 .getOrCreate().stop())
