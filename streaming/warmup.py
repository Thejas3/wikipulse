"""Run at image build time: resolve the Spark Maven packages into the image's Ivy cache."""

from common import build_spark

build_spark("warmup", s3=False).stop()
