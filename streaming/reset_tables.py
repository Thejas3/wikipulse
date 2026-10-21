"""Delete downstream tables and their checkpoints so they rebuild from upstream on next start.

    python reset_tables.py gold     # Gold rebuilds from Silver
    python reset_tables.py silver   # Silver rebuilds from Bronze, then Gold from Silver

Used after a change to Silver or Gold logic or layout (PRD §9 "rebuilding Gold after logic
changes"). Bronze is never touched: it is the replay source. Stop the pipeline first;
`make reset-gold` / `make reset-silver` do this.
"""

import sys

import boto3

from common import Config

LAYERS = {
    "gold": ["gold/", "_checkpoints/gold/"],
    # Gold's checkpoint points at Silver table versions, so it must be rebuilt with Silver.
    "silver": ["silver/", "_checkpoints/silver/", "gold/", "_checkpoints/gold/"],
}


def main() -> None:
    layer = sys.argv[1] if len(sys.argv) > 1 else ""
    if layer not in LAYERS:
        sys.exit(f"usage: reset_tables.py {{{'|'.join(LAYERS)}}}")
    config = Config()
    s3 = boto3.client("s3", endpoint_url=config.s3_endpoint, aws_access_key_id=config.s3_access_key,
                      aws_secret_access_key=config.s3_secret_key, region_name="us-east-1")
    for prefix in LAYERS[layer]:
        deleted = 0
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=config.bucket, Prefix=prefix):
            keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if keys:
                s3.delete_objects(Bucket=config.bucket, Delete={"Objects": keys})
                deleted += len(keys)
        print(f"deleted {deleted} objects under s3a://{config.bucket}/{prefix}")


if __name__ == "__main__":
    main()
