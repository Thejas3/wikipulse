"""Publish the exported dashboard snapshot (PRD §9). Run after export.py.

Two targets, chosen by which settings exist (in .env / Airflow's environment):

  Cloudflare R2   R2_ENDPOINT, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET
                  Uploads the files; data files first, manifest.json last, so a reader never
                  sees a manifest pointing at files that aren't there yet.
  data branch     DATA_BRANCH_REMOTE (a git URL this machine can push to, e.g. with a deploy key)
                  Force-pushes a single commit with dashboard/data/ to the `data` branch, so the
                  repo history doesn't grow. The commit also carries .github/workflows/pages.yml:
                  GitHub only runs a push-triggered workflow if its file is on the pushed branch.

With neither set it does nothing and says so (local development).

    python publish.py --data dashboard/data
"""

import argparse
import os
import subprocess
import sys
import tempfile
from pathlib import Path

CONTENT_TYPES = {".parquet": "application/vnd.apache.parquet", ".json": "application/json"}


def publish_r2(data: Path) -> None:
    import boto3

    s3 = boto3.client("s3", endpoint_url=os.environ["R2_ENDPOINT"], region_name="auto",
                      aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
                      aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"])
    bucket = os.environ["R2_BUCKET"]
    files = sorted(data.glob("*.parquet")) + [data / "manifest.json"]  # manifest last
    for f in files:
        s3.upload_file(str(f), bucket, f.name, ExtraArgs={
            "ContentType": CONTENT_TYPES[f.suffix],
            "CacheControl": "public, max-age=60",  # snapshots change every 15 minutes
        })
        print(f"uploaded {f.name} to r2://{bucket}/")


def publish_branch(data: Path) -> None:
    remote = os.environ["DATA_BRANCH_REMOTE"]
    with tempfile.TemporaryDirectory() as tmp:
        repo = Path(tmp)
        target = repo / "dashboard" / "data"
        target.mkdir(parents=True)
        for f in sorted(data.glob("*.parquet")) + [data / "manifest.json"]:
            (target / f.name).write_bytes(f.read_bytes())
        workflow = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "pages.yml"
        (repo / ".github" / "workflows").mkdir(parents=True)
        # Drop the `paths:` filter from the data branch's copy: every snapshot is an unrelated
        # force-pushed commit, and GitHub skipped path-filtered runs for those after the first.
        text = "".join(line for line in workflow.read_text().splitlines(keepends=True)
                       if not line.lstrip().startswith("paths:"))
        (repo / ".github" / "workflows" / "pages.yml").write_text(text)

        def git(*args):
            result = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True)
            if result.returncode:  # surface git's own message, e.g. a rejected deploy key
                raise RuntimeError(f"git {args[0]} failed: {result.stderr.strip()}")

        git("init", "-q", "-b", "data")
        git("add", "dashboard/data", ".github")
        git("-c", "user.name=wikipulse-export", "-c", "user.email=export@wikipulse.invalid",
            "commit", "-q", "-m", "Dashboard snapshot")
        git("push", "-q", "--force", remote, "data:data")
    print("force-pushed the snapshot to the data branch")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default=str(Path(__file__).resolve().parent / "data"))
    data = Path(parser.parse_args().data)
    if not (data / "manifest.json").exists():
        sys.exit(f"no export found in {data}; run export.py first")
    if os.getenv("R2_BUCKET"):
        publish_r2(data)
    elif os.getenv("DATA_BRANCH_REMOTE"):
        publish_branch(data)
    else:
        print("no publish target configured (R2_* or DATA_BRANCH_REMOTE); snapshot stays local")


if __name__ == "__main__":
    main()
