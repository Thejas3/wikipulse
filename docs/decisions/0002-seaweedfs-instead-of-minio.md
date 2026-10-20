# 2. Use SeaweedFS as the S3-compatible object store instead of MinIO

Date: 2026-10-20 · Status: accepted

## Context

The PRD puts the Delta lake on MinIO so the stack runs locally while talking the same S3 API as a cloud bucket. When Milestone 2 started, MinIO's community Docker images were gone: `minio/minio` returns 404 on Docker Hub (every tag, including `latest`), and the release tags are gone from Quay as well. MinIO stopped distributing community binaries and images in 2025.

What the lake needs from the store: the S3 API through Hadoop's S3A connector (path-style addressing), reliable listing and overwrite for the Delta log, a single container, and an open-source license.

## Decision

Use **SeaweedFS 4.48** (`chrislusf/seaweedfs`, Apache-2.0), run as `weed server -s3`: one container providing master, volume, filer and S3 gateway. Credentials come from `.env` and are written to an S3 identities file when the container starts. The Spark jobs create the `lakehouse` bucket on startup if it's missing.

## Consequences

- No Spark code changes: S3A only sees an endpoint, a key pair and path-style access, so swapping in R2, real S3, or MinIO built from source later is a configuration change.
- Delta's default S3 log store assumes a single writer per table. Each table here has exactly one streaming writer (Bronze or Silver), which holds.
- Pinned to a release that was 3 weeks old at adoption.

## Alternatives considered

- **Build MinIO from source:** possible (AGPL, Go), but then we maintain our own image builds, with no upstream security updates for the image.
- **Garage:** S3-compatible and lightweight, but needs a layout and bucket setup step before first use, and its S3 coverage is narrower.
- **Local filesystem paths for Delta:** simplest, but drops the object-storage behavior (eventual listing, no atomic rename) that the PRD wants the pipeline to be honest about.
