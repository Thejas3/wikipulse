# 7. The dashboard renders with hyparquet; DuckDB-WASM loads on demand

Date: 2026-10-21 · Status: accepted

## Context

PRD §9 builds the public dashboard on DuckDB-WASM querying Parquet snapshots in the browser, and PRD §10 requires it to load in **under 3 s on broadband, cold cache**.

Measured while building it:

- DuckDB-WASM's binary is **35.7 MB** raw. jsDelivr (its default CDN) served it uncompressed in **38 s**. unpkg serves it gzip-compressed at **7.0 MB**, in about 3 s on a fast line.
- Even with the binary cached, compiling it and starting the worker took the page to **6.4 s** before the first panel rendered.
- The data itself is about **40 KB** across five files, and every panel's query is a sort, filter or small group-by.

## Decision

- **The panels read Parquet directly with [hyparquet](https://github.com/hyparam/hyparquet)**, a pure-JS reader of about 50 KB, and do their few aggregations in JavaScript. The export writes browser-friendly types (snappy compression, INTEGER/DOUBLE instead of BIGINT, plain UTC timestamps plus epoch-ms columns), so the client needs no extra codecs or type juggling.
- **DuckDB-WASM stays, loaded only when someone opens "Explore with SQL"**, from unpkg so it arrives compressed. It reuses the Parquet bytes the page already downloaded. That's where full SQL is worth the wait.

## Consequences

- Time to a fully rendered page: **463 ms** locally, warm (down from 6.4 s). The cold measurement on GitHub Pages is part of Milestone 6's exit check.
- The PRD's in-browser SQL over Parquet is still there; it's just not on the critical path. A first SQL query takes about 4.5 s, while DuckDB loads; later queries are fast.
- Panel queries now live in JavaScript instead of SQL. They are small, and the export already does the heavy lifting in DuckDB on the server.

## Alternatives considered

- **DuckDB-WASM for everything, self-hosted with brotli:** about 6 MB still has to download and compile before first paint, which doesn't reliably hit 3 s.
- **Pre-rendered JSON instead of Parquet:** loses the "query the same Parquet in the browser" story, and the files stop being reusable by other tools.
