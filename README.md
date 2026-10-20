# WikiPulse

A live dashboard of what's happening on Wikipedia right now:

- **Live edits** streamed from [Wikimedia EventStreams](https://stream.wikimedia.org/?doc): edits per minute (human vs. bot), most-edited articles, busiest wikis, and a live feed.
- **Trending articles** from the [Pageviews API](https://doc.wikimedia.org/generated-data-platform/aqs/analytics-api/): the most-viewed articles from the latest full day, and the ones rising fastest compared with the day before.

Built with FastAPI and plain HTML/SVG. Live stats are kept in memory; per-minute edit counts are also saved to a local SQLite file (`wikipulse.db`), so the hourly chart survives restarts. Minutes when the server wasn't running are left blank rather than shown as zero.

## Run it

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
uvicorn wikipulse.app:app --reload
```

Then open http://127.0.0.1:8000. If port 8000 is already in use (`Address already in use`), add `--port 8001` and open that port instead.

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `WIKIPULSE_CONTACT` | `local development` | Contact info sent in the User-Agent, per [Wikimedia's policy](https://foundation.wikimedia.org/wiki/Policy:Wikimedia_Foundation_User-Agent_Policy). Set it to a project URL before deploying. |
| `WIKIPULSE_STREAM` | `1` | Set to `0` to turn off the live stream (e.g. for development). |
| `WIKIPULSE_TRENDING_TTL` | `1800` | Seconds to cache trending results per wiki. |
| `WIKIPULSE_DB` | `wikipulse.db` | Path of the SQLite file for per-minute history. |
| `WIKIPULSE_RETENTION_DAYS` | `30` | Days of per-minute history to keep. |

## API

- `GET /api/stats`: live edit aggregates
- `GET /api/trending?project=en.wikipedia`: top and rising articles
- `GET /api/health`

## Tests

```bash
pytest
```
