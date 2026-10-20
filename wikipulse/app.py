import asyncio
import logging
import time
from contextlib import asynccontextmanager, suppress
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse

from .config import settings
from .stats import EditStats
from .storage import MinuteStore
from .stream import consume
from .trending import TrendingService

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)
STATIC = Path(__file__).parent / "static"


async def flush_forever(stats: EditStats, store: MinuteStore, run_id: int) -> None:
    """Persist changed minutes periodically and drop rows past the retention period."""
    while True:
        await asyncio.sleep(settings.flush_interval)
        try:
            await asyncio.to_thread(store.save, stats.take_dirty())
            await asyncio.to_thread(store.touch_run, run_id, time.time())
            cutoff = int(time.time() // 60) - settings.retention_days * 24 * 60
            await asyncio.to_thread(store.prune, cutoff)
        except Exception:
            log.exception("failed to persist minute counts")


@asynccontextmanager
async def lifespan(app: FastAPI):
    store = MinuteStore(settings.db_path)
    since = int(time.time() // 60) - 60
    history = store.load(since_minute=since)
    app.state.stats = stats = EditStats(
        history_minutes=60, history=history, history_complete=store.complete_minutes(since)
    )
    run_id = store.start_run(stats.started)
    log.info("loaded %d minutes of history from %s", len(history), settings.db_path)

    app.state.http = httpx.AsyncClient(headers={"User-Agent": settings.user_agent}, timeout=15.0)
    app.state.trending = TrendingService(app.state.http, settings.pageviews_url, settings.trending_ttl)
    tasks = [asyncio.create_task(flush_forever(stats, store, run_id))]
    if settings.stream_enabled:
        tasks.append(asyncio.create_task(consume(stats, settings.stream_url, settings.user_agent)))
    yield
    for task in tasks:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
    store.save(stats.take_dirty())
    store.touch_run(run_id, time.time())
    await app.state.http.aclose()


app = FastAPI(title="WikiPulse", lifespan=lifespan)


@app.get("/", include_in_schema=False)
async def index() -> FileResponse:
    return FileResponse(STATIC / "index.html")


@app.get("/api/health")
async def health() -> dict:
    return {"ok": True}


@app.get("/api/stats")
async def stats(request: Request) -> dict:
    return request.app.state.stats.snapshot()


@app.get("/api/trending")
async def trending(
    request: Request,
    project: str = Query("en.wikipedia", pattern=r"^[a-z0-9-]+\.(wikipedia|wiktionary|wikinews|wikivoyage)$"),
) -> dict:
    try:
        return await request.app.state.trending.get(project)
    except LookupError as exc:
        raise HTTPException(404, str(exc)) from exc
    except httpx.HTTPError as exc:
        raise HTTPException(502, f"Pageviews API error: {exc}") from exc
