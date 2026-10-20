import asyncio
import logging
from contextlib import asynccontextmanager, suppress
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse

from .config import settings
from .stats import EditStats
from .stream import consume
from .trending import TrendingService

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
STATIC = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.stats = EditStats()
    app.state.http = httpx.AsyncClient(headers={"User-Agent": settings.user_agent}, timeout=15.0)
    app.state.trending = TrendingService(app.state.http, settings.pageviews_url, settings.trending_ttl)
    task = None
    if settings.stream_enabled:
        task = asyncio.create_task(consume(app.state.stats, settings.stream_url, settings.user_agent))
    yield
    if task:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
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
