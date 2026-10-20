"""Most-viewed and fastest-rising articles from the Wikimedia Pageviews API."""

import time
from datetime import date, datetime, timedelta, timezone

import httpx

# Pages that are always near the top and say nothing about what is trending.
EXCLUDED_TITLES = {"Main_Page", "-", "Wikipedia", "Undefined"}
EXCLUDED_PREFIXES = (
    "Special:", "Wikipedia:", "File:", "Portal:", "Help:", "Template:",
    "Category:", "User:", "Talk:", "Draft:", "Module:",
)


def is_article(title: str) -> bool:
    return title not in EXCLUDED_TITLES and not title.startswith(EXCLUDED_PREFIXES)


def compute_trending(current: dict[str, int], previous: dict[str, int], limit: int = 25,
                     min_views: int = 20000) -> dict:
    """Rank by views, and separately by growth versus the previous day.

    The API only returns each day's top 1000, so an article missing from `previous`
    had fewer views than that day's 1000th entry; growth against that floor is a lower bound.
    """
    floor = min(previous.values()) if previous else 0

    def row(title: str, views: int) -> dict:
        prev = previous.get(title)
        base = prev if prev is not None else floor
        return {
            "title": title,
            "views": views,
            "previous": prev,
            "new": prev is None,
            "change": round(views / base, 2) if base else None,
        }

    rows = [row(t, v) for t, v in current.items() if is_article(t)]
    top = sorted(rows, key=lambda r: r["views"], reverse=True)[:limit]
    rising = sorted(
        (r for r in rows if r["views"] >= min_views and r["change"]),
        key=lambda r: r["change"],
        reverse=True,
    )[:limit]
    return {"top": top, "rising": rising}


class TrendingService:
    def __init__(self, client: httpx.AsyncClient, base_url: str, ttl: int = 1800):
        self.client = client
        self.base_url = base_url
        self.ttl = ttl
        self._cache: dict[str, tuple[float, dict]] = {}

    async def _top(self, project: str, day: date) -> dict[str, int] | None:
        url = f"{self.base_url}/{project}/all-access/{day:%Y/%m/%d}"
        response = await self.client.get(url)
        if response.status_code == 404:  # that day hasn't been published yet
            return None
        response.raise_for_status()
        articles = response.json()["items"][0]["articles"]
        return {a["article"]: a["views"] for a in articles}

    async def get(self, project: str, today: date | None = None) -> dict:
        cached = self._cache.get(project)
        if cached and time.monotonic() - cached[0] < self.ttl:
            return cached[1]

        today = today or datetime.now(timezone.utc).date()
        for offset in (1, 2, 3):  # daily data usually lands a few hours after midnight UTC
            day = today - timedelta(days=offset)
            current = await self._top(project, day)
            if current:
                break
        else:
            raise LookupError(f"no pageview data for {project} in the last 3 days")

        previous = await self._top(project, day - timedelta(days=1)) or {}
        result = {"project": project, "date": day.isoformat(), **compute_trending(current, previous)}
        self._cache[project] = (time.monotonic(), result)
        return result
