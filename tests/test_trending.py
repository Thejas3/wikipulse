from datetime import date

import httpx

from wikipulse.trending import TrendingService, compute_trending, is_article


def test_is_article():
    assert is_article("Python_(programming_language)")
    assert not is_article("Main_Page")
    assert not is_article("Special:Search")


def test_compute_trending_ranks_growth_and_marks_new():
    current = {"Main_Page": 5_000_000, "Steady": 100_000, "Spike": 90_000, "Fresh": 60_000}
    previous = {"Steady": 100_000, "Spike": 10_000, "Floor": 20_000}
    result = compute_trending(current, previous)

    assert [r["title"] for r in result["top"]] == ["Steady", "Spike", "Fresh"]
    assert [r["title"] for r in result["rising"]] == ["Spike", "Fresh", "Steady"]
    fresh = next(r for r in result["rising"] if r["title"] == "Fresh")
    assert fresh["new"] and fresh["previous"] is None and fresh["change"] == 6.0  # vs 10k floor


async def test_service_falls_back_to_earlier_day_and_caches():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.url.path.endswith("2026/10/18"):
            return httpx.Response(404)
        views = 50_000 if "10/17" in request.url.path else 10_000
        return httpx.Response(200, json={"items": [{"articles": [{"article": "Spike", "views": views}]}]})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    service = TrendingService(client, "https://example.test/top", ttl=60)
    result = await service.get("en.wikipedia", today=date(2026, 10, 19))

    assert result["date"] == "2026-10-17"
    assert result["rising"][0]["change"] == 5.0
    await service.get("en.wikipedia", today=date(2026, 10, 19))
    assert len(calls) == 3  # 10/18 (404), 10/17, 10/16; second call served from cache
