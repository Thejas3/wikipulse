"""Consumes the Wikimedia EventStreams recentchange feed (server-sent events)."""

import asyncio
import json
import logging

import httpx

from .stats import EditStats, parse_event

log = logging.getLogger(__name__)


class SSEParser:
    """Incremental server-sent-events parser: feed lines, get complete events back."""

    def __init__(self) -> None:
        self.last_id: str | None = None
        self._data: list[str] = []

    def feed(self, line: str) -> str | None:
        if line.startswith(":"):
            return None
        if line == "":
            if not self._data:
                return None
            data, self._data = "\n".join(self._data), []
            return data
        field, _, value = line.partition(":")
        value = value.removeprefix(" ")
        if field == "data":
            self._data.append(value)
        elif field == "id":
            self.last_id = value
        return None


async def consume(stats: EditStats, url: str, user_agent: str) -> None:
    """Stream edits into `stats` forever, resuming from the last event after a drop."""
    parser = SSEParser()
    backoff = 1.0
    timeout = httpx.Timeout(10.0, read=60.0)
    while True:
        headers = {"User-Agent": user_agent, "Accept": "text/event-stream"}
        if parser.last_id:
            headers["Last-Event-ID"] = parser.last_id
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                async with client.stream("GET", url, headers=headers) as response:
                    response.raise_for_status()
                    log.info("connected to %s", url)
                    backoff = 1.0
                    async for line in response.aiter_lines():
                        data = parser.feed(line)
                        if data is None:
                            continue
                        try:
                            edit = parse_event(json.loads(data))
                        except (ValueError, TypeError, KeyError):
                            continue
                        if edit:
                            stats.add(edit)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # network errors, HTTP errors, stream resets
            log.warning("stream error (%s); reconnecting in %.0fs", exc, backoff)
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 60.0)
