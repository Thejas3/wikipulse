"""Rolling aggregates over the live edit stream, kept in memory."""

import time
from collections import Counter, deque
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class Edit:
    ts: float
    wiki: str
    title: str
    user: str
    bot: bool
    kind: str  # edit | new | log | categorize
    namespace: int
    delta: int
    url: str


def parse_event(data: dict) -> Edit | None:
    """Turn a recentchange event into an Edit, or None if it should be ignored."""
    meta = data.get("meta") or {}
    if meta.get("domain") == "canary" or "wiki" not in data:
        return None
    length = data.get("length") or {}
    return Edit(
        ts=float(data.get("timestamp") or time.time()),
        wiki=data["wiki"],
        title=data.get("title", ""),
        user=data.get("user", ""),
        bot=bool(data.get("bot")),
        kind=data.get("type", "edit"),
        namespace=int(data.get("namespace", -1)),
        delta=(length.get("new") or 0) - (length.get("old") or 0),
        url=meta.get("uri", ""),
    )


class EditStats:
    """Keeps a short window of raw edits plus per-minute counts for the last hour."""

    def __init__(self, window: int = 300, history_minutes: int = 60, recent: int = 30):
        self.window = window
        self.history_minutes = history_minutes
        self._edits: deque[Edit] = deque()
        self._minutes: dict[int, list[int]] = {}  # minute -> [human, bot]
        self._recent: deque[Edit] = deque(maxlen=recent)
        self.total = 0
        self.started = time.time()

    def add(self, edit: Edit) -> None:
        self.total += 1
        self._edits.append(edit)
        self._recent.append(edit)
        counts = self._minutes.setdefault(int(edit.ts // 60), [0, 0])
        counts[1 if edit.bot else 0] += 1

    def _prune(self, now: float) -> None:
        while self._edits and self._edits[0].ts < now - self.window:
            self._edits.popleft()
        oldest = int(now // 60) - self.history_minutes
        for minute in [m for m in self._minutes if m <= oldest]:
            del self._minutes[minute]

    def snapshot(self, now: float | None = None) -> dict:
        now = time.time() if now is None else now
        self._prune(now)
        edits = list(self._edits)
        last_minute = [e for e in edits if e.ts >= now - 60]
        # Wikidata items (Q123...) are namespace 0 too, but only Wikipedia articles are readable titles.
        article_edits = [
            e for e in edits
            if e.namespace == 0 and e.kind in ("edit", "new") and ".wikipedia.org/" in e.url
        ]
        articles = Counter((e.wiki, e.title) for e in article_edits)
        urls = {(e.wiki, e.title): e.url for e in article_edits}
        current = int(now // 60)
        # Minutes before the server started are unknown, not zero, so leave them out.
        first = max(current - self.history_minutes + 1, int(self.started // 60))
        return {
            "total": self.total,
            "uptime": round(now - self.started),
            "edits_per_minute": len(last_minute),
            "bot_share": round(sum(e.bot for e in edits) / len(edits), 3) if edits else 0.0,
            "active_wikis": len({e.wiki for e in edits}),
            "window_seconds": self.window,
            "history_minutes": self.history_minutes,
            "now": now,
            "per_minute": [
                # The first minute (if the server started mid-minute) and the current one are partial.
                {"minute": m * 60, "human": c[0], "bot": c[1],
                 "partial": m == current or m * 60 < self.started}
                for m in range(first, current + 1)
                for c in [self._minutes.get(m, [0, 0])]
            ],
            "top_articles": [
                {"wiki": wiki, "title": title, "edits": n, "url": urls[(wiki, title)]}
                for (wiki, title), n in articles.most_common(10)
            ],
            "top_wikis": [
                {"wiki": wiki, "edits": n}
                for wiki, n in Counter(e.wiki for e in edits).most_common(8)
            ],
            "recent": [asdict(e) for e in reversed(self._recent)],
        }
