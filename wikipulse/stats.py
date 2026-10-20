"""Rolling aggregates over the live edit stream, kept in memory (per-minute counts are persisted)."""

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

    def __init__(self, window: int = 300, history_minutes: int = 60, recent: int = 30,
                 history: dict[int, list[int]] | None = None,
                 history_complete: set[int] | None = None):
        self.window = window
        self.history_minutes = history_minutes
        self._edits: deque[Edit] = deque()
        self._minutes: dict[int, list[int]] = {m: list(c) for m, c in (history or {}).items()}
        self._dirty: set[int] = set()
        self._recent: deque[Edit] = deque(maxlen=recent)
        self.total = 0
        self.started = time.time()
        # Minutes recorded by earlier runs, and which of those a run watched from start to finish.
        self._history = set(self._minutes)
        self._history_complete = history_complete or set()

    def add(self, edit: Edit) -> None:
        self.total += 1
        self._edits.append(edit)
        self._recent.append(edit)
        minute = int(edit.ts // 60)
        if minute < self.started // 60:  # a late event from before this run; that minute is unknowable
            return
        counts = self._minutes.setdefault(minute, [0, 0])
        counts[1 if edit.bot else 0] += 1
        self._dirty.add(minute)

    def take_dirty(self) -> list[tuple[int, int, int]]:
        """Minutes changed since the last call, as (minute, human, bot) rows to persist."""
        rows = [(m, *self._minutes[m]) for m in sorted(self._dirty) if m in self._minutes]
        self._dirty.clear()
        return rows

    def _minute_points(self, current: int) -> list[dict]:
        start = int(self.started // 60)
        points = []
        for m in range(current - self.history_minutes + 1, current + 1):
            if m >= start:  # covered by this run; a missing minute really had no edits
                counts = self._minutes.get(m, [0, 0])
                partial = m == current or m * 60 < self.started
            elif m in self._history:  # covered by an earlier run
                counts = self._minutes[m]
                partial = m not in self._history_complete
            else:  # nobody was watching: unknown, not zero
                continue
            points.append({"minute": m * 60, "human": counts[0], "bot": counts[1], "partial": partial})
        return points

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
        return {
            "total": self.total,
            "uptime": round(now - self.started),
            "edits_per_minute": len(last_minute),
            "bot_share": round(sum(e.bot for e in edits) / len(edits), 3) if edits else 0.0,
            "active_wikis": len({e.wiki for e in edits}),
            "window_seconds": self.window,
            "history_minutes": self.history_minutes,
            "now": now,
            "per_minute": self._minute_points(int(now // 60)),
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
