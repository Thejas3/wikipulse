"""SQLite persistence for per-minute edit counts, so history survives restarts."""

import math
import sqlite3
from pathlib import Path


class MinuteStore:
    def __init__(self, path: str | Path):
        self.path = str(path)
        with self._connect() as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS minutes ("
                " minute INTEGER PRIMARY KEY,"  # unix time // 60
                " human INTEGER NOT NULL,"
                " bot INTEGER NOT NULL)"
            )
            # When the server was listening, so a minute it only saw part of isn't shown as complete.
            db.execute(
                "CREATE TABLE IF NOT EXISTS runs ("
                " id INTEGER PRIMARY KEY,"
                " started REAL NOT NULL,"
                " last_seen REAL NOT NULL)"
            )

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path)

    def load(self, since_minute: int) -> dict[int, list[int]]:
        with self._connect() as db:
            rows = db.execute(
                "SELECT minute, human, bot FROM minutes WHERE minute >= ? ORDER BY minute",
                (since_minute,),
            ).fetchall()
        return {minute: [human, bot] for minute, human, bot in rows}

    def complete_minutes(self, since_minute: int) -> set[int]:
        """Minutes that some earlier run watched from start to finish."""
        with self._connect() as db:
            runs = db.execute(
                "SELECT started, last_seen FROM runs WHERE last_seen >= ?", (since_minute * 60,)
            ).fetchall()
        minutes: set[int] = set()
        for started, last_seen in runs:
            minutes.update(range(max(math.ceil(started / 60), since_minute), math.floor(last_seen / 60)))
        return minutes

    def save(self, rows: list[tuple[int, int, int]]) -> None:
        if not rows:
            return
        with self._connect() as db:
            db.executemany("INSERT OR REPLACE INTO minutes (minute, human, bot) VALUES (?, ?, ?)", rows)

    def start_run(self, started: float) -> int:
        with self._connect() as db:
            return db.execute(
                "INSERT INTO runs (started, last_seen) VALUES (?, ?)", (started, started)
            ).lastrowid

    def touch_run(self, run_id: int, now: float) -> None:
        with self._connect() as db:
            db.execute("UPDATE runs SET last_seen = ? WHERE id = ?", (now, run_id))

    def prune(self, before_minute: int) -> None:
        with self._connect() as db:
            db.execute("DELETE FROM minutes WHERE minute < ?", (before_minute,))
            db.execute("DELETE FROM runs WHERE last_seen < ?", (before_minute * 60,))
