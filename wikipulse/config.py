import os
from dataclasses import dataclass

from . import __version__


@dataclass(frozen=True)
class Settings:
    stream_url: str = "https://stream.wikimedia.org/v2/stream/recentchange"
    pageviews_url: str = "https://wikimedia.org/api/rest_v1/metrics/pageviews/top"
    # Wikimedia asks API clients to identify themselves with a contact URL or address.
    contact: str = os.getenv("WIKIPULSE_CONTACT", "local development")
    stream_enabled: bool = os.getenv("WIKIPULSE_STREAM", "1") != "0"
    trending_ttl: int = int(os.getenv("WIKIPULSE_TRENDING_TTL", "1800"))

    @property
    def user_agent(self) -> str:
        return f"WikiPulse/{__version__} ({self.contact})"


settings = Settings()
